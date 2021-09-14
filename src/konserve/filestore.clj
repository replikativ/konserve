(ns konserve.filestore
  (:require
   [clojure.java.io :as io]
   [konserve.serializers :refer [key->serializer]]
   [konserve.compressor :refer [null-compressor]]
   [konserve.encryptor :refer [null-encryptor]]
   [konserve.impl.default :refer [update-blob new-default-store]]
   [konserve.protocols :refer [-deserialize]]
   [clojure.string :refer [includes? ends-with?]]
   [konserve.impl.storage-layout :refer [PBackingStore
                                         -atomic-move
                                         -create-store -delete-store
                                         -copy -create-blob -delete -exists
                                         -keys -path -sync-store
                                         PBackingBlob -close -get-lock -sync
                                         -read-header -read-meta -read-value -read-binary
                                         -write-header -write-meta -write-value -write-binary
                                         PBackingLock -release header-size]]
   [konserve.nio-helpers :refer [blob->channel]]
   [konserve.utils :refer [async+sync *default-sync-translation*]]
   [superv.async :refer [go-try- <?-]]
   [clojure.core.async :as async
    :refer [go <!! chan close! put!]]
   [taoensso.timbre :as timbre :refer [info trace]])
  (:import
   [java.io ByteArrayOutputStream ByteArrayInputStream FileInputStream]
   [java.nio.channels FileChannel AsynchronousFileChannel CompletionHandler]
   [java.nio ByteBuffer]
   [java.nio.file Files StandardCopyOption FileSystems Path Paths OpenOption LinkOption
    StandardOpenOption CopyOption]
   [java.util Arrays]
   [sun.nio.ch FileLockImpl]))

(def ^:dynamic *sync-translation*
  (merge *default-sync-translation*
         '{AsynchronousFileChannel FileChannel}))

(defn- sync-base
  "Helper Function to synchronize the base of the filestore"
  [base]
  (let [p (.getPath (FileSystems/getDefault) base (into-array String []))
        fc (FileChannel/open p (into-array OpenOption []))]
    (.force fc true)
    (.close fc)))

(defn- check-and-create-backing-store
  "Helper Function to Check if Base is not writable"
  [base]
  (let [f         (io/file base)
        test-file (io/file (str base "/" (java.util.UUID/randomUUID)))]
    (when-not (.exists f)
      (.mkdir f))
    (when-not (.createNewFile test-file)
      (throw (ex-info "Cannot write to base." {:type   :not-writable
                                               :base base})))
    (.delete test-file)))

(defn delete-store
  "Permanently deletes the base of the store with all files."
  [base]
  (let [f             (io/file base)
        parent-base (.getParent f)]
    (doseq [c (.list (io/file base))]
      (.delete (io/file (str base "/" c))))
    (.delete f)
    (try
      (sync-base parent-base)
      (catch Exception e
        e))))

(defrecord BackingFilestore [base]
  PBackingStore
  (-create-blob [this path env]
    (let [{:keys [sync?]} env
          standard-open-option (into-array StandardOpenOption
                                           [StandardOpenOption/WRITE
                                            StandardOpenOption/READ
                                            StandardOpenOption/CREATE])
          ac               (if sync?
                             (FileChannel/open path standard-open-option)
                             (AsynchronousFileChannel/open path standard-open-option))]
      (if sync? ac (go ac))))
  (-delete [this path env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (Files/delete path))))
  (-exists [this path env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (Files/exists path (into-array LinkOption [])))))
  (-keys [this path env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (vec (Files/newDirectoryStream path)))))
  (-path [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (Paths/get ^String store-key (into-array String [])))))
  (-copy [this from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (Files/copy ^Path from ^Path to
                             (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING])))))
  (-atomic-move [this from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (Files/move from to (into-array [StandardCopyOption/ATOMIC_MOVE])))))
  (-create-store [this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (check-and-create-backing-store base))))
  (-sync-store [this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (sync-base base))))
  (-delete-store [this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (delete-store (:base env))))))

(extend-type AsynchronousFileChannel
  PBackingBlob
  (-sync [this env] (go-try- (.force this true)))
  (-close [this env] (go-try- (.close this)))
  (-get-lock [this env]
    (go-try-
     (loop []
       (if-let [l (try
                    (.get (.lock this))
                    (catch Exception e
                      (trace "Failed to acquire lock: " e)
                      nil))]
         l
         (do
           (Thread/sleep (rand-int 20))
           (recur))))))
  (-write-header [this header env]
    (let [{:keys [msg]} env
          ch (chan)
          buffer (ByteBuffer/wrap header)]
      (try
        (.write this buffer 0 header-size
                (proxy [CompletionHandler] []
                  (completed [res att]
                    (close! ch))
                  (failed [t att]
                    (put! ch (ex-info "Could not write key file."
                                      (assoc msg :exception t)))
                    (close! ch))))
        (finally
          (.clear buffer)))
      ch))
  (-write-meta [this meta-arr env]
    (let [{:keys [msg]} env
          ch (chan)
          meta-size (alength meta-arr)
          buffer (ByteBuffer/wrap meta-arr)]
      (try
        (.write this buffer header-size (+ header-size meta-size)
                (proxy [CompletionHandler] []
                  (completed [res att]
                    (close! ch))
                  (failed [t att]
                    (put! ch (ex-info "Could not write key file."
                                      (assoc msg :exception t)))
                    (close! ch))))
        (finally
          (.clear buffer)))
      ch))
  (-write-value [this value-arr meta-size env]
    (let [{:keys [msg]} env
          ch (chan)
          total-size (.size this)
          buffer (ByteBuffer/wrap value-arr)]
      (try
        (.write this buffer (+ header-size meta-size) total-size
                (proxy [CompletionHandler] []
                  (completed [res att]
                    (close! ch))
                  (failed [t att]
                    (put! ch (ex-info "Could not write key file."
                                      (assoc msg :exception t)))
                    (close! ch))))
        (finally
          (.clear buffer)))
      ch))
  (-write-binary [this meta-size blob env]
    (let [{:keys [msg buffer-size]} env
          [bis read] (blob->channel blob buffer-size)
          buffer     (ByteBuffer/allocate buffer-size)
          start      (+ header-size meta-size)
          stop       (+ buffer-size start)]
      (go-try-
       (loop [start-byte start
              stop-byte  stop]
         (let [size   (read bis buffer)
               _      (.flip buffer)
               ch (chan)]
           (when-not (= size -1)
             (.write this buffer start-byte stop-byte
                     (proxy [CompletionHandler] []
                       (completed [res att]
                         (close! ch))
                       (failed [t att]
                         (put! ch (ex-info "Could not write key file."
                                           (assoc msg :exception t)))
                         (close! ch))))
             (<?- ch)
             (.clear buffer)
             (recur (+ buffer-size start-byte) (+ buffer-size stop-byte)))))
       (catch Exception e
         (trace "write-binary error: " e)
         e)
       (finally
         (.close bis)
         (.clear buffer)))))
  (-read-header [this env]
    (let [{:keys [msg]} env
          ch (chan)
          buffer (ByteBuffer/allocate header-size)]
      (try
        (.read this buffer 0 header-size
               (proxy [CompletionHandler] []
                 (completed [res _]
                   (put! ch (.array buffer)))
                 (failed [t att]
                   (put! ch (ex-info "Could not read key."
                                     (assoc msg :exception t))))))
        (finally
          (.clear buffer)))
      ch))
  (-read-meta [this meta-size env]
    (let [{:keys [msg]} env
          ch (chan)
          buffer (ByteBuffer/allocate meta-size)]
      (try
        (.read this buffer header-size (+ header-size meta-size)
               (proxy [CompletionHandler] []
                 (completed [res _]
                   (put! ch (.array buffer)))
                 (failed [t att]
                   (put! ch (ex-info "Could not read key."
                                     (assoc msg :exception t))))))
        (finally
          (.clear buffer)))
      ch))
  (-read-value [this meta-size env]
    (let [{:keys [msg]} env
          total-size (.size this)
          ch (chan)
          buffer (ByteBuffer/allocate (- total-size
                                         meta-size
                                         header-size))]
      (try
        (.read this buffer (+ header-size meta-size)
               (- total-size
                  meta-size
                  header-size)
               (proxy [CompletionHandler] []
                 (completed [res _]
                   (put! ch (.array buffer)))
                 (failed [t att]
                   (put! ch (ex-info "Could not read key."
                                     (assoc msg :exception t))))))
        (finally
          (.clear buffer)))
      ch))
  (-read-binary [this meta-size locked-cb env]
    (let [{:keys [msg]} env
          total-size (.size this)]
      ;; TODO use FileInputStream to not load the file in memory
      (go-try-
       (<?-
        (locked-cb {:input-stream (ByteArrayInputStream.
                                   (<?-
                                    (let [ch (chan)
                                          buffer (ByteBuffer/allocate (- total-size
                                                                         meta-size
                                                                         header-size))]
                                      (try
                                        (.read this buffer (+ header-size meta-size)
                                               total-size
                                               (proxy [CompletionHandler] []
                                                 (completed [res _]
                                                   (put! ch (.array buffer)))
                                                 (failed [t att]
                                                   (put! ch (ex-info "Could not read key."
                                                                     (assoc msg :exception t))))))
                                        (finally
                                          (.clear buffer)))
                                      ch)))
                    :size         total-size}))
       (catch Exception e
         (trace "read-binary error: " e)
         e)))))

(extend-type FileChannel
  PBackingBlob
  (-sync [this env] (.force this true))
  (-close [this env] (.close this))
  (-get-lock [this env]
    (loop []
      (if-let [l (try
                   (.lock this)
                   (catch Exception e
                     (trace "Failed to acquire lock: " e)
                     nil))]
        l
        (do
          (Thread/sleep (rand-int 20))
          (recur)))))
  (-write-header [this header env]
    (let [buffer (ByteBuffer/wrap header)]
      (try
        (.write this buffer 0)
        (finally
          (.clear buffer)))))
  (-write-meta [this meta-arr env]
    (let [buffer (ByteBuffer/wrap meta-arr)]
      (try
        (.write this buffer header-size)
        (finally
          (.clear buffer)))))
  (-write-value [this value-arr meta-size env]
    (let [buffer (ByteBuffer/wrap value-arr)]
      (try
        (.write this buffer (+ header-size meta-size))
        (finally
          (.clear buffer)))))
  (-write-binary [this meta-size blob env]
    (let [{:keys [buffer-size]} env
          [bis read] (blob->channel blob buffer-size)
          buffer     (ByteBuffer/allocate buffer-size)
          start      (+ header-size meta-size)
          stop       (+ buffer-size start)]
      (try
        (loop [start-byte start
               stop-byte  stop]
          (let [size   (read bis buffer)
                _      (.flip buffer)]
            (when-not (= size -1)
              (.write this buffer start-byte)
              (.clear buffer)
              (recur (+ buffer-size start-byte) (+ buffer-size stop-byte)))))
        (finally
          (.close bis)
          (.clear buffer)))))
  (-read-header [this env]
    (let [buffer (ByteBuffer/allocate header-size)]
      (try
        (.read this buffer 0)
        (.array buffer)
        (finally
          (.clear buffer)))))
  (-read-meta [this meta-size env]
    (let [buffer (ByteBuffer/allocate meta-size)]
      (try
        (.read this buffer header-size)
        (.array buffer)
        (finally
          (.clear buffer)))))
  (-read-value [this meta-size env]
    (let [total-size (.size this)
          buffer (ByteBuffer/allocate (- total-size
                                         meta-size
                                         header-size))]
      (try
        (.read this buffer (+ header-size meta-size))
        (.array buffer)
        (finally
          (.clear buffer)))))
  (-read-binary [this meta-size locked-cb env]
    (let [total-size (.size this)]
      ;; TODO use FileInputStream to not load the file in memory
      (locked-cb {:input-stream (ByteArrayInputStream.
                                 (let [buffer (ByteBuffer/allocate (- total-size
                                                                      meta-size
                                                                      header-size))]
                                   (try
                                     (.read this buffer (+ header-size meta-size))
                                     (.array buffer)
                                     (finally
                                       (.clear buffer)))))
                  :size         total-size}))))

(extend-type FileLockImpl
  PBackingLock
  (-release [this env]
    (go-try- (.release this))))


;; ====================== Migration code ======================


(defn- -read [this start-byte stop-byte msg env]
  (if (:sync? env)
    (let [buffer (ByteBuffer/allocate (- stop-byte start-byte))]
      (try
        (.read ^FileChannel this buffer start-byte)
        (.array buffer)
        (finally
          (.clear buffer))))
    (let [buffer (ByteBuffer/allocate (- stop-byte start-byte))]
      (try
        (let [res-ch (chan)
              handler (proxy [CompletionHandler] []
                        (completed [res _]
                          (put! res-ch (.array buffer)))
                        (failed [t att]
                          (put! res-ch (ex-info "Could not read key."
                                                (assoc msg :exception t)))))]
          (.read ^AsynchronousFileChannel this buffer start-byte stop-byte handler)
          res-ch)
        (finally
          (.clear buffer))))))

(defn- migrate-file-v1
  "Migration Function For Konserve Storage-Layout, who has old file-schema."
  [{:keys [base key-vec storage-layout input up-fn detect-old-blobs
           compressor encryptor operation locked-cb buffer-size sync?]
    :as env}
   old-store-key new-store-key serializer read-handlers write-handlers]
  (let [key (first key-vec)
        standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])
        new-path             (Paths/get new-store-key (into-array String []))
        data-path            (Paths/get old-store-key (into-array String []))
        ac-data-file         (if sync?
                               (FileChannel/open data-path standard-open-option)
                               (AsynchronousFileChannel/open data-path standard-open-option))
        binary?              (includes? old-store-key "B_")]
    (async+sync sync? *sync-translation*
                (go-try-
                 (let [[[nkey] data] (if binary?
                                       [[key] true]
                                       (->>
                                        (<?- (-read ac-data-file 0 (.size ac-data-file)
                                                    {:type :read-data-old-error
                                                     :path data-path}
                                                    env))
                                        (ByteArrayInputStream.)
                                        (-deserialize serializer read-handlers)))
                       [meta old]    (if binary?
                                       [{:key key :type :binary :last-write (java.util.Date.)}
                                        {:operation :write-binary
                                         :input     (if input input (FileInputStream. old-store-key))
                                         :msg       {:type :write-binary-error
                                                     :key  key}}]
                                       [{:key nkey :type :edn :last-write (java.util.Date.)}
                                        {:operation :write-edn
                                         :up-fn     (if up-fn (up-fn data) (fn [_] data))
                                         :msg       {:type :write-edn-error
                                                     :key  key}}])
                       env           (merge {:storage-layout    storage-layout
                                             :compressor compressor
                                             :encryptor  encryptor
                                             :store-key  new-store-key
                                             :buffer-size buffer-size
                                             :base base
                                             :key-vec [nkey]
                                             :up-fn-meta (fn [_] meta)}
                                            old)
                       return-value  (fn [r]
                                       (Files/delete data-path)
                                       (swap! detect-old-blobs disj old-store-key)
                                       r)]
                   (if (contains? #{:write-binary :write-edn} operation)
                     (<?- (update-blob (BackingFilestore. base) new-path serializer write-handlers env [nil nil]))
                     (let [value (<?- (update-blob (BackingFilestore. base) new-path serializer write-handlers env [nil nil]))]
                       (if (= operation :read-meta)
                         (return-value meta)
                         (if (= operation :read-binary)
                           (let [file-size  (.size ^AsynchronousFileChannel ac-data-file)
                                 start-byte 0
                                 stop-byte  file-size
                                 res (<?- (-read ac-data-file start-byte stop-byte
                                                 {:type :migration-v1-read-binary-error
                                                  :key key}
                                                 (assoc env
                                                        :locked-cb locked-cb
                                                        :operation :read-binary)))]
                             (<?- (locked-cb {:input-stream (ByteArrayInputStream. res)})))
                           (return-value (second value)))))))
                 (finally
                   (when binary?
                     (Files/delete data-path))
                   (.close ^AsynchronousFileChannel ac-data-file))))))

(defn- migrate-file-v2
  "Migration Function For Konserve Storage-Layout, who has Meta and Data Bases.
   Write old file into new Konserve directly."
  [{:keys [base storage-layout input up-fn detect-old-blobs locked-cb operation compressor encryptor
           sync? buffer-size] :as env}
   old-store-key new-store-key serializer read-handlers write-handlers]
  (let [standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])
        new-path             (Paths/get new-store-key (into-array String []))
        meta-path            (Paths/get old-store-key (into-array String []))
        data-store-key       (clojure.string/replace old-store-key #"meta" "data")
        data-path            (Paths/get data-store-key (into-array String []))
        ac-meta-file         (if sync?
                               (FileChannel/open meta-path standard-open-option)
                               (AsynchronousFileChannel/open meta-path standard-open-option))
        ac-data-file         (if sync?
                               (FileChannel/open data-path standard-open-option)
                               (AsynchronousFileChannel/open data-path standard-open-option))
        size-meta            (.size ac-meta-file)]
    (async+sync
     sync? *sync-translation*
     (go-try-
      (let [{:keys [format key]} (->> (<?- (-read ac-meta-file 0 size-meta
                                                  {:type :read-meta-old-error
                                                   :path meta-path}
                                                  env))
                                      (ByteArrayInputStream.)
                                      (-deserialize serializer read-handlers))
            data         (when (= :edn format)
                           (->> (<?- (-read ac-data-file 0 (.size ac-data-file)
                                            {:type :read-data-old-error
                                             :path data-path}
                                            env))
                                (ByteArrayInputStream.)
                                (-deserialize serializer read-handlers)))
            [meta old]   (if (= :binary format)
                           [{:key key :type :binary :last-write (java.util.Date.)}
                            {:operation :write-binary
                             :input     (if input input (FileInputStream. data-store-key))
                             :msg       {:type :write-binary-error
                                         :key  key}}]
                           [{:key key :type :edn :last-write (java.util.Date.)}
                            {:operation :write-edn
                             :up-fn     (if up-fn (up-fn data) (fn [_] data))
                             :msg       {:type :write-edn-error
                                         :key  key}}])
            env          (merge {:storage-layout    storage-layout
                                 :compressor compressor
                                 :encryptor  encryptor
                                 :store-key  new-store-key
                                 :buffer-size buffer-size
                                 :base base
                                 :key-vec [key]
                                 :up-fn-meta (fn [_] meta)}
                                old)
            return-value (fn [r]
                           (Files/delete meta-path)
                           (Files/delete data-path)
                           (swap! detect-old-blobs disj old-store-key) r)]
        (if (contains? #{:write-binary :write-edn} operation)
          (<?- (update-blob (BackingFilestore. base) new-path serializer write-handlers  env [nil nil]))
          (let [value (<?- (update-blob (BackingFilestore. base) new-path serializer write-handlers env [nil nil]))]
            (if (= operation :read-meta)
              (return-value meta)
              (if (= operation :read-binary)
                (let [file-size  (.size ac-data-file)
                      start-byte 0
                      stop-byte  file-size
                      res (<?- (-read ac-data-file start-byte stop-byte
                                      {:type :migration-v2-binary-read-error
                                       :key key}
                                      (assoc env
                                             :locked-cb locked-cb
                                             :operation :read-binary)))]
                  (<?- (locked-cb {:input-stream (ByteArrayInputStream. res)}))
                  (Files/delete meta-path)
                  (Files/delete data-path))
                (return-value (second value)))))))
      (finally
        (<?- (-close ac-data-file env))
        (<?- (-close ac-meta-file env)))))))

(defn migrate-in-list-keys [backing path base env serializer read-handlers write-handlers
                            list-keys file-paths]
  (async+sync (:sync? env)
              *default-sync-translation*
              (go-try-
               (let [store-key (str path)
                     fn-l      (str base "/" (-> path .getFileName .toString) ".ksv")
                     env       (update-in env [:msg :keys] (fn [_] store-key))]
                 (cond
                      ;; ignore the data base
                   (includes? store-key "data")
                   [list-keys file-paths]

                   (includes? store-key "meta")
                   [(into list-keys
                          (loop [meta-list-keys #{}
                                 [meta-path & meta-store-keys]
                                 (<?- (-keys backing path env))]
                            (if meta-path
                              (let [old-store-key (-> meta-path .toString)
                                    store-key     (str base "/" (-> meta-path .getFileName .toString) ".ksv")
                                    env           (assoc-in env [:msg :keys] old-store-key)
                                    env           (assoc env :operation :read-meta)]
                                (recur
                                 (conj meta-list-keys
                                       (<?- (migrate-file-v2 env old-store-key store-key
                                                             serializer read-handlers write-handlers)))
                                 meta-store-keys))
                              meta-list-keys)))
                    file-paths]

                   (includes? store-key "B_")
                   [(conj list-keys
                          {:store-key store-key
                           :type      :stale-binary
                           :msg       "Old binary file detected. Use bget instead of keys for migration."})
                    file-paths]

                   :else
                   [(conj list-keys
                          (<?- (migrate-file-v1 env store-key
                                                store-key serializer read-handlers write-handlers)))
                    file-paths])))))

(defn migrate-in-io-operation [old-store-key store-key env serializer read-handlers write-handlers]
  (if (clojure.string/includes? old-store-key "meta")
    (migrate-file-v2 env old-store-key store-key
                     serializer read-handlers write-handlers)
    (migrate-file-v1 env old-store-key store-key
                     serializer read-handlers write-handlers)))

(defn detect-old-file-schema [base]
  (reduce
   (fn [old-list path]
     (let [store-key (-> path .toString)]
       (cond
         (or
          (includes? store-key "data")
          (ends-with? store-key ".ksv"))    old-list
         (re-find #"meta(?!\S)" store-key) (into old-list (detect-old-file-schema store-key))
         :else                             (into old-list [store-key]))))
   #{}
   (Files/newDirectoryStream (Paths/get base (into-array String [])))))

(defn new-fs-store
  "Create Filestore in given path.
  Optional serializer, read-handerls, write-handlers, buffer-size and config (for fsync) can be changed.
  Defaults are
  {:base         path
   :serializer     fressian-serializer
   :read-handlers  empty
   :write-handlers empty
   :buffer-size    1 MB
   :config         config} "
  [path & {:keys [default-serializer serializers compressor encryptor
                  read-handlers write-handlers
                  buffer-size config detect-old-file-schema? opts]
           :or   {default-serializer :FressianSerializer
                  compressor         null-compressor
                  ;; lz4-compressor
                  encryptor          null-encryptor
                  read-handlers      (atom {})
                  write-handlers     (atom {})
                  buffer-size        (* 1024 1024)
                  opts               {:sync? false}
                  config             {:fsync? true
                                      :in-place? false
                                      :lock-blob? true}}
           :as params}]
  ;; check config
  (let [detect-old-storage-layout (when detect-old-file-schema?
                                    (atom (detect-old-file-schema path)))
        _                  (when detect-old-file-schema?
                             (when-not (empty? @detect-old-storage-layout)
                               (info (count @detect-old-storage-layout) "files in old storage schema detected. Migration for each key will happen transparently the first time a key is accessed. Invoke konserve.core/keys to do so at once. Once all keys are migrated you can deactivate this initial check by setting detect-old-file-schema to false.")))
        backing            (BackingFilestore. path)]
    (new-default-store path backing detect-old-storage-layout
                       migrate-in-io-operation
                       migrate-in-list-keys
                       params)))

(comment

  (require '[konserve.protocols :refer [-assoc-in -get -get-meta -keys -bget -bassoc]])

  (require '[konserve.core :as k])

  (do
    (delete-store "/tmp/konserve")
    (def store (<!! (new-fs-store "/tmp/konserve"))))

  (<!! (-assoc-in store ["bar"] (fn [e] {:foo "OoO"}) 1123123123123123123123123 {:sync? false}))

  (<!! (k/exists? store "bar" {:sync? false}))

  (-get store "bar" true)

  (-assoc-in store ["bar"] (fn [e] {:foo "foo"}) 42 true)

  (-keys store true)

  (<!! (-bassoc store "baz" (fn [e] {:foo "baz"}) (byte-array [1 2 3]) false))

  (-bget store "baz" (fn [input] (println input)) true)

  (defn reader-helper [start-byte stop-byte store-key]
    (let [path      (Paths/get store-key (into-array String []))
          ac        (AsynchronousFileChannel/open path (into-array StandardOpenOption [StandardOpenOption/READ]))
          file-size (.size ac)
          bb        (ByteBuffer/allocate (- stop-byte start-byte))]
      (.read
       ac
       bb
       start-byte
       stop-byte
       (proxy [CompletionHandler] []
         (completed [res att]
           (let [arr-bb    (.array bb)
                 buff-meta (ByteBuffer/wrap arr-bb)
                 meta-size (.getInt buff-meta)
                 _         (.clear buff-meta)]
             (prn (map #(get arr-bb %) (range 0 4)))))
         (failed [t att]
           (prn "fail"))))))

  (reader-helper 0 4 "/tmp/konserve/2c8e57a6-ed4e-5746-9f7e-af7ff2ac25c5.ksv"))
