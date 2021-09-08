(ns konserve.filestore
  (:refer-clojure :exclude [read write])
  (:require
   [clojure.java.io :as io]
   [konserve.serializers :refer [key->serializer]]
   [konserve.compressor :refer [null-compressor]]
   [konserve.encryptor :refer [null-encryptor]]
   [hasch.core :refer [uuid]]
   [clojure.string :refer [includes? ends-with?]]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore
                               PBinaryAsyncKeyValueStore
                               -serialize -deserialize
                               PKeyIterable]]
   [konserve.storage-layout :refer [PLinearLayout
                                    PLowlevelStore
                                    -atomic-move -copy -create-object -delete -exists
                                    -keys -path -sync-store
                                    PLowlevelObject -close -get-lock -size -sync
                                    PLowlevelLinearObject -read -write
                                    PLowlevelSplitObject -commit -read-meta -write-meta
                                    PLowlevelLock -release
                                    linear-layout-id split-layout-id
                                    header-size
                                    parse-header create-header]]
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
   [sun.nio.ch FileLockImpl]))

(def ^:dynamic *sync-translation*
  (merge *default-sync-translation*
         '{AsynchronousFileChannel FileChannel}))

(defn- sync-folder
  "Helper Function to synchronize the folder of the filestore"
  [folder]
  (let [p (.getPath (FileSystems/getDefault) folder (into-array String []))
        fc (FileChannel/open p (into-array OpenOption []))]
    (.force fc true)
    (.close fc)))

(defn delete-store
  "Permanently deletes the folder of the store with all files."
  [folder]
  (let [f             (io/file folder)
        parent-folder (.getParent f)]
    (doseq [c (.list (io/file folder))]
      (.delete (io/file (str folder "/" c))))
    (.delete f)
    (try
      (sync-folder parent-folder)
      (catch Exception e
        e))))

(def ^:dynamic *default-storage-layout* linear-layout-id)

(defn- write-binary
  "Write binary object. InputStreams, ByteArrays, CharArray, Reader and Strings as
  inputs are supported."
  [ac buffer-size key start input env]
  (let [{:keys [sync?]} env
        [bis read] (blob->channel input buffer-size)
        buffer     (ByteBuffer/allocate buffer-size)
        stop       (+ buffer-size start)]
    (async+sync sync? *sync-translation*
                (go-try-
                 (loop [start-byte start
                        stop-byte  stop]
                   (let [size   (read bis buffer)
                         _      (.flip buffer)]
                     (when-not (= size -1)
                       (do (<?- (-write ac buffer start-byte stop-byte
                                        {:type :write-binary-error
                                         :key  key}
                                        env))
                           (.clear buffer)
                           (recur (+ buffer-size start-byte) (+ buffer-size stop-byte))))))
                 (finally
                   (.close bis)
                   (.clear buffer))))))

(defrecord LowlevelFilestore [folder]
  PLowlevelStore
  (-create-object [this path env]
    (let [{:keys [sync?]} env
          standard-open-option (into-array StandardOpenOption
                                           [StandardOpenOption/WRITE
                                            StandardOpenOption/READ
                                            StandardOpenOption/CREATE])
          ac               (if sync?
                             (FileChannel/open path standard-open-option)
                             (AsynchronousFileChannel/open path standard-open-option))]
      ac))
  (-delete [this path env]
    (Files/delete path))
  (-exists [this path env]
    (Files/exists path (into-array LinkOption [])))
  (-keys [this path env]
    (vec (Files/newDirectoryStream path)))
  (-path [this store-key env]
    (Paths/get ^String store-key (into-array String [])))
  (-copy [this from to env]
    (Files/copy ^Path from ^Path to
                (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING])))
  (-atomic-move [this from to env]
    (Files/move from to (into-array [StandardCopyOption/ATOMIC_MOVE])))
  (-sync-store [this env]
    (sync-folder folder)))

(extend-type AsynchronousFileChannel
  PLowlevelObject
  (-size [this env] (.size this))
  (-sync [this env] (.force this true))
  (-close [this env] (.close this))
  (-get-lock [this env] (.get (.lock this)))
  PLowlevelLinearObject
  (-write [c buffer start-byte stop-byte msg env]
    (let [ch (chan)]
      (.write c buffer start-byte stop-byte
              (proxy [CompletionHandler] []
                (completed [res att]
                  (close! ch))
                (failed [t att]
                  (put! ch (ex-info "Could not write key file."
                                    (assoc msg :exception t)))
                  (close! ch))))
      ch))
  (-read [this start-byte stop-byte msg env]
    (let [buffer (ByteBuffer/allocate (- stop-byte start-byte))]
      (try
        (let [res-ch (chan)
              handler (proxy [CompletionHandler] []
                        (completed [res _]
                          (put! res-ch (.array buffer)))
                        (failed [t att]
                          (put! res-ch (ex-info "Could not read key."
                                                (assoc msg :exception t)))))]
          (.read this buffer start-byte stop-byte handler)
          res-ch)
        (finally
          (.clear buffer))))))

(extend-type FileChannel
  PLowlevelObject
  (-size [this env] (.size this))
  (-sync [this env] (.force this true))
  (-close [this env] (.close this))
  (-get-lock [this env] (.lock this))
  PLowlevelLinearObject
  (-write [this buffer start-byte stop-byte msg env]
    (.write this buffer start-byte))
  (-read [this start-byte stop-byte msg env]
    (let [buffer (ByteBuffer/allocate (- stop-byte start-byte))]
      (try
        (.read this buffer start-byte)
        (.array buffer)
        (finally
          (.clear buffer))))))

(extend-type FileLockImpl
  PLowlevelLock
  (-release [this env]
    (.release this)))

(defn- update-file
  "Write file into file system. It write first the meta-size, that is stored in (1Byte),
  the meta-data and the actual data."
  [folder path serializer write-handlers buffer-size [key & rkey]
   {:keys [compressor encryptor store-key up-fn up-fn-meta
           config operation input sync? storage-layout] :as env} [old-meta old-value]]
  (let [store (LowlevelFilestore. folder)
        to-array (fn [value]
                   (let [bos (ByteArrayOutputStream.)]
                     (try (-serialize (encryptor (compressor serializer))
                                      bos write-handlers value)
                          (.toByteArray bos)
                          (finally (.close bos)))))
        meta                 (up-fn-meta old-meta)
        value                (when (= operation :write-edn)
                               (if-not (empty? rkey)
                                 (update-in old-value rkey up-fn)
                                 (up-fn old-value)))

        path-new             (-path store (if (:in-place? config)
                                            store-key
                                            (str store-key ".new"))
                                    env)
        _ (when (:in-place? config) ;; let's back things up before writing then
            (trace "backing up to file: " (str store-key ".backup") " for key " key)
            ;; TODO make sync
            (-copy store path-new (str store-key ".backup") env))
        ac-new               (-create-object store path-new env)
        meta-arr             (to-array meta)
        meta-size            (count meta-arr)
        meta-buffer          (ByteBuffer/wrap meta-arr)
        header               (create-header storage-layout
                                            serializer compressor encryptor meta-size)
        header-buffer        (ByteBuffer/wrap header)]
    (async+sync
     sync? *sync-translation*
     (go-try-
      (cond (= storage-layout linear-layout-id)
            (do
              (<?- (-write ac-new header-buffer 0 header-size
                           {:type :write-header-error
                            :key  key
                            :header header}
                           env))
              (<?- (-write ac-new meta-buffer
                           header-size (+ header-size meta-size)
                           {:type :write-meta-error :key  key :meta meta}
                           env))
              (if (= operation :write-binary)
                (<?- (write-binary ac-new buffer-size key (+ header-size meta-size) input env))
                (let [value-arr            (to-array value)
                      value-buffer         (ByteBuffer/wrap value-arr)
                      value-size           (count value-arr)]
                  (try
                    (<?- (-write ac-new value-buffer
                                 (+ header-size meta-size)
                                 (+ header-size meta-size value-size)
                                 {:type :write-value-error :key key :value value}
                                 env))
                    (finally (.clear value-buffer))))))
            (= storage-layout split-layout-id)
            (do
              (<?- (-write-meta ac-new header-buffer 0 header-size
                                {:type :write-meta-header-error
                                 :key  key
                                 :header header}
                                env))
              (<?- (-write-meta ac-new meta-buffer
                                header-size (+ header-size meta-size)
                                {:type :write-meta-error :key  key :meta meta}
                                env))
              (<?- (-write ac-new header-buffer 0 header-size
                           {:type :write-header-error
                            :key  key
                            :header header}
                           env))
              (if (= operation :write-binary)
                (<?- (write-binary ac-new buffer-size key header-size input env))
                (let [value-arr            (to-array value)
                      value-buffer         (ByteBuffer/wrap value-arr)
                      value-size           (count value-arr)]
                  (try
                    (<?- (-write ac-new value-buffer
                                 header-size
                                 (+ header-size value-size)
                                 {:type :write-value-error :key key :value value}
                                 env))
                    (finally (.clear value-buffer)))))
              (-commit ac-new env)))

      (when (:fsync? config)
        (trace "syncing for " key)
        (-sync ac-new env)
        (-sync-store store env))
      (-close ac-new env)

      (when-not (:in-place? config)
        (trace "moving file: " key)
        (-atomic-move store path-new path env))
      (if (= operation :write-edn) [old-value value] true)
      (finally
        (.clear meta-buffer)
        (.clear header-buffer)
        (-close ac-new env))))))

(defn read-header [ac serializers env]
  (let [{:keys [sync?]} env]
    (async+sync sync? *sync-translation*
                (go-try-
                 (let [arr (<?- (-read ac 0 header-size {:type :read-meta-size-error
                                                         :key  key}
                                       env))]
                   (parse-header arr serializers))))))

(defn- read-file
  "Read meta, edn and binary."
  [ac serializer read-handlers
   {:keys [compressor encryptor sync? operation store-key locked-cb msg
           storage-layout] :as env} meta-size]
  (let [size (-size ac env)
        {:keys [start-byte stop-byte]}
        (cond
          (= operation :write-edn)
          {:start-byte header-size
           :stop-byte  size}
          (or (= operation :read-meta)  (= operation :write-binary))
          {:start-byte header-size
           :stop-byte  (+ meta-size header-size)}
          :else
          {:start-byte (+ meta-size header-size)
           :stop-byte  size})]
    (async+sync
     sync? *sync-translation*
     (go-try-
      (let [arr (<?- (-read ac start-byte stop-byte msg env))
            fn-read (partial -deserialize
                             (compressor (encryptor serializer))
                             read-handlers)]
        (case operation
          (:read-edn :read-meta) (let [bais-read (ByteArrayInputStream. arr)
                                       value     (fn-read bais-read)
                                       _         (.close bais-read)]
                                   value)
          :write-binary          (let [bais-read (ByteArrayInputStream. arr)
                                       value     (fn-read bais-read)
                                       _         (.close bais-read)]
                                   [value nil])
          :write-edn             (let [bais-meta  (ByteArrayInputStream. arr 0 meta-size)
                                       meta       (fn-read bais-meta)
                                       _          (.close bais-meta)
                                       bais-value (ByteArrayInputStream. arr meta-size size)
                                       value     (fn-read bais-value)
                                       _          (.close bais-value)]
                                   [meta value])
          :read-binary           (<?- (locked-cb {:input-stream (ByteArrayInputStream. arr)
                                                  :size         size
                                                  :store-key    store-key}))))))))

(defn- delete-file
  "Remove/Delete key-value pair of Filestore by given key. If success it will return true."
  [key folder env]
  (let [store        (LowlevelFilestore. folder)
        store-key    (str folder "/" (uuid key) ".ksv")
        path         (-path store store-key env)
        file-exists? (-exists store path env)]
    (async+sync (:sync? env) *sync-translation*
                (if file-exists?
                  (go-try-
                   (-delete store path env)
                   true
                   (catch Exception e
                     (throw (ex-info "Could not delete key."
                                     {:key key
                                      :folder folder
                                      :exeption e}))))
                  (go false)))))

(declare migrate-file-v1 migrate-file-v2)

(defn- io-operation
  "Read/Write file. For better understanding use the flow-chart of Konserve."
  [key-vec base serializers read-handlers write-handlers buffer-size
   {:keys [detect-old-files operation default-serializer sync? overwrite? config] :as env}]
  (let [store         (LowlevelFilestore. base)
        key           (first  key-vec)
        uuid-key      (uuid key)
        store-key     (str base "/" uuid-key ".ksv")
        env           (assoc env :store-key store-key)
        path          (-path store store-key env)
        store-key-exists?  (-exists store path env)
        old-store-key (when detect-old-files
                        (let [old-meta (str base "/meta/" uuid-key)
                              old (str base "/"  uuid-key)
                              old-binary (str base "/B_"  uuid-key)]
                          (or (@detect-old-files old-meta)
                              (@detect-old-files old)
                              (@detect-old-files old-binary))))
        serializer    (get serializers default-serializer)]
    (if (and old-store-key (not store-key-exists?))
      (if (clojure.string/includes? old-store-key "meta")
        (migrate-file-v2 base env buffer-size old-store-key store-key
                         serializer read-handlers write-handlers)
        (migrate-file-v1 base key env buffer-size old-store-key store-key
                         serializer read-handlers write-handlers))
      (if (or store-key-exists? (= :write-edn operation) (= :write-binary operation))
        (let [ac (-create-object store path env)
              lock   (when (:lock-file? config)
                       (trace "Acquiring file lock for: " (first key-vec) (str ac))
                       (-get-lock ac env))]
          (async+sync
           sync? *sync-translation*
           (go-try-
            (let [old (if (and store-key-exists? (not overwrite?))
                        (let [[_ serializer compressor encryptor meta-size]
                              (<?- (read-header ac serializers env))]
                          (<?- (read-file ac serializer read-handlers
                                          (assoc env
                                                 :compressor compressor
                                                 :encryptor encryptor)
                                          meta-size)))
                        [nil nil])]
              (if (or (= :write-edn operation) (= :write-binary operation))
                (<?- (update-file base path serializer write-handlers
                                  buffer-size key-vec env old))
                old))
            (finally
              (when (:lock-file? config)
                (trace "Releasing lock for " (first key-vec) (str ac))
                (-release lock env))
              (-close ac env)))))
        (if sync? nil (go nil))))))

(defn- list-keys
  "Return all keys in the store."
  [folder serializers read-handlers write-handlers buffer-size {:keys [sync?] :as env}]
  (let [store      (LowlevelFilestore. folder)
        path (-path store folder env)
        serializer (get serializers (:default-serializer env))
        file-paths (-keys store path env)]
    (async+sync sync? *sync-translation*
                (go-try-
                 (loop [list-keys  #{}
                        [path & file-paths] file-paths]
                   (if path
                     (cond
                       (ends-with? (.toString path) ".new")
                       (recur list-keys file-paths)

                       (ends-with? (.toString path) ".ksv")
                       (let [ac          (-create-object store path env)
                             path-name   (.toString path)
                             env         (update-in env [:msg :keys] (fn [_] path-name))]
                         (recur
                          (try
                            (let [[_ serializer compressor encryptor meta-size]
                                  (<?- (read-header ac serializers env))]
                              (conj list-keys
                                    (<?- (read-file ac serializer read-handlers
                                                    (assoc env
                                                           :compressor compressor
                                                           :encrypotr encryptor)
                                                    meta-size))))
                            ;; it can be that the file has been deleted, ignore reading errors
                            (catch Exception _
                              list-keys)
                            (finally
                              (-close ac env)))
                          file-paths))

                       :else ;; need migration
                       (let [store-key (-> path .toString)
                             fn-l      (str folder "/" (-> path .getFileName .toString) ".ksv")
                             env       (update-in env [:msg :keys] (fn [_] store-key))]
                         (cond
                           ;; ignore the data folder
                           (includes? store-key "data")
                           (recur list-keys file-paths)

                           (includes? store-key "meta")
                           (recur (into list-keys
                                        (loop [meta-list-keys #{}
                                               [meta-path & meta-store-keys]
                                               (-keys store path env)]
                                          (if meta-path
                                            (let [old-store-key (-> meta-path .toString)
                                                  store-key     (str folder "/" (-> meta-path .getFileName .toString) ".ksv")
                                                  env           (assoc-in env [:msg :keys] old-store-key)
                                                  env           (assoc env :operation :read-meta)]
                                              (recur
                                               (conj meta-list-keys
                                                     (<?- (migrate-file-v2 folder env buffer-size old-store-key store-key
                                                                           serializer read-handlers write-handlers)))
                                               meta-store-keys))
                                            meta-list-keys)))
                                  file-paths)

                           (includes? store-key "B_")
                           (recur
                            (conj list-keys
                                  {:store-key store-key
                                   :type      :stale-binary
                                   :msg       "Old binary file detected. Use bget insteat of keys for migration."})
                            file-paths)

                           :else
                           (recur
                            (conj list-keys (<?- (migrate-file-v1 folder fn-l env buffer-size store-key
                                                                  store-key serializer read-handlers write-handlers)))
                            file-paths))))
                     list-keys))))))

(defrecord FileSystemStore [folder serializers default-serializer compressor encryptor
                            read-handlers write-handlers buffer-size detect-old-storage-layout locks config]

  PEDNAsyncKeyValueStore
  (-exists? [_ key opts]
    (let [{:keys [sync?]} opts
          path (str folder "/" (uuid key) ".ksv")
          res (.exists (io/file path))]
      (if sync? res (go res))))
  (-get [_ key opts]
    (let [{:keys [sync?]} opts]
      (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                    {:operation :read-edn
                     :compressor compressor
                     :encryptor encryptor
                     :format    :data
                     :storage-layout *default-storage-layout*
                     :sync? sync?
                     :config config
                     :default-serializer default-serializer
                     :detect-old-files detect-old-storage-layout
                     :msg       {:type :read-edn-error
                                 :key  key}})))
  (-get-meta [_ key opts]
    (let [{:keys [sync?]} opts]
      (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                    {:operation :read-meta
                     :compressor compressor
                     :encryptor encryptor
                     :detect-old-files detect-old-storage-layout
                     :default-serializer default-serializer
                     :storage-layout *default-storage-layout*
                     :sync? sync?
                     :config config
                     :msg       {:type :read-meta-error
                                 :key  key}})))

  (-assoc-in [_ key-vec meta-up val opts]
    (let [{:keys [sync?]} opts]
      (io-operation key-vec folder serializers read-handlers write-handlers buffer-size
                    {:operation  :write-edn
                     :compressor compressor
                     :encryptor encryptor
                     :detect-old-files detect-old-storage-layout
                     :storage-layout *default-storage-layout*
                     :default-serializer default-serializer
                     :up-fn      (fn [_] val)
                     :up-fn-meta meta-up
                     :config     config
                     :sync? sync?
                     :overwrite? true
                     :msg        {:type :write-edn-error
                                  :key  (first key-vec)}})))

  (-update-in [_ key-vec meta-up up-fn opts]
    (let [{:keys [sync?]} opts]
      (io-operation key-vec folder serializers read-handlers write-handlers buffer-size
                    {:operation  :write-edn
                     :compressor compressor
                     :encryptor encryptor
                     :detect-old-files detect-old-storage-layout
                     :storage-layout *default-storage-layout*
                     :default-serializer default-serializer
                     :up-fn      up-fn
                     :up-fn-meta meta-up
                     :config     config
                     :sync? sync?
                     :msg        {:type :write-edn-error
                                  :key  (first key-vec)}})))
  (-dissoc [_ key opts]
    (delete-file key folder
                 {:operation  :write-edn
                  :compressor compressor
                  :encryptor encryptor
                  :detect-old-files detect-old-storage-layout
                  :storage-layout *default-storage-layout*
                  :default-serializer default-serializer
                  :config     config
                  :sync?      (:sync? opts)
                  :msg        {:type :deletion-error
                               :key  key}}))

  PBinaryAsyncKeyValueStore
  (-bget [_ key locked-cb opts]
    (let [{:keys [sync?]} opts]
      (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                    {:operation :read-binary
                     :detect-old-files detect-old-storage-layout
                     :default-serializer default-serializer
                     :compressor compressor
                     :encryptor encryptor
                     :config    config
                     :storage-layout *default-storage-layout*
                     :sync? sync?
                     :locked-cb locked-cb
                     :msg       {:type :read-binary-error
                                 :key  key}})))
  (-bassoc [_ key meta-up input opts]
    (let [{:keys [sync?]} opts]
      (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                    {:operation  :write-binary
                     :detect-old-files detect-old-storage-layout
                     :default-serializer default-serializer
                     :compressor compressor
                     :encryptor  encryptor
                     :input      input
                     :storage-layout *default-storage-layout*
                     :up-fn-meta meta-up
                     :config     config
                     :sync?      sync?
                     :msg        {:type :write-binary-error
                                  :key  key}})))

  PKeyIterable
  (-keys [_ opts]
    (let [{:keys [sync?]} opts]
      (list-keys folder serializers read-handlers write-handlers buffer-size
                 {:operation :read-meta
                  :default-serializer default-serializer
                  :detect-old-files detect-old-storage-layout
                  :storage-layout *default-storage-layout*
                  :compressor compressor
                  :encryptor encryptor
                  :config config
                  :sync? sync?
                  :msg {:type :read-all-keys-error}})))

  PLinearLayout
  (-get-raw [_ key opts]
    (let [{:keys [sync?]} opts
          is (io/input-stream
              (io/as-file (str folder "/" (uuid key) ".ksv")))
          arr (byte-array (.available is))]
      (.read is arr)
      (if sync? arr (go arr))))
  (-put-raw [_ key blob opts]
    (let [err (ex-info "Not implemented yet." {:type :not-implemented})]
      (if (:sync? opts) err (go err)))))

(defn- check-and-create-folder
  "Helper Function to Check if Folder is not writable"
  [path]
  (let [f         (io/file path)
        test-file (io/file (str path "/" (java.util.UUID/randomUUID)))]
    (when-not (.exists f)
      (.mkdir f))
    (when-not (.createNewFile test-file)
      (throw (ex-info "Cannot write to folder." {:type   :not-writable
                                                 :folder path})))
    (.delete test-file)))

(defn- migrate-file-v1
  "Migration Function For Konserve Storage-Layout, who has old file-schema."
  [folder key {:keys [storage-layout input up-fn detect-old-files compressor encryptor operation locked-cb sync?]
               :as env}
   buffer-size old-store-key new-store-key serializer read-handlers write-handlers]
  (let [standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])
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
                                       (->> (<?- (-read ac-data-file 0 (-size ac-data-file env)
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
                                             :up-fn-meta (fn [_] meta)}
                                            old)
                       return-value  (fn [r]
                                       (Files/delete data-path)
                                       (swap! detect-old-files disj old-store-key)
                                       r)]
                   (if (contains? #{:write-binary :write-edn} operation)
                     (<?- (update-file folder new-path serializer write-handlers buffer-size [nkey] env [nil nil]))
                     (let [value (<?- (update-file folder new-path serializer write-handlers buffer-size [nkey]
                                                   env [nil nil]))]
                       (if (= operation :read-meta)
                         (return-value meta)
                         (if (= operation :read-binary)
                           (let [file-size  (.size ^AsynchronousFileChannel ac-data-file)
                                 start-byte 0
                                 stop-byte  file-size]
                             (<?- (-read ac-data-file start-byte stop-byte
                                         {:type :migration-v1-read-binary-error
                                          :key key}
                                         (assoc env
                                                :locked-cb locked-cb
                                                :operation :read-binary))))
                           (return-value (second value)))))))
                 (finally
                   (when binary?
                     (Files/delete data-path))
                   (.close ^AsynchronousFileChannel ac-data-file))))))

(defn- migrate-file-v2
  "Migration Function For Konserve Storage-Layout, who has Meta and Data Folders.
   Write old file into new Konserve directly."
  [folder {:keys [storage-layout input up-fn detect-old-files locked-cb operation compressor encryptor sync?] :as env}
   buffer-size old-store-key new-store-key serializer read-handlers write-handlers]
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
    (async+sync sync? *sync-translation*
                (go-try-
                 (let [{:keys [format key]} (->> (<?- (-read ac-meta-file 0 size-meta
                                                             {:type :read-meta-old-error
                                                              :path meta-path}
                                                             env))
                                                 (ByteArrayInputStream.)
                                                 (-deserialize serializer read-handlers))
                       data         (when (= :edn format)
                                      (->> (<?- (-read ac-data-file 0 (-size ac-data-file env)
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
                                            :up-fn-meta (fn [_] meta)}
                                           old)
                       return-value (fn [r]
                                      (Files/delete meta-path)
                                      (Files/delete data-path)
                                      (swap! detect-old-files disj old-store-key) r)]
                   (if (contains? #{:write-binary :write-edn} operation)
                     (<?- (update-file folder new-path serializer write-handlers buffer-size [key] env [nil nil]))
                     (let [value (<?- (update-file folder new-path serializer write-handlers buffer-size [key] env [nil nil]))]
                       (if (= operation :read-meta)
                         (return-value meta)
                         (if (= operation :read-binary)
                           (let [file-size  (.size ac-data-file)
                                 start-byte 0
                                 stop-byte  file-size]
                             (<?- (go-try-
                                   (<?- (-read ac-data-file start-byte stop-byte
                                               {:type :migration-v2-binary-read-error
                                                :key key}
                                               (assoc env
                                                      :locked-cb locked-cb
                                                      :operation :read-binary)))
                                   (finally
                                     (Files/delete meta-path)
                                     (Files/delete data-path)))))
                           (return-value (second value)))))))
                 (finally
                   (-close ac-data-file env)
                   (-close ac-meta-file env))))))

(defn detect-old-file-schema [folder]
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
   (Files/newDirectoryStream (Paths/get folder (into-array String [])))))

(defn new-fs-store
  "Create Filestore in given path.
  Optional serializer, read-handerls, write-handlers, buffer-size and config (for fsync) can be changed.
  Defaults are
  {:folder         path
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
                                      :lock-file? true}}}]
  (let [detect-old-storage-layout (when detect-old-file-schema?
                                    (atom (detect-old-file-schema path)))
        _                  (when detect-old-file-schema?
                             (when-not (empty? @detect-old-storage-layout)
                               (info (count @detect-old-storage-layout) "files in old storage schema detected. Migration for each key will happen transparently the first time a key is accessed. Invoke konserve.core/keys to do so at once. Once all keys are migrated you can deactivate this initial check by setting detect-old-file-schema to false.")))
        _                  (check-and-create-folder path)
        store              (map->FileSystemStore {:detect-old-storage-layout detect-old-storage-layout
                                                  :folder             path
                                                  :default-serializer default-serializer
                                                  :serializers        (merge key->serializer serializers)
                                                  :compressor         compressor
                                                  :encryptor          encryptor
                                                  :read-handlers      read-handlers
                                                  :write-handlers     write-handlers
                                                  :buffer-size        buffer-size
                                                  :locks              (atom {})
                                                  :config             (merge {:fsync? true
                                                                              :in-place? false
                                                                              :lock-file? true}
                                                                             config)})]
    (if (:sync? opts)
      store
      (go store))))

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
