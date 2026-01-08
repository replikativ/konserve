(ns konserve.filestore
  (:require
   [clojure.core.async :refer [go <!! chan close! put!]]
   [clojure.java.io :as io]
   [clojure.string :refer [includes? ends-with?]]
   [konserve.compressor :refer [null-compressor]]
   [konserve.encryptor :refer [null-encryptor]]
   [konserve.impl.defaults :refer [update-blob connect-default-store key->store-key store-key->uuid-key]]
   [konserve.impl.storage-layout :refer [PBackingStore
                                         PBackingBlob -close
                                         PBackingLock header-size]]
   [konserve.nio-helpers :refer [blob->channel]]
   [konserve.protocols :as kp]
   [konserve.utils :refer [async+sync *default-sync-translation*]]
   [superv.async :refer [go-try- <?-]]
   [taoensso.timbre :refer [info trace]])
  (:import
   [java.io ByteArrayInputStream FileInputStream Closeable]
   [java.nio.channels FileChannel AsynchronousFileChannel CompletionHandler FileLock]
   [java.nio ByteBuffer]
   [java.nio.file Files StandardCopyOption FileSystem FileSystems Path Paths OpenOption LinkOption StandardOpenOption]
   [java.util Date UUID]))

;; =============================================================================
;; Path Helpers - Support for custom FileSystems (e.g., Jimfs for testing)
;; =============================================================================

(defn- get-path
  "Get a Path from the given filesystem. If filesystem is nil, uses default."
  ^Path [^FileSystem filesystem ^String base & parts]
  (if filesystem
    (.getPath filesystem base (into-array String (or parts [])))
    (Paths/get base (into-array String (or parts [])))))

(def ^:dynamic *sync-translation*
  (merge *default-sync-translation*
         '{AsynchronousFileChannel FileChannel}))

(defn- sync-base
  "Helper Function to synchronize the base of the filestore.
   Note: Jimfs doesn't support directory sync via FileChannel, so we skip it."
  ([base] (sync-base nil base))
  ([filesystem base]
   ;; Only sync directories on the default filesystem (Jimfs doesn't support this)
   (when-not filesystem
     (let [p (get-path filesystem base)
           fc (FileChannel/open p (into-array OpenOption []))]
       (.force fc true)
       (.close fc)))))

(defn- check-and-create-backing-store
  "Helper Function to Check if Base is not writable"
  ([base] (check-and-create-backing-store nil base))
  ([filesystem base]
   (if filesystem
     ;; For custom filesystems (like Jimfs), use NIO APIs
     (let [base-path (get-path filesystem base)
           test-path (.resolve base-path (str (UUID/randomUUID)))]
       (when-not (Files/exists base-path (into-array LinkOption []))
         (Files/createDirectories base-path (into-array java.nio.file.attribute.FileAttribute [])))
       (Files/createFile test-path (into-array java.nio.file.attribute.FileAttribute []))
       (Files/delete test-path))
     ;; For default filesystem, use io/file for compatibility
     (let [f         (io/file base)
           test-file (io/file (str base "/" (UUID/randomUUID)))]
       (when-not (.exists f)
         (.mkdirs f))
       (when-not (.createNewFile test-file)
         (throw (ex-info "Cannot write to base." {:type   :not-writable
                                                  :base base})))
       (.delete test-file)))))

(defn delete-store
  "Permanently deletes the base of the store with all files."
  ([base] (delete-store nil base))
  ([filesystem base]
   (if filesystem
     ;; For custom filesystems, use NIO APIs
     (let [base-path (get-path filesystem base)
           parent-path (.getParent base-path)]
       (when (Files/exists base-path (into-array LinkOption []))
         (let [ds (Files/newDirectoryStream base-path)]
           (doseq [child ds]
             (Files/delete child))
           (.close ds))
         (Files/delete base-path))
       (when parent-path
         (try
           (sync-base filesystem (str parent-path))
           (catch Exception e e))))
     ;; For default filesystem, use io/file
     (let [f           (io/file base)
           parent-base (.getParent f)]
       (doseq [c (.list (io/file base))]
         (.delete (io/file (str base "/" c))))
       (.delete f)
       (try
         (sync-base parent-base)
         (catch Exception e e))))))

(defn list-files
  "Lists all files on the first level of a directory."
  ([directory]
   (list-files nil directory (fn [_] false)))
  ([directory ephemeral?]
   (list-files nil directory ephemeral?))
  ([filesystem directory ephemeral?]
   (let [root (get-path filesystem directory)
         ds (Files/newDirectoryStream root)
         files (mapv (fn [^Path path] (str (.relativize root path)))
                     (remove ephemeral? ds))]
     (.close ds)
     files)))

(defn count-konserve-keys
  "Counts konserve files in the directory."
  ([dir] (count-konserve-keys nil dir))
  ([filesystem dir]
   (reduce (fn [c path] (if (.endsWith ^String path ".ksv") (inc c) c))
           0
           (list-files filesystem dir (fn [_] false)))))

(defn store-exists?
  "Checks if path exists."
  ([base] (store-exists? nil base))
  ([filesystem base]
   (let [exists? (if filesystem
                   (Files/exists (get-path filesystem base) (into-array LinkOption []))
                   (.exists (io/file base)))]
     (if exists?
       (do (trace "Store directory at " (str base) " exists with " (count-konserve-keys filesystem base) " konserve keys.")
           true)
       (do (trace "Store directory at " (str base) " does not exist.")
           false)))))

(declare migrate-old-files migrate-file-v2 migrate-file-v1)

(defrecord BackingFilestore [base detected-old-blobs ephemeral? filesystem]
  PBackingStore
  (-create-blob [_this store-key env]
    (let [{:keys [sync?]} env
          path (get-path filesystem base store-key)
          standard-open-option (into-array StandardOpenOption
                                           [StandardOpenOption/WRITE
                                            StandardOpenOption/READ
                                            StandardOpenOption/CREATE])
          ac               (if sync?
                             (FileChannel/open path standard-open-option)
                             (AsynchronousFileChannel/open path standard-open-option))]
      (if sync? ac (go ac))))
  (-delete-blob [_this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (Files/delete (get-path filesystem base store-key)))))
  (-blob-exists? [_this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (Files/exists (get-path filesystem base store-key) (into-array LinkOption [])))))

  (-migratable [_this _key store-key env]                    ;; or importable
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (when detected-old-blobs
                           (let [uuid-key (store-key->uuid-key store-key)]
                             (or (@detected-old-blobs (str base "/meta/" uuid-key))
                                 (@detected-old-blobs (str base "/" uuid-key))
                                 (@detected-old-blobs (str base "/B_" uuid-key))))))))

  (-migrate [this old-path key-vec serializer read-handlers write-handlers env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (if detected-old-blobs
                           (if (clojure.string/includes? old-path "meta")
                             (<?- (migrate-file-v2 this old-path serializer read-handlers write-handlers env))
                             (if (or key-vec (includes? old-path "B_"))
                               (<?- (migrate-file-v1 this old-path key-vec serializer read-handlers write-handlers env))
                               (do (info "Migration of" old-path "not possible without knowing original key.")
                                   false)))
                           false))))

  (-handle-foreign-key [this file-or-dir serializer read-handlers write-handlers env]
    (migrate-old-files this file-or-dir serializer read-handlers write-handlers env))

  (-keys [_this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (into [] (list-files filesystem base ephemeral?)))))

  (-copy [_this from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if filesystem
                   ;; For custom filesystems, use NIO copy
                   (Files/copy (get-path filesystem base from)
                               (get-path filesystem base to)
                               (into-array [StandardCopyOption/REPLACE_EXISTING]))
                   ;; For default filesystem, use io/copy (native-image compatible)
                   (io/copy (.toFile (get-path nil base from))
                            (.toFile (get-path nil base to)))))))
  (-atomic-move [_this from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (Files/move (get-path filesystem base from)
                             (get-path filesystem base to)
                             (into-array [StandardCopyOption/ATOMIC_MOVE
                                          StandardCopyOption/REPLACE_EXISTING])))))
  (-create-store [_this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (check-and-create-backing-store filesystem base))))
  (-delete-store [_this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (delete-store filesystem base))))
  (-store-exists? [_this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (store-exists? filesystem base))))
  (-sync-store [_this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (sync-base filesystem base)))))

(extend-type AsynchronousFileChannel
  PBackingBlob
  (-sync [this _env] (go-try- (.force this true)))
  (-close [this _env] (go-try- (.close this)))
  (-get-lock [this _env] (go-try- (.get (.lock this))))
  (-write-header [this header env]
    (let [{:keys [msg]} env
          ch (chan)
          buffer (ByteBuffer/wrap header)]
      (.write this buffer 0 header-size
              (proxy [CompletionHandler] []
                (completed [_res _att]
                  (close! ch))
                (failed [t _att]
                  (put! ch (ex-info "Could not write key file."
                                    (assoc msg :exception t)))
                  (close! ch))))
      ch))
  (-write-meta [this meta-arr env]
    (let [{:keys [msg]} env
          ch (chan)
          meta-size (alength ^bytes meta-arr)
          buffer (ByteBuffer/wrap meta-arr)]
      (.write this buffer header-size (+ header-size meta-size)
              (proxy [CompletionHandler] []
                (completed [_res _att]
                  (close! ch))
                (failed [t _att]
                  (put! ch (ex-info "Could not write key file."
                                    (assoc msg :exception t)))
                  (close! ch))))
      ch))
  (-write-value [this value-arr meta-size env]
    (let [{:keys [msg]} env
          ch (chan)
          total-size (.size this)
          buffer (ByteBuffer/wrap value-arr)]
      (.write this buffer (+ header-size meta-size) total-size
              (proxy [CompletionHandler] []
                (completed [_res _att]
                  (close! ch))
                (failed [t _att]
                  (put! ch (ex-info "Could not write key file."
                                    (assoc msg :exception t)))
                  (close! ch))))
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
                       (completed [_res _att]
                         (.clear buffer)
                         (close! ch))
                       (failed [t _att]
                         (put! ch (ex-info "Could not write key file."
                                           (assoc msg :exception t)))
                         (close! ch))))
             (<?- ch)
             (recur (+ buffer-size start-byte) (+ buffer-size stop-byte)))))
       (catch Exception e
         (trace "write-binary error: " e)
         e)
       (finally
         (.close ^Closeable bis)))))
  (-read-header [this env]
    (let [{:keys [msg]} env
          ch (chan)
          buffer (ByteBuffer/allocate header-size)]
      (.read this buffer 0 header-size
             (proxy [CompletionHandler] []
               (completed [bytes-read _att]
                 (if (or (= 20 bytes-read) (= 8 bytes-read))
                   (put! ch (.array buffer))
                   (put! ch (ex-info "Header size mismatch"
                                     (assoc msg :expected [20 8] :actual bytes-read)))))
               (failed [t _att]
                 (put! ch (ex-info "Could not read key."
                                   (assoc msg :exception t))))))
      ch))
  (-read-meta [this meta-size env]
    (let [{:keys [msg header-size]} env
          ch (chan)
          buffer (ByteBuffer/allocate meta-size)]
      (.read this buffer header-size (+ header-size meta-size)
             (proxy [CompletionHandler] []
               (completed [_res _]
                 (put! ch (.array buffer)))
               (failed [t _att]
                 (put! ch (ex-info "Could not read key."
                                   (assoc msg :exception t))))))
      ch))
  (-read-value [this meta-size env]
    (let [{:keys [msg header-size]} env
          total-size (.size this)
          ch (chan)
          buffer (ByteBuffer/allocate (- total-size
                                         meta-size
                                         header-size))]
      (.read this buffer (+ header-size meta-size)
             (- total-size
                meta-size
                header-size)
             (proxy [CompletionHandler] []
               (completed [_res _]
                 (put! ch (.array buffer)))
               (failed [t _att]
                 (put! ch (ex-info "Could not read key."
                                   (assoc msg :exception t))))))
      ch))
  (-read-binary [this meta-size locked-cb env]
    (let [{:keys [msg header-size]} env
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
                                      (.read this buffer (+ header-size meta-size)
                                             total-size
                                             (proxy [CompletionHandler] []
                                               (completed [_res _]
                                                 (put! ch (.array buffer)))
                                               (failed [t _att]
                                                 (put! ch (ex-info "Could not read key."
                                                                   (assoc msg :exception t))))))
                                      ch)))
                    :size         total-size}))
       (catch Exception e
         (trace "read-binary error: " e)
         e)))))

(extend-type FileChannel
  PBackingBlob
  (-sync [this _env] (.force this true))
  (-close [this _env] (.close this))
  (-get-lock [this _env] (.lock this))
  (-write-header [this header _env]
    (let [buffer (ByteBuffer/wrap header)]
      (.write this buffer 0)))
  (-write-meta [this meta-arr _env]
    (let [buffer (ByteBuffer/wrap meta-arr)]
      (.write this buffer header-size)))
  (-write-value [this value-arr meta-size _env]
    (let [buffer (ByteBuffer/wrap value-arr)]
      (.write this buffer (+ header-size meta-size))))
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
          (.close ^Closeable bis)))))
  (-read-header [this _env]
    (let [buffer (ByteBuffer/allocate header-size)
          len (.read this buffer 0)]
      (when (= len -1)
        (throw (ex-info "File is empty or deleted during read"
                        {:length len :expected [20 8]})))
      (when-not (or (= 20 len) (= 8 len))
        (throw (ex-info "Header size mismatch"
                        {:expected [20 8] :actual len})))
      (.array buffer)))
  (-read-meta [this meta-size env]
    (let [{:keys [header-size]} env
          buffer (ByteBuffer/allocate meta-size)]
      (.read this buffer header-size)
      (.array buffer)))
  (-read-value [this meta-size env]
    (let [{:keys [header-size]} env
          total-size (.size this)
          buffer (ByteBuffer/allocate (- total-size
                                         meta-size
                                         header-size))]
      (.read this buffer (+ header-size meta-size))
      (.array buffer)))
  (-read-binary [this meta-size locked-cb env]
    (let [{:keys [header-size]} env
          total-size (.size this)]
      ;; TODO use FileInputStream to not load the file in memory
      (locked-cb {:input-stream (ByteArrayInputStream.
                                 (let [buffer (ByteBuffer/allocate (- total-size
                                                                      meta-size
                                                                      header-size))]
                                   (.read this buffer (+ header-size meta-size))
                                   (.array buffer)))
                  :size         total-size}))))

(extend-type FileLock
  PBackingLock
  (-release [this env]
    (if (:sync? env)
      (.release this)
      (go-try- (.release this)))))

;; ====================== Migration code ======================

(defn- -read [this start-byte stop-byte msg env]
  (if (:sync? env)
    (let [buffer (ByteBuffer/allocate (- stop-byte start-byte))]
      (.read ^FileChannel this buffer start-byte)
      (.array buffer))
    (let [buffer (ByteBuffer/allocate (- stop-byte start-byte))
          res-ch (chan)
          handler (proxy [CompletionHandler] []
                    (completed [_res _]
                      (put! res-ch (.array buffer)))
                    (failed [t _att]
                      (put! res-ch (ex-info "Could not read key."
                                            (assoc msg :exception t)))))]
      (.read ^AsynchronousFileChannel this buffer start-byte stop-byte handler)
      res-ch)))

(defn get-file-channel [path sync?]
  (let [standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])]
    (if sync?
      (let [channel (FileChannel/open path standard-open-option)]
        [channel (.size channel)])
      (let [channel (AsynchronousFileChannel/open path standard-open-option)]
        [channel (.size channel)]))))

(defn- migrate-file-v1
  "Migration function for konserve version with old file-schema."
  [{:keys [base detected-old-blobs] :as backing}
   old-path key-vec
   serializer read-handlers write-handlers
   {:keys [version input up-fn compressor encryptor operation locked-cb buffer-size sync?]
    :as env}]
  (let [key (first key-vec)
        store-key (key->store-key key)
        data-path            (Paths/get old-path (into-array String []))
        [c-data-file size-data] (get-file-channel data-path sync?)
        binary?              (includes? old-path "B_")]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [[[nkey] data] (if binary?
                                       [[key] true]
                                       (->>
                                        (<?- (-read c-data-file 0 size-data
                                                    {:type :read-data-old-error
                                                     :path data-path}
                                                    env))
                                        (ByteArrayInputStream.)
                                        (kp/-deserialize serializer read-handlers)))
                       [meta old]    (if binary?
                                       [{:key key :type :binary :last-write (Date.)}
                                        {:operation :write-binary
                                         :input     (if input input (FileInputStream. ^String old-path))
                                         :msg       {:type :write-binary-error
                                                     :key  key}}]
                                       [{:key nkey :type :edn :last-write (Date.)}
                                        {:operation :write-edn
                                         :up-fn     (if up-fn (up-fn data) (fn [_] data))
                                         :msg       {:type :write-edn-error
                                                     :key  key}}])
                       env           (merge {:version    version
                                             :compressor compressor
                                             :encryptor  encryptor
                                             :store-key  store-key
                                             :buffer-size buffer-size
                                             :base base
                                             :key-vec [nkey]
                                             :up-fn-meta (fn [_] meta)}
                                            old)
                       return-value  (fn [r]
                                       (Files/delete data-path)
                                       (swap! detected-old-blobs disj old-path)
                                       r)]
                   (if (contains? #{:write-binary :write-edn} operation)
                     (<?- (update-blob backing store-key serializer write-handlers env [nil nil]))
                     (let [value (<?- (update-blob backing store-key serializer write-handlers env [nil nil]))]
                       (if (= operation :read-meta)
                         (return-value meta)
                         (if (= operation :read-binary)
                           (let [start-byte 0
                                 stop-byte size-data
                                 res (<?- (-read c-data-file start-byte stop-byte
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
                   (.close ^AsynchronousFileChannel c-data-file))))))

(defn- migrate-file-v2
  "Migration Function For Konserve Version, who has Meta and Data Bases.
   Write old file into new Konserve directly."
  [{:keys [base detected-old-blobs] :as backing}
   old-path
   serializer read-handlers write-handlers
   {:keys [version input up-fn locked-cb operation compressor encryptor sync? buffer-size] :as env}]
  (let [meta-path            (Paths/get old-path (into-array String []))
        data-store-key       (clojure.string/replace old-path #"meta" "data")
        data-path            (Paths/get data-store-key (into-array String []))
        [c-meta-file size-meta] (get-file-channel meta-path sync?)
        [c-data-file size-data] (get-file-channel data-path sync?)]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [{:keys [format key]} (->> (<?- (-read c-meta-file 0 size-meta
                                                             {:type :read-meta-old-error
                                                              :path meta-path}
                                                             env))
                                                 (ByteArrayInputStream.)
                                                 (kp/-deserialize serializer read-handlers))
                       store-key (key->store-key key)
                       data         (when (= :edn format)
                                      (->> (<?- (-read c-data-file 0 size-data
                                                       {:type :read-data-old-error
                                                        :path data-path}
                                                       env))
                                           (ByteArrayInputStream.)
                                           (kp/-deserialize serializer read-handlers)))
                       [meta old]   (if (= :binary format)
                                      [{:key key :type :binary :last-write (Date.)}
                                       {:operation :write-binary
                                        :input     (if input input (FileInputStream. data-store-key))
                                        :msg       {:type :write-binary-error
                                                    :key  key}}]
                                      [{:key key :type :edn :last-write (Date.)}
                                       {:operation :write-edn
                                        :up-fn     (if up-fn (up-fn data) (fn [_] data))
                                        :msg       {:type :write-edn-error
                                                    :key  key}}])
                       env          (merge {:version    version
                                            :compressor compressor
                                            :encryptor  encryptor
                                            :buffer-size buffer-size
                                            :base base
                                            :store-key  store-key
                                            :key-vec [key]
                                            :up-fn-meta (fn [_] meta)}
                                           old)
                       return-value (fn [r]
                                      (Files/delete meta-path)
                                      (Files/delete data-path)
                                      (swap! detected-old-blobs disj old-path) r)]
                   (if (contains? #{:write-binary :write-edn} operation)
                     (<?- (update-blob backing store-key serializer write-handlers env [nil nil]))
                     (let [value (<?- (update-blob backing store-key serializer write-handlers env [nil nil]))]
                       (if (= operation :read-meta)
                         (return-value meta)
                         (if (= operation :read-binary)
                           (let [start-byte 0
                                 stop-byte size-data
                                 res (<?- (-read c-data-file start-byte stop-byte
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
                   (<?- (-close c-data-file env))
                   (<?- (-close c-meta-file env)))))))

(defn migrate-old-files [{:keys [base] :as backing} old-store-key serializer read-handlers write-handlers {:keys [sync?] :as env}]
  (async+sync
   sync? *sync-translation*
   (go-try-
    (let [env       (update-in env [:msg :keys] (fn [_] old-store-key))]
      (cond
      ;; ignore the data base
        (includes? old-store-key "data") (do "data" [])

        (includes? old-store-key "meta")
        (vec
         (let [meta-base (str base "/meta")]
           (loop [meta-list-keys #{}
                  [meta-key & meta-store-keys] (list-files meta-base)]
             (if meta-key
               (let [old-path (str meta-base "/" meta-key)
                     env           (assoc-in env [:msg :keys] old-path)
                     env           (assoc env :operation :read-meta)]
                 (recur
                  (conj meta-list-keys
                        (<?- (migrate-file-v2 backing
                                              old-path
                                              serializer read-handlers write-handlers
                                              env)))
                  meta-store-keys))
               meta-list-keys))))
        (includes? old-store-key "B_")
        [{:store-key old-store-key
          :type      :stale-binary
          :msg       "Old binary file detected. Use bget instead of keys for migration."}]

        :else
        [(let [old-path (str base "/" old-store-key)
               key-vec nil
               key (<?- (migrate-file-v1 backing
                                         old-path key-vec
                                         serializer read-handlers write-handlers
                                         env))]
           key)])))))

(defn detect-old-file-schema
  "Detect files using old storage schema for migration."
  ([base] (detect-old-file-schema nil base))
  ([filesystem base]
   (reduce
    (fn [old-list path]
      (let [store-key (-> ^Path path .toString)]
        (cond
          (or
           (includes? store-key "data")
           (ends-with? store-key ".ksv"))    old-list
          (re-find #"meta(?!\S)" store-key) (into old-list (detect-old-file-schema filesystem store-key))
          :else                             (into old-list [store-key]))))
    #{}
    (Files/newDirectoryStream (get-path filesystem base)))))

(defn connect-fs-store
  "Create Filestore in given path.
  Optional serializer, read-handlers, write-handlers, buffer-size and config (for fsync) can be changed.

  The :filesystem option allows using a custom java.nio.file.FileSystem (e.g., Jimfs for testing).
  When provided, all path operations will use that filesystem instead of the default.

  Defaults are
  {:base           path
   :serializer     fressian-serializer
   :read-handlers  empty
   :write-handlers empty
   :buffer-size    1 MB
   :filesystem     nil (uses default filesystem)
   :config         config} "
  [path & {:keys [detect-old-file-schema? ephemeral? config filesystem]
           :or {detect-old-file-schema? false
                ephemeral? (fn [^Path path]
                             (some #(re-matches % (-> path .getFileName .toString))
                                   [#"\.nfs.*"]))
                filesystem nil}
           :as params}]
  ;; check config
  (let [store-config (merge {:default-serializer :FressianSerializer
                             :compressor         null-compressor
                             :encryptor          null-encryptor
                             :read-handlers      (atom {})
                             :write-handlers     (atom {})
                             :buffer-size        (* 1024 1024)
                             :opts               {:sync? false}
                             :config             (merge {:sync-blob? true
                                                         :in-place? false
                                                         :lock-blob? true}
                                                        config)}
                            (dissoc params :config :filesystem))
        detect-old-blob (when detect-old-file-schema?
                          (atom (detect-old-file-schema filesystem path)))
        _                  (when detect-old-file-schema?
                             (when-not (empty? @detect-old-blob)
                               (info (count @detect-old-blob) "files in old storage schema detected. Migration for each key will happen transparently the first time a key is accessed. Invoke konserve.core/keys to do so at once. Once all keys are migrated you can deactivate this initial check by setting detect-old-file-schema to false.")))
        backing            (BackingFilestore. path detect-old-blob ephemeral? filesystem)]
    (connect-default-store backing store-config)))

(comment
  (defn reader-helper [start-byte stop-byte store-key]
    (let [path      (Paths/get store-key (into-array String []))
          ac        (AsynchronousFileChannel/open path (into-array StandardOpenOption [StandardOpenOption/READ]))
          bb        (ByteBuffer/allocate (- stop-byte start-byte))]
      (.read
       ac
       bb
       start-byte
       stop-byte
       (proxy [CompletionHandler] []
         (completed [res att]
           (let [arr-bb    (.array bb)]
             (prn (map #(get arr-bb %) (range 0 4)))))
         (failed [t att]
           (prn "fail"))))))

  (reader-helper 0 4 "/tmp/konserve/2c8e57a6-ed4e-5746-9f7e-af7ff2ac25c5.ksv")

  (require '[konserve.core :as k])

  ;; sync

  (def store (connect-fs-store "/tmp/store" :opts {:sync? true}))

  (k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? true})
  (k/get-in store ["foo"] nil {:sync? true})
  (k/exists? store "foo" {:sync? true})

  (k/assoc-in store [:bar] 42 {:sync? true})
  (k/update-in store [:bar] inc {:sync? true})
  (k/get-in store [:bar] nil {:sync? true})
  (k/dissoc store :bar {:sync? true})

  (k/append store :error-log {:type :horrible} {:sync? true})
  (k/log store :error-log {:sync? true})

  (k/bassoc store :binbar (byte-array (range 10)) {:sync? true})
  (k/bget store :binbar (fn [{:keys [input-stream]}]
                          (map byte (slurp input-stream)))
          {:sync? true})

  ;; async

  (def store (<!! (connect-fs-store "/tmp/store")))

  (<!! (k/assoc-in store ["foo" :bar] {:foo "baz"}))
  (<!! (k/get-in store ["foo"]))
  (<!! (k/exists? store "foo"))

  (<!! (k/assoc-in store [:bar] 42))
  (<!! (k/update-in store [:bar] inc))
  (<!! (k/get-in store [:bar]))
  (<!! (k/dissoc store :bar))

  (<!! (k/append store :error-log {:type :horrible}))
  (<!! (k/log store :error-log))

  (<!! (k/bassoc store :binbar (byte-array (range 10)) {:sync? false}))
  (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                               (map byte (slurp input-stream)))
               {:sync? false})))
