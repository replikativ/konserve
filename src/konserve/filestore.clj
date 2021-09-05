(ns konserve.filestore
  (:refer-clojure :exclude [read write])
  (:require
   [clojure.java.io :as io]
   [konserve.serializers :refer [byte->key byte->serializer serializer-class->byte key->serializer]]
   [konserve.compressor :refer [byte->compressor compressor->byte null-compressor]]
   [konserve.encryptor :refer [encryptor->byte byte->encryptor null-encryptor]]
   [hasch.core :refer [uuid]]
   [clojure.string :refer [includes? ends-with?]]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore
                               PBinaryAsyncKeyValueStore
                               -serialize -deserialize
                               PKeyIterable]]
   [konserve.storage-layout :refer [PLinearLayout
                                    linear-layout-id
                                    header-size
                                    parse-header create-header]]
   [konserve.nio-helpers :refer [blob->channel]]
   [konserve.utils :refer [async+sync *default-sync-translation*]]
   [superv.async :refer [go-try- <?-]]
   [clojure.core.async :as async
    :refer [<!! <! >! chan go close! put!]]
   [taoensso.timbre :as timbre :refer [info trace]])
  (:import
   [java.io ByteArrayOutputStream ByteArrayInputStream FileInputStream]
   [java.nio.channels Channel FileChannel AsynchronousFileChannel CompletionHandler]
   [java.nio ByteBuffer]
   [java.nio.file Files StandardCopyOption FileSystems Paths OpenOption LinkOption
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

(defn- completion-write-handler
  "Callback Function for Binary write. It only close the go-channel."
  [ch msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (close! ch))
    (failed [t att]
      (put! ch (ex-info "Could not write key file."
                        (assoc msg :exception t)))
      (close! ch))))

(defn write [sync? c buffer start-byte stop-byte handler]
  (if sync?
    (.write ^FileChannel c buffer start-byte)
    (.write ^AsynchronousFileChannel c buffer start-byte stop-byte handler)))

(defn- write-binary
  "Write Binary into file. InputStreams, ByteArrays, CharArray, Reader and Strings as inputs are supported."
  [input-stream ac buffer-size key start sync?]
  (let [[bis read] (blob->channel input-stream buffer-size)
        buffer     (ByteBuffer/allocate buffer-size)
        stop       (+ buffer-size start)]
    (async+sync sync? *sync-translation*
                (go-try-
                 (loop [start-byte start
                        stop-byte  stop]
                   (let [size   (read bis buffer)
                         _      (.flip buffer)
                         res-ch (chan)]
                     (if (= size -1)
                       (close! res-ch)
                       (do (write sync? ac buffer start-byte stop-byte
                                  (completion-write-handler res-ch
                                                            {:type :write-binary-error
                                                             :key  key}))
                           (<?- res-ch)
                           (.clear buffer)
                           (recur (+ buffer-size start-byte) (+ buffer-size stop-byte))))))
                 (finally
                   (.close bis)
                   (.clear buffer))))))

(defn- write-header
  "Write file-header"
  [ac-new key bytearray sync?]
  (let [buffer    (ByteBuffer/wrap bytearray)
        result-ch (chan)]
    (async+sync sync?
                *sync-translation*
                (go-try-
                 (write sync? ac-new buffer 0 header-size
                        (completion-write-handler result-ch
                                                  {:type :write-edn-error
                                                   :key  key}))
                 (<?- result-ch)
                 (finally
                   (close! result-ch)
                   (.clear buffer))))))

(defn- write-edn
  "Write Operation for edn. In the Filestore it would be for meta-data or data-value."
  [ac-new key serializer compressor encryptor write-handlers start-byte value sync?]
  (let [bos       (ByteArrayOutputStream.)
        _         (-serialize (encryptor (compressor serializer)) bos write-handlers value)
        stop-byte (.size bos)
        buffer    (ByteBuffer/wrap (.toByteArray bos))
        result-ch (chan)]
    (async+sync sync? *sync-translation*
                (go-try-
                 (write sync? ac-new buffer start-byte stop-byte
                        (completion-write-handler result-ch {:type :write-edn-error
                                                             :key  key}))
                 (<?- result-ch)
                 (finally
                   (close! result-ch)
                   (.clear buffer)
                   (.close bos))))))

(defn- update-file
  "Write file into file system. It write first the meta-size, that is stored in (1Byte),
  the meta-data and the actual data."
  [folder path serializer write-handlers buffer-size [key & rkey]
   {:keys [compressor encryptor storage-layout file-name up-fn
           up-fn-meta config operation input sync?]} [old-meta old-value]]
  (let [meta                 (up-fn-meta old-meta)
        value                (when (= operation :write-edn)
                               (if-not (empty? rkey)
                                 (update-in old-value rkey up-fn)
                                 (up-fn old-value)))

        path-new             (Paths/get (if (:in-place? config)
                                          file-name
                                          (str file-name ".new"))
                                        (into-array String []))
        _ (when (:in-place? config) ;; let's back things up before writing then
            (trace "backing up to file: " (str file-name ".backup") " for key " key)
            (Files/copy path-new (Paths/get (str file-name ".backup")
                                            (into-array String []))
                        (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING])))
        standard-open-option (into-array StandardOpenOption
                                         [StandardOpenOption/WRITE
                                          StandardOpenOption/CREATE])
        ac-new               (if sync?
                               (FileChannel/open path-new standard-open-option)
                               (AsynchronousFileChannel/open path-new standard-open-option))
        bos                  (ByteArrayOutputStream.)
        _                    (-serialize (encryptor (compressor serializer)) bos write-handlers meta)
        meta-size            (.size bos)
        _                    (.close bos)
        start-byte           (+ header-size meta-size)
        byte-array           (create-header *default-storage-layout* serializer compressor encryptor meta-size)]
    (async+sync sync? *sync-translation*
                (go-try-
                 (<?- (write-header ac-new key byte-array sync?))
                 (<?- (write-edn ac-new key serializer compressor encryptor write-handlers header-size meta sync?))
                 (<?- (if (= operation :write-binary)
                        (write-binary input ac-new buffer-size key start-byte sync?)
                        (write-edn ac-new key serializer compressor encryptor write-handlers start-byte value sync?)))
                 (when (:fsync? config)
                   (trace "fsyncing for " key)
                   (.force ^AsynchronousFileChannel ac-new true)
                   (sync-folder folder))
                 (.close ^AsynchronousFileChannel ac-new)

                 (when-not (:in-place? config)
                   (trace "moving file: " key)
                   (Files/move path-new path
                               (into-array [StandardCopyOption/ATOMIC_MOVE])))
                 (if (= operation :write-edn) [old-value value] true)
                 (finally
                   (.close ^AsynchronousFileChannel ac-new))))))

(defn completion-read-handler
  "Callback function for read-file. It can return following data: binary/edn/meta.
  The Binary is within a locked function turnt back."
  [res-ch ^ByteBuffer bb meta-size file-size {:keys [msg operation locked-cb file-name]} fn-read]
  (proxy [CompletionHandler] []
    (completed [res _]
      (try
        (case operation
          (:read-edn :read-meta) (let [bais-read (ByteArrayInputStream. (.array bb))
                                       value     (fn-read bais-read)
                                       _         (.close bais-read)]
                                   (when value
                                     (put! res-ch value))
                                   (close! res-ch))
          :write-binary          (let [bais-read (ByteArrayInputStream. (.array bb))
                                       value     (fn-read bais-read)
                                       _         (.close bais-read)]
                                   (put! res-ch [value nil]))
          :write-edn             (let [bais-meta  (ByteArrayInputStream. (.array bb) 0 meta-size)
                                       meta       (fn-read bais-meta)
                                       _          (.close bais-meta)
                                       bais-value (ByteArrayInputStream. (.array bb) meta-size file-size)
                                       value     (fn-read bais-value)
                                       _          (.close bais-value)]
                                   (put! res-ch [meta value]))
          :read-storage-layout          (let [array   (.array bb)
                                              storage-layout (.readShort array)]
                                          (put! res-ch storage-layout))
          :read-serializer       (let [array   (.array bb)
                                       ser     (.readShort array)]
                                   (put! res-ch ser))
          :read-binary           (go
                                   (let [res (<! (locked-cb {:input-stream (ByteArrayInputStream. (.array bb))
                                                             :size         file-size
                                                             :file         file-name}))]
                                     (if (nil? res) (close! res-ch) (>! res-ch res)))))
        (catch Exception e
          (put! res-ch (ex-info "Could not read key."
                                (assoc msg :exception e))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception t))))))

(defn- read [sync? c buffer start-byte stop-byte ^CompletionHandler handler res-ch]
  (if sync?
    (do
      ;; directly invoke handler synchronously
      (.completed handler (.read ^FileChannel c buffer start-byte) nil)
      ;; value has to be ready now
      (async/poll! res-ch))
    (do
      (.read ^AsynchronousFileChannel c buffer start-byte stop-byte handler)
      res-ch)))

(defn- read-file
  "Read meta, edn and binary."
  [^Channel ac serializer read-handlers {:keys [compressor encryptor operation sync?] :as env} meta-size]
  (let [file-size                         (if sync?
                                            (.size ^FileChannel ac)
                                            (.size ^AsynchronousFileChannel ac))
        {:keys [^ByteBuffer bb start-byte stop-byte]}
        (cond
          (= operation :write-edn)
          {:bb         (ByteBuffer/allocate file-size)
           :start-byte header-size
           :stop-byte  file-size}
          (= operation :read-storage-layout)
          {:bb         (ByteBuffer/allocate 1)
           :start-byte 0
           :stop-byte  1}
          (= operation :read-serializer)
          {:bb         (ByteBuffer/allocate 1)
           :start-byte 3
           :stop-byte  4}
          (or (= operation :read-meta)  (= operation :write-binary))
          {:bb         (ByteBuffer/allocate meta-size)
           :start-byte header-size
           :stop-byte  (+ meta-size header-size)}
          :else
          {:bb         (ByteBuffer/allocate (- file-size meta-size header-size))
           :start-byte (+ meta-size header-size)
           :stop-byte  file-size})
        res-ch                            (chan)]
    (async+sync sync? *sync-translation*
                (let [^AsynchronousFileChannel ac ac]
                  (go-try-
                   (<?- (read sync? ac bb start-byte stop-byte
                              (completion-read-handler res-ch bb meta-size file-size env
                                                       (partial -deserialize
                                                                (compressor (encryptor serializer))
                                                                read-handlers))
                              res-ch))
                   (finally
                     (.clear bb)))))))

(defn completion-read-old-handler [res-ch bb serializer read-handlers msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (let [bais (ByteArrayInputStream. (.array bb))]
        (try
          (let [value (-deserialize serializer read-handlers bais)]
            (put! res-ch value)
            (close! res-ch))
          (catch Exception e
            (put! res-ch
                  (ex-info "Could not read key."
                           (assoc msg :exception e)))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception t))))))

(defn- delete-file
  "Remove/Delete key-value pair of Filestore by given key. If success it will return true."
  [key folder sync?]
  (let [fn           (str folder "/" (uuid key) ".ksv")
        path         (Paths/get fn (into-array String []))
        file-exists? (Files/exists path (into-array LinkOption []))]
    (async+sync sync? *sync-translation*
                (if file-exists?
                  (go-try-
                   (Files/delete path)
                   true
                   (catch Exception e
                     (throw (ex-info "Could not delete key."
                                     {:key key
                                      :folder folder
                                      :exeption e}))))
                  (go false)))))

(defn completion-read-header-handler
  "Callback Function for io/operation. Return the Meta-size that are stored in the ByteBuffer."
  [res-ch ^ByteBuffer bb msg serializers]
  (proxy [CompletionHandler] []
    (completed [res att]
      (try
        (put! res-ch (parse-header (.array bb) serializers))
        (catch Exception e
          (put! res-ch (ex-info "Could not read key."
                                (assoc msg :exception e))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception t))))))

(defn read-header [sync? ac serializers]
  (let [bb (ByteBuffer/allocate header-size)]
    (if sync?
      (do
        (.read ^FileChannel ac bb)
        (parse-header (.array bb) serializers))
      (let [res-ch (chan)]
        (.read ^AsynchronousFileChannel ac bb 0 header-size
               (completion-read-header-handler res-ch bb
                                               {:type :read-meta-size-error
                                                :key  key}
                                               serializers))
        res-ch))))

(declare migrate-file-v1 migrate-file-v2)

(defn- io-operation
  "Read/Write file. For better understanding use the flow-chart of Konserve."
  [key-vec folder serializers read-handlers write-handlers buffer-size
   {:keys [detect-old-files operation default-serializer sync? overwrite? config] :as env}]
  (let [key           (first  key-vec)
        uuid-key      (uuid key)
        file-name     (str folder "/" uuid-key ".ksv")
        env           (assoc env :file-name file-name)
        path          (Paths/get file-name (into-array String []))
        file-exists?  (Files/exists path (into-array LinkOption []))
        old-file-name (when detect-old-files
                        (let [old-meta (str folder "/meta/" uuid-key)
                              old (str folder "/"  uuid-key)
                              old-binary (str folder "/B_"  uuid-key)]
                          (or (@detect-old-files old-meta)
                              (@detect-old-files old)
                              (@detect-old-files old-binary))))
        serializer    (get serializers default-serializer)]
    (if (and old-file-name (not file-exists?))
      (if (clojure.string/includes? old-file-name "meta")
        (migrate-file-v2 folder env buffer-size old-file-name file-name serializer read-handlers write-handlers)
        (migrate-file-v1 folder key env buffer-size old-file-name file-name serializer read-handlers write-handlers))
      (if (or file-exists? (= :write-edn operation) (= :write-binary operation))
        (let [standard-open-option (if file-exists?
                                     (into-array StandardOpenOption [StandardOpenOption/READ
                                                                     StandardOpenOption/WRITE])
                                     (into-array StandardOpenOption [StandardOpenOption/WRITE
                                                                     StandardOpenOption/CREATE]))
              ac                   (if sync?
                                     (FileChannel/open path standard-open-option)
                                     (AsynchronousFileChannel/open path standard-open-option))
              ^FileLockImpl lock   (when (:lock-file? config)
                                     (trace "Acquiring file lock for: " (first key-vec) (str ac))
                                     (if sync?
                                       (.lock ^FileChannel ac)
                                       (.get (.lock ^AsynchronousFileChannel ac))))]
          (async+sync sync? *sync-translation*
                      (go-try-
                       (let [old (if (and file-exists? (not overwrite?))
                                   (let [[_ serializer compressor _ meta-size] (<?- (read-header sync? ac serializers))]
                                     (<?- (read-file ac serializer read-handlers
                                                     (assoc env :compressor compressor)
                                                     meta-size)))
                                   [nil nil])]
                         (if (or (= :write-edn operation) (= :write-binary operation))
                           (<?- (update-file folder path serializer write-handlers buffer-size key-vec env old))
                           old))
                       (finally
                         (when (:lock-file? config)
                           (trace "Releasing lock for " (first key-vec) (str ac))
                           (.release lock))
                         (.close ^AsynchronousFileChannel ac)))))
        (if sync? nil (go nil))))))

(defn- list-keys
  "Return all keys in the store."
  [folder serializers read-handlers write-handlers buffer-size {:keys [sync?] :as env}]
  (let [path       (Paths/get folder (into-array String []))
        serializer (get serializers (:default-serializer env))
        file-paths (vec (Files/newDirectoryStream path))]
    (async+sync sync? *sync-translation*
                (go-try-
                 (loop [list-keys  #{}
                        [path & file-paths] file-paths]
                   (if path
                     (cond
                       (ends-with? (.toString path) ".new")
                       (recur list-keys file-paths)

                       (ends-with? (.toString path) ".ksv")
                       (let [options (into-array StandardOpenOption [StandardOpenOption/READ])
                             ^AsynchronousFileChannel ac          (if sync?
                                                                    (FileChannel/open path options)
                                                                    (AsynchronousFileChannel/open path options))
                             bb          (ByteBuffer/allocate header-size)
                             path-name   (.toString path)
                             env         (update-in env [:msg :keys] (fn [_] path-name))
                             res-ch      (chan)]
                         (recur
                          (try
                            (let [[_ serializer compressor _ meta-size] (<?- (read-header sync? ac serializers))]
                              (conj list-keys
                                    (<?- (read-file ac serializer read-handlers
                                                    ;; TODO why do we need to add the compressor?
                                                    (assoc env :compressor compressor)
                                                    meta-size))))
                     ;; it can be that the file has been deleted, ignore reading errors
                            (catch Exception _
                              list-keys)
                            (finally
                              (.clear bb)
                              (.close ac)
                              (close! res-ch)))
                          file-paths))

                       :else ;; need migration
                       (let [file-name (-> path .toString)
                             fn-l      (str folder "/" (-> path .getFileName .toString) ".ksv")
                             env       (update-in env [:msg :keys] (fn [_] file-name))]
                         (cond
                    ;; ignore the data folder
                           (includes? file-name "data")
                           (recur list-keys file-paths)

                           (includes? file-name "meta")
                           (recur (into list-keys
                                        (loop [meta-list-keys #{}
                                               [meta-path & meta-file-names]
                                               (vec (Files/newDirectoryStream path))]
                                          (if meta-path
                                            (let [old-file-name (-> meta-path .toString)
                                                  file-name     (str folder "/" (-> meta-path .getFileName .toString) ".ksv")
                                                  env           (assoc-in env [:msg :keys] old-file-name)
                                                  env           (assoc env :operation :read-meta)]
                                              (recur
                                               (conj meta-list-keys
                                                     (<?- (migrate-file-v2 folder env buffer-size old-file-name file-name
                                                                           serializer read-handlers write-handlers)))
                                               meta-file-names))
                                            meta-list-keys)))
                                  file-paths)

                           (includes? file-name "B_")
                           (recur
                            (conj list-keys
                                  {:file-name file-name
                                   :type      :stale-binary
                                   :msg       "Old binary file detected. Use bget insteat of keys for migration."})
                            file-paths)

                           :else
                           (recur
                            (conj list-keys (<?- (migrate-file-v1 folder fn-l env buffer-size file-name
                                                                  file-name serializer read-handlers write-handlers)))
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

  (-assoc-in [this key-vec meta-up val opts]
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
    (delete-file key folder (:sync? opts)))

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
                     :encryptor encryptor
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

(defn read-old-file [sync? ac-data-file binary? serializer read-handlers data-path]
  (let [size-data            (if sync?
                               (.size ^FileChannel ac-data-file)
                               (.size ^AsynchronousFileChannel ac-data-file))
        bb-data              (when-not binary? (ByteBuffer/allocate size-data))
        res-ch-data          (chan)]
    (try
      (if binary?
        (if sync? [[key] true] (go [[key] true]))
        (if sync?
          (let [bais (ByteArrayInputStream. (.array bb-data))]
            (try
              (.read ^FileChannel ac-data-file bb-data)
              (-deserialize serializer read-handlers bais)
              (catch Exception e
                (throw (ex-info "Could not read key."
                                {:type :read-data-old-error
                                 :path data-path
                                 :exception e})))))
          (do
            (.read ^AsynchronousFileChannel ac-data-file bb-data 0 size-data
                   (completion-read-old-handler res-ch-data bb-data serializer read-handlers
                                                {:type :read-data-old-error
                                                 :path data-path}))
            res-ch-data)))
      (finally
        (when-not binary?
          (.clear bb-data))))))

(defn- migrate-file-v1
  "Migration Function For Konserve Storage-Layout, who has old file-schema."
  [folder key {:keys [storage-layout input up-fn detect-old-files compressor encryptor operation locked-cb sync?]}
   buffer-size old-file-name new-file-name serializer read-handlers write-handlers]
  (let [standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])
        new-path             (Paths/get new-file-name (into-array String []))
        data-path            (Paths/get old-file-name (into-array String []))
        ac-data-file         (if sync?
                               (FileChannel/open data-path standard-open-option)
                               (AsynchronousFileChannel/open data-path standard-open-option))
        binary?              (includes? old-file-name "B_")]
    (async+sync sync? *sync-translation*
                (go-try-
                 (let [[[nkey] data] (<?- (read-old-file sync? ac-data-file binary?
                                                         serializer read-handlers data-path))
                       [meta old]    (if binary?
                                       [{:key key :type :binary :last-write (java.util.Date.)}
                                        {:operation :write-binary
                                         :input     (if input input (FileInputStream. old-file-name))
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
                                             :file-name  new-file-name
                                             :up-fn-meta (fn [_] meta)}
                                            old)
                       return-value  (fn [r]
                                       (Files/delete data-path)
                                       (swap! detect-old-files disj old-file-name)
                                       r)]
                   (if (contains? #{:write-binary :write-edn} operation)
                     (<?- (update-file folder new-path serializer write-handlers buffer-size [nkey] env [nil nil]))
                     (let [value (<?- (update-file folder new-path serializer write-handlers buffer-size [nkey]
                                                   env [nil nil]))]
                       (if (= operation :read-meta)
                         (return-value meta)
                         (if (= operation :read-binary)
                           (let [file-size  (.size ^AsynchronousFileChannel ac-data-file)
                                 bb         (ByteBuffer/allocate file-size)
                                 start-byte 0
                                 stop-byte  file-size
                                 res-ch     (chan)]
                             (try
                               (<?- (read sync? ac-data-file bb start-byte stop-byte
                                          (completion-read-handler res-ch bb nil file-size
                                                                   (assoc env
                                                                          :locked-cb locked-cb
                                                                          :operation :read-binary)
                                                                   (partial -deserialize
                                                                            serializer
                                                                            read-handlers))
                                          res-ch))
                               (finally
                                 (.clear bb))))
                           (return-value (second value)))))))
                 (finally
                   (when binary?
                     (Files/delete data-path))
                   (.close ^AsynchronousFileChannel ac-data-file))))))

(defn- migrate-file-v2
  "Migration Function For Konserve Storage-Layout, who has Meta and Data Folders.
   Write old file into new Konserve directly."
  [folder {:keys [storage-layout input up-fn detect-old-files locked-cb operation compressor encryptor sync?]}
   buffer-size old-file-name new-file-name serializer read-handlers write-handlers]
  (let [standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])
        new-path             (Paths/get new-file-name (into-array String []))
        meta-path            (Paths/get old-file-name (into-array String []))
        data-file-name       (clojure.string/replace old-file-name #"meta" "data")
        data-path            (Paths/get data-file-name (into-array String []))
        ac-meta-file         (if sync?
                               (FileChannel/open meta-path standard-open-option)
                               (AsynchronousFileChannel/open meta-path standard-open-option))
        ac-data-file         (if sync?
                               (FileChannel/open data-path standard-open-option)
                               (AsynchronousFileChannel/open data-path standard-open-option))
        size-meta            (.size ac-meta-file)
        bb-meta              (ByteBuffer/allocate size-meta)
        res-ch-data          (chan)
        res-ch-meta          (chan)]
    (async+sync sync? *sync-translation*
                (go-try-
                 (let [{:keys [format key]} (<?- (read sync? ac-meta-file bb-meta 0 size-meta
                                                       (completion-read-old-handler res-ch-meta bb-meta serializer read-handlers
                                                                                    {:type :read-meta-old-error
                                                                                     :path meta-path})
                                                       res-ch-meta))
                       data         (when (= :edn format)
                                      (<?-
                                       (let [size-data (.size ac-data-file)
                                             bb-data   (ByteBuffer/allocate size-data)]
                                         (try
                                           (read sync?
                                                 ac-data-file bb-data 0 size-data
                                                 (completion-read-old-handler res-ch-data bb-data serializer read-handlers
                                                                              {:type :read-data-old-error
                                                                               :path data-path})
                                                 res-ch-data)
                                           (finally
                                             (.clear bb-data))))))
                       [meta old]   (if (= :binary format)
                                      [{:key key :type :binary :last-write (java.util.Date.)}
                                       {:operation :write-binary
                                        :input     (if input input (FileInputStream. data-file-name))
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
                                            :file-name  new-file-name
                                            :up-fn-meta (fn [_] meta)}
                                           old)
                       return-value (fn [r]
                                      (Files/delete meta-path)
                                      (Files/delete data-path)
                                      (swap! detect-old-files disj old-file-name) r)]
                   (if (contains? #{:write-binary :write-edn} operation)
                     (<?- (update-file folder new-path serializer write-handlers buffer-size [key] env [nil nil]))
                     (let [value (<?- (update-file folder new-path serializer write-handlers buffer-size [key] env [nil nil]))]
                       (if (= operation :read-meta)
                         (return-value meta)
                         (if (= operation :read-binary)
                           (let [file-size  (.size ac-data-file)
                                 bb         (ByteBuffer/allocate file-size)
                                 start-byte 0
                                 stop-byte  file-size
                                 res-ch     (chan)]
                             (<?- (go-try-
                                   (<?- (read sync? ac-data-file bb start-byte stop-byte
                                              (completion-read-handler res-ch bb nil file-size
                                                                       (assoc env
                                                                              :locked-cb locked-cb
                                                                              :operation :read-binary)
                                                                       (partial -deserialize serializer read-handlers))
                                              res-ch))
                                   (finally
                                     (Files/delete meta-path)
                                     (Files/delete data-path)
                                     (close! res-ch)
                                     (.clear bb)))))
                           (return-value (second value)))))))
                 (finally
                   (close! res-ch-data)
                   (.clear bb-meta)
                   (.close ac-data-file)
                   (.close ac-meta-file))))))

(defn detect-old-file-schema [folder]
  (reduce
   (fn [old-list path]
     (let [file-name (-> path .toString)]
       (cond
         (or
          (includes? file-name "data")
          (ends-with? file-name ".ksv"))    old-list
         (re-find #"meta(?!\S)" file-name) (into old-list (detect-old-file-schema file-name))
         :else                             (into old-list [file-name]))))
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

  (defn reader-helper [start-byte stop-byte file-name]
    (let [path      (Paths/get file-name (into-array String []))
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
