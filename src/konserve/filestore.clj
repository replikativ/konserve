(ns konserve.filestore
  (:require
   [clojure.java.io :as io]
   [konserve.serializers :refer [byte->key byte->serializer serializer-class->byte key->serializer]]
   [konserve.compressor :refer [byte->compressor compressor->byte lz4-compressor]]
   [konserve.encryptor :refer [encryptor->byte byte->encryptor null-encryptor]]
   [hasch.core :refer [uuid]]
   [clojure.string :refer [includes? ends-with?]]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore
                               -get -get-meta -update-in -dissoc -assoc-in
                               PBinaryAsyncKeyValueStore -bget -bassoc
                               -serialize -deserialize
                               PKeyIterable
                               -keys
                               -serialize -deserialize]]
   [konserve.storage-layout :refer [LinearLayout -get-raw]]
   [superv.async :refer [go-try- <?-]]
   [clojure.core.async :as async
    :refer [<!! <! >! chan go close! put!]]
   [taoensso.timbre :as timbre :refer [info]])
  (:import
   [java.io Reader File InputStream StringReader Closeable DataInput
     ByteArrayOutputStream ByteArrayInputStream FileInputStream]
   [java.nio.channels Channels FileChannel AsynchronousFileChannel CompletionHandler ReadableByteChannel FileLock]
   [java.nio ByteBuffer]
   [java.nio.file Files StandardCopyOption FileSystems Path Paths OpenOption LinkOption
    StandardOpenOption]))

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

(def
  ^{:doc "Type object for a Java primitive byte array."
    :private true
    }
  byte-array-type (class (make-array Byte/TYPE 0)))

(def
  ^{:doc "Type object for a Java primitive char array."
    :private true}
  char-array-type (class (make-array Character/TYPE 0)))

(def version 1)

(defprotocol BlobToChannel
  (blob->channel [input buffer-size]))

(extend-protocol BlobToChannel
  InputStream
  (blob->channel [input buffer-size]
    [(Channels/newChannel input)
     (fn [bis buffer] (.read ^ReadableByteChannel bis ^ByteBuffer buffer))])

  File
  (blob->channel [input buffer-size]
    [(Channels/newChannel (FileInputStream. ^String input))
     (fn [bis buffer] (.read ^ReadableByteChannel bis buffer))])

  String
  (blob->channel [input buffer-size]
    [(Channels/newChannel (ByteArrayInputStream. (.getBytes input)))
     (fn [bis buffer] (.read ^ReadableByteChannel bis buffer))])

  Reader
  (blob->channel [input buffer-size]
    [input
     (fn [bis nio-buffer]
       (let [char-array (make-array Character/TYPE buffer-size)
             size (.read ^StringReader bis ^chars char-array)]
         (try
           (when-not (= size -1)
             (let [char-array (java.util.Arrays/copyOf ^chars char-array size)]
               (.put ^ByteBuffer nio-buffer ^"[B" (.getBytes (String. char-array)))))
           size
           (catch Exception e
             (throw e)))))]))

(extend
  byte-array-type
  BlobToChannel
  {:blob->channel (fn [input _]
                    [(Channels/newChannel (ByteArrayInputStream. input))
                   (fn [bis buffer] (.read ^ReadableByteChannel bis buffer))])})

(extend
  char-array-type
  BlobToChannel
  {:blob->channel (fn [input _]
                    [(Channels/newChannel (ByteArrayInputStream. (.getBytes (String. ^chars input))))
                     (fn [bis buffer] (.read ^ReadableByteChannel bis buffer))])})

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

(defn- write-binary
  "Write Binary into file. InputStreams, ByteArrays, CharArray, Reader and Strings as inputs are supported."
  [input-stream ^AsynchronousFileChannel ac buffer-size key start]
  (let [[bis read] (blob->channel input-stream buffer-size)
        buffer     (ByteBuffer/allocate buffer-size)
        stop       (+ buffer-size start)]
    (go-try-
        (loop [start-byte start
               stop-byte  stop]
          (let [size   (read bis buffer)
                _      (.flip buffer)
                res-ch (chan)]
            (if (= size -1)
              (close! res-ch)
              (do (.write ac buffer start-byte stop-byte (completion-write-handler res-ch
                                                                                   {:type :write-binary-error
                                                                                    :key  key}))
                  (<?- res-ch)
                  (.clear ^ByteBuffer buffer)
                  (recur (+ buffer-size start-byte) (+ buffer-size stop-byte))))))
        (finally
          (.close ^Closeable bis)
          (.clear ^ByteBuffer buffer)))))

(defn- write-header
  "Write File-Header"
  [^AsynchronousFileChannel ac-new key byte-array]
  (let [bos       (ByteArrayOutputStream.)
        _         (.write bos byte-array 0 (count byte-array))
        buffer    (ByteBuffer/wrap (.toByteArray bos))
        result-ch (chan)]
    (go-try-
     (.write ac-new buffer 0 8 (completion-write-handler result-ch {:type :write-edn-error
                                                                    :key  key}))
     (<?- result-ch)
     (finally
       (close! result-ch)
       (.clear ^ByteBuffer buffer)
       (.close bos)))))

(defn- write-edn
  "Write Operation for edn. In the Filestore it would be for meta-data or data-value."
  [^AsynchronousFileChannel ac-new key serializer compressor encryptor write-handlers start-byte value]
  (let [bos       (ByteArrayOutputStream.)
        _         (-serialize (encryptor (compressor serializer)) bos write-handlers value)
        stop-byte (.size bos)
        buffer    (ByteBuffer/wrap (.toByteArray bos))
        result-ch (chan)]
    (go-try-
        (.write ac-new buffer start-byte stop-byte
                (completion-write-handler result-ch {:type :write-edn-error
                                                     :key  key}))
     (<?- result-ch)
     (finally
       (close! result-ch)
       (.clear buffer)
       (.close bos)))))

(defn to-byte-array
  "Return 8 Byte Array with following content
     1th Byte = Version of Konserve
     2th Byte = Serializer Type
     3th Byte = Compressor Type
     4th Byte = Encryptor Type
   5-8th Byte = Meta-Size"
  [version serializer compressor encryptor meta]
  (let [int-bb           (ByteBuffer/allocate 4)
        _                (.putInt int-bb meta)
        meta-array       (.array int-bb)
        env-array        (byte-array [version serializer compressor encryptor])
        return-buffer    (ByteBuffer/allocate 8)
        _                (.put return-buffer env-array)
        _                (.put return-buffer meta-array)
        return-array     (.array return-buffer)]
    (.clear return-buffer)
    return-array))

(defn- update-file
  "Write file into filesystem. It write first the meta-size, that is stored in (1Byte),
  the meta-data and the actual data."
  [folder path serializer write-handlers buffer-size [key & rkey]
   {:keys [compressor encryptor version file-name up-fn up-fn-args
           up-fn-meta config operation input]} [old-meta old-value]]
  (let [path-new             (Paths/get (str file-name ".new") (into-array String []))
        standard-open-option (into-array StandardOpenOption
                                         [StandardOpenOption/WRITE
                                          StandardOpenOption/CREATE])
        ac-new               (AsynchronousFileChannel/open path-new standard-open-option)
        meta                 (up-fn-meta old-meta)
        value                (when (= operation :write-edn)
                               (if-not (empty? rkey)
                                 (apply update-in old-value rkey up-fn up-fn-args)
                                 (apply up-fn old-value up-fn-args)))
        bos                  (ByteArrayOutputStream.)
        _                    (-serialize (encryptor (compressor serializer)) bos write-handlers meta)
        meta-size            (.size bos)
        _                    (.close bos)
        start-byte           (+ Long/BYTES meta-size)
        serializer-id        (get serializer-class->byte (type serializer))
        compressor-id        (get compressor->byte compressor)
        encryptor-id         (get encryptor->byte encryptor)
        byte-array           (to-byte-array version serializer-id compressor-id encryptor-id meta-size)]
      (go-try-
        (<?- (write-header ac-new key byte-array))
        (<?- (write-edn ac-new key serializer compressor encryptor write-handlers 8 meta))
        (<?- (if (= operation :write-binary)
                (write-binary input ac-new buffer-size key start-byte)
                (write-edn ac-new key serializer compressor encryptor write-handlers start-byte value)))
        (when (:fsync config)
          (.force ac-new true))
        (.close ac-new)
        (Files/move path-new path
                    (into-array [StandardCopyOption/ATOMIC_MOVE]))
        (when (:fsync config)
          (sync-folder folder))
        (if (= operation :write-edn) [old-value value] true)
        (finally
          (.close ac-new)))))

(defn completion-read-handler
  "Callback function for read-file. It can return following data: binary/edn/meta.
  The Binary is within a locked function turnt back."
  [res-ch ^ByteBuffer bb meta-size file-size {:keys [msg operation locked-cb file-name]} fn-read]
  (proxy [CompletionHandler] []
    (completed [res att]
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
          :read-version          (let [array   (.array bb)
                                       version (.readShort ^DataInput array)]
                                   (put! res-ch version))
          :read-serializer       (let [array   (.array bb)
                                       ser     (.readShort ^DataInput array)]
                                   (put! res-ch ser))
          :read-binary           (go (>! res-ch (<! (locked-cb {:input-stream (ByteArrayInputStream. (.array bb))
                                                                :size         file-size
                                                                :file         file-name})))))
          (catch Exception e
            (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception e))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception t))))))

(defn- read-file
  "Read meta, edn and binary."
  [^AsynchronousFileChannel ac serializer read-handlers {:keys [compressor encryptor operation] :as env} meta-size]
  (let [file-size                         (.size ac)
        {:keys [^ByteBuffer bb start-byte stop-byte]} (cond
                                            (= operation :write-edn)
                                            {:bb         (ByteBuffer/allocate file-size)
                                             :start-byte Long/BYTES
                                             :stop-byte  file-size}
                                            (= operation :read-version)
                                            {:bb         (ByteBuffer/allocate 1)
                                             :start-byte 0
                                             :stop-byte  1}
                                            (= operation :read-serializer)
                                            {:bb         (ByteBuffer/allocate 1)
                                             :start-byte 3
                                             :stop-byte  4}
                                            (or (= operation :read-meta)  (= operation :write-binary))
                                            {:bb         (ByteBuffer/allocate meta-size)
                                             :start-byte Long/BYTES 
                                             :stop-byte  (+ meta-size Long/BYTES)}
                                            :else
                                            {:bb         (ByteBuffer/allocate (- file-size meta-size Long/BYTES))
                                             :start-byte (+ meta-size Long/BYTES)
                                             :stop-byte  file-size})
        res-ch                            (chan)]
    (go-try-
      (.read ac bb start-byte stop-byte
          (completion-read-handler res-ch bb meta-size file-size env
                                   (partial -deserialize (compressor (encryptor serializer)) read-handlers)))
      (<?- res-ch)
      (finally
        (.clear bb)))))

(defn completion-read-header-handler
  "Callback Function for io/operation. Return the Meta-size that are stored in the ByteBuffer."
  [res-ch ^ByteBuffer bb msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (let [arr                               (.array bb)
            buff-meta                         (ByteBuffer/wrap arr 4 4)
            meta-size                         (.getInt buff-meta)
            _                                 (.clear buff-meta)
            [_ serializer-id compressor-id _] arr ]
        (try
          (if (nil? meta-size)
            (close! res-ch)
            (put! res-ch [serializer-id compressor-id meta-size]))
          (catch Exception e
            (put! res-ch (ex-info "Could not read key."
                                  (assoc msg :exception e)))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception t))))))

(defn completion-read-old-handler [res-ch bb serializer read-handlers msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (let [bais (ByteArrayInputStream. (.array ^ByteBuffer bb))]
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

(defn- migrate-file-v1
  "Migration Function For Konserve Version, who has old file-schema."
  [folder key {:keys [version input up-fn detect-old-files compressor encryptor operation locked-cb]}
   buffer-size old-file-name new-file-name serializer read-handlers write-handlers]
  (let [standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])
        new-path             (Paths/get new-file-name (into-array String []))
        data-path            (Paths/get old-file-name (into-array String []))
        ac-data-file         (AsynchronousFileChannel/open data-path standard-open-option)
        binary?              (includes? old-file-name "B_")
        size-data            (.size ac-data-file)
        bb-data              (when-not binary? (ByteBuffer/allocate size-data))
        res-ch-data          (chan)]
    (go-try-
        (when-not binary?
          (.read ac-data-file bb-data 0 size-data
                 (completion-read-old-handler res-ch-data bb-data serializer read-handlers
                                              {:type :read-data-old-error
                                               :path data-path})))
      (let [[[nkey] data] (if binary?
                            [[key] true]
                            (<?- res-ch-data))
            [meta old]    (if binary?
                            [{:key key :type :binary :konserve.core/timestamp (java.util.Date.)}
                             {:operation :write-binary
                              :input     (if input input (FileInputStream. ^String old-file-name))
                              :msg       {:type :write-binary-error
                                          :key  key}}]
                            [{:key nkey :type :edn :konserve.core/timestamp (java.util.Date.)}
                             {:operation :write-edn
                              :up-fn     (if up-fn (up-fn data) (fn [_] data))
                              :msg       {:type :write-edn-error
                                          :key  key}}])
            env           (merge {:version    version
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
          (let [value (<?- (update-file folder new-path serializer write-handlers buffer-size [nkey] env [nil nil]))]
            (if (= operation :read-meta)
              (return-value meta)
              (if (= operation :read-binary)
                (let [file-size  (.size ac-data-file)
                      bb         (ByteBuffer/allocate file-size)
                      start-byte 0
                      stop-byte  file-size
                      res-ch     (chan)]
                  (<?- (go-try-
                           (.read ac-data-file bb start-byte stop-byte
                                  (completion-read-handler res-ch bb nil file-size (assoc env
                                                                                          :locked-cb locked-cb
                                                                                          :operation :read-binary)
                                                           (partial -deserialize serializer read-handlers)))
                         (<?- res-ch)
                         (finally
                           (close! res-ch)
                           (.clear ^ByteBuffer bb)))))
                (return-value (second value)))))))
      (finally
        (if binary?
          (Files/delete data-path) 
          (.clear bb-data))
        (close! res-ch-data)
        (.close ac-data-file)))))

(defn- migrate-file-v2
  "Migration Function For Konserve Version, who has Meta and Data Folders.
   Write old file into new Konserve directly."
  [folder {:keys [version input up-fn detect-old-files locked-cb operation compressor encryptor]}
   buffer-size old-file-name new-file-name serializer read-handlers write-handlers]
  (let [standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])
        new-path             (Paths/get new-file-name (into-array String []))
        meta-path            (Paths/get old-file-name (into-array String []))
        data-file-name       (clojure.string/replace old-file-name #"meta" "data")
        data-path            (Paths/get data-file-name (into-array String []))
        ac-meta-file         (AsynchronousFileChannel/open meta-path standard-open-option)
        ac-data-file         (AsynchronousFileChannel/open data-path standard-open-option)
        size-meta            (.size ac-meta-file)
        bb-meta              (ByteBuffer/allocate size-meta)
        res-ch-data          (chan)
        res-ch-meta          (chan)]
      (go-try-
          (.read ac-meta-file bb-meta 0 size-meta
                 (completion-read-old-handler res-ch-meta bb-meta serializer read-handlers
                                              {:type :read-meta-old-error
                                               :path meta-path}))
        (let [{:keys [format key]} (<?- res-ch-meta)]
          (when (= :edn format)
            (let [size-data (.size ac-data-file)
                  bb-data   (ByteBuffer/allocate size-data)]
              (.read ac-data-file bb-data 0 size-data
                     (completion-read-old-handler res-ch-data bb-data serializer read-handlers
                                                  {:type :read-data-old-error
                                                   :path data-path}))))
          (let [data         (when (= :edn format) (<?- res-ch-data))
                [meta old]   (if (= :binary format)
                               [{:key key :type :binary :konserve.core/timestamp (java.util.Date.)}
                                {:operation :write-binary
                                 :input     (if input input (FileInputStream. ^String data-file-name))
                                 :msg       {:type :write-binary-error
                                             :key  key}}]
                               [{:key key :type :edn :konserve.core/timestamp (java.util.Date.)}
                                {:operation :write-edn
                                 :up-fn     (if up-fn (up-fn data) (fn [_] data))
                                 :msg       {:type :write-edn-error
                                             :key  key}}])
                env          (merge {:version    version
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
                               (.read ac-data-file bb start-byte stop-byte
                                      (completion-read-handler res-ch bb nil file-size
                                                               (assoc env
                                                                      :locked-cb locked-cb
                                                                      :operation :read-binary)
                                                               (partial -deserialize serializer read-handlers)))
                             (<?- res-ch)
                             (finally
                               (Files/delete meta-path)
                               (Files/delete data-path)
                               (close! res-ch)
                               (.clear ^ByteBuffer bb)))))
                    (return-value (second value))))))))
        (finally
          (close! res-ch-data)
          (.clear bb-meta)
          (.close ac-data-file)
          (.close ac-meta-file)))))

(defn- delete-file
  "Remove/Delete key-value pair of Filestore by given key. If success it will return true."
  [key folder]
  (let [fn           (str folder "/" (uuid key) ".ksv")
        path         (Paths/get fn (into-array String []))
        res-ch       (chan)
        file-exists? (Files/exists path (into-array LinkOption []))]
    (if file-exists?
      (do (Files/delete path) 
          (put! res-ch true)
          (close! res-ch))
      (close! res-ch))
    res-ch))

(defn- io-operation
  "Read/Write file with AsynchronousFileChannel. For better understanding use the flow-chart of Konserve."
  [key-vec folder serializers read-handlers write-handlers buffer-size
   {:keys [detect-old-files operation default-serializer] :as env}]
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
              ac                   (AsynchronousFileChannel/open path standard-open-option)
              lock                 (.get (.lock ac))]
          (if file-exists?
            (let [header-size 8
                  bb          (ByteBuffer/allocate header-size)
                  res-ch      (chan)]
              (go-try-
                  (.read ^AsynchronousFileChannel ac ^ByteBuffer bb 0 header-size
                         (completion-read-header-handler res-ch bb
                                                         {:type :read-meta-size-error
                                                          :key  key}))
                  (let [[serializer-id compressor-id meta-size] (<?- res-ch)
                        serializer-key                          (byte->key serializer-id)
                        serializer                              (get serializers serializer-key)
                        compressor                              (byte->compressor compressor-id)
                        old                                     (<?- (read-file ac serializer read-handlers
                                                                                (assoc env :compressor compressor)
                                                                                meta-size))]
                    (if (or (= :write-edn operation) (= :write-binary operation))
                      (<?- (update-file folder path serializer write-handlers buffer-size key-vec env old))
                      old))
                (finally
                  (close! res-ch)
                  (.clear bb)
                  (.release ^FileLock lock)
                  (.close ac))))
            (go-try-
                (<?- (update-file folder path serializer write-handlers buffer-size key-vec env [nil nil]))
              (finally
                (.release ^FileLock lock)
                (.close ac)))))
        (go nil)))))

(defn- list-keys
  "Return all Keys of the filestore."
  [folder serializers read-handlers write-handlers buffer-size env]
  (let [path       (Paths/get folder (into-array String []))
        serializer (get serializers (:default-serializer env))
        file-paths (vec (Files/newDirectoryStream path))]
    (go-try-
     (loop [list-keys  #{}
            [path & file-paths] file-paths]
       (if path
         (cond
           (ends-with? (.toString ^Path path) ".new")
           (recur list-keys file-paths)

           (ends-with? (.toString ^Path path) ".ksv")
           (let [ac          (AsynchronousFileChannel/open
                              path (into-array StandardOpenOption [StandardOpenOption/READ]))
                 header-size 8
                 bb          (ByteBuffer/allocate header-size)
                 path-name   (.toString ^Path path)
                 env         (update-in env [:msg :keys] (fn [_] path-name))
                 res-ch      (chan)]
             (recur
              (try
                (.read ^AsynchronousFileChannel ac ^ByteBuffer bb 0 Long/BYTES
                       (completion-read-header-handler res-ch bb
                                                       {:type :read-list-keys
                                                        :path path-name}))
                (let [[serializer-id compressor-id meta-size] (<?- res-ch)
                      serializer-key                          (byte->key serializer-id)
                      serializer                              (get serializers serializer-key)
                      compressor                              (byte->compressor compressor-id)]
                  (conj list-keys
                        (<?- (read-file ac serializer read-handlers
                                        ;; TODO why do we need to add the compressor?
                                        (assoc env :compressor compressor)
                                        meta-size))))
                ;; it can be that the file has been deleted, ignore reading errors
                (catch Exception
                    list-keys)
                (finally
                  (.clear ^ByteBuffer bb)
                  (.close ^AsynchronousFileChannel ac)
                  (close! res-ch)))
              file-paths))

           :else ;; need migration
           (let [file-name (-> ^Path path .toString)
                 fn-l      (str folder "/" (-> ^Path path ^Path (.getFileName) .toString) ".ksv")
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
                                (let [old-file-name (-> ^Path meta-path .toString)
                                      file-name     (str folder "/" (-> ^Path meta-path ^Path (.getFileName) .toString) ".ksv")
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
         list-keys)))))



(defrecord FileSystemStore [folder serializers default-serializer compressor encryptor
                            read-handlers write-handlers buffer-size detect-old-version locks config]
  PEDNAsyncKeyValueStore
  (-get [this key]
    (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                  {:operation :read-edn
                   :compressor compressor
                   :encryptor encryptor
                   :format    :data
                   :version version
                   :default-serializer default-serializer
                   :detect-old-files detect-old-version
                   :msg       {:type :read-edn-error
                               :key  key}}))
  (-get-meta [this key]
    (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                  {:operation :read-meta
                   :compressor compressor
                   :encryptor encryptor
                   :detect-old-files detect-old-version
                   :default-serializer default-serializer
                   :version version
                   :msg       {:type :read-meta-error
                               :key  key}}))

  (-assoc-in [this key-vec meta-up val] (-update-in this key-vec meta-up (fn [_] val) []))

  (-update-in [this key-vec meta-up up-fn args]
    (io-operation key-vec folder serializers read-handlers write-handlers buffer-size
                  {:operation  :write-edn
                   :compressor compressor
                   :encryptor encryptor
                   :detect-old-files detect-old-version
                   :version version
                   :default-serializer default-serializer
                   :up-fn      up-fn
                   :up-fn-args args
                   :up-fn-meta meta-up
                   :config     config
                   :msg        {:type :write-edn-error
                                :key  (first key-vec)}}))
  (-dissoc [this key]
    (delete-file key folder))
  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                  {:operation :read-binary
                   :detect-old-files detect-old-version
                   :default-serializer default-serializer
                   :compressor compressor
                   :encryptor encryptor
                   :config    config
                   :version version
                   :locked-cb locked-cb
                   :msg       {:type :read-binary-error
                               :key  key}}))
  (-bassoc [this key meta-up input]
    (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                  {:operation  :write-binary
                   :detect-old-files detect-old-version
                   :default-serializer default-serializer
                   :compressor compressor
                   :encryptor encryptor
                   :input      input
                   :version version
                   :up-fn-meta meta-up
                   :config     config
                   :msg        {:type :write-binary-error
                                :key  key}}))
  PKeyIterable
  (-keys [this]
    (list-keys folder serializers read-handlers write-handlers buffer-size
               {:operation :read-meta
                :default-serializer default-serializer
                :detect-old-files detect-old-version
                :version version
                :compressor compressor
                :encryptor encryptor
                :config config
                :msg {:type :read-all-keys-error}}))


  LinearLayout
  (-get-raw [this key]
    (go
      (slurp (str folder "/" (uuid key) ".ksv"))))
  (-put-raw [this key blob]
    (go
      (throw (ex-info "Not implemented yet." {:type :not-implemented})))))

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

(defn detect-old-file-schema [folder]
  (reduce
   (fn [old-list path]
     (let [file-name (-> ^Path path .toString)]
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
  [path & {:keys [default-serializer serializers compressor encryptor read-handlers write-handlers
                  buffer-size config detect-old-file-schema?]
           :or   {default-serializer :FressianSerializer
                  compressor         lz4-compressor
                  encryptor          null-encryptor
                  read-handlers      (atom {})
                  write-handlers     (atom {})
                  buffer-size        (* 1024 1024)
                  config             {:fsync true}}}]
  (let [detect-old-version (when detect-old-file-schema?
                             (atom (detect-old-file-schema path)))
        _                  (when detect-old-file-schema?
                             (when-not (empty? @detect-old-version)
                               (info (count @detect-old-version) "files in old storage schema detected. Migration for each key will happen transparently the first time a key is accessed. Invoke konserve.core/keys to do so at once. Once all keys are migrated you can deactivate this initial check by setting detect-old-file-schema to false.")))
        _                  (check-and-create-folder path)
        store              (map->FileSystemStore {:detect-old-version detect-old-version
                                                  :folder             path
                                                  :default-serializer default-serializer
                                                  :serializers        (merge key->serializer serializers)
                                                  :compressor         compressor
                                                  :encryptor          encryptor
                                                  :read-handlers      read-handlers
                                                  :write-handlers     write-handlers
                                                  :buffer-size        buffer-size
                                                  :locks              (atom {})
                                                  :config             config})]
    (go store)))


(comment
  ;TODO check error propagation
                                        ;compressor => function , encrpto => function

  ;=> ser then compressoion then encrpt

;=> TODO add compressor ns

  ;TODO list-keys => old-files + test => for migration
                                        ;TODO gc test => create own gc

  ;TODO gc write in store => 5 => timestamp => Mark 3 => gc needs whitelist {keys}

 ;TODO gc => safe timestampe after 5 writes => make a 6 write after that

;TODO gc make namespace

;TODO lz4 compression => serializer compression make compression

  #_(clojure :variables clojure-enable-linters 'clj-kondo)
                                        ;in otspacemacs-additional-packages flycheck-clj-kondo hinzufuegen

                                        ; :100,120s/"erse"/"ersete"/g => va
  (do
    (delete-store "/tmp/konserve")
    (def store (<!! (new-fs-store "/tmp/konserve"))))

  (<!! (-assoc-in store ["bar"] (fn [e] {:foo "foooooooooooooooooooooooooooooooooooOOOOOooo"}) 1123123123123123123123123))

  (<!! (-get store "foo4"))

  (<!! (-get-meta store "bar"))

  (<!! (-update-in store ["bar"] (fn [e] {:foo "foo"}) inc []))

  (<!! (-keys store))

  (<!! (-get-version store "bar"))

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

  (reader-helper 0 4 "/tmp/konserve/2c8e57a6-ed4e-5746-9f7e-af7ff2ac25c5.ksv")
  

  )
