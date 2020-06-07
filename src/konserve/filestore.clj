(ns konserve.filestore
  (:require
   [clojure.java.io :as io]
   [konserve.serializers :as ser]
   [hasch.core :refer [uuid]]
   [clojure.data.fressian :as fress]
   [incognito.fressian :refer [incognito-read-handlers
                               incognito-write-handlers]]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore
                               -get -get-meta -get-version -update-in -dissoc -assoc-in
                               PBinaryAsyncKeyValueStore -bget -bassoc
                               -serialize -deserialize
                               PKeyIterable
                               -keys]]
  [konserve.utils :refer [go-try <? throw-if-exception]]
   [clojure.core.async :as async
    :refer [<!! <! >! timeout chan go close! put!]])
  (:import
   [java.io Reader File InputStream
    ByteArrayOutputStream ByteArrayInputStream FileInputStream]
   [java.nio.channels Channels FileChannel AsynchronousFileChannel CompletionHandler]
   [java.nio ByteBuffer]
   [java.nio.file Files StandardCopyOption FileSystems Path Paths OpenOption LinkOption
    StandardOpenOption FileVisitOption]))

(defn- sync-folder
  "Helper Function to Synchrenize the Folder of the Filestore"
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
        nil))))

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

(def compresson 1)

(def encrypt 1)

(defprotocol BlobToChannel
  (blob->channel [input buffer-size]))

(extend-protocol BlobToChannel
  InputStream
  (blob->channel [input buffer-size]
    [(Channels/newChannel input)
     (fn [bis buffer]  (.read bis buffer))])

  File
  (blob->channel [input buffer-size]
    [(Channels/newChannel (FileInputStream. input))
     (fn [bis buffer]  (.read bis buffer))])

  String
  (blob->channel [input buffer-size]
    [(Channels/newChannel (ByteArrayInputStream. (.getBytes input)))
     (fn [bis buffer]  (.read bis buffer))])

  Reader
  (blob->channel [input buffer-size]
    [input
     (fn [bis nio-buffer]
       (let [char-array (make-array Character/TYPE buffer-size)
             size (.read bis char-array)]
         (try
           (when-not (= size -1)
             (let [char-array (java.util.Arrays/copyOf char-array size)]
               (.put nio-buffer (.getBytes (String. char-array)))))
           size
           (catch Exception e
             (throw e)))))]))

(extend
  byte-array-type
  BlobToChannel
  {:blob->channel (fn [input buffer-size]
                    [(Channels/newChannel (ByteArrayInputStream. input))
                   (fn [bis buffer] (.read bis buffer))])})

(extend
  char-array-type
  BlobToChannel
  {:blob->channel (fn [input buffer-size]
                    [(Channels/newChannel (ByteArrayInputStream. (.getBytes (String. input))))
                     (fn [bis buffer] (.read bis buffer))])})

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
    (go-try
        (loop [start-byte start
               stop-byte  stop]
          (let [size   (read bis buffer)
                _      (.flip buffer)
                res-ch (chan)]
            (if (= size -1)
              (close! res-ch)
              (do (.write ac buffer start-byte stop-byte (completion-write-handler res-ch {:type :write-binary-error
                                                                                           :key  key}))
                  (<? res-ch)
                  (.clear buffer)
                  (recur (+ buffer-size start-byte) (+ buffer-size stop-byte))))))
        (finally
          (.close bis)
          (.clear buffer)))))

(defn- write-edn
  "Write Operation for Edn. In the Filestore it would be for meta-data or data-value."
  [^AsynchronousFileChannel ac-new key serializer write-handlers start-byte value byte-array]
  (let [bos       (ByteArrayOutputStream.)
        _         (when byte-array  (.write bos byte-array))
        _         (-serialize serializer bos write-handlers value)
        stop-byte (.size bos)
        buffer    (ByteBuffer/wrap (.toByteArray bos))
        result-ch (chan)]
    (go-try
        (.write ac-new buffer start-byte stop-byte (completion-write-handler result-ch {:type :write-edn-error
                                                                                        :key  key}))
        (<? result-ch)
        (finally
          (close! result-ch)
          (.clear buffer)
          (.close bos)))))

(defn to-byte-array
  "Return 8 Byte Array with following content
     1th Byte = Version of Konserve
     2th Byte = Compression Type
     3th Byte = Encrypt Type
     4th Byte = Serializer Type
   5-8th Byte = Meta-Size"
  [version serializer meta]
  (let [int-bb           (ByteBuffer/allocate 4)
        _                (.putInt int-bb meta)
        meta-array       (.array int-bb)
        env-array        (byte-array [version compresson encrypt serializer])
        return-buffer    (ByteBuffer/allocate 8)
        _                (.put return-buffer env-array)
        _                (.put return-buffer meta-array)
        return-array     (.array return-buffer)]
    (.clear return-buffer)
    return-array))

(defn- update-file
  "Write File into Filesystem. It Write first the meta-size, that is stored in (1Byte), the meta-data and the actual data."
  [folder config path serializer write-handlers buffer-size [key & rkey] {:keys [compressor encryptor version file-name up-fn up-fn-args up-fn-meta config operation input msg]} [old-meta old-value]]
  (try
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
          _                    (-serialize serializer bos write-handlers meta)
          meta-size            (.size bos)
          _                    (.close bos)
          start-byte           (+ Long/BYTES meta-size)
          serializer-id        (->> (type serializer) (get ser/class->serializer) (get ser/serializer->int))
          byte-array           (to-byte-array version serializer-id meta-size)]
      (go-try
        (<? (write-edn ac-new key serializer write-handlers 0 meta byte-array))
        (<? (if (= operation :write-binary)
                (write-binary input ac-new buffer-size key start-byte)
                (write-edn ac-new key serializer write-handlers start-byte value nil)))
          (.close ac-new) ;add config fileesync
          (Files/move path-new path
                      (into-array [StandardCopyOption/ATOMIC_MOVE]))
          (when (:fsync config)
            (sync-folder folder))
          (if (= operation :write-edn) [old-value value] true)
          (finally
            (.close ac-new))))))

(defn completion-read-handler
  "Callback function for read-file. It can return following data: binary/edn/meta. The Binary is within a locked function turnt back."
  [res-ch ^ByteBuffer bb meta-size file-size {:keys [msg operation locked-cb file-name]} fn-read]
  (proxy [CompletionHandler] []
    (completed [res att]
      (try
        (case operation
          (:read-edn :read-meta) (let [bais-read (ByteArrayInputStream. (.array bb))
                                       value     (fn-read bais-read)
                                       _         (.close bais-read)]
                                   (put! res-ch value))
          :write-binary          (let [bais-read (ByteArrayInputStream. (.array bb))
                                       value     (fn-read bais-read)
                                       _         (.close bais-read)]
                                   (put! res-ch [value nil]))
          :write-edn             (let [bais-meta  (ByteArrayInputStream. (.array bb) 0 meta-size)
                                       meta       (fn-read bais-meta)
                                       _          (.close bais-meta)
                                       bais-value (ByteArrayInputStream. (.array bb) meta-size file-size)
                                       value      (fn-read bais-value)
                                       _          (.close bais-value)]
                                   (put! res-ch [meta value]))
          :read-version          (let [array   (.array bb)
                                       version (.readShort array)]
                                   (put! res-ch version))
          :read-serializer       (let [array   (.array bb)
                                       ser     (.readShort array)]
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
  [^AsynchronousFileChannel ac serializer read-handlers {:keys [operation] :as env} meta-size]
  (let [file-size                         (.size ac)
        {:keys [bb start-byte stop-byte]} (cond
                                            (= operation :write-edn)                                   {:bb         (ByteBuffer/allocate file-size)
                                                                                                        :start-byte Long/BYTES
                                                                                                        :stop-byte  file-size}
                                            (= operation :read-version)                                {:bb         (ByteBuffer/allocate 2)
                                                                                                        :start-byte 0
                                                                                                        :stop-byte  2}
                                            (= operation :read-serializer)                             {:bb         (ByteBuffer/allocate 2)
                                                                                                        :start-byte 2
                                                                                                        :stop-byte  4}
                                            (or (= operation :read-meta)  (= operation :write-binary)) {:bb         (ByteBuffer/allocate meta-size)
                                                                                                        :start-byte Long/BYTES 
                                                                                                        :stop-byte  (+ meta-size Long/BYTES)}
                                            :else                                                      {:bb         (ByteBuffer/allocate (- file-size meta-size Long/BYTES))
                                                                                                        :start-byte (+ meta-size Long/BYTES)
                                                                                                        :stop-byte  file-size})
        res-ch                            (chan)]
    (go-try
      (.read ac bb start-byte stop-byte
          (completion-read-handler res-ch bb meta-size file-size env
                                   (partial -deserialize serializer read-handlers)))
      (<? res-ch)
      (finally
        (.clear bb)))))

(defn completion-read-meta-size-handler
  "Callback Function for io/operation. Return the Meta-size that are stored in the ByteBuffer."
  [res-ch ^ByteBuffer bb msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (let [arr-bb         (.array bb)
            buff-serializer (ByteBuffer/wrap arr-bb 0 2)
            buff-meta       (ByteBuffer/wrap arr-bb 2 4)
            meta-size       (.getInt buff-meta)
            serializer-id   (.getShort buff-serializer)
            _               (.clear buff-meta)
            _               (.clear buff-serializer)]
        (try
          (if (nil? meta-size)
            (close! res-ch)
            (put! res-ch [serializer-id meta-size]))
          (catch Exception e
            (ex-info "Could not read key."
                     (assoc msg :exception e))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception t))))))

(defn completion-read-old-handler [res-ch bb serializer read-handlers msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (let [bais (ByteArrayInputStream. (.array bb))]
        (try
          (let [value (-deserialize serializer read-handlers bais)]
            (put! res-ch value)
            (close! res-ch))
          (catch Exception e
            (ex-info "Could not read key."
                     (assoc msg :exception e))))))))

(defn- migrate-file-v1
  "Migration Function For Konserve Version, who has old file-schema."
  [folder key {:keys [version input up-fn detect-old-files operation msg]} buffer-size old-file-name new-file-name serializer read-handlers write-handlers]
  (let [standard-open-option (into-array StandardOpenOption [StandardOpenOption/READ])
        new-path             (Paths/get new-file-name (into-array String []))
        data-path            (Paths/get old-file-name (into-array String []))
        ac-data-file         (AsynchronousFileChannel/open data-path standard-open-option)
        binary?              (clojure.string/includes? old-file-name "B_")
        res-ch               (chan)
        size-data            (.size ac-data-file)
        bb-data              (when-not binary? (ByteBuffer/allocate size-data))
        res-ch-data          (chan)]
    (go-try
        (when-not binary?
          (.read ac-data-file bb-data 0 size-data
                 (completion-read-old-handler res-ch-data bb-data serializer read-handlers
                                              {:type :read-data-old-error
                                               :path data-path})))
      (let [[[nkey] data]   (if binary?
                              [[key] true]
                              (<? res-ch-data))
            [meta old] (if binary?
                         [{:key key :type :binary ::timestamp (java.util.Date.)}
                          {:operation :write-binary
                           :input     (if input input (FileInputStream. old-file-name))
                           :msg       {:type :write-binary-error
                                       :key  key}}]
                         [{:key nkey :type :edn ::timestamp (java.util.Date.)}
                          {:operation :write-edn
                           :up-fn     (if up-fn (up-fn data) (fn [_] data))
                           :msg       {:type :write-edn-error
                                       :key  key}}])
            env        (merge {:version    version
                               :file-name  new-file-name
                               :up-fn-meta (fn [_] meta)}
                              old)
            return-value (fn [r]
                           (do
                             (Files/delete data-path)
                             (swap! detect-old-files disj old-file-name)
                             r))]
        (if (contains? #{:write-binary :write-edn} operation)
          (<? (update-file folder nil new-path serializer write-handlers buffer-size [nkey] env [nil nil]))
          (let [[_ value] (<? (update-file folder nil new-path serializer write-handlers buffer-size [nkey] env [nil nil]))]
            (if (= operation :read-meta)
              (return-value meta)
              (if (= operation :read-binary)
                (let [file-size  (.size ac-data-file)
                      bb         (ByteBuffer/allocate file-size)
                      start-byte 0
                      stop-byte  file-size
                      res-ch     (chan)]
                  (go-try
                      (.read ac-data-file bb start-byte stop-byte
                             (completion-read-handler res-ch bb nil file-size env
                                                      (partial -deserialize serializer read-handlers)))
                    (<? res-ch)
                    (finally
                      (close! res-ch)
                      (.clear bb))))
                (return-value value))))))
      (finally
        (when-not binary? (.clear bb-data))
        (close! res-ch-data)
        (.close ac-data-file)))))

(defn- migrate-file-v2
  "Migration Function For Konserve Version, who has Meta and Data Folders.
   Write old file into new Konserve directly."
  [folder key {:keys [version input up-fn detect-old-files operation msg locked-cb]} buffer-size old-file-name new-file-name serializer read-handlers write-handlers]
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
      (go-try
          (.read ac-meta-file bb-meta 0 size-meta
                 (completion-read-old-handler res-ch-meta bb-meta serializer read-handlers
                                              {:type :read-meta-old-error
                                               :path meta-path}))
        (let [{:keys [format key]} (<? res-ch-meta)]
          (when (= :edn format)
            (let [size-data (.size ac-data-file)
                  bb-data   (ByteBuffer/allocate size-data)]
              (.read ac-data-file bb-data 0 size-data
                     (completion-read-old-handler res-ch-data bb-data serializer read-handlers
                                                  {:type :read-data-old-error
                                                   :path data-path}))))
          (let [data       (when-not (= :edn format) (<? res-ch-data))
                [meta old] (if (= :binary format)
                             [{:key key :type :binary ::timestamp (java.util.Date.)}
                              {:operation :write-binary
                               :input     (if input input (FileInputStream. data-file-name))
                               :msg       {:type :write-binary-error
                                           :key  key}}]
                             [{:key key :type :edn ::timestamp (java.util.Date.)}
                              {:operation :write-edn
                               :up-fn     (if up-fn (up-fn data) (fn [] data))
                               :msg       {:type :write-edn-error
                                           :key  key}}])
                env        (merge {:version    version
                                   :file-name  new-file-name
                                   :up-fn-meta (fn [_] meta)}
                                  old)
                return-value (fn [r] (do (Files/delete meta-path) (Files/delete data-path) (swap! detect-old-files disj old-file-name) r))]
            (if (contains? #{:write-binary :write-edn} operation)
              (<? (update-file folder nil new-path serializer write-handlers buffer-size [key] env [nil nil]))
              (let [[_ value] (<? (update-file folder nil new-path serializer write-handlers buffer-size [key] env [nil nil]))]
                (if (= operation :read-meta)
                  (return-value meta)
                  (if (= operation :read-binary)
                    (let [file-size  (.size ac-data-file)
                          bb         (ByteBuffer/allocate file-size)
                          start-byte 0
                          stop-byte  file-size
                          res-ch     (chan)]
                      (go-try
                          (.read ac-data-file bb start-byte stop-byte
                                 (completion-read-handler res-ch bb nil file-size env
                                                          (partial -deserialize serializer read-handlers)))
                        (<? res-ch)
                        (finally
                          (close! res-ch)
                          (.clear bb))))
                    (return-value value)))))))
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

;check if key already exist in new store
(defn- io-operation
  "Read/Write File with AsynchronousFileChannel. For better understanding use Flow-Chart of Konserve."
  [key-vec folder config serializer read-handlers write-handlers buffer-size {:keys [version input up-fn detect-old-files operation msg] :as env}]
  (let [key          (first  key-vec)
        uuid-key (uuid key)
        fn           (str folder "/" uuid-key ".ksv")
        env          (assoc env :file-name fn)
        path         (Paths/get fn (into-array String []))
        file-exists? (Files/exists path (into-array LinkOption []))
        old-file-name (when detect-old-files (first (filter #(clojure.string/includes? % (str uuid-key)) @detect-old-files)))]
    (if (and old-file-name (not file-exists?))
      (if (clojure.string/includes? old-file-name "meta")
        (migrate-file-v2 folder key env buffer-size old-file-name fn serializer read-handlers write-handlers)
        (migrate-file-v1 folder key env buffer-size old-file-name fn serializer read-handlers write-handlers))
      (if (or file-exists? (= :write-edn operation) (= :write-binary operation))
        (let [standard-open-option (if file-exists?
                                     (into-array StandardOpenOption [StandardOpenOption/READ])
                                     (into-array StandardOpenOption [StandardOpenOption/WRITE
                                                                     StandardOpenOption/CREATE]))
              ac                   (AsynchronousFileChannel/open path standard-open-option)]
          (if file-exists?
            (let [bb (ByteBuffer/allocate 6)
                  res-ch (chan)]
              (go-try
                  (.read ac bb 2 Long/BYTES
                         (completion-read-meta-size-handler res-ch bb
                                                            {:type :read-meta-size-error
                                                             :key  key}))
                (let [[ser meta-size] (<? res-ch)
                      old       (<? (read-file ac serializer read-handlers env meta-size))
                      ]
                  (if (or (= :write-edn operation) (= :write-binary operation))
                    (<? (update-file folder config path serializer write-handlers buffer-size key-vec env old))
                    old))
                (finally
                  (close! res-ch)
                  (.clear bb)
                  (.close ac))))
            (go-try
                (<? (update-file folder config path serializer write-handlers buffer-size key-vec env [nil nil]))
              (finally
                (.close ac)))))
        (go nil)))))

(defn- list-keys
  "Return all Keys of the Filestore."
  [folder serializer read-handlers write-handlers buffer-size env]
  (let [path       (Paths/get folder (into-array String []))
        file-paths (Files/newDirectoryStream path)]
    (->> (reduce
          (fn [list-keys path]
            (if (clojure.string/includes? (.toString path) ".ksv")
              (let [ac        (AsynchronousFileChannel/open path (into-array StandardOpenOption [StandardOpenOption/READ]))
                    bb        (ByteBuffer/allocate 6)
                    path-name (.toString path)
                    env       (update-in env [:msg :keys] (fn [old] path-name))
                    res-ch    (chan)]
                (conj list-keys (go-try
                                    (.read ac bb 2 Long/BYTES
                                           (completion-read-meta-size-handler res-ch bb
                                                                              {:type :read-list-keys
                                                                               :path path-name}))
                                  (let [[_ meta] (<? res-ch)]
                                    (<? (read-file ac serializer read-handlers env meta)))
                                  (finally
                                    (.clear bb)
                                    (.close ac)
                                    (close! res-ch)))))
              (let [file-name (-> path .toString)
                    fn-l      (-> path .getFileName .toString)
                    env       (update-in env [:msg :keys] (fn [_] file-name))
                           ]
                (if (clojure.string/includes? file-name "meta")
                  (into list-keys (map
                                   #(let [old-file-name (-> % .toString)
                                          fn            (-> % .getFileName .toString)
                                          env           (assoc-in env [:msg :keys] old-file-name)]
                                      (migrate-file-v2 folder fn-l env buffer-size old-file-name file-name serializer read-handlers write-handlers))
                                   (Files/newDirectoryStream (Paths/get file-name (into-array String [])))))
                  (conj list-keys (migrate-file-v1 folder fn-l env buffer-size file-name file-name serializer read-handlers write-handlers))))))
          #{}
          file-paths)
         async/merge
         (async/into #{}))))

(defrecord FileSystemStore [folder serializer compressor encryptor read-handlers write-handlers buffer-size detect-old-version locks config]
  PEDNAsyncKeyValueStore
  (-get [this key]
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation :read-edn
                                                                                           :compressor compressor
                                                                                           :encryptor  encryptor
                                                                                           :format    :data
                                                                                           :version version
                                                                                           :detect-old-files detect-old-version
                                                                                           :msg       {:type :read-edn-error
                                                                                                       :key  key}}))
  (-get-meta [this key]
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation :read-meta
                                                                                           :detect-old-files detect-old-version
                                                                                           :version version
                                                                                           :msg       {:type :read-meta-error
                                                                                                       :key  key}}))
  (-get-version [this key]
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation :read-version
                                                                                           :detect-old-files detect-old-version
                                                                                           :version version
                                                                                           :msg       {:type :read-version-error
                                                                                                       :key  key}}))
  (-assoc-in [this key-vec meta-up val] (-update-in this key-vec meta-up (fn [_] val) []))

  (-update-in [this key-vec meta-up up-fn args]
    (io-operation key-vec folder config serializer read-handlers write-handlers buffer-size {:operation  :write-edn
                                                                                             :detect-old-files detect-old-version
                                                                                             :version version
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
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation :read-binary
                                                                                           :detect-old-files detect-old-version
                                                                                           :config    config
                                                                                           :version version
                                                                                           :locked-cb locked-cb
                                                                                           :msg       {:type :read-binary-error
                                                                                                       :key  key}}))
  (-bassoc [this key meta-up input]
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation  :write-binary
                                                                                           :detect-old-files detect-old-version
                                                                                           :input      input
                                                                                           :version version
                                                                                           :up-fn-meta meta-up
                                                                                           :config     config
                                                                                           :msg        {:type :write-binary-error
                                                                                                        :key  key}}))
  PKeyIterable
  (-keys [this]
    (list-keys folder serializer read-handlers write-handlers buffer-size {:operation :read-meta
                                                                           :detect-old-files detect-old-version
                                                                           :version version
                                                                           :config config
                                                                           :msg {:type :read-all-keys-error}})))

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
     (let [file-name (-> path .toString)]
       (if (clojure.string/includes? (-> path .getFileName .toString) "meta")
         (into old-list (detect-old-file-schema file-name))
         (if (clojure.string/includes? file-name ".ksv")
           old-list
           (into old-list [file-name])))))
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
  [path & {:keys [serializer compressor encryptor read-handlers write-handlers buffer-size config version detect-old-file-schema?]
           :or   {serializer     (ser/fressian-serializer)
                  read-handlers  (atom {})
                  write-handlers (atom {})
                  buffer-size    (* 1024 1024)
                  config         {:fsync true}}}]
  (let [detect-old-version (when detect-old-file-schema? (atom (detect-old-file-schema path)))
        _                  (check-and-create-folder path)
        store              (map->FileSystemStore {:detect-old-version detect-old-version
                                                  :folder             path
                                                  :serializer         serializer
                                                  :read-handlers      read-handlers
                                                  :write-handlers     write-handlers
                                                  :buffer-size        buffer-size    
                                                  :locks              (atom {})
                                                  :config             config})]
    (go store)))

(comment
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
    (def store (<!! (new-fs-store "/tmp/konserve-test"))))

                                  ;:serializer-map :fress :detect-old-file-schema? true))))

  




  (<!! (-assoc-in store ["foo4"] (fn [e] {:foo "foo4"}) 1))

  (<!! (-get store "foo4"))

  (<!! (-get-meta store "foo4"))

  (<!! (-update-in store ["foo4"] (fn [e] {:foo "foo"}) inc []))

  (<!! (-keys store))



)
