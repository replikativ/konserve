(ns konserve.old-filestore
  "Bare file-system implementation."
  (:require [konserve.serializers :as ser]
            [konserve.core :refer [go-locked]]
            [clojure.java.io :as io]
            [hasch.core :refer [uuid]]
            [konserve.protocols :refer [PStoreSerializer
                                        -serialize -deserialize]]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop close! put!]]
            [taoensso.timbre :refer [debug]])
  (:import [java.io
            DataInputStream DataOutputStream
            FileInputStream FileOutputStream
            ByteArrayInputStream File Writer]
           [java.nio.channels FileChannel AsynchronousFileChannel CompletionHandler]
           [java.nio ByteBuffer]
           [java.nio.file Files StandardCopyOption FileSystems OpenOption
            StandardOpenOption]
           (java.util UUID)))

;; A useful overview over fsync on Linux:
;; https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai

;; Protocols
(defprotocol PEDNAsyncKeyValueStoreV1
  "Allows to access a store similar to hash-map in EDN."
  (-exists? [this key] "Checks whether value is in the store.")
  (-get-in [this key-vec] "Returns the value stored described by key-vec or nil if the path is not resolvable.")
  (-update-in [this key-vec up-fn] [this key-vec up-fn up-fn-args] "Updates a position described by key-vec by applying up-fn and storing the result atomically. Returns a vector [old new] of the previous value and the result of applying up-fn (the newly stored value).")
  (-assoc-in [this key-vec val])
  (-dissoc [this key]))

(defprotocol PBinaryAsyncKeyValueStoreV1
  "Allows binary data byte array storage."
  (-bget [this key locked-cb] "Calls locked-cb with a platform specific binary representation inside the lock, e.g. wrapped InputStream on the JVM and Blob in JavaScript. You need to properly close/dispose the object when you are done!")
  (-bassoc [this key val] "Copies given value (InputStream, Reader, File, byte[] or String on JVM, Blob in JavaScript) under key in the store."))

(defprotocol PKeyIterableV1
  "Allows lazy iteration of keys in this store."
  (-keys [this]
    "Return a channel that will continuously yield keys in this store."))
                                        ;Filestore V1 < Konserve 0.4
(defn- on-windows? []
  (>= (.indexOf (.toLowerCase (System/getProperty "os.name")) "win") 0))

(defn- sync-folder [folder]
  (when-not (on-windows?) ;; not necessary (and possible) on windows
    (let [p (.getPath (FileSystems/getDefault) folder (into-array String []))
          fc (FileChannel/open p (into-array OpenOption []))]
      (.force fc true)
      (.close fc))))

(defn list-keys
  "Lists all keys in this binary store. This operation *does not block concurrent operations* and might return an outdated key set. Keys of binary blobs are not tracked atm."
  [{:keys [folder serializer read-handlers] :as store}]
  (let [fns (->> (io/file folder)
                 .list
                 seq
                 (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                                      %))
                 (map (fn [fn]
                        (go-locked
                         store fn
                         (let [f (io/file (str folder "/" fn))]
                           (when (.exists f)
                             (let [fis (DataInputStream. (FileInputStream. f))]
                               (try
                                 (first (-deserialize serializer read-handlers fis))
                                 (catch Exception e
                                   (ex-info "Could not read key."
                                            {:type :read-error
                                             :key fn
                                             :exception e}))
                                 (finally
                                   (.close fis)))))))))
                 async/merge
                 (async/into #{}))]
    fns))

(defn delete-store
  "Permanently deletes the folder of the store with all files."
  [folder]
  (let [f (io/file folder)]
    (doseq [c (.list f)]
      (.delete (io/file (str folder "/" c))))
    (.delete f)
    (try
      (sync-folder folder)
      (catch Exception e
        nil))))

(defn filestore-schema-update
  "Lists all keys in this binary store. This operation *does not block concurrent operations* and might return an outdated key set. Keys of binary blobs are not tracked atm."
  [{:keys [folder serializer read-handlers] :as store}]
  (let [fns (->> (io/file folder)
                 .list
                 seq
                 (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                                      %))
                 (map (fn [fn]
                        (go-locked
                         store fn
                         (let [f (io/file (str folder "/" fn))]
                           (when (.exists f)
                             (let [fis (DataInputStream. (FileInputStream. f))]
                               (try
                                 (first (-deserialize serializer read-handlers fis))
                                 (catch Exception e
                                   (ex-info "Could not read key."
                                            {:type :read-error
                                             :key fn
                                             :exception e}))
                                 (finally
                                   (.close fis)))))))))
                 async/merge
                 (async/into #{}))]
    fns))

(defrecord FileSystemStoreV1 [folder serializer read-handlers write-handlers locks config]
  PEDNAsyncKeyValueStoreV1
  (-exists? [this key]
    (let [fn (uuid key)
          bfn (str "B_" (uuid key))
          f (io/file (str folder "/" fn))
          bf (io/file (str folder "/" bfn))
          res (chan)]
      (put! res (or (.exists f) (.exists bf)))
      (close! res)
      res))

    ;; non-blocking async version
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          fn (uuid fkey)
          f (io/file (str folder "/" fn))
          res-ch (chan)]
      (if-not (.exists f)
        (close! res-ch)
        (try
          (let [ac (AsynchronousFileChannel/open (.getPath (FileSystems/getDefault)
                                                           (str folder "/" fn)
                                                           (into-array String []))
                                                 (into-array StandardOpenOption
                                                             [StandardOpenOption/READ]))
                bb (ByteBuffer/allocate (.size ac))]
            (.read ac
                   bb
                   0
                   nil
                   (proxy [CompletionHandler] []
                     (completed [res att]
                       (let [bais (ByteArrayInputStream. (.array bb))]
                         (try
                           (put! res-ch
                                 (get-in
                                  (second (-deserialize serializer read-handlers bais))
                                  rkey))
                           (catch Exception e
                             (ex-info "Could not read key."
                                      {:type :read-error
                                       :key fkey
                                       :exception e}))
                           (finally
                             (.close ac)))))
                     (failed [t att]
                       (put! res-ch (ex-info "Could not read key."
                                             {:type :read-error
                                              :key fkey
                                              :exception t}))
                       (.close ac)))))
          (catch Exception e
            (put! res-ch (ex-info "Could not read key."
                                  {:type :read-error
                                   :key fkey
                                   :exception e})))))
      res-ch))

  (-update-in [this key-vec up-fn] (-update-in this key-vec up-fn []))
  (-update-in [this key-vec up-fn up-fn-args]
    (async/thread
      (let [[fkey & rkey] key-vec
            fn (uuid fkey)
            f (io/file (str folder "/" fn))
            new-file (io/file (str folder "/" fn ".new"))]
        (let [old (when (.exists f)
                    (let [fis (DataInputStream. (FileInputStream. f))]
                      (try
                        (second (-deserialize serializer read-handlers fis))
                        (catch Exception e
                          (ex-info "Could not read key."
                                   {:type :read-error
                                    :key fkey
                                    :exception e}))
                        (finally
                          (.close fis)))))
              fos (FileOutputStream. new-file)
              dos (DataOutputStream. fos)
              fd (.getFD fos)
              new (if-not (empty? rkey)
                    (apply update-in old rkey up-fn up-fn-args)
                    (apply up-fn old up-fn-args))]
          (if (instance? Throwable old)
            old ;; return read error
            (try
              (-serialize serializer dos write-handlers [key-vec new])
              (.flush dos)
              (when (:fsync config)
                (.sync fd))
              (.close dos)
              (Files/move (.toPath new-file) (.toPath f)
                          (into-array [StandardCopyOption/ATOMIC_MOVE]))
              (when (:fsync config)
                (sync-folder folder))
              [(get-in old rkey)
               (get-in new rkey)]
              (catch Exception e
                (.delete new-file)
                  ;; TODO maybe need fsync new-file here?
                (ex-info "Could not write key."
                         {:type :write-error
                          :key fkey
                          :exception e}))
              (finally
                (.close dos))))))))

  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))

  (-dissoc [this key]
    (async/thread
      (let [fn (uuid key)
            f (io/file (str folder "/" fn))]
        (.delete f)
        (when (:fsync config)
          (sync-folder folder))
        nil)))

  PBinaryAsyncKeyValueStoreV1
  (-bget [this key locked-cb]
    (let [fn (str "B_" (uuid key))
          f (io/file (str folder "/" fn))
          res-ch (chan)]
      (if-not (.exists f)
        (close! res-ch)
        (try
          (let [ac (AsynchronousFileChannel/open (.getPath (FileSystems/getDefault)
                                                           (str folder "/" fn)
                                                           (into-array String []))
                                                 (into-array StandardOpenOption
                                                             [StandardOpenOption/READ]))
                bb (ByteBuffer/allocate (.size ac))]
            (.read ac
                   bb
                   0
                   nil
                   (proxy [CompletionHandler] []
                     (completed [res att]
                       (let [bais (ByteArrayInputStream. (.array bb))]
                         (try
                           (locked-cb {:input-stream bais
                                       :size (.length f)
                                       :file f})
                           (catch Exception e
                             (ex-info "Could not read key."
                                      {:type :read-error
                                       :key key
                                       :exception e}))
                           (finally
                             (close! res-ch)
                             (.close ac)))))
                     (failed [t att]
                       (put! res-ch (ex-info "Could not read key."
                                             {:type :read-error
                                              :key key
                                              :exception t}))
                       (close! res-ch)
                       (.close ac)))))
          (catch Exception e
            (put! res-ch (ex-info "Could not read key."
                                  {:type :read-error
                                   :key key
                                   :exception e})))))
      res-ch))

  (-bassoc [this key input]
    (let [fn (uuid key)
          f (io/file (str folder "/B_" fn))
          new-file (io/file (str folder "/B_" fn ".new"))]
      (async/thread
        (let [fos (FileOutputStream. new-file)
              dos (DataOutputStream. fos)
              fd (.getFD fos)]
          (try
            (io/copy input dos)
            (.flush dos)
            (when (:fsync config)
              (.sync fd))
            (.close fos) ;; required for windows
            (Files/move (.toPath new-file) (.toPath f)
                        (into-array [StandardCopyOption/ATOMIC_MOVE]))
            (when (:fsync config)
              (sync-folder folder))
            (catch Exception e
              (.delete new-file)
              (ex-info "Could not write key."
                       {:type :write-error
                        :key key
                        :exception e}))
            (finally
              (.close fos))))))))

(defmethod print-method FileSystemStoreV1
  [^FileSystemStoreV1 store ^Writer writer]
  (.write writer (str "FileStore[\"" (:folder store) ", " (.hasheq store) "\"]")))

(defn new-fs-store-v1
  [path & {:keys [serializer read-handlers write-handlers config]
           :or {serializer (ser/fressian-serializer)
                read-handlers (atom {})
                write-handlers (atom {})
                config {:fsync true}}}]
  (let [f (io/file path)
        locks (atom {})
        test-file (io/file (str path "/" (UUID/randomUUID)))]
    (when-not (.exists f)
      (.mkdir f))
      ;; simple test to ensure we can write to the folder
    (when-not (.createNewFile test-file)
      (throw (ex-info "Cannot write to folder." {:type :not-writable
                                                 :folder path})))
    (.delete test-file)
    (go
      (map->FileSystemStoreV1 {:folder path
                               :serializer serializer
                               :read-handlers read-handlers
                               :write-handlers write-handlers
                               :locks locks
                               :config config}))))

                                        ;Filestore V2 konserve 0.4 < Konserve 0.6

(defn list-keys-v2
  "Lists all keys in this binary store. This operation *does not block concurrent operations* and might return an outdated key set. Keys of binary blobs are not tracked atm."
  [{:keys [folder serializer read-handlers] :as store}]
  (let [fns (->> (io/file (str folder "/meta"))
                 .list
                 seq
                 (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                                      %))
                 (map (fn [fn]
                        (go-locked
                         store fn
                         (let [f  (io/file (str folder "/meta/" fn))
                               fd (io/file (str folder "/data/" fn))]
                           (if (and (.exists f) (.exists fd))
                             (async/<!
                              (async/thread
                                (let [fis (DataInputStream. (FileInputStream. ^File f))]
                                  (try
                                    (-deserialize serializer read-handlers fis)
                                    (catch Exception e
                                      (ex-info "Could not read key."
                                               {:type      :read-error
                                                :key       fn
                                                :exception e}))
                                    (finally
                                      (.close fis))))))
                             (debug "Stale key file detected: " fn))))))
                 async/merge
                 (async/into #{}))]
    fns))

(defn delete-store-v2
  "Permanently deletes the folder of the store with all files."
  [folder]
  (let [f (io/file folder)
        parent-folder (.getParent f)]
    (doseq [path [(str folder "/meta") (str folder "/data") folder]]
      (doseq [c (.list (io/file path))]
        (.delete (io/file (str path "/" c)))))
    (.delete f)
    (try
      (sync-folder parent-folder)
      (catch Exception e
        nil))))

(defn- read-key
  "help function for -update-in"
  [folder ^File f fkey serializer read-handlers]
  (when (.exists f)
    (let [fis (DataInputStream. (FileInputStream. f))]
      (try
        (-deserialize serializer read-handlers fis)
        (catch Exception e
          (ex-info "Could not read key."
                   {:type      :read-error
                    :key       fkey
                    :exception e}))
        (finally
          (.close fis))))))

(defn- write-edn
  "help function for -update-in"
  [serializer write-handlers read-handlers folder key-vec fn up-fn up-fn-args config]
  (let [[fkey & rkey] key-vec
        f             (io/file (str folder fn))
        old           (read-key folder f fkey serializer read-handlers)
        new-file      (io/file (str folder fn ".new"))
        fos           (FileOutputStream. new-file)
        dos           (DataOutputStream. fos)
        fd            (.getFD fos)
        value           (if-not (empty? rkey)
                          (apply update-in old rkey up-fn up-fn-args)
                          (apply up-fn old up-fn-args))]
    (if (instance? Throwable old)
      old ;; return read error
      (try
        (-serialize serializer dos write-handlers value)
        (.flush dos)
        (when (:fsync config)
          (.sync fd))
        (.close dos)
        (Files/move (.toPath new-file) (.toPath f)
                    (into-array [StandardCopyOption/ATOMIC_MOVE]))
        (when (:fsync config)
          (sync-folder folder))
        [(get-in old rkey)
         (get-in value rkey)]
        (catch Exception e
          (.delete new-file)
          ;; TODO maybe need fsync new-file here?
          (throw
           (ex-info "Could not write key."
                    {:type      :write-error
                     :key       fkey
                     :exception e})))
        (finally
          (.close dos))))))

(defn- write-edn-key
  "help function for -update-in"
  [serializer write-handlers read-handlers folder fn {:keys [key] :as key-meta} config]
  (let [f             (io/file (str folder fn))
        new-file      (io/file (str folder fn ".new"))
        fos           (FileOutputStream. new-file)
        dos           (DataOutputStream. fos)
        fd            (.getFD fos)]
    (try
      (-serialize serializer dos write-handlers key-meta)
      (.flush dos)
      (when (:fsync config)
        (.sync fd))
      (.close dos)
      (Files/move (.toPath new-file) (.toPath f)
                  (into-array [StandardCopyOption/ATOMIC_MOVE]))
      (when (:fsync config)
        (sync-folder folder))
      (catch Exception e
        (.delete new-file)
        ;; TODO maybe need fsync new-file here?
        (throw
         (ex-info "Could not write key file."
                  {:type      :write-key-error
                   :key       key
                   :exception e})))
      (finally
        (.close dos)))))

(defn- read-edn
  "Helper Function for -get-in"
  [^File f res-ch folder fn fkey rkey serializer read-handlers]
  (if-not (.exists f)
    (close! res-ch)
    (try
      (let [ac (AsynchronousFileChannel/open (.getPath (FileSystems/getDefault)
                                                       (str folder "/data/" fn)
                                                       (into-array String []))
                                             (into-array StandardOpenOption
                                                         [StandardOpenOption/READ]))
            bb (ByteBuffer/allocate (.size ac))]
        (.read ac
               bb
               0
               nil
               (proxy [CompletionHandler] []
                 (completed [res att]
                   (let [bais (ByteArrayInputStream. (.array bb))]
                     (try
                       (let [value (-deserialize serializer read-handlers bais)]
                         (if-let [value (if (not-empty rkey) (get-in value rkey) value)]
                           (put! res-ch value)
                           (close! res-ch)))
                       (catch Exception e
                         (ex-info "Could not read key."
                                  {:type      :read-error
                                   :key       fkey
                                   :exception e}))
                       (finally
                         (.close ac)))))
                 (failed [t att]
                   (put! res-ch (ex-info "Could not read key."
                                         {:type      :read-error
                                          :key       fkey
                                          :exception t}))
                   (.close ac)))))
      (catch Exception e
        (put! res-ch (ex-info "Could not read key."
                              {:type      :read-error
                               :key       fkey
                               :exception e}))))))

(defn- delete-entry
  "Delete Filestore Entry"
  [fn folder config]
  (let [file (io/file (str folder "/" fn))]
    (.delete file)
    (when (:fsync config)
      (sync-folder folder))
    nil))

(defn- read-binary
  "Helper function for -bget"
  [^File f res-ch folder fn key locked-cb]
  (if-not (.exists f)
    (close! res-ch)
    (try
      (let [ac (AsynchronousFileChannel/open (.getPath (FileSystems/getDefault)
                                                       (str folder "/data/" fn)
                                                       (into-array String []))
                                             (into-array StandardOpenOption
                                                         [StandardOpenOption/READ]))
            bb (ByteBuffer/allocate (.size ac))]
        (.read ac
               bb
               0
               nil
               (proxy [CompletionHandler] []
                 (completed [res att]
                   (let [bais (ByteArrayInputStream. (.array bb))]
                     (try
                       (go
                         (>! res-ch (<! (locked-cb {:input-stream bais
                                                    :size         (.length f)
                                                    :file         f}))))
                       (catch Exception e
                         (ex-info "Could not read key."
                                  {:type      :read-error
                                   :key       key
                                   :exception e}))
                       (finally
                         (close! res-ch)
                         (.close ac)))))
                 (failed [t att]
                   (put! res-ch (ex-info "Could not read key."
                                         {:type      :read-error
                                          :key       key
                                          :exception t}))
                   (close! res-ch)
                   (.close ac)))))
      (catch Exception e
        (put! res-ch (ex-info "Could not read key."
                              {:type      :read-error
                               :key       key
                               :exception e}))))))

(defn- read-binary-old [^File f folder fn key locked-cb]
  (let [res-ch-old (chan)]
    (try
      (let [ac (AsynchronousFileChannel/open (.getPath (FileSystems/getDefault)
                                                       (str folder "/B_" fn)
                                                       (into-array String []))
                                             (into-array StandardOpenOption
                                                         [StandardOpenOption/READ]))
            bb (ByteBuffer/allocate (.size ac))]
        (.read ac
               bb
               0
               nil
               (proxy [CompletionHandler] []
                 (completed [res att]
                   (let [bais (ByteArrayInputStream. (.array bb))]
                     (try
                       (locked-cb {:input-stream bais
                                   :size (.length f)
                                   :file f})
                       (catch Exception e
                         (ex-info "Could not read key."
                                  {:type :read-error
                                   :key key
                                   :exception e}))
                       (finally
                         (close! res-ch-old)
                         (.close ac)))))
                 (failed [t att]
                   (put! res-ch-old (ex-info "Could not read key."
                                             {:type :read-error
                                              :key key
                                              :exception t}))
                   (close! res-ch-old)
                   (.close ac)))))
      (catch Exception e
        (put! res-ch-old (ex-info "Could not read key."
                                  {:type :read-error
                                   :key key
                                   :exception e}))))))

(defn- write-binary
  "Helper Function for Binary Write"
  [folder fn key input config]
  (let [f        (io/file (str folder "/data/" fn))
        new-file (io/file (str folder "/data/" fn ".new"))
        fos      (FileOutputStream. new-file)
        dos      (DataOutputStream. fos)
        fd       (.getFD fos)]
    (try
      (io/copy input dos)
      (.flush dos)
      (when (:fsync config)
        (.sync fd))
      (.close fos) ;; required for windows
      (Files/move (.toPath new-file) (.toPath f)
                  (into-array [StandardCopyOption/ATOMIC_MOVE]))
      (when (:fsync config)
        (sync-folder folder))
      (catch Exception e
        (.delete new-file)
        (ex-info "Could not write key."
                 {:type      :write-error
                  :key       key
                  :exception e}))
      (finally
        (.close fos)))))

(defrecord FileSystemStoreV2 [folder serializer read-handlers write-handlers locks config
                              stale-binaries?]
  PEDNAsyncKeyValueStoreV1
  (-exists? [this key]
    (let [fn  (uuid key)
          f   (io/file (str folder "/data/" fn))
          res (chan)]
      (put! res (.exists f))
      (close! res)
      res))
  ;; non-blocking async version
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          fn            (uuid fkey)
          f             (io/file (str folder "/data/" fn))
          res-ch        (chan)]
      (read-edn f res-ch folder fn fkey rkey serializer read-handlers)
      res-ch))
  (-update-in [this key-vec up-fn] (-update-in this key-vec up-fn []))
  (-update-in [this key-vec up-fn args]
    (async/thread
      (try
        (let [file-name   (uuid (first key-vec))
              data-folder (str folder "/data/")
              key-folder  (str folder "/meta/")]
          (write-edn-key serializer write-handlers read-handlers key-folder file-name {:key (first key-vec) :format :edn} config)
          (write-edn serializer write-handlers read-handlers data-folder key-vec file-name up-fn args config))
        (catch Exception e
          e))))

  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))

  (-dissoc [this key]
    (async/thread
      (let [fn          (uuid key)
            key-folder  (str folder "/meta")
            data-folder (str folder "/data")]
        (delete-entry fn key-folder config)
        (delete-entry fn data-folder config))))

  PBinaryAsyncKeyValueStoreV1
  (-bget [this key locked-cb]
    (let [fn (str (uuid key))
          f         (io/file (str folder "/data/" fn))
          res-ch    (chan)]
      ;; migrate old schema
      (if (and stale-binaries? (.exists (io/file (str folder "/B_" fn))))
        (async/thread
          (do
            (read-binary-old (io/file (str folder "/B_" fn)) folder fn key #(write-binary folder fn key (:input-stream %) config))
            (write-edn-key serializer write-handlers read-handlers (str folder "/meta/") fn {:key key :format :binary} config)
            (delete-entry (str "/B_" fn) folder config)
            (read-binary f res-ch folder fn key locked-cb)))
        (read-binary f res-ch folder fn key locked-cb))
      res-ch))

  (-bassoc [this key input]
    (let [file-name  (uuid key)
          key-folder (str folder "/meta/")]
      (async/thread
        (do (write-edn-key serializer write-handlers read-handlers key-folder file-name {:key key :format :binary} config)
            (write-binary folder (str file-name) key input config)))))

  PKeyIterableV1
  (-keys [this]
    (let [ch (async/chan)]
      (async/take!
       (list-keys this)
       (fn [ks]
         (async/onto-chan ch (map :key ks))))
      ch)))

(defmethod print-method FileSystemStoreV2
  [^FileSystemStoreV2 store ^Writer writer]
  (.write writer (str "FileStore[\"" (:folder store) ", " (.hasheq store) "\"]")))

(defn- check-and-create-folder [path]
  (let [f         (io/file path)
        test-file (io/file (str path "/" (UUID/randomUUID)))]
    (when-not (.exists f)
      (.mkdir f))
    ;; simple test to ensure we can write to the folders
    (when-not (.createNewFile test-file)
      (throw (ex-info "Cannot write to folder." {:type   :not-writable
                                                 :folder path})))
    (.delete test-file)))

(defn filestore-schema-update
  "Lists all keys in this binary store. This operation *does not block concurrent operations* and might return an outdated key set. Keys of binary blobs are not tracked atm."
  [{:keys [folder serializer read-handlers] :as store}]
  (let [fns (->> (io/file folder)
                 .list
                 seq
                 (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                                      %))
                 (map (fn [fn]
                        (go-locked
                         store fn
                         (let [^File f (io/file (str folder "/" fn))]
                           (when (.exists f)
                             (let [fis (DataInputStream. (FileInputStream. f))]
                               (try
                                 (let [[key value] (-deserialize serializer read-handlers fis)]
                                   (<! (-assoc-in store key value))
                                   (.delete f))
                                 (catch Exception e
                                   (ex-info "Could not read key."
                                            {:type :read-error
                                             :key fn
                                             :exception e}))
                                 (finally
                                   (.close ^DataInputStream fis)))))))))
                 async/merge
                 (async/into #{}))]
    fns))

(defn new-fs-store
  "Filestore contains a Key and a Data Folder"
  [path  & {:keys [serializer read-handlers write-handlers config]
            :or   {serializer     (ser/fressian-serializer)
                   read-handlers  (atom {})
                   write-handlers (atom {})
                   config         {:fsync true}}}]
  (let [_               (check-and-create-folder path)
        key-path        (str path "/meta")
        data-path       (str path "/data")
        _               (check-and-create-folder key-path)
        _               (check-and-create-folder data-path)
        stale-binaries? (->> (io/file path)
                             .list
                             seq
                             (filter #(re-matches
                                       #"B\\_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                                       %))
                             empty?)
        stale-edn?      (->> (io/file path)
                             .list
                             seq
                             (filter #(re-matches
                                       #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                                       %))
                             empty?)
        store           (map->FileSystemStoreV2 {:folder          path
                                                 :serializer      serializer
                                                 :read-handlers   read-handlers
                                                 :write-handlers  write-handlers
                                                 :stale-binaries? stale-binaries?
                                                 :locks           (atom {})
                                                 :config          config})]
    (if-not stale-edn?
      (do
        (go
          (<! (filestore-schema-update store))
          store))
      (go store))))

