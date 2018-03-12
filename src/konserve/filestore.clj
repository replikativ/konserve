(ns konserve.filestore
  "Bare file-system implementation."
  (:require [konserve.serializers :as ser]
            [konserve.core :refer [go-locked]]
            [clojure.java.io :as io]
            [hasch.core :refer [uuid]]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop close! put!]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get-in -update-in -dissoc -assoc-in
                                        PBinaryAsyncKeyValueStore -bget -bassoc
                                        -serialize -deserialize]])
  (:import [java.io
            DataInputStream DataOutputStream
            FileInputStream FileOutputStream
            ByteArrayOutputStream ByteArrayInputStream]
           [java.nio.channels FileChannel AsynchronousFileChannel CompletionHandler]
           [java.nio ByteBuffer]
           [java.nio.file Files StandardCopyOption FileSystems Path OpenOption
            StandardOpenOption]))

;; A useful overview over fsync on Linux:
;; https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai
(defn- on-windows? []
  (>= (.indexOf (.toLowerCase (System/getProperty "os.name")) "win") 0))

(defn- sync-folder [folder]
  (when-not (on-windows?) ;; not necessary (and possible) on windows
    (let [p (.getPath (FileSystems/getDefault) folder (into-array String []))
          fc (FileChannel/open p (into-array OpenOption []))]
      (.force fc true)
      (.close fc))))

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

(defn list-keys
  "Lists all keys in this binary store. This operation *does not block concurrent operations* and might return an outdated key set. Keys of binary blobs are not tracked atm."
  [{:keys [folder serializer read-handlers ] :as store}]
  (let [fns (->> (io/file (str folder "/Key"))
                 .list
                 seq
                 (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                                      %))
                 (map (fn [fn]
                        (go-locked
                         store fn
                         (let [f  (io/file (str folder "/Key/" fn))
                               fd (io/file (str folder "/Data/" fn))]
                           (if (and (.exists f) (.exists fd))
                             (let [fis (DataInputStream. (FileInputStream. f))]
                               (try
                                 (-deserialize serializer read-handlers fis)
                                 (catch Exception e
                                   (ex-info "Could not read key."
                                            {:type      :read-error
                                             :key       fn
                                             :exception e}))
                                 (finally
                                   (.close fis))))
                             (println "Stale key file detected: " fn))))))
                 async/merge
                 (async/into #{}))]
    fns))



;; Helper Function for FileSystemStore
(defn- read-key
  "help function for -update-in"
  [folder f fkey serializer read-handlers]
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
  [serializer write-handlers read-handlers folder key-vec fn up-fn config]
  (let [[fkey & rkey] key-vec
        f             (io/file (str folder fn))
        old           (read-key folder f fkey serializer read-handlers)
        new-file      (io/file (str folder fn ".new"))
        fos           (FileOutputStream. new-file)
        dos           (DataOutputStream. fos)
        fd            (.getFD fos)
        value           (if-not (empty? rkey)
                        (update-in old rkey up-fn)
                        (up-fn old))]
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

(defn- read-memory-entry
  "Helper Function for -get-in"
  [f res-ch folder fn fkey rkey serializer read-handlers]
  (if-not (.exists f)
    (close! res-ch)
    (try
      (let [ac (AsynchronousFileChannel/open (.getPath (FileSystems/getDefault)
                                                       (str folder "/Data/" fn)
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
                             (-deserialize serializer read-handlers bais))
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

(defn- binary-read [f res-ch folder fn key locked-cb]
  (if-not (.exists f)
    (close! res-ch)
    (try
      (let [ac (AsynchronousFileChannel/open (.getPath (FileSystems/getDefault)
                                                       (str folder "/Data/" fn)
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
                                   :size         (.length f)
                                   :file         f})
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

(defn- write-binary
  "Helper Function for Binary Write"
  [folder fn key input config]
  (let [f        (io/file (str folder "/Data/" fn))
        new-file (io/file (str folder "/Data/" fn ".new"))
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

(defrecord FileSystemStore [folder serializer read-handlers write-handlers locks config]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [fn  (uuid key)
          f   (io/file (str folder "/Data/" fn))
          res (chan)]
      (put! res (.exists f))
      (close! res)
      res))
  ;; non-blocking async version
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          fn            (uuid fkey)
          f             (io/file (str folder "/Data/" fn))
          res-ch        (chan)]
      (read-memory-entry f res-ch folder fn fkey rkey serializer read-handlers)
      res-ch))
  (-update-in [this key-vec up-fn]
    (async/thread
      (try
        (let [file-name   (uuid (first key-vec))
              data-folder (str folder "/Data/")
              key-folder  (str folder "/Key/")]
          (write-edn-key serializer write-handlers read-handlers key-folder file-name {:key (first key-vec) :format :edn} config)
          (write-edn serializer write-handlers read-handlers data-folder key-vec file-name up-fn config))
        (catch Exception e
          e))))

  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))

  (-dissoc [this key]
    (async/thread
      (let [fn          (uuid key)
            key-folder  (str folder "/Key")
            data-folder (str folder "/Data")]
          (delete-entry fn key-folder config)
          (delete-entry fn data-folder config))))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [fn     (str (uuid key))
          f      (io/file (str folder "/Data/" fn))
          res-ch (chan)]
      (binary-read f res-ch folder fn key locked-cb)
      res-ch))

  (-bassoc [this key input]
    (let [file-name    (uuid key)
          key-folder   (str folder "/Key/")]
      (async/thread
        (do (write-edn-key serializer write-handlers read-handlers key-folder file-name {:key key :format :binary} config)
            (write-binary folder (str file-name) key input config))))))

(defmethod print-method FileSystemStore
  [store writer]
  (.write writer (str "FileStore[\"" (:folder store ) ", " (.hasheq store) "\"]")))

(defn- check-and-create-folder [path]
  (let [f         (io/file path)
        test-file (io/file (str path "/" (java.util.UUID/randomUUID)))]
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
                 ((fn [e] (prn "foo: " e) e))
                 (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                                      %))
                 (map (fn [fn]
                        (go-locked
                         store fn
                         (let [f (io/file (str folder "/" fn))]
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
                                   (.close fis)))))))))
                 async/merge
                 (async/into #{}))]
        fns))

(defn new-fs-store
  "Filestore contains a Key and a Data Folder"
  [path  & {:keys [serializer read-handlers write-handlers config automatic-schema-update]
            :or   {serializer     (ser/fressian-serializer)
                   read-handlers  (atom {})
                   write-handlers (atom {})
                   config         {:fsync true}
                   automatic-schema-update false}}]
  (println "auto: " automatic-schema-update)
  (let [_          (check-and-create-folder path)
        old-format? (not (empty? (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" %)
                                                      (seq (.list (io/file path))))))
        key-path   (str path "/Key")
        data-path  (str path "/Data")
        _          (check-and-create-folder key-path)
        _          (check-and-create-folder data-path)
        store (map->FileSystemStore {:folder         path
                                     :serializer     serializer
                                     :read-handlers  read-handlers
                                     :write-handlers write-handlers
                                     :locks          (atom {})})]
    (if (and old-format? (true? automatic-schema-update))
      (do
        (go
          (println "Updating to new store schema.")
          (<! (filestore-schema-update store))
          store))
      (go store))))

(comment

  (time (def store (<!! (new-fs-store "/tmp/old-CCCP" :automatic-schema-update true))))

  (<!! (-assoc-in store [":123"] 43))

  (<!! (-update-in store [":123"] inc))

  (<!! (-get-in store [["51"]]))

  (<!! (-dissoc store ":123"))

  (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" %)
                       (seq (.list (io/file "/tmp/old-CCCP"))))


  (seq (.list (io/file "/tmp/old-CCCP")))

(<!! (filestore-schema-update store))










 (time (doseq [i (apply vector (range 1 1000))]
                          (<!! (-assoc-in store [(str i)] i))))

 (<!! (list-keys store))

 (defn myfunc []
   2)

 (= (myfunc) 2)

 (= (inc) 2)

 (myfunc)

 (seq (.list (io/file "/tmp/CCCP/Key")))
 

  (defn file->bytes [file]
    (with-open [xin  (io/input-stream file)
                xout (java.io.ByteArrayOutputStream.)]
      (io/copy xin xout)
      (.toByteArray xout)))

  (<!! (-bassoc store :exe (file->bytes (io/file "/tmp/CCCP/Data/test.txt"))))

  (defn copy-fn [blob]
    (let [is   (:input-stream blob)
          baos (ByteArrayOutputStream.)
          _    (io/copy is baos)
          bais (ByteArrayInputStream. (.toByteArray baos))]
      bais))

  (with-open [in  (<!! (-bget store :exe copy-fn))
              out (io/output-stream (io/file "/tmp/CCCP/Data/testo.txt"))]
    (io/copy in out))

  (def foo (<!! (-bget store :test identity)))

  (<!! (-bget store :exe copy-fn))


  (<!! (list-keys store))


  )
 
