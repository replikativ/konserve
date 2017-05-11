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
                                        -exists? -get-in -update-in -dissoc
                                        PBinaryAsyncKeyValueStore -bget -bassoc
                                        -serialize -deserialize]])
  (:import [java.io
            DataInputStream DataOutputStream
            FileInputStream FileOutputStream]))


;; A useful overview over fsync on Linux:
;; https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai


(defn delete-store
  "Permanently deletes the folder of the store with all files."
  [folder]
  (let [f (io/file folder)]
    (doseq [c (.list f)]
      (.delete (io/file (str folder "/" c))))
    (.delete f)))


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


(defrecord FileSystemStore [folder serializer read-handlers write-handlers locks config]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [fn (uuid key)
          f (io/file (str folder "/" fn))
          res (chan)]
      (put! res (.exists f))
      (close! res)
      res))


  (-get-in [this key-vec]
    (async/thread
      (let [[fkey & rkey] key-vec
            fn (uuid fkey)
            f (io/file (str folder "/" fn))]
        (when (.exists f)
          (let [fis (DataInputStream. (FileInputStream. f))]
            (try
              (get-in
               (second (-deserialize serializer read-handlers fis)) 
               rkey)
              (catch Exception e
                (ex-info "Could not read key."
                         {:type :read-error
                          :key fkey
                          :exception e}))
              (finally
                (.close fis))))))))

  (-update-in [this key-vec up-fn]
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
                    (update-in old rkey up-fn)
                    (up-fn old))]
          (if (instance? Throwable old)
            old ;; return read error
            (try
              (-serialize serializer dos write-handlers [key-vec new])
              (.flush dos)
              (when (:fsync config)
                (.sync fd))
              (.renameTo new-file f)
              (when (:fsync config)
                (.sync fd)
                (.sync (.getFD (io/file folder))))
              [(get-in old rkey)
               (get-in new rkey)]
              (catch Exception e
                (.delete new-file)
                (.sync fd)
                (ex-info "Could not write key."
                         {:type :write-error
                          :key fkey
                          :exception e}))
              (finally
                (.close fos))))))))

  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))

  (-dissoc [this key]
    (async/thread
      (let [fn (uuid key)
            f (io/file (str folder "/" fn))
            fos (FileOutputStream. f)
            fd (.getFD fos)]
        (.delete f)
        (when (:fsync config)
          (.sync fd))
        nil)))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [fn (uuid key)
          f (io/file (str folder "/B_" fn))]
      (if-not (.exists f)
        (go nil)
        (async/thread
          (let [fis (DataInputStream. (FileInputStream. f))]
            (try
              (locked-cb {:input-stream fis
                          :size (.length f)
                          :file f})
              (catch Exception e
                (ex-info "Could not read key."
                         {:type :read-error
                          :key key
                          :exception e}))
              (finally
                (.close fis))))))))

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
            (.renameTo new-file f)
            (when (:fsync config)
              (.sync fd)
              (.sync (.getFD (io/file folder))))
            (catch Exception e
              (.delete new-file)
              (.sync fd)
              (ex-info "Could not write key."
                       {:type :write-error
                        :key key
                        :exception e}))
            (finally
              (.close fos))))))))



(defn new-fs-store
  "Note that filename length is usually restricted as are pr-str'ed keys at the moment."
  [path & {:keys [serializer read-handlers write-handlers config]
           :or {serializer (ser/fressian-serializer)
                read-handlers (atom {})
                write-handlers (atom {})
                config {:fsync true}}}]
  (let [f (io/file path)
        locks (atom {})
        test-file (io/file (str path "/" (java.util.UUID/randomUUID)))]
    (when-not (.exists f)
      (.mkdir f))
    ;; simple test to ensure we can write to the folder
    (when-not (.createNewFile test-file)
      (throw (ex-info "Cannot write to folder." {:type :not-writable
                                                 :folder path})))
    (.delete test-file)
    (go
      (map->FileSystemStore {:folder path
                             :serializer serializer
                             :read-handlers read-handlers
                             :write-handlers write-handlers
                             :locks locks
                             :config config}))))




(comment
  (def store (<!! (new-fs-store "/tmp/store")))

  ;; investigate https://github.com/stuartsierra/parallel-async
  (let [res (chan (async/sliding-buffer 1))
        v (vec (range 5000))]
    (time (->>  (range 5000)
                (map #(-update-in store [%] (fn [_] v)))
                async/merge
                (async/pipeline-blocking 4 res identity)
                <!!))) ;; 38 secs
  (<!! (-get-in store [2000]))

  (let [res (chan (async/sliding-buffer 1))
        ba (byte-array (* 10 1024) (byte 42))]
    (time (->>  (range 10000)
                (map #(-bassoc store % ba))
                async/merge
                (async/pipeline-blocking 4 res identity)
                #_(async/into [])
                <!!))) ;; 19 secs


  (let [v (vec (range 5000))]
    (time (doseq [i (range 10000)]
            (<!! (-assoc-in store [i] i))))) ;; 19 secs

  (time (doseq [i (range 10000)]
          (<!! (-get-in store [i])))) ;; 2706 msecs

  (<!! (-get-in store [11]))

  (<!! (-assoc-in store ["foo"] nil))
  (<!! (-assoc-in store ["foo"] {:bar {:foo "baz"}}))
  (<!! (-assoc-in store ["foo"] (into {} (map vec (partition 2 (range 1000))))))
  (<!! (-update-in store ["foo" :bar :foo] #(str % "foo")))
  (type (<!! (-get-in store ["foo"])))

  (<!! (-assoc-in store ["baz"] #{1 2 3}))
  (<!! (-assoc-in store ["baz"] (java.util.HashSet. #{1 2 3})))
  (type (<!! (-get-in store ["baz"])))

  (<!! (-assoc-in store ["bar"] (range 10)))
  (.read (<!! (-bget store "bar" :input-stream)))
  (<!! (-update-in store ["bar"] #(conj % 42)))
  (type (<!! (-get-in store ["bar"])))

  (<!! (-assoc-in store ["boz"] [(vec (range 10))]))
  (<!! (-get-in store ["boz"]))



  (<!! (-assoc-in store [:bar] 42))
  (<!! (-update-in store [:bar] inc))
  (<!! (-get-in store [:bar]))

  (import [java.io ByteArrayInputStream ByteArrayOutputStream])
  (let [ba (byte-array (* 10 1024 1024) (byte 42))
        is (io/input-stream ba)]
    (time (<!! (-bassoc store "banana" is))))
  (def foo (<!! (-bget store "banana" identity)))
  (let [ba (ByteArrayOutputStream.)]
    (io/copy (io/input-stream (:input-stream foo)) ba)
    (alength (.toByteArray ba)))

  (<!! (-assoc-in store ["monkey" :bar] (int-array (* 10 1024 1024) (int 42))))
  (<!! (-get-in store ["monkey"]))

  (<!! (-assoc-in store [:bar/foo] 42))

  (defrecord Test [a])
  (<!! (-assoc-in store [42] (Test. 5)))
  (<!! (-get-in store [42]))



  (assoc-in nil [] {:bar "baz"})





  (defrecord Test [t])

  (require '[clojure.java.io :as io])

  (def fsstore (io/file "resources/fsstore-test"))

  (.mkdir fsstore)

  (require '[clojure.reflect :refer [reflect]])
  (require '[clojure.pprint :refer [pprint]])
  (require '[clojure.edn :as edn])

  (import '[java.nio.channels FileLock]
          '[java.nio ByteBuffer]
          '[java.io RandomAccessFile PushbackReader])

  (pprint (reflect fsstore))


  (defn locked-access [f trans-fn]
    (let [raf (RandomAccessFile. f "rw")
          fc (.getChannel raf)
          l (.lock fc)
          res (trans-fn fc)]
      (.release l)
      res))


  ;; doesn't really lock on quick linux check with outside access
  (locked-access (io/file "/tmp/lock2")
                 (fn [fc]
                   (let [ba (byte-array 1024)
                         bf (ByteBuffer/wrap ba)]
                     (Thread/sleep (* 60 1000))
                     (.read fc bf)
                     (String. (java.util.Arrays/copyOf ba (.position bf))))))


  (.createNewFile (io/file "/tmp/lock2"))
  (.renameTo (io/file "/tmp/lock2") (io/file "/tmp/lock-test"))


  (.start (Thread. (fn []
                     (locking "foo"
                       (println "locking foo and sleeping...")
                       (Thread/sleep (* 60 1000))))))

  (locking "foo"
    (println "another lock on foo"))

  (time (doseq [i (range 10000)]
          (spit (str "/tmp/store/" i) (pr-str (range i)))))
  )
