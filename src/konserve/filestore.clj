(ns konserve.filestore
  "Experimental bare file-system implementation."
  (:require [incognito.fressian :refer [incognito-read-handlers
                                        incognito-write-handlers]]
            [incognito.base :as ib]
            [clojure.java.io :as io]
            [clojure.data.fressian :as fress]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop close! put!]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [konserve.protocols :refer [IEDNAsyncKeyValueStore -exists? -get-in -assoc-in -update-in
                                        IBinaryAsyncKeyValueStore -bget -bassoc]])
  (:import [java.io FileOutputStream FileInputStream DataInputStream DataOutputStream]
           [org.fressian.handlers WriteHandler ReadHandler]))

;; TODO safe filename encoding
(defn dumb-encode [s]
  (when (re-find #"_DUMBSLASH42_" s)
    (throw (ex-info "Collision in encoding!"
                    {:type :dumbslash-found
                     :value s})))
  (str/replace s #"/" "_DUMBSLASH42_"))

(defn- get-lock [locks key]
  (or (get @locks key)
      (get (swap! locks conj key) key)))

;; TODO either remove cache or find proper way
(defrecord FileSystemStore [folder read-handlers write-handlers locks cache]
  IEDNAsyncKeyValueStore
  (-exists? [this key]
    (locking (get-lock locks key)
      (let [fn (dumb-encode (pr-str key))
            f (io/file (str folder "/" fn))
            res (chan)]
        (put! res (.exists f))
        (close! res)
        res)))

  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))]
      (if-let [c (get-in (@cache fkey) rkey)]
        (go c)
        (if-not (.exists f)
          (go nil)
          (async/thread
            (locking (get-lock locks fkey)
              (let [fis (DataInputStream. (FileInputStream. f))]
                (try
                  (get-in
                   (fress/read fis
                               :handlers (-> (merge fress/clojure-read-handlers
                                                    (incognito-read-handlers read-handlers))
                                             fress/associative-lookup))
                   rkey)
                  (catch Exception e
                    (ex-info "Could not read key."
                             {:type :read-error
                              :key fkey
                              :exception e}))
                  (finally
                    (.close fis))))))))))

  (-assoc-in [this key-vec value]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))
          new-file (io/file (str folder "/" fn ".new"))]
      (async/thread
        (locking (get-lock locks fkey)
          (let [old (or (@cache fkey)
                        (when (.exists f)
                          (locking (get-lock locks fkey)
                            (let [fis (DataInputStream. (FileInputStream. f))]
                              (try
                                (fress/read fis
                                            :handlers (-> (merge
                                                           fress/clojure-read-handlers
                                                           (incognito-read-handlers read-handlers))
                                                          fress/associative-lookup))
                                (catch Exception e
                                  (ex-info "Could not read key."
                                           {:type :read-error
                                            :key fkey
                                            :exception e}))
                                (finally
                                  (.close fis)))))))
                fos (FileOutputStream. new-file)
                dos (DataOutputStream. fos)
                fd (.getFD fos)
                w (fress/create-writer dos :handlers (-> (merge
                                                          fress/clojure-write-handlers
                                                          (incognito-write-handlers write-handlers))
                                                         fress/associative-lookup
                                                         fress/inheritance-lookup))
                new (if-not (empty? rkey)
                      (assoc-in old rkey value)
                      value)]
            (if (instance? Throwable old)
              old ;; propagate error
              (try
                (if (nil? new)
                  (do
                    (swap! cache dissoc fkey)
                    (.delete f)
                    (.sync fd))
                  (do
                    (fress/write-object w new)
                    (.flush dos)
                    (.sync fd)
                    (.renameTo new-file f)
                    (.sync fd)))
                #_(if (> (count @cache) 1000) ;; TODO make tunable
                  (reset! cache {})
                  (swap! cache assoc fkey (ib/incognito-writer @write-handlers new)))
                nil
                (catch Exception e
                  (.delete new-file)
                  (.sync fd)
                  (ex-info "Could not write key."
                           {:type :write-error
                            :key fkey
                            :exception e}))
                (finally
                  (.close fos)))))))))

  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))
          new-file (io/file (str folder "/" fn ".new"))]
      (async/thread
        (locking (get-lock locks fkey)
          (let [old (or (@cache fkey)
                        (when (.exists f)
                          (let [fis (DataInputStream. (FileInputStream. f))]
                            (try
                              (fress/read fis
                                          :handlers (-> (merge
                                                         fress/clojure-read-handlers
                                                         (incognito-read-handlers read-handlers))
                                                        fress/associative-lookup))
                              (catch Exception e
                                (ex-info "Could not read key."
                                         {:type :read-error
                                          :key fkey
                                          :exception e}))
                              (finally
                                (.close fis))))))
                fos (FileOutputStream. new-file)
                dos (DataOutputStream. fos)
                fd (.getFD fos)
                w (fress/create-writer dos :handlers (-> (merge
                                                          fress/clojure-write-handlers
                                                          (incognito-write-handlers write-handlers))
                                                         fress/associative-lookup
                                                         fress/inheritance-lookup))
                new (if-not (empty? rkey)
                      (update-in old rkey up-fn)
                      (up-fn old))]
            (if (instance? Throwable old)
              old ;; return read error
              (try
                (if (nil? new)
                  (do
                    (swap! cache dissoc fkey)
                    (.delete f)
                    (.sync fd))
                  (do
                    (fress/write-object w new)
                    (.flush dos)
                    (.sync fd)
                    (.renameTo new-file f)
                    (.sync fd)))
                (if (> (count @cache) 1000) ;; TODO dumb cache, make tunable
                  (reset! cache {})
                  #_(swap! cache assoc fkey new))
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
                  (.close fos)))))))))

  IBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [fn (dumb-encode (pr-str key))
          f (io/file (str folder "/" fn))]
      (if-not (.exists f)
        (go nil)
        (async/thread
          (locking (get-lock locks key)
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
                  (.close fis)))))))))

  (-bassoc [this key input]
    (let [fn (dumb-encode (pr-str key))
          f (io/file (str folder "/" fn))
          new-file (io/file (str folder "/" fn ".new"))]
      (async/thread
        (locking (get-lock locks key)
          (let [fos (FileOutputStream. new-file)
                dos (DataOutputStream. fos)
                fd (.getFD fos)]
            (try
              (io/copy input dos)
              (.flush dos)
              (.sync fd)
              (.renameTo new-file f)
              (.sync fd)
              (catch Exception e
                (.delete new-file)
                (.sync fd)
                (ex-info "Could not write key."
                         {:type :write-error
                          :key key
                          :exception e}))
              (finally
                (.close fos)))))))))



(defn new-fs-store
  "Note that filename length is usually restricted as are pr-str'ed keys at the moment."
  ([path] (new-fs-store path (atom {}) (atom {})))
  ([path read-handlers write-handlers]
   (new-fs-store path read-handlers write-handlers (atom #{})))
  ([path read-handlers write-handlers locks]
   (let [f (io/file path)
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
                              :read-handlers read-handlers
                              :write-handlers write-handlers
                              :locks locks
                              :cache (atom {})})))))


(comment
  (def store (<!! (new-fs-store "/tmp/store")))

  ;; investigate https://github.com/stuartsierra/parallel-async
  (let [res (chan (async/sliding-buffer 1))
        v (vec (range 5000))]
    (time (->>  (range 5000)
                (map #(-assoc-in store [%] v))
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
