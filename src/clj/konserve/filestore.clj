(ns konserve.filestore
  "Experimental bare file-system implementation."
  (:use konserve.literals)
  (:require [konserve.platform :refer [read-string-safe]]
            [clojure.java.io :as io]
            [clojure.data.fressian :as fress]
            #_[taoensso.nippy :as nippy]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop close! put!]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [com.ashafa.clutch :refer [couch create!] :as cl]
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
                   #_(nippy/thaw-from-in! fis)
                   (fress/read fis
                               :handlers (-> (merge @read-handlers
                                                    fress/clojure-read-handlers)
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
                                #_(nippy/thaw-from-in! fis)
                                (fress/read fis
                                            :handlers (-> (merge @read-handlers
                                                                 fress/clojure-read-handlers)
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
                w (fress/create-writer dos :handlers (-> (merge @write-handlers
                                                                fress/clojure-write-handlers)
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
                    (.delete f))
                  (do
                    #_(nippy/freeze-to-out! dos new)
                    (fress/write-object w new)
                    (.sync fd)
                    (.renameTo new-file f)
                    (.sync fd)))
                (if (> (count @cache) 1000) ;; TODO make tunable
                  (reset! cache {})
                  (swap! cache assoc fkey new))
                nil
                (catch Exception e
                  (.delete new-file)
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
                              #_(nippy/thaw-from-in! fis)
                              (fress/read fis
                                          :handlers (-> (merge @read-handlers
                                                               fress/clojure-read-handlers)
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
                w (fress/create-writer dos :handlers (-> (merge @write-handlers
                                                                fress/clojure-write-handlers)
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
                    (.delete f))
                  (do
                    #_(nippy/freeze-to-out! dos new)
                    (fress/write-object w new)
                    (.sync fd)
                    (.renameTo new-file f)
                    (.sync fd)))
                (if (> (count @cache) 1000) ;; TODO dumb cache, make tunable
                  (reset! cache {})
                  (swap! cache assoc fkey new))
                [(get-in old rkey)
                 (get-in new rkey)]
                (catch Exception e
                  (.delete new-file)
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
              (.sync fd)
              (.renameTo new-file f)
              (.sync fd)
              (catch Exception e
                (.delete new-file)
                (ex-info "Could not write key."
                         {:type :write-error
                          :key key
                          :exception e}))
              (finally
                (.close fos)))))))))


(def map-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (let [^List kvs (.readObject reader)]
        (into {} kvs)))))

(def set-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (let [^List l (.readObject reader)]
        (into #{} l)))))

(def plist-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (let [len (.readInt reader)]
        (->> (range len)
             (map (fn [_] (.readObject reader)))
             reverse
             (into '()))))))

(def plist-writer
  (reify WriteHandler
    (write [_ writer plist]
      (.writeTag writer "plist" 2)
      (.writeInt writer (count plist))
      (doseq [e plist]
        (.writeObject writer e)))))

(def pvec-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (let [len (.readInt reader)]
        (->> (range len)
             (map (fn [_] (.readObject reader)))
             (into []))))))

(def pvec-writer
  (reify WriteHandler
    (write [_ writer pvec]
      (.writeTag writer "pvec" 2)
      (.writeInt writer (count pvec))
      (doseq [e pvec]
        (.writeObject writer e)))))



(defn new-fs-store
  "Note that filename length is usually restricted as are pr-str'ed keys at the moment."
  ([path] (new-fs-store path (atom {}) (atom {}) (atom #{})))
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
     (swap! read-handlers merge {  ;"map" map-reader ; not necessary, should be due to fressian docs??
                                 "set" set-reader
                                 ;; TODO list doesn't work, not in org.fressian.Handlers (?!)
                                 "plist" plist-reader
                                 "pvec" pvec-reader})
     (swap! write-handlers merge {clojure.lang.PersistentList {"plist" plist-writer}
                                  clojure.lang.PersistentList$EmptyList {"plist" plist-writer}
                                  clojure.lang.LazySeq {"plist" plist-writer}
                                  clojure.lang.PersistentVector {"pvec" pvec-writer}})
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
                <!!))) ;; 190 secs
  (<!! (-get-in store [4000]))

  (let [res (chan (async/sliding-buffer 1))
        ba (byte-array (* 10 1024) (byte 42))]
    (time (->>  (range 10000)
                (map #(-bassoc store % ba))
                async/merge
                (async/pipeline-blocking 4 res identity)
                #_(async/into [])
                <!!)))


  (let [v (vec (range 5000))]
    (time (doseq [i (range 10000)]
            (<!! (-assoc-in store [i] i))))) ;; 190 secs

  (time (doseq [i (range 10000)]
          (<!! (-get-in store [i])))) ;; 9 secs

  (<!! (-get-in store [10]))

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
