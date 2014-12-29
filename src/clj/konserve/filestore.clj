(ns konserve.filestore
  "Experimental bare file-system implementation."
  (:use konserve.literals)
  (:require [konserve.platform :refer [log read-string-safe]]
            [clojure.java.io :as io]
            [clojure.data.fressian :as fress]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop close! put!]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [com.ashafa.clutch :refer [couch create!] :as cl]
            [konserve.protocols :refer [IEDNAsyncKeyValueStore -get-in -assoc-in -update-in
                                        IBinaryAsyncKeyValueStore -bget -bassoc]])
  (:import [java.io FileOutputStream FileInputStream]
           [org.fressian.handlers WriteHandler ReadHandler]))

;; TODO safe filename encoding
(defn dumb-encode [s]
  (when (re-find #"_DUMBSLASH42_" s)
    (throw (ex-info "Collision in encoding!"
                    {:type :dumbslash-found
                     :value s})))
  (str/replace s #"/" "_DUMBSLASH42_"))

(defrecord FileSystemStore [folder read-handlers write-handlers]
  IEDNAsyncKeyValueStore
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))
          res (chan)]
      (if-not (.exists f)
        (close! res)
        (locking fn
          (let [fis (FileInputStream. f)]
            (try
              (put! res (get-in (fress/read fis
                                            :handlers (-> (merge @read-handlers
                                                                 fress/clojure-read-handlers)
                                                          fress/associative-lookup))
                                rkey))
              (catch Exception e
                (log e)
                (throw e))
              (finally
                (.close fis)
                (close! res))))))
      res))

  (-assoc-in [this key-vec value]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))
          backup (io/file (str folder "/" fn ".backup"))
          res (chan)]
      (locking fn
        (let [old (when (.exists f)
                    (let [fis (FileInputStream. f)]
                      (try
                        (fress/read fis
                                    :handlers (-> (merge @read-handlers
                                                         fress/clojure-read-handlers)
                                                  fress/associative-lookup))
                        (catch Exception e
                          (log e)
                          (throw e))
                        (finally
                          (.close fis)))))
              _ (.renameTo f backup)
              fos (FileOutputStream. f)
              w (fress/create-writer fos :handlers (-> (merge @write-handlers
                                                              fress/clojure-write-handlers)
                                                       fress/associative-lookup
                                                       fress/inheritance-lookup))
              new (if-not (empty? rkey)
                    (assoc-in old rkey value)
                    value)]
          (try
            (fress/write-object w new)
            (catch Exception e
              (.renameTo backup f)
              (log e)
              (throw e))
            (finally
              (.close fos)
              (.delete backup)))
          (close! res)))
      res))

  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))
          backup (io/file (str folder "/" fn ".backup"))
          res (chan)]
      (locking fn
        (let [old (when (.exists f)
                    (let [fis (FileInputStream. f)]
                      (try
                        (fress/read fis
                                    :handlers (-> (merge @read-handlers
                                                         fress/clojure-read-handlers)
                                                  fress/associative-lookup))
                        (catch Exception e
                          (log e)
                          (throw e))
                        (finally
                          (.close fis)))))
              _ (.renameTo f backup)
              fos (FileOutputStream. f)
              w (fress/create-writer fos :handlers (-> (merge @write-handlers
                                                              fress/clojure-write-handlers)
                                                       fress/associative-lookup
                                                       fress/inheritance-lookup))
              new (if-not (empty? rkey)
                    (update-in old rkey up-fn)
                    (up-fn old))]
          (try
            (fress/write-object w new)
            (catch Exception e
              (.renameTo backup f)
              (log e)
              (throw e))
            (finally
              (.close fos)
              (.delete backup)))
          (put! res [(get-in old rkey)
                     (get-in new rkey)])
          (close! res)
          res))))

  IBinaryAsyncKeyValueStore
  (-bget [this key]
    (let [fn (dumb-encode (pr-str key))
          f (io/file (str folder "/" fn))
          res (chan)]
      (when (.exists f)
        (locking fn
          (let [fin (FileInputStream. f)]
            (try
              (put! res {:input-stream fin
                         :size (.length f)})
              (catch Exception e
                (log e)
                (throw e)
                (.close fin))
              (finally
                (close! res))))))
      res))

  (-bassoc [this key input]
    (let [fn (dumb-encode (pr-str key))
          f (io/file (str folder "/" fn))
          backup (io/file (str folder "/" fn ".backup"))
          res (chan)]
      (locking fn
        (.renameTo f backup)
        (let [fos (FileOutputStream. f)]
          (try
            (io/copy input fos)
            (catch Exception e
              (.renameTo backup f)
              (log e)
              (throw e))
            (finally
              (.close fos)
              (.delete backup)
              (close! res)))
          res)))))


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
  ([path] (new-fs-store path (atom {}) (atom {})))
  ([path read-handlers write-handlers]
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
                              :write-handlers write-handlers})))))


(comment
  (def store (<!! (new-fs-store "/tmp/store")))

  ;; investigate https://github.com/stuartsierra/parallel-async
  (time (->>  (range 10000)
              (map #(-assoc-in store [%] (vec (range %))))
              async/merge
              (async/into [])
              <!!)) ;; 190 secs

  (time (doseq [i (range 10000)]
          (<!! (-assoc-in store [i] (vec (range i)))))) ;; 190 secs

  (time (doseq [i (range 10000)]
          (<!! (-get-in store [i])))) ;; 9 secs

  (<!! (-assoc-in store ["foo"] {:bar {:foo "baz"}}))
  (<!! (-assoc-in store ["foo"] (into {} (map vec (partition 2 (range 1000))))))
  (<!! (-update-in store ["foo" :bar :foo] #(str % "foo")))
  (type (<!! (-get-in store ["foo"])))

  (<!! (-assoc-in store ["baz"] #{1 2 3}))
  (type (<!! (-get-in store ["baz"])))

  (<!! (-assoc-in store ["bar"] (range 10)))
  (clojure.pprint/pprint (clojure.reflect/reflect (:input-stream (<!! (-bget store "bar"))) ))
  (.read (:input-stream (<!! (-bget store "bar"))))
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
  (def foo (<!! (-bget store "banana")))

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
