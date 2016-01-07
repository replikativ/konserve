(ns konserve.core
  (:refer-clojure :exclude [get-in update-in assoc-in exists?])
  (:require [konserve.protocols :refer [-exists? -get-in -update-in -bget -bassoc]]
            #?(:clj [clojure.core.async :refer [go]]))
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go go-loop]])))


(defn exists? [store key]
  "Checks whether value is in the store."
  (-exists? store key))

(defn get-in
  "Returns the value stored described by key-vec or nil if the path is
  not resolvable."
  [store key-vec]
  (-get-in store key-vec))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key-vec fn]
  (-update-in store key-vec fn))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  [store key-vec val]
  (update-in store key-vec (fn [_] val))
  (go nil))


(defn bget [store key locked-cb]
  "Calls locked-cb with a platform specific binary representation inside
  the lock, e.g. wrapped InputStream on the JVM and Blob in
  JavaScript. You need to properly close/dispose the object when you
  are done!"
  (-bget store key locked-cb))


(defn bassoc [store key val]
  "Copies given value (InputStream, Reader, File, byte[] or String on
  JVM, Blob in JavaScript) under key in the store."
  (-bassoc store key val))


(comment
  (require '[clojure.core.async :refer [<!! chan go] :as async])
  ;; cljs
  (go (def store (<! (new-indexeddb-store "konserve" #_(atom {'konserve.indexeddb.Test
                                                               map->Test})))))




  ;; clj
  (require '[konserve.filestore :refer [new-fs-store]]
           '[clojure.core.async :refer [<!! chan] :as async])

  (def store (<!! (new-fs-store "/tmp/store")))

  ;; investigate https://github.com/stuartsierra/parallel-async
  (let [res (chan (async/sliding-buffer 1))
        v (vec (range 5000))]
    (time (->>  (range 5000)
                (map #(assoc-in store [%] v))
                async/merge
                #_(async/pipeline-blocking 4 res identity)
                ))) ;; 38 secs
  (go (println "2000" (<! (get-in store [2000]))))

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
  (<!! (update-in store ["foo" :bar :foo] #(str % "foo")))
  (type (<!! (get-in store ["foo"])))

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
