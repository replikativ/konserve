(ns konserve.filestore
  "Experimental bare file-system implementation."
  (:use konserve.literals)
  (:require [konserve.platform :refer [log read-string-safe]]
            [clojure.java.io :as io]
            [clojure.data.fressian :as fress]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [com.ashafa.clutch :refer [couch create!] :as cl]
            [konserve.protocols :refer [IEDNAsyncKeyValueStore -get-in -assoc-in -update-in
                                        IBinaryAsyncKeyValueStore -bget -bassoc]])
  (:import [java.io FileOutputStream FileInputStream]))


;; TODO safe filename encoding
(defn dumb-encode [s]
  (when (re-find #"_DUMBSLASH42_" s)
    (throw (ex-info "Collision in encoding!"
                    {:type :dumbslash-found
                     :value s})))
  (str/replace s #"/" "_DUMBSLASH42_"))

(defrecord FileSystemStore [folder tag-table]
  IEDNAsyncKeyValueStore
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))]
      (go (when (.exists f)
            (locking fn
              (let [fis (FileInputStream. f)]
                (try
                  (get-in (fress/read fis) rkey)
                  (catch Exception e
                    (log e)
                    (throw e))
                  (finally
                    (.close fis)))))))))

  (-assoc-in [this key-vec value]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))]
      (go
        (locking fn
          (let [old (when (.exists f)
                      (let [fis (FileInputStream. f)]
                        (try
                          (fress/read fis)
                          (catch Exception e
                            (log e)
                            (throw e))
                          (finally
                            (.close fis)))))
                fos (FileOutputStream. f)
                w (fress/create-writer fos)
                new (if-not (empty? rkey)
                      (assoc-in old rkey value)
                      value)]
            (try
              (fress/write-object w new)
              (catch Exception e
                (log e)
                (throw e))
              (finally
                (.close fos)))
            nil)))))

  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          fn (dumb-encode (pr-str fkey))
          f (io/file (str folder "/" fn))]
      (go
        (locking fn
          (let [old (when (.exists f)
                      (let [fis (FileInputStream. f)]
                        (try
                          (fress/read fis)
                          (catch Exception e
                            (log e)
                            (throw e))
                          (finally
                            (.close fis)))))
                fos (FileOutputStream. f)
                w (fress/create-writer fos)
                new (if-not (empty? rkey)
                      (update-in old rkey up-fn)
                      (up-fn old))]
            (try
              (fress/write-object w new)
              (catch Exception e
                (log e)
                (throw e))
              (finally
                (.close fos)))
            nil)))))

  IBinaryAsyncKeyValueStore
  (-bget [this key]
    (let [fn (dumb-encode (pr-str key))
          f (io/file (str folder "/" fn))]
      (go (when (.exists f)
            (locking fn
              (let [fin (FileInputStream. f)
                    ba (byte-array (.length f))]
                (try
                  (.read fin ba)
                  ba
                  (catch Exception e
                    (log e)
                    (throw e))
                  (finally
                    (.close fin)))))))))

  (-bassoc [this key bytes]
    (let [fn (dumb-encode (pr-str key))
          f (io/file (str folder "/" fn))]
      (go
        (locking fn
          (let [fos (FileOutputStream. f)]
            (try
              (.write fos bytes)
              (catch Exception e
                (log e)
                (throw e))
              (finally
                (.close fos)))))))))


(defn new-fs-store
  [path tag-table]
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
      (map->FileSystemStore {:folder path :tag-table tag-table}))))

(comment
  (def store (<!! (new-fs-store "/tmp/store" (atom {}))))

  (<!! (-assoc-in store ["foo" :bar] {:foo "baz"}))
  (<!! (-get-in store ["foo"]))

  (<!! (-assoc-in store [:bar] 42))
  (<!! (-update-in store [:bar] inc))
  (<!! (-get-in store [:bar]))

  (let [ba (byte-array (* 10 1024 1024) (byte 42))]
    (time (<!! (-bassoc store "banana" ba))))
  (take 10 (map byte (<!! (-bget store "banana"))))

  (<!! (-assoc-in store ["monkey" :bar] (int-array (* 10 1024 1024) (int 42))))
  (<!! (-get-in store ["monkey"]))

  (<!! (-assoc-in store [:bar/foo] 42))

  (defrecord Test [a])
  (<!! (-assoc-in store [42] (Test. 5)))
  (<!! (-get-in store [42]))



  (assoc-in nil [] {:bar "baz"})



  (import [java.io ByteArrayInputStream ByteArrayOutputStream
           FileOutputStream FileInputStream])

  (defrecord Test [t])

  ;; Write data to a stream
  (def out (ByteArrayOutputStream. 4096))
  (def fout (FileOutputStream. "/tmp/fout-stream"))
  (def writer (tr/writer fout :msgpack))
  (def reader (tr/reader (FileInputStream. "/tmp/fout-stream")
                         :msgpack))

  (time (tr/write writer (byte-array (* 1024 1024 10) (byte 42))))
  (def read-in (tr/read reader))

  (take 10 read-in)

  (def fout2 (FileOutputStream. "/tmp/fout-stream2"))
  (.write (io/writer "/tmp/fout-stream2"))

  #_(transit/write writer (take 1000 (repeatedly (partial rand-int Integer/MAX_VALUE ))))

  #_(transit/write writer "foo")
  #_(transit/write writer {:a [1 2]})
  (transit/write writer (pr-str (->Test 5)))

  (println out)

  (.size out)

  (def in (ByteArrayInputStream. (.toByteArray out)))
  (def reader (transit/reader in :msgpack {:handlers {konserve.platform.Test map->Test}}))
  (println (type (transit/read reader)))


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

  )
