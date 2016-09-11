(ns konserve.carmine
  (:require [konserve.serializers :as ser]
            [konserve.core :refer [go-locked]]
            [clojure.java.io :as io]
            [hasch.core :refer [uuid]]

            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop close! put!]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore -exists? -get-in -update-in
                                        PBinaryAsyncKeyValueStore -bget -bassoc
                                        -serialize -deserialize]]
            [taoensso.carmine :as car :refer [wcar]])
  (:import [java.io
            ByteArrayInputStream ByteArrayOutputStream]))




;; TODO document how redis guarantees fsync in order of messages (never loses
;; intermediary writes) on a single peer

(defrecord CarmineStore [conn serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [fn (str (uuid key))
          res (chan)]
      (put! res (= (car/wcar conn (car/exists fn)) 1))
      (close! res)
      res))


  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          id (str (uuid fkey))]
      (if-not (= (car/wcar conn (car/exists id)) 1)
        (go nil)
        (let [res-ch (chan)]
          (try
            (let [bais (ByteArrayInputStream. (car/wcar conn (car/parse-raw (car/get id))))]
              (put! res-ch
                    (get-in
                     (second (-deserialize serializer read-handlers bais))
                     rkey)))
            res-ch
            (catch Exception e
              (put! res-ch (ex-info "Could not read key."
                                   {:type :read-error
                                    :key fkey
                                    :exception e}))
              res-ch)
            (finally
              (close! res-ch)))))))

  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          id (str (uuid fkey))]
      (let [res-ch (chan)]
        (try
          (let [old-bin (car/wcar conn (car/parse-raw (car/get id)))
                old (when old-bin
                      (let [bais (ByteArrayInputStream. (car/wcar conn (car/parse-raw (car/get id))))]
                        (second (-deserialize serializer write-handlers bais))))
                new (if (empty? rkey)
                      (up-fn old)
                      (update-in old rkey up-fn))]
            (when new
              (let [baos (ByteArrayOutputStream.)]
                (-serialize serializer baos write-handlers [key-vec new])
                (car/wcar conn (car/set id (car/raw (.toByteArray baos))))))
            (put! res-ch [(get-in old rkey)
                          (get-in new rkey)]))
          res-ch
          (catch Exception e
            (put! res-ch (ex-info "Could not write key."
                                  {:type :write-error
                                   :key fkey
                                   :exception e}))
            res-ch)
          (finally
            (close! res-ch))))))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [id (uuid key)]
      (if-not (= (car/wcar conn (car/exists id)) 1)
        (go nil)
        (go
          (try
            (let [bin (car/wcar conn (car/parse-raw (car/get id)))
                  bais (ByteArrayInputStream. bin)]
              (locked-cb {:input-stream bais
                          :size (count bin)}))
            (catch Exception e
              (ex-info "Could not read key."
                       {:type :read-error
                        :key key
                        :exception e})))))))

  (-bassoc [this key input]
    (let [id (uuid key)]
      (go
        (try
          (car/wcar conn (car/set id (car/raw input)))
          nil
          (catch Exception e
            (ex-info "Could not write key."
                     {:type :write-error
                      :key key
                      :exception e})))))))



(defn new-carmine-store
  ([]
   (new-carmine-store {:pool {} :spec {}}))
  ([carmine-conn & {:keys [serializer read-handlers write-handlers]
                    :or {serializer (ser/fressian-serializer)
                         read-handlers (atom {})
                         write-handlers (atom {})}}]
   (go (map->CarmineStore {:conn carmine-conn
                           :read-handlers read-handlers
                           :write-handlers write-handlers
                           :serializer serializer
                           :locks (atom {})}))))

(comment
  (def store (<!! (new-carmine-store)))


  (let [numbers (doall (range 1024))]
    (time
     (doseq [i (range 1000)]
       (<!! (-update-in store [i] (fn [_] numbers))))))

  (<!! (-get-in store [100]))

  (drop 2 (byte-array [1 2 3]))


  (<!! (-exists? store "bars"))

  (<!! (-update-in store ["bars"] (fn [_] 1)))

  (<!! (-update-in store ["bars"] inc))

  (<!! (-get-in store ["bars"]))


  (<!! (-bassoc store "bbar" (byte-array (range 5))))

  (<!! (-bget store "bbar" (fn [{:keys [input-stream]}]
                             (map byte (slurp input-stream)))))

                                        ; See `wcar` docstring for opts
  (def conn {:pool {} :spec {}})
  (defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

  (map byte (car/wcar conn (car/parse-raw (car/get "foo"))))
  (98 97 114)

  (car/wcar conn (car/parse-raw (car/set "foo" (byte-array [1 2 3]))))

  (wcar* (car/ping))

  (wcar*
   (car/ping)
   (car/set "foo" "bar")
   (car/get "foo")) 

  (wcar* (car/set "clj-key" {:bigint (bigint 31415926535897932384626433832795)
                             :vec    (vec (range 5))
                             :set    #{true false :a :b :c :d}
                             :bytes  (byte-array 5)
                             ;; ...
                             })
         (car/get "clj-key"))


  (wcar*
   (car/swap "clj-key" (fn [old nx?]
                         (let [new (assoc old :foo :bar)]
                           (println old)
                           [new [old new]]))))


  (wcar*
   (car/exists "foo"))


  )
