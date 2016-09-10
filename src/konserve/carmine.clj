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
            [taoensso.carmine :as car :refer [wcar]]))


;; TODO if redis guarantees fsync in order of messages (never lose intermediary writes)
;; then it should be possible to not force redis to fsync here

(defrecord CarmineStore [conn read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [fn (str (uuid key))
          res (chan)]
      (put! res (car/wcar conn (car/exists fn)))
      (close! res)
      res))


  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          fn (str (uuid fkey))]
      (if-not (car/wcar conn (car/exists fn))
        (go nil)
        (let [res-ch (chan)]
          (try
            (put! res-ch
                  (get-in
                   #_(second (-deserialize serializer read-handlers fis))
                   (second (car/wcar conn (car/get fn))) 
                   rkey))
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
          (let [[old new] (wcar conn
                                (car/swap id (fn [old nx?]
                                               (let [[k v] old
                                                     new (if (empty? rkey)
                                                           (up-fn v)
                                                           (update-in v rkey up-fn))]
                                                 (println k v new)
                                                 #_(second (-deserialize serializer read-handlers fis))
                                                 #_(-serialize serializer dos write-handlers [key-vec new])
                                                 [[k new] [old new]]))))]
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
            (close! res-ch)))))))


(comment
  (def store (map->CarmineStore {:conn {:pool {} :spec {}}
                                 :read-handlers (atom {})
                                 :write-handlers (atom {})
                                 :locks (atom {})}))


  (<!! (-exists? store "foo"))

  (<!! (-update-in store ["foo"] inc))

  (<!! (-get-in store ["foo"]))


  (def server1-conn {:pool {} :spec {}}) ; See `wcar` docstring for opts
  (defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

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
