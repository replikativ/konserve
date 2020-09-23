(ns konserve.serializers
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            #?@(:clj [[clojure.data.fressian :as fress]
                      [incognito.fressian :refer [incognito-read-handlers
                                                  incognito-write-handlers]]])
            #?@(:cljs [[fress.api :as fress]
                       [incognito.fressian :refer [incognito-read-handlers incognito-write-handlers]]])
            [incognito.edn :refer [read-string-safe]])
  #?(:clj (:import [java.io FileOutputStream FileInputStream DataInputStream DataOutputStream]
                   [org.fressian.handlers WriteHandler ReadHandler])))

#?(:clj
   (defrecord FressianSerializer [custom-read-handlers custom-write-handlers]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (fress/read bytes
                   :handlers (-> (merge fress/clojure-read-handlers
                                        custom-read-handlers
                                        (incognito-read-handlers read-handlers))
                                 fress/associative-lookup)))

     (-serialize [_ bytes write-handlers val]
       (let [w (fress/create-writer bytes :handlers (-> (merge
                                                         fress/clojure-write-handlers
                                                         custom-write-handlers
                                                         (incognito-write-handlers write-handlers))
                                                        fress/associative-lookup
                                                        fress/inheritance-lookup))]
         (fress/write-object w val)))))

#?(:cljs
   (defrecord FressianSerializer [custom-read-handlers custom-write-handlers]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [buf->arr (.from js/Array (.from js/Int8Array bytes))
             buf      (fress.impl.buffer/BytesOutputStream. buf->arr (count buf->arr))
             reader   (fress/create-reader buf
                                           :handlers (merge custom-read-handlers
                                                            (incognito-read-handlers read-handlers)))
             read     (fress/read-object reader)]
         read))
     (-serialize [_ bytes write-handlers val]
       (let [writer (fress/create-writer bytes
                                         :handlers (merge
                                                    custom-write-handlers
                                                    (incognito-write-handlers write-handlers)))]
         (fress/write-object writer val)))))

(defn fressian-serializer
  ([] (fressian-serializer {} {}))
  ([read-handlers write-handlers] (map->FressianSerializer {:custom-read-handlers read-handlers
                                                            :custom-write-handlers write-handlers})))
(defrecord StringSerializer []
  PStoreSerializer
  (-deserialize [_ read-handlers s]
    (read-string-safe @read-handlers s))
  (-serialize [_ output-stream _ val]
    #?(:clj
       (binding [clojure.core/*out* output-stream]
         (pr val)))
    #?(:cljs
         (pr-str val))))

(defn string-serializer []
  (map->StringSerializer {}))

#?(:clj
   (defn construct->class [m]
     (->> (map (fn [[k v]] [(class v) k]) m)
          (into {}))))

#?(:cljs
   (defn construct->class [m]
     (->> (map (fn [[k v]] [(pr-str (type v)) k]) m)
          (into {}))))

(def byte->serializer
  {0 (string-serializer)
   1 (fressian-serializer)})

(def serializer-class->byte
  (construct->class byte->serializer))

#(:clj
  (defn construct->keys [m]
    (->> (map (fn [[k v]] [(-> v class .getSimpleName keyword) v]) m)
         (into {}))))

#(:cljs
  (defn construct->keys [m]
    (->> (map (fn [[k v]] [(-> v type pr-str) v]) m)
         (into {}))))


(def key->serializer
  (construct->keys byte->serializer))

(defn construct->byte [m n]
  (->> (map (fn [[k0 v0] [k1 v1]] [k0 k1]) m n)
       (into {})))

(def byte->key
  (construct->byte byte->serializer key->serializer))
