(ns konserve.serializers
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            #?@(:clj [[clojure.data.fressian :as fress]
                      [incognito.fressian :refer [incognito-read-handlers
                                                  incognito-write-handlers]]])

            [incognito.edn :refer [read-string-safe]])
  #?(:clj (:import [java.io FileOutputStream FileInputStream DataInputStream DataOutputStream]
                   [org.fressian.handlers WriteHandler ReadHandler])))


#?(:clj
   (defrecord FressianSerializer []
     PStoreSerializer
     (-deserialize [_ bytes read-handlers]
       (fress/read bytes
                   :handlers (-> (merge fress/clojure-read-handlers
                                        (incognito-read-handlers read-handlers))
                                 fress/associative-lookup)))

     (-serialize [_ bytes val write-handlers]
       (let [w (fress/create-writer bytes :handlers (-> (merge
                                                         fress/clojure-write-handlers
                                                         (incognito-write-handlers write-handlers))
                                                        fress/associative-lookup
                                                        fress/inheritance-lookup))]
         (fress/write-object w val)))))

#?(:clj
   (defn fressian-serializer []
     (map->FressianSerializer {})))


(defrecord StringSerializer []
  PStoreSerializer
  (-deserialize [_ s read-handlers]
    (read-string-safe @read-handlers s))
  (-serialize [_ output-stream val _]
    #?(:clj
       (binding [clojure.core/*out* output-stream]
         (pr val))
       :cljs (pr-str val))))


(defn string-serializer []
  (map->StringSerializer {}))
