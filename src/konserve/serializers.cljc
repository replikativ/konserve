(ns konserve.serializers
  (:require #?(:clj [clj-cbor.core :as cbor])
            #?(:clj [clojure.data.fressian :as fress] :cljs [fress.api :as fress])
            [konserve.protocols :refer [PStoreSerializer]]
            [incognito.fressian :refer [incognito-read-handlers incognito-write-handlers]]
            [incognito.edn :refer [read-string-safe]]))

#?(:clj
   (defrecord CBORSerializer [codec]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (when-not (empty? @read-handlers)
         (throw (ex-info "Read handlers not supported yet." {:type :handlers-not-supported-yet})))
       (cbor/decode codec bytes))
     (-serialize [_ bytes write-handlers val]
       (when-not (empty? @write-handlers)
         (throw (ex-info "Write handlers not supported yet." {:type :handlers-not-supported-yet})))
       (cbor/encode codec bytes val))))

#?(:clj
   (defn cbor-serializer
     ([] (cbor-serializer {} {}))
     ([read-handlers write-handlers]
      (let [codec (cbor/cbor-codec
                   :write-handlers (merge cbor/default-write-handlers write-handlers)
                   :read-handlers (merge cbor/default-read-handlers read-handlers))]
        (map->CBORSerializer {:codec codec})))))

(defrecord FressianSerializer [custom-read-handlers custom-write-handlers]
  #?@(:cljs (INamed ;clojure.lang.Named
             (-name [_] "FressianSerializer")
             (-namespace [_] "konserve.serializers")))
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (let [handlers #?(:cljs (merge custom-read-handlers (incognito-read-handlers read-handlers))
                      :clj (-> (merge fress/clojure-read-handlers
                                      custom-read-handlers
                                      (incognito-read-handlers read-handlers))
                               fress/associative-lookup))]
      (fress/read bytes :handlers handlers)))
  (-serialize [_ #?(:clj bytes :cljs _) write-handlers val]
    (let [handlers #?(:clj (-> (merge
                                fress/clojure-write-handlers
                                custom-write-handlers
                                (incognito-write-handlers write-handlers))
                               fress/associative-lookup
                               fress/inheritance-lookup)
                      :cljs (merge custom-write-handlers
                                   (incognito-write-handlers write-handlers)))]
      #?(:clj (let [writer (fress/create-writer bytes :handlers handlers)]
                (fress/write-object writer val))
         :cljs (fress/write val :handlers handlers)))))

(defn fressian-serializer
  ([] (fressian-serializer {} {}))
  ([read-handlers write-handlers] (map->FressianSerializer {:custom-read-handlers read-handlers
                                                            :custom-write-handlers write-handlers})))

(defrecord StringSerializer []
  #?@(:cljs (INamed
             (-name [_] "StringSerializer")
             (-namespace [_] "konserve.serializers")))
  PStoreSerializer
  (-deserialize [_ read-handlers s]
    (read-string-safe @read-handlers s))
  (-serialize [_ #?(:clj output-stream :cljs _) _ val]
    #?(:cljs (pr-str val)
       :clj (binding [clojure.core/*out* output-stream]
              (pr val)))))

(defn string-serializer []
  (map->StringSerializer {}))

(defn construct->class [m]
  (->> (map (fn [[k v]] [#?(:clj (class v)
                            :cljs (type v)) k]) m)
       (into {})))

(def byte->serializer
  {0 (string-serializer)
   1 (fressian-serializer)
   #?@(:clj [2 (cbor-serializer)])})

(def serializer-class->byte
  (construct->class byte->serializer))

(defn construct->keys [m]
  (->> (map (fn [[_ v]]
              [#?(:clj (-> v class .getSimpleName keyword)
                  :cljs (-> v name keyword)) v]) m)
       (into {})))

(def key->serializer
  (construct->keys byte->serializer))

(defn construct->byte [m n]
  (->> (map (fn [[k0 _v0] [k1 _v1]] [k0 k1]) m n)
       (into {})))

(def byte->key
  (construct->byte byte->serializer key->serializer))
