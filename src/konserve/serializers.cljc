(ns konserve.serializers
  (:require #?(:clj [clj-cbor.core :as cbor])
            [boring.core :as boring]
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
                      ;; CLJS fress expects flat {Type fn} format.
                      ;; Custom handlers must provide this format directly.
                      :cljs (merge custom-write-handlers
                                   (incognito-write-handlers write-handlers)))]
      #?(:clj (let [writer (fress/create-writer bytes :handlers handlers)]
                (fress/write-object writer val))
         :cljs (fress/write val :handlers handlers)))))

(defn fressian-serializer
  ([] (fressian-serializer {} {}))
  ([read-handlers write-handlers] (map->FressianSerializer {:custom-read-handlers read-handlers
                                                            :custom-write-handlers write-handlers})))

;; ---------------------------------------------------------------------------
;; boring (CBOR) -- serializer byte 3.
;;
;; Added ALONGSIDE the clj-cbor serializer (byte 2) rather than replacing it.
;; The serializer id is written into every blob header, so it is durable wire
;; state: repurposing byte 2 would silently reinterpret data already on disk.
;; Byte 2 stays readable forever; byte 3 is what new stores write.
;;
;; Two things it fixes over byte 2:
;;   - byte 2 THROWS if given any read or write handler, so it could never
;;     serialize a record or a PSS node. This one supports handlers.
;;   - byte 2 is #?(:clj ...) only, because clj-cbor has no ClojureScript.
;;     This one runs on both platforms from the same code.
;; ---------------------------------------------------------------------------

(defn- record-registry
  "Fold incognito-style record handlers into a boring registry.

  incognito keys its handlers by `(-> r type pr-str normalize-ns symbol)` --
  the type name with `/` -> `.` and `-` -> `_`. That is exactly boring's own
  wire name for a record (`boring.data/record-type-name`), so the mapping is
  a straight rename with no translation.

  Note boring does not NEED write handlers for records: it emits the type name
  natively via CBOR tag 27, which is the problem incognito exists to work
  around for fressian. Only the read direction has to be taught anything, and
  an unregistered record still decodes to an inert value carrying its name and
  fields rather than being lost."
  [registry read-handlers]
  (let [handlers (some-> read-handlers deref)]
    (if (seq handlers)
      (reduce-kv (fn [reg tag ctor] (boring/register-record reg (str tag) ctor))
                 registry handlers)
      registry)))

(defrecord BoringSerializer [registry encode-opts]
  #?@(:cljs (INamed
             (-name [_] "BoringSerializer")
             (-namespace [_] "konserve.serializers")))
  PStoreSerializer
  (-deserialize [_ read-handlers input]
    (let [opts (assoc encode-opts :registry (record-registry registry read-handlers))]
      #?(:clj  (boring/decode (.readAllBytes ^java.io.InputStream input) opts)
         :cljs (boring/decode input opts))))
  (-serialize [_ #?(:clj output-stream :cljs _) write-handlers val]
    ;; write-handlers are incognito's optional per-record value transforms.
    ;; boring carries the type natively, so nothing here needs them; they are
    ;; accepted and ignored rather than rejected, because byte 2 REJECTING them
    ;; is precisely why it was unusable.
    (let [opts (assoc encode-opts :registry registry)]
      #?(:clj  (.write ^java.io.OutputStream output-stream ^bytes (boring/encode val opts))
         :cljs (boring/encode val opts)))))

(defn boring-serializer
  "A portable CBOR serializer backed by boring.

  `opts` are boring's encode options; `:shapes true` is worth enabling for
  stores holding many same-shaped maps -- it strips repeated keys and was
  measured at 36% smaller on datom-like content.

  `registry` is a boring tag registry for custom types (see boring's
  doc/EXTENDING.md). Records need no registration to be WRITTEN."
  ([] (boring-serializer (boring/tag-registry) {}))
  ([registry] (boring-serializer registry {}))
  ([registry opts] (map->BoringSerializer {:registry registry :encode-opts opts})))

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
  "Serializer id -> instance. THE ID IS PERSISTED IN EVERY BLOB HEADER, so this
  map is durable wire state: an id's meaning must never change, and a new
  serializer takes the next free id rather than reusing one."
  {0 (string-serializer)
   1 (fressian-serializer)
   #?@(:clj [2 (cbor-serializer)])          ; legacy, JVM-only, rejects handlers
   3 (boring-serializer)})

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
