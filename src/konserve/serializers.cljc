(ns konserve.serializers
  (:require #?(:clj [clj-cbor.core :as cbor])
            [boring.core :as boring]
            [clojure.string :as str]
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

(defn- boring-wire-name
  "incognito's munged key as boring >= 0.1.11 spells it, or nil if it is
  already in that form.

  `my_ns.core.MyRecord` -> `my-ns.core/MyRecord`: the LAST dot becomes the
  namespace/name separator, and underscores in the namespace part become
  hyphens. A key that already contains `/` is left alone -- it is either
  boring's own spelling or a hand-picked name, and neither wants munging."
  [k]
  (let [s (str k)]
    (when-not (str/includes? s "/")
      (let [i (str/last-index-of s ".")]
        (when (and i (pos? i))
          (str (str/replace (subs s 0 i) "_" "-")
               "/" (subs s (inc i))))))))

(defn- with-boring-names
  "`handlers` plus an entry under each key's boring wire name, where that
  differs. The original key wins if both spell the same thing."
  [handlers]
  (reduce-kv (fn [m k v]
               (if-let [n (boring-wire-name k)]
                 (assoc m n v)
                 m))
             handlers
             handlers))

(defn- record-registry
  "Fold incognito-style record handlers into a boring registry.

  incognito keys its handlers by `(-> r type pr-str normalize-ns symbol)` --
  the type name with `/` -> `.` and `-` -> `_`. That USED to be exactly
  boring's own wire name for a record, making the mapping a straight rename.

  **It stopped being true in boring 0.1.11**, which writes a record's true
  `namespace/Name` as written and munges nothing. So a handler keyed
  `my_ns.MyRecord` no longer matches a record boring now names
  `my-ns/MyRecord`, and the symptom is silent: the record decodes to an
  `UnknownRecord` carrying the right name and fields, with no error.

  BOTH SPELLINGS ARE REGISTERED, and that is a persistence requirement rather
  than a convenience. A konserve store is durable: records written under
  boring 0.1.6 carry the munged name on disk forever, and records written from
  now on carry the true one. A reader that knows only one of them silently
  fails on half the store. Registering both makes an existing store keep
  working across the upgrade.

  The un-munging is best-effort by construction -- `my-ns` and `my_ns` both
  munge to `my_ns`, so the inverse cannot be exact, and boring resolves the
  same ambiguity on the JVM by scanning loaded namespaces (which ClojureScript
  cannot do). Here it does not have to be exact: the key as given is
  registered too, so a genuine underscore namespace still resolves through
  that one.

  Note boring does not NEED write handlers for records: it emits the type name
  natively via CBOR tag 27, which is the problem incognito exists to work
  around for fressian. Only the read direction has to be taught anything, and
  an unregistered record still decodes to an inert value carrying its name and
  fields rather than being lost.

  Takes the handler MAP, already deref'd: `registry-for` below derefs once and
  memoises on the result, so dereferencing again here would both re-read a
  changing atom mid-computation and fail outright when handed the map.

  One `register-records` rather than a fold of `register-record`: a registry
  copies its whole backing map per registration, so the fold was quadratic in
  the handler count -- 4.84 us at 20 handlers against 0.82. The cache below
  means this runs rarely, but a cache miss should not be quadratic either.
  Symbol keys are stringified by boring, so incognito's map needs no
  preparation."
  [registry handlers]
  (if (seq handlers)
    (boring/register-records registry (with-boring-names handlers))
    registry))

(def ^:private ^:const registry-cache-size
  "How many (base, handlers) pairs one serializer instance remembers.

  Not 1. `byte->serializer` holds a SINGLE boring serializer instance, shared by
  every store that does not override it in its config -- so a one-entry cache
  thrashes the moment two stores with different handler maps are open at once,
  and falls straight back to refolding per read (measured: 0.20 us -> 5.56 us).
  A handful of entries covers the realistic case; the scan is pointer
  comparisons."
  4)

(defn- registry-cache []
  (atom []))

(defn- registry-for
  "The read registry for `base` plus `handlers-atom`'s current handlers,
  memoised.

  Keyed on BOTH the base registry and the handler map, by identity. Keying on
  the handlers alone would serve a stale registry after someone `swap!`s a new
  tag into the serializer's own registry: the encoder derefs it per frame, so
  the same change would take effect for writing and never for reading."
  [cache base handlers-atom]
  (let [h (if handlers-atom @handlers-atom {})
        entries @cache
        hit (some (fn [e] (when (and (identical? (:base e) base)
                                     (identical? (:src e) h))
                            e))
                  entries)]
    (if hit
      (:reg hit)
      (let [reg (record-registry base h)]
        (swap! cache (fn [es]
                       (vec (take registry-cache-size
                                  (cons {:base base :src h :reg reg} es)))))
        reg))))

(defrecord BoringSerializer [registry encode-opts cache]
  #?@(:cljs (INamed
             (-name [_] "BoringSerializer")
             (-namespace [_] "konserve.serializers")))
  PStoreSerializer
  (-deserialize [_ read-handlers input]
    (let [opts (assoc encode-opts :registry (registry-for cache registry read-handlers))]
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
  ([registry opts] (map->BoringSerializer {:registry registry :encode-opts opts
                                           :cache (registry-cache)})))

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
