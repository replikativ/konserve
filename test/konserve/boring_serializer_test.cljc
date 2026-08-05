(ns konserve.boring-serializer-test
  "The boring (CBOR) serializer, byte 3.

  Two things these tests pin that the clj-cbor serializer (byte 2) could not
  do at all: it accepts read handlers rather than throwing on them, and it runs
  on both platforms from one implementation.

  ## Why these go through bytes, not through a store

  An earlier version of this file built a `MemoryStore` with
  `:default-serializer :BoringSerializer` and asserted on `k/assoc` /`k/get`.
  Every one of those assertions was vacuous: `konserve.memory` declares
  `PAssocSerializers` as a no-op (\"no serializers needed for memory\") and keeps
  values as live objects in an atom, so nothing was ever encoded. The tests
  passed identically with `:default-serializer :TotallyBogusSerializer`.

  So the round trips here go through the `PStoreSerializer` boundary and real
  bytes, which is the only thing that actually exercises the codec, and
  `memory-store-does-not-serialize` pins the trap itself so it cannot be
  re-introduced. The store-level wiring — header byte, on-disk blobs — is
  covered separately by `filestore-writes-serializer-byte-3`."
  (:require [clojure.test :refer [deftest testing is]]
            [konserve.core :as k]
            [konserve.memory :refer [map->MemoryStore]]
            [konserve.protocols :as p]
            [konserve.serializers :as ser]
            [boring.core :as boring]
            [boring.data :as bdata]
            #?(:clj [boring.nav :as nav])
            #?@(:clj [[clojure.java.io :as io]
                      [konserve.filestore :refer [connect-fs-store delete-store]]]))
  #?(:clj (:import [java.io ByteArrayInputStream ByteArrayOutputStream])))

(defrecord BPoint [x y])

(defn- round-trip
  "Encode `v` to bytes with the boring serializer and decode them back.

  `-serialize` writes to a stream on the JVM and returns the encoded value on
  ClojureScript, so the two arms differ in plumbing only."
  ([v] (round-trip v {}))
  ([v read-handlers]
   (let [s (ser/boring-serializer)]
     #?(:clj (let [baos (ByteArrayOutputStream.)]
               (p/-serialize s baos (atom {}) v)
               (p/-deserialize s (atom read-handlers)
                               (ByteArrayInputStream. (.toByteArray baos))))
        :cljs (p/-deserialize s (atom read-handlers)
                              (p/-serialize s nil (atom {}) v))))))

(deftest boring-serializer-is-registered-as-byte-3
  (testing "the id is persisted in every blob header, so it must be stable"
    (is (= :BoringSerializer (get ser/byte->key 3)))
    (is (instance? konserve.serializers.BoringSerializer
                   (get ser/byte->serializer 3)))
    (testing "and it must not have displaced an existing id"
      (is (= :StringSerializer   (get ser/byte->key 0)))
      (is (= :FressianSerializer (get ser/byte->key 1)))
      #?(:clj (is (= :CBORSerializer (get ser/byte->key 2)))))))

(deftest memory-store-does-not-serialize
  (testing "konserve.memory keeps live objects in an atom and ignores
            :default-serializer entirely. Asserting a round trip through it
            proves nothing about a codec — a deliberately bogus serializer name
            round-trips a record perfectly. This test exists so that trap is
            visible rather than silently re-entered."
    (let [store (map->MemoryStore {:state (atom {})
                                   :read-handlers (atom {})
                                   :write-handlers (atom {})
                                   :locks (atom {})
                                   :default-serializer :TotallyBogusSerializer})]
      (k/assoc store "p" (->BPoint 3 4) {:sync? true})
      (let [back (k/get store "p" nil {:sync? true})]
        (is (= BPoint (type back)))
        (is (identical? back (second (get @(:state store) "p")))
            "the very same object came back — no encode/decode happened")))))

(deftest round-trips-clojure-values
  (doseq [[label v] [["scalar"      42]
                     ["string"      "hello"]
                     ["keyword"     :some.ns/kw]
                     ["map"         {:a 1 :b "two" :c [1 2 3]}]
                     ["set"         #{:x :y :z}]
                     ["nested"      {:xs (vec (range 50)) :m {:k :v}}]
                     ["uuid"        #uuid "9682952b-fafa-4b41-8e4a-31ae948d6f08"]
                     ["nil-value"   nil]
                     #?@(:clj [["bigdec" 1.50M]
                               ["ratio"  (/ 22 7)]])]]
    (testing label
      (is (= v (round-trip v)) label))))

#?(:clj
   (deftest bigdecimal-keeps-its-scale
     (testing "1.50M and 1.5M must stay distinguishable — `=` does NOT check
               scale on BigDecimal, so this compares the string form"
       (is (= "1.50" (str (round-trip 1.50M)))))))

(deftest records-round-trip-with-a-read-handler
  (testing "incognito-style handlers are keyed by the MUNGED type symbol
            (`/` -> `.`, `-` -> `_`). That was boring's own wire name until
            0.1.11, which writes the true `namespace/Name` instead — so the
            bridge registers both spellings and this key must keep working."
    (let [back (round-trip (->BPoint 3 4)
                           {'konserve.boring_serializer_test.BPoint map->BPoint})]
      (is (= (->BPoint 3 4) back))
      (is (= BPoint (type back)))))

  (testing "and a handler keyed by boring's OWN spelling works too, since a
            caller who reads boring's docs rather than incognito's will write
            it that way"
    (let [back (round-trip (->BPoint 3 4)
                           {'konserve.boring-serializer-test/BPoint map->BPoint})]
      (is (= BPoint (type back))))))

(deftest a-record-written-before-0-1-11-still-reads
  (testing "THE UPGRADE CASE, and the reason both spellings are registered.
            A konserve store is durable: every record written through boring
            0.1.6 carries the MUNGED name on disk forever, while everything
            written from now on carries the true one. A reader that knows only
            the new spelling silently returns an UnknownRecord for half the
            store — no error, just the wrong type.

            The bytes here are a tag-27 frame naming `konserve.boring_serializer
            _test.BPoint`, which is exactly what 0.1.6 wrote, decoded through
            the same handler map a user upgrading would already have."
    (let [old-bytes (boring/encode
                     (bdata/unknown-record
                      "konserve.boring_serializer_test.BPoint" {:x 3 :y 4}))
          ser       (ser/boring-serializer)
          handlers  (atom {'konserve.boring_serializer_test.BPoint map->BPoint})
          back      (p/-deserialize ser handlers
                                    #?(:clj (java.io.ByteArrayInputStream. old-bytes)
                                       :cljs old-bytes))]
      (is (= BPoint (type back)) "old-format record must reconstruct")
      (is (= (->BPoint 3 4) back)))))

(deftest records-survive-without-a-read-handler
  (testing "an unregistered record must not be LOST. boring writes the type
            name natively via tag 27, so it comes back as an inert value
            carrying the same name and fields rather than vanishing into a
            plain map — which is the failure incognito exists to prevent."
    (let [back (round-trip (->BPoint 3 4))]
      (is (some? back))
      (is (= 3 (:x back)))
      (is (= 4 (:y back))))))

(deftest handlers-are-accepted-not-rejected
  (testing "the clj-cbor serializer THREW on any handler, which is why it could
            never serialize a record or an index node. This one must not."
    (is (= {:a 1} (round-trip {:a 1} {'some.Thing identity})))))

;; ---------------------------------------------------------------------------
;; Store-level wiring. JVM only, because the filestore is; the ClojureScript
;; side of the codec is covered by the byte-level round trips above and by
;; boring's own cross-platform golden corpus.
;; ---------------------------------------------------------------------------

#?(:clj
   (deftest filestore-writes-serializer-byte-3
     (testing "the serializer id lands in the blob header, so a store written
               today is still readable when byte 3 is one of several"
       (let [dir (str (System/getProperty "java.io.tmpdir")
                      "/konserve-boring-header-test")]
         (delete-store dir)
         (try
           (let [store (connect-fs-store
                        dir
                        :opts {:sync? true}
                        :default-serializer :BoringSerializer)]
             (k/assoc store "p" (->BPoint 3 4) {:sync? true})
             (testing "and the value survives a real encode/decode to disk"
               (let [back (k/get store "p" nil {:sync? true})]
                 (is (= 3 (:x back)))
                 (is (= 4 (:y back)))))
             (testing "header byte 1 is the serializer id"
               (let [f (->> (file-seq (io/file dir))
                            (filter #(.isFile ^java.io.File %))
                            (filter #(re-find #"\.ksv$" (.getName ^java.io.File %)))
                            first)
                     head (byte-array 4)]
                 (is (some? f) "a blob file was written")
                 (with-open [in (io/input-stream f)]
                   (.read in head))
                 (is (= 3 (aget head 1))
                     "byte 3 — NOT 1 (fressian) and NOT 2 (clj-cbor)"))))
           (finally
             (delete-store dir)))))))

#?(:clj
   (deftest values-are-indexed-and-navigable
     (testing "a stored value carries an offset index by default, so it can be
               navigated without materialising the rest of it. Small values are
               exempt automatically: nothing clears boring's :index-min, so no
               frame is emitted and the bytes are unchanged."
       (let [big  (into {} (for [i (range 200)]
                             [(str "customer-" i) {"name" (str "name-" i)}]))
             ser  (ser/boring-serializer)
             ->b  (fn [s v] (let [o (ByteArrayOutputStream.)]
                              (p/-serialize s o (atom {}) v)
                              (.toByteArray o)))
             bs   (->b ser big)]
         (testing "it still decodes to the same value"
           (is (= big (round-trip big))))
         (testing "and boring.nav can reach one key through the index"
           (is (= "name-137"
                  (nav/value (get-in (nav/source bs) ["customer-137" "name"])))))
         (testing "a small value gets no frame, so it costs nothing"
           (is (= (seq (boring/encode {:a 1} {:stringref false}))
                  (seq (->b ser {:a 1})))))
         (testing "{:index 0} is the off switch and restores stringref"
           (let [plain (->b (ser/boring-serializer (boring/tag-registry) {:index 0}) big)]
             (is (= big (boring/decode plain)))
             (is (< (alength plain) (alength bs))
                 "unindexed keeps stringref, so it is smaller uncompressed")))))))
