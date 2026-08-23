(ns konserve.mmap-test
  "`konserve.mmap`, which navigates a filestore value in place.

  JDK GUARD. This namespace reaches `boring.mmap`, which touches
  `java.lang.foreign` and needs JDK 22. The refusal tests do NOT — they only
  read a header — so they run everywhere and are the ones that matter for the
  guards. Only the mapping test is skipped on an older JVM."
  (:require [clojure.test :refer [deftest testing is]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store]]
            [konserve.mmap :as kmm]
            [boring.nav :as nav]
            [konserve.store :as st]
            [konserve.compressor :refer [null-compressor lz4-compressor]])
  (:import [java.io File FileInputStream FileOutputStream]
           [java.nio ByteOrder]))

(def ^:private ffm?
  (try ((requiring-resolve 'clojure.core/require) 'boring.mmap) true
       (catch Throwable _ false)))

(defn- fresh-dir [n]
  (let [d (str (System/getProperty "java.io.tmpdir") "/konserve-mmap-test-" n)]
    (when (.exists (File. d))
      (run! #(.delete ^File %) (reverse (file-seq (File. d)))))
    d))

(defn- blob-of [dir]
  (first (filter #(.isFile ^File %) (file-seq (File. dir)))))

(defn- boring-store [dir]
  (connect-fs-store dir :default-serializer :BoringSerializer :opts {:sync? true}))

(def ^:private value
  (into {} (for [i (range 200)] [(str "customer-" i) {"name" (str "name-" i)}])))

(deftest navigates-a-value-in-place
  (when ffm?
    (let [dir   (fresh-dir "nav")
          store (boring-store dir)]
      (k/assoc store "customers" value {:sync? true})
      (testing "the same answer konserve/get gives, without materialising the rest"
        (is (= "name-137"
               (kmm/with-mmap-value [c store "customers"]
                 (nav/value (get-in c ["customer-137" "name"])))))
        (is (= "name-137"
               (get-in (k/get store "customers" nil {:sync? true})
                       ["customer-137" "name"]))))
      (testing "count is O(1) off the container head, not a walk"
        (is (= 200 (kmm/with-mmap-value [c store "customers"] (count c))))))))

(defn- segment-buffer [segment]
  (clojure.lang.Reflector/invokeInstanceMethod segment "asByteBuffer" (object-array 0)))

(deftest maps-bulk-payloads-without-realising-them
  (when ffm?
    (let [dir (fresh-dir "payload")
          store (boring-store dir)
          bytes (byte-array [1 2 3 4])
          floats (float-array [1.5 -2.25 3.0])
          arrays {:shorts (short-array [-1 2])
                  :ints (int-array [-1 2])
                  :longs (long-array [-1 2])
                  :floats floats
                  :doubles (double-array [-1.0 2.0])}]
      (k/assoc store :tensors (merge {:bytes bytes :scalar 42} arrays) {:sync? true})
      (testing "a nested byte string is exposed as its raw mapped payload"
        (kmm/with-mmap-payload [payload store :tensors [:bytes]]
          (let [buffer (segment-buffer (:segment payload))
                actual (byte-array (.remaining buffer))]
            (.get buffer actual)
            (is (= [1 2 3 4] (vec actual)))
            (is (= {:byte-size 4 :element-count 4 :element-size 1
                    :element-type :uint8 :byte-order nil :tag nil}
                   (select-keys payload [:byte-size :element-count :element-size
                                         :element-type :byte-order :tag])))
            (is (pos? (:file-offset payload))))))
      (testing "an RFC 8746 float array retains its dtype and endian contract"
        (kmm/with-mmap-payload [payload store :tensors [:floats]]
          (let [buffer (doto (segment-buffer (:segment payload))
                         (.order ByteOrder/LITTLE_ENDIAN))
                actual (float-array (:element-count payload))]
            (.get (.asFloatBuffer buffer) actual)
            (is (= [1.5 -2.25 3.0] (vec actual)))
            (is (= {:byte-size 12 :element-count 3 :element-size 4
                    :element-type :float32 :byte-order :little-endian :tag 85}
                   (select-keys payload [:byte-size :element-count :element-size
                                         :element-type :byte-order :tag]))))))
      (testing "all primitive-array formats Boring emits have explicit metadata"
        (doseq [[field element-count element-type element-size tag]
                [[:shorts 2 :int16 2 77]
                 [:ints 2 :int32 4 78]
                 [:longs 2 :int64 8 79]
                 [:floats 3 :float32 4 85]
                 [:doubles 2 :float64 8 86]]]
          (kmm/with-mmap-payload [payload store :tensors [field]]
            (is (= {:element-count element-count :element-size element-size
                    :element-type element-type :byte-order :little-endian :tag tag}
                   (select-keys payload [:element-count :element-size :element-type
                                         :byte-order :tag]))))))
      (testing "the top-level value is addressed by the default empty path"
        (k/assoc store :raw bytes {:sync? true})
        (kmm/with-mmap-payload [payload store :raw]
          (is (= 4 (:byte-size payload)))))
      (testing "absence and scalar values fail rather than decoding or copying"
        (is (= :konserve/payload-not-found
               (:type (ex-data (try (kmm/mmap-payload store :tensors [:missing])
                                    (catch Exception e e))))))
        (is (= :konserve/not-a-bulk-payload
               (:type (ex-data (try (kmm/mmap-payload store :tensors [:scalar])
                                    (catch Exception e e))))))))))

(deftest value-location-points-past-the-header-and-meta
  (let [dir   (fresh-dir "loc")
        store (boring-store dir)]
    (k/assoc store "customers" value {:sync? true})
    (let [[path offset] (kmm/value-location store "customers")
          blob (blob-of dir)]
      (is (= (.getPath ^File blob) path))
      (testing "the offset is 20 + meta-size, and the bytes there are a CBOR
                item rather than the header"
        (is (> offset 20) "meta is not empty")
        (is (< offset (.length ^File blob)))))))

(deftest a-missing-key-is-a-typed-error
  (let [store (boring-store (fresh-dir "missing"))]
    (is (= :konserve/key-not-found
           (:type (ex-data (try (kmm/value-location store "nope")
                                (catch Exception e e))))))))

(deftest a-store-that-is-not-a-filestore-is-refused
  (testing "only the filestore keeps values in files this can map"
    (is (= :konserve/not-a-filestore
           (:type (ex-data (try (kmm/value-location {:backing {}} "k")
                                (catch Exception e e))))))))

(deftest a-fressian-blob-is-refused-rather-than-misread
  (testing "reading another codec's bytes as CBOR would not error, it would
            return nonsense -- which is the whole reason this checks"
    (let [dir   (fresh-dir "fressian")
          store (connect-fs-store dir :opts {:sync? true})]  ; default serializer
      (k/assoc store "customers" value {:sync? true})
      (let [d (ex-data (try (kmm/value-location store "customers")
                            (catch Exception e e)))]
        (is (= :konserve/not-navigable (:type d)))
        (is (= 1 (:serializer d)) "byte 1 is fressian")))))

;; The compression and encryption guards read a header byte, so they are tested
;; by writing that byte. Going through `connect-fs-store` would test konserve's
;; option plumbing instead -- and in fact cannot: passing `:compressor` there
;; leaves the store on `null-compressor`, so a blob written that way carries a
;; 0 and exercises nothing.
(defn- doctor-header!
  "Rewrite one header byte of `dir`'s blob in place."
  [dir idx v]
  (let [^File f (blob-of dir)
        bs (byte-array (.length f))]
    (with-open [in (FileInputStream. f)] (.read in bs))
    (aset-byte bs idx (byte v))
    (with-open [out (FileOutputStream. f)] (.write out bs))))

(deftest a-compressed-blob-is-refused
  (testing "a compressed blob must be decompressed whole before anything can
            navigate it, which is exactly the cost this exists to avoid. It
            throws rather than falling back, because a silent fallback would
            give correct answers at full price with no way to notice."
    (let [dir   (fresh-dir "compressed")
          store (boring-store dir)]
      (k/assoc store "customers" value {:sync? true})
      (doctor-header! dir 2 2)                      ; compressor byte -> zstd
      (let [d (ex-data (try (kmm/value-location store "customers")
                            (catch Exception e e)))]
        (is (= :konserve/not-navigable (:type d)))
        (is (= 2 (:compressor d)))))))

(deftest an-encrypted-blob-is-refused
  (let [dir   (fresh-dir "encrypted")
        store (boring-store dir)]
    (k/assoc store "customers" value {:sync? true})
    (doctor-header! dir 3 1)                        ; encryptor byte
    (let [d (ex-data (try (kmm/value-location store "customers")
                          (catch Exception e e)))]
      (is (= :konserve/not-navigable (:type d)))
      (is (= 1 (:encryptor d))))))

;; --------------------------------------------------------------------------
;; The bug this file's compression guard could not be tested against.

(deftest compression-is-configured-under-config
  (testing "`:config {:compressor {:type :lz4}}` is the spelling that works,
            and it must land in the blob header. `connect-default-store` reads
            both compressor and encryptor from `config`, never from a
            top-level key."
    (let [dir   (fresh-dir "cmp-config")
          store (connect-fs-store dir :default-serializer :BoringSerializer
                                  :config {:compressor {:type :lz4}}
                                  :opts {:sync? true})]
      (k/assoc store "customers" value {:sync? true})
      (let [^bytes hdr (with-open [in (FileInputStream. ^File (blob-of dir))]
                         (let [b (byte-array 4)] (.read in b) b))]
        (is (= 1 (bit-and (aget hdr 2) 0xff)) "compressor byte 1 = lz4"))
      (testing "and konserve.mmap refuses it, since a compressed blob has to be
                decompressed whole before anything can navigate it"
        (is (= :konserve/not-navigable
               (:type (ex-data (try (kmm/value-location store "customers")
                                    (catch Exception e e))))))))))

(deftest a-top-level-compressor-is-refused-not-ignored
  (testing "passing a compressor FUNCTION at the top level reads like it should
            work and did nothing: the store came back on null-compressor and
            wrote a 0 into every header while the caller believed their data
            was compressed. `connect-fs-store` even shipped
            `:compressor null-compressor` as a default that was never read.

            Silence is the wrong answer for a durable property nobody
            re-checks, so this now throws and names the right spelling."
    (is (= :store-configuration-error
           (:type (ex-data (try (connect-fs-store
                                 (fresh-dir "cmp-toplevel")
                                 :default-serializer :BoringSerializer
                                 :compressor lz4-compressor
                                 :opts {:sync? true})
                                (catch Exception e e))))))))

;; --------------------------------------------------------------------------
;; The lifecycle API must not drop what it is given.

(deftest lifecycle-config-reaches-the-backend
  (testing "`konserve.store/create-store` used to forward four keys by name --
            :path, :config, :filesystem, :opts -- and silently drop the rest.
            A config asking for `:default-serializer :BoringSerializer` produced
            a FRESSIAN store, header byte 1, without a word.

            Asserted on the HEADER rather than the store record, because the
            header is what a later reader actually dispatches on, and every bug
            in this family was invisible precisely because nothing checked the
            resulting bytes."
    (let [hdr-of (fn [dir]
                   (with-open [in (FileInputStream. ^File (blob-of dir))]
                     (let [b (byte-array 4)] (.read in b)
                          (vec (map #(bit-and % 0xff) b)))))
          mk (fn [dir cfg]
               (let [store (st/create-store
                            (merge {:backend :file :path dir
                                    :id (java.util.UUID/randomUUID)}
                                   cfg)
                            {:sync? true})]
                 (k/assoc store "customers" value {:sync? true})
                 [(hdr-of dir) (:default-serializer store)]))]
      (testing "the default is still fressian, byte 1"
        (is (= [[1 1 0 0] :FressianSerializer] (mk (fresh-dir "life-default") {}))))
      (testing ":default-serializer reaches the store AND the header"
        (is (= [[1 3 0 0] :BoringSerializer]
               (mk (fresh-dir "life-boring")
                   {:default-serializer :BoringSerializer}))))
      (testing "and it composes with :config, which was the one key that did
                get forwarded"
        (is (= [[1 3 1 0] :BoringSerializer]
               (mk (fresh-dir "life-both")
                   {:default-serializer :BoringSerializer
                    :config {:compressor {:type :lz4}}})))))))

(deftest a-null-compressor-at-the-top-level-is-tolerated
  (testing "konserve-rocksdb passes `:compressor null-compressor` as a dead
            default -- the same pattern konserve's own filestore carried until
            it was removed. Throwing on that would break a maintained backend
            on upgrade for a value that asks for nothing. Only a MEANINGFUL
            compressor is refused, because that is someone trying to configure
            compression and silently getting none."
    (is (some? (connect-fs-store (fresh-dir "null-cmp")
                                 :compressor null-compressor
                                 :opts {:sync? true})))))

(deftest encoding-is-per-blob-not-per-store
  (testing "a store's serializer applies to what it writes NEXT. Blobs already
            on disk keep the encoding they were written with, and every read
            dispatches on that blob's own header -- so switching a store's
            default needs no migration and leaves a mix, by design.

            konserve.mmap must therefore judge the BLOB, never the store: a
            boring-configured store still holds fressian blobs it cannot
            navigate, and that is correct rather than a failure."
    (let [dir (fresh-dir "mixed")
          fressian-store (connect-fs-store dir :opts {:sync? true})
          _ (k/assoc fressian-store "old" {:a 1} {:sync? true})
          boring-store (connect-fs-store
                        dir :default-serializer :BoringSerializer
                        :opts {:sync? true})
          _ (k/assoc boring-store "new" value {:sync? true})]
      (testing "both blobs read correctly through EITHER store"
        (is (= {:a 1} (k/get boring-store "old" nil {:sync? true})))
        (is (some? (k/get fressian-store "new" nil {:sync? true}))))
      (testing "the boring blob is navigable"
        (is (true? (kmm/navigable? boring-store "new")))
        (when ffm?
          (is (= "name-137" (kmm/with-mmap-value [c boring-store "new"]
                              (nav/value (get-in c ["customer-137" "name"])))))))
      (testing "and the fressian one is not -- judged by its own header, not by
                the store that happens to be asking"
        (is (false? (kmm/navigable? boring-store "old")))
        (is (= 1 (:serializer (ex-data (try (kmm/value-location boring-store "old")
                                            (catch Exception e e)))))))
      (testing "a missing key is not navigable rather than throwing"
        (is (false? (kmm/navigable? boring-store "absent")))))))
