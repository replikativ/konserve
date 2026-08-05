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
            [boring.nav :as nav])
  (:import [java.io File FileInputStream FileOutputStream]))

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
