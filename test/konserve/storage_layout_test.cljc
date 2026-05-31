(ns konserve.storage-layout-test
  "Guards the blob-header byte layout (konserve.impl.storage-layout). The meta-size is a
  4-byte big-endian int at bytes 4-7 on BOTH the JVM (.putInt/.getInt) and cljs. A previous
  cljs regression wrote/read it as a single byte at offset 4: same-host cljs was self-
  consistent (so a round-trip test passed) but JVM<->cljs cross-host exchange broke (cljs
  read meta-size 0 for any JVM blob with meta-size < 16MB). These tests pin the exact bytes
  (cross-host) AND round-trip sizes > 255 (which the single-byte format truncated)."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.impl.storage-layout :as sl]
            [konserve.serializers :as ser]
            [konserve.compressor :refer [null-compressor]]
            [konserve.encryptor :refer [null-encryptor]]))

(defn- header-bytes [meta-size]
  (mapv #(bit-and % 0xff)
        (vec (sl/create-header sl/default-version
                               (ser/fressian-serializer)
                               null-compressor null-encryptor
                               meta-size))))

(defn- roundtrip-meta-size [meta-size]
  (let [header (sl/create-header sl/default-version (ser/fressian-serializer)
                                 null-compressor null-encryptor meta-size)
        ;; parse-header takes the store's serializers map (key -> instance)
        [_version _ser _comp _enc parsed-meta-size _hsize]
        (sl/parse-header header ser/key->serializer)]
    parsed-meta-size))

(deftest header-meta-size-byte-layout
  (testing "meta-size occupies bytes 4-7 as a big-endian int (canonical JVM/doc layout)"
    ;; 300 = 0x0000012C  -> bytes 4..7 = [0 0 1 44]
    (let [b (header-bytes 300)]
      (is (= 20 (count b)) "header is 20 bytes")
      (is (= [0 0 1 44] (subvec b 4 8)) "meta-size 300 big-endian at bytes 4-7"))
    ;; 70000 = 0x00011170 -> bytes 4..7 = [0 1 17 112]
    (is (= [0 1 17 112] (subvec (header-bytes 70000) 4 8)))
    ;; a value with a non-zero high byte (offset 4) the old single-byte format dropped
    ;; 16777216 = 0x01000000 -> [1 0 0 0]
    (is (= [1 0 0 0] (subvec (header-bytes 16777216) 4 8)))))

(deftest header-meta-size-roundtrip
  (testing "create-header -> parse-header round-trips meta-size, including > 255"
    ;; Note: cljs reserves the pattern [b4≠0, b5=b6=b7=0] (exact multiples of 16MB) for
    ;; legacy single-byte detection, so those sizes don't round-trip — but a metadata blob
    ;; is never 16MB, so this only excludes unrealistic sizes.
    (doseq [ms [0 1 44 200 255 256 300 1000 70000 1000000 16777215 16777217 16777300]]
      (is (= ms (roundtrip-meta-size ms)) (str "meta-size " ms)))))

#?(:cljs
   (deftest header-legacy-cljs-single-byte-read
     (testing "parse-header still reads a LEGACY cljs single-byte meta-size (byte 4, bytes 5-7 = 0)"
       ;; Simulate a header written by the old cljs writer: canonical bytes 0-3, then the
       ;; meta-size as a single byte at offset 4, bytes 5-19 left zero (Uint8Array zero-init).
       (doseq [ms [1 44 200 255]]
         (let [legacy (js/Uint8Array. (sl/create-header sl/default-version (ser/fressian-serializer)
                                                        null-compressor null-encryptor 0))]
           (aset legacy 4 ms) (aset legacy 5 0) (aset legacy 6 0) (aset legacy 7 0)
           (let [[_ _ _ _ parsed _] (sl/parse-header legacy ser/key->serializer)]
             (is (= ms parsed) (str "legacy single-byte meta-size " ms))))))))
