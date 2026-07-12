(ns konserve.filestore-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!! go chan put! close! <!] :as async]
            [clojure.java.io :as io]
            [konserve.core :as k :refer [bassoc bget keys]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.tests.cache :as ct]
            [konserve.tests.encryptor :as et]
            [konserve.tests.gc :as gct]
            [konserve.tests.serializers :as st]
            [konserve.tests.tiered :as tiered-tests]
            [konserve.memory :as memory]
            [konserve.tiered :as tiered]))

(deftest filestore-compliance-test
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (<!! (connect-fs-store folder))]
    (testing "Compliance test with default config."
      (compliance-test store))))

(deftest filestore-compliance-test-no-fsync
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (connect-fs-store folder :opts {:sync? true} :config {:sync-blob? false})]
    (testing "Compliance test without syncing."
      (compliance-test store))))

(deftest filestore-compliance-test-no-file-lock
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (<!! (connect-fs-store folder :config {:lock-blob? false}))]
    (testing "Compliance test without file locking."
      (compliance-test store))))

(defn create-tiered-stores [folder]
  (delete-store folder)
  {:frontend (<!! (memory/new-mem-store))
   :backend (<!! (connect-fs-store folder))})

(deftest tiered-store-filestore-backend-test
  (testing "Tiered Store with Filestore Backend"
    (let [folder "/tmp/konserve-tiered-fs-test"]

      (testing "Compliance (Async)"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-tiered-compliance-async frontend backend))
          (delete-store folder)))

      (testing "Compliance (Sync)"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (tiered-tests/test-tiered-compliance-sync frontend backend)
          (delete-store folder)))

      (testing "Write Policies"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-write-policies-async frontend backend))
          (delete-store folder)))

      (testing "Read Policies"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-read-policies-async frontend backend))
          (delete-store folder)))

      (testing "Key Operations"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-key-operations-async frontend backend))
          (delete-store folder)))

      (testing "Binary Operations"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-binary-operations-async frontend backend))
          (delete-store folder)))

      (testing "Sync on Connect"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-sync-on-connect-async frontend backend))
          (delete-store folder)))

      (testing "Error Handling"
        (tiered-tests/test-error-handling nil nil)))))

(deftest binary-polymorhism-test
  (testing "Test storage of different binary input formats."
    (let [folder "/tmp/konserve-fs-test"
          _      (spit "/tmp/foo" (range 1 10))
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))]
      (testing "Binary"
        (testing "ByteArray"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :byte-array (byte-array (range 10))))))
            (is (= true (<!! (bget store :byte-array
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (mapv byte (slurp input-stream)))))))))
            (is (=  (mapv byte (byte-array (range 10))) (<!! res-ch)))
            (close! res-ch)))
        (testing "CharArray"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :char-array (char-array "foo")))))
            (is (= true (<!! (bget store :char-array
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  "foo" (<!! res-ch)))))
        (testing "File Inputstream"
          (let [res-ch (chan)]
            (spit "/tmp/foo" (range 1 10))
            (is (= true (<!! (bassoc store :file-input-stream (java.io.FileInputStream. "/tmp/foo")))))
            (is (= true (<!! (bget store :file-input-stream
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  (str (range 1 10)) (<!! res-ch)))))
        (testing "Byte Array Inputstream"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :input-stream (java.io.ByteArrayInputStream. (byte-array (range 10)))))))
            (is (= true (<!! (bget store :input-stream
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (map byte (slurp input-stream)))))))))
            (is (=  (map byte (byte-array (range 10))) (<!! res-ch)))
            (close! res-ch)))
        (testing "String"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :string "foo bar"))))
            (is (= true (<!! (bget store :string
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (= "foo bar" (<!! res-ch)))))
        (testing "Reader"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :reader (java.io.StringReader. "foo bar")))))
            (is (= true (<!! (bget store :reader
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  "foo bar" (<!! res-ch))))))
      (delete-store folder)
      (let [store (<!! (connect-fs-store folder))]
        (is (= (<!! (keys store))
               #{}))))))

#!============
#! Cache tests

(deftest cache-PEDNKeyValueStore-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})]
    (<!! (ct/test-cached-PEDNKeyValueStore-async store))))

(deftest cache-PKeyIterable-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})]
    (<!! (ct/test-cached-PKeyIterable-async store))))

(deftest cache-PBin-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})
        f (fn [{:keys [input-stream]}]
            (async/to-chan! [input-stream]))]
    (<!! (ct/test-cached-PBin-async store f))))

#!============
#! GC tests

(deftest async-gc-test
  (delete-store "/tmp/gc-store")
  (let [store (connect-fs-store "/tmp/gc-store" :opts {:sync? true})]
    (<!! (gct/test-gc-async store))))

#!==================
#! Serializers tests

(deftest fressian-serializer-test
  (<!! (st/test-fressian-serializers-async "/tmp/serializers-test"
                                           connect-fs-store
                                           (fn [p] (go (delete-store p)))
                                           (fn [{:keys [input-stream]}]
                                             (async/to-chan! [input-stream])))))

(deftest CBOR-serializer-test
  (st/cbor-serializer-test "/tmp/konserve-fs-cbor-test"
                           connect-fs-store
                           (fn [p] (go (delete-store p)))))

#!==================
#! Encryptor tests

(deftest encryptor-sync-test
  (et/sync-encryptor-test "/tmp/encryptor-test"
                          connect-fs-store
                          delete-store))

(deftest encryptor-async-test
  (<!! (et/async-encryptor-test "/tmp/encryptor-test"
                                connect-fs-store
                                (fn [p] (go (delete-store p))))))

(deftest aes-gcm-sync-test
  (et/sync-aes-gcm-test "/tmp/aes-gcm-test"
                        connect-fs-store
                        delete-store))

(deftest aes-gcm-async-test
  (<!! (et/async-aes-gcm-test "/tmp/aes-gcm-test"
                              connect-fs-store
                              (fn [p] (go (delete-store p))))))

(deftest aes-gcm-wrong-key-test
  (<!! (et/async-aes-gcm-wrong-key-test "/tmp/aes-gcm-test"
                                        connect-fs-store
                                        (fn [p] (go (delete-store p))))))

(deftest seal-unseal-test
  (<!! (et/async-seal-unseal-test "/tmp/aes-gcm-test"
                                  connect-fs-store
                                  (fn [p] (go (delete-store p))))))

(deftest aes-gcm-tamper-test
  (testing "a flipped byte in a stored blob is rejected, not deserialized"
    (delete-store "/tmp/aes-gcm-tamper")
    (let [config {:encryptor {:type :aes-gcm :key et/gcm-key}}
          store  (<!! (connect-fs-store "/tmp/aes-gcm-tamper" :config config))]
      (<!! (k/assoc store :tampered {:balance 100}))
      (is (= {:balance 100} (<!! (k/get store :tampered)))))
    ;; flip the last byte of every blob: that lands in the GCM tag or the ciphertext
    (doseq [f (->> (file-seq (io/file "/tmp/aes-gcm-tamper"))
                   (filter #(.isFile ^java.io.File %)))]
      (let [bs (java.nio.file.Files/readAllBytes (.toPath ^java.io.File f))
            i  (dec (alength bs))]
        (aset bs i (unchecked-byte (bit-xor (aget bs i) 1)))
        (java.nio.file.Files/write (.toPath ^java.io.File f) bs
                                   ^"[Ljava.nio.file.OpenOption;" (into-array java.nio.file.OpenOption []))))
    (let [config {:encryptor {:type :aes-gcm :key et/gcm-key}}
          store  (<!! (connect-fs-store "/tmp/aes-gcm-tamper" :config config))
          res    (<!! (k/get store :tampered))]
      (is (instance? Throwable res) "tampering is detected"))
    (delete-store "/tmp/aes-gcm-tamper")))
