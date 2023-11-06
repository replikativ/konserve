(ns konserve.filestore-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!! go chan put! close! <!] :as async]
            [konserve.core :refer [bassoc bget keys]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.tests.cache :as ct]
            [konserve.tests.gc :as gct]
            [konserve.tests.serializers :as st]))

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
