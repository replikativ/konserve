(ns konserve.core-test
  (:refer-clojure :exclude [get update-in assoc-in dissoc exists? keys])
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!! <! go chan put! close!] :as async]
            [konserve.core :refer :all]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store delete-store]]
            [clojure.java.io :as io]))

(deftest memory-store-compliance-test
  (compliance-test (<!! (new-mem-store))))

(deftest append-store-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-mem-store))]
      (<!! (append store :foo {:bar 42}))
      (<!! (append store :foo {:bar 43}))
      (is (= (<!! (log store :foo))
             '({:bar 42}
               {:bar 43})))
      (is (= (<!! (reduce-log store
                              :foo
                              (fn [acc elem]
                                (conj acc elem))
                              []))
             [{:bar 42} {:bar 43}]))
      (let [{:keys [key type :konserve.core/timestamp]} (<!! (get-meta store :foo))]
        (are [x y] (= x y)
          :foo           key
          :append-log    type
          java.util.Date (clojure.core/type timestamp))))))

(deftest filestore-test
  (testing "Test the file store functionality."
    (let [folder "/tmp/konserve-fs-test"
          _      (spit "/tmp/foo" (range 1 10))
          _      (delete-store folder)
          store  (<!! (new-fs-store folder))]
      (testing "edn" (is (= (<!! (get store :foo))
                            nil))
               (<!! (assoc-in store [:foo] {:bar :baz}))
               (is (= (<!! (get store :foo))
                      {:bar :baz}))
               (<!! (assoc-in store [:baz] 0))
               (<!! (update-in store [:baz] inc))
               (is (= (<!! (get store :baz)) 1))
               (is (= (<!! (keys store))
                      #{(<!! (get-meta store :foo)) (<!! (get-meta store :baz))}))
               (<!! (dissoc store :foo))
               (is (= (<!! (get store :foo))
                      nil))
               (is (= (<!! (keys store))
                      #{(<!! (get-meta store :baz))})))
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
            (is (=  "foo bar" (<!! res-ch)))))
        (testing "Reader"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :reader (java.io.StringReader. "foo bar")))))
            (is (= true (<!! (bget store :reader
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  "foo bar" (<!! res-ch))))))
      (delete-store folder)
      (let [store (<!! (new-fs-store folder))]
        (is (= (<!! (keys store))
               #{}))))))


