(ns konserve.core-test
  (:refer-clojure :exclude [get-in update-in assoc-in dissoc exists?])
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [konserve.core :refer :all]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store delete-store list-keys]]
            [clojure.java.io :as io]))

(deftest memory-store-test
  (testing "Test the core API."
    (let [store (<!! (new-mem-store))]
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (assoc-in store [:foo] :bar))
      (is (= (<!! (get-in store [:foo]))
             :bar))
      (<!! (dissoc store :foo))
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (bassoc store :binbar (byte-array (range 10))))
      (<!! (bget store :binbar (fn [{:keys [input-stream]}]
                                 (is (= (map byte (slurp input-stream))
                                        (range 10)))))))))


(deftest append-store-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-mem-store))]
      (<!! (append store :foo {:bar 42}))
      (<!! (append store :foo {:bar 43}))
      (is (= (<!! (log store :foo))
             '({:bar 42}{:bar 43})))
      (is (= (<!! (reduce-log store
                              :foo
                              (fn [acc elem]
                                (conj acc elem))
                              []))
             [{:bar 42} {:bar 43}])))))


(deftest filestore-test
  (testing "Test the file store functionality."
    (let [folder "/tmp/konserve-fs-test"
          _      (delete-store folder)
          store  (<!! (new-fs-store folder))]
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (assoc-in store [:foo] :bar))
      (is (= (<!! (get-in store [:foo]))
             :bar))
      (is (= (<!! (list-keys store))
             #{{:key :foo, :format :edn} {:key :binbar, :format :binary}}))
      (<!! (dissoc store :foo))
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (bassoc store :binbar (byte-array (range 10))))
      (let [binbar (atom nil)]
        (<!! (bget store :binbar (fn [{:keys [input-stream]}]
                                   (reset! binbar (map byte (slurp input-stream))))))
        (is (= @binbar (range 10))))

      (is (= (<!! (list-keys store))
             #{{:key :binbar, :format :binary}})))))



