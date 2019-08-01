(ns konserve.core-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists?])
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!! go]]
            [konserve.core :refer :all]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store delete-store list-keys]]
            [clojure.java.io :as io]))

(deftest memory-store-test
  (testing "Test the core API."
    (let [store (<!! (new-mem-store))]
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (assoc store :foo :bar))
      (is (= (<!! (get store :foo))
             :bar))
      (<!! (assoc-in store [:foo] :bar2))
      (is (= :bar2 (<!! (get store :foo))))
      (is (= :default
             (<!! (get-in store [:fuu] :default))))
      (<!! (update store :foo name))
      (is (= "bar2"
             (<!! (get store :foo))))
      (<!! (assoc-in store [:baz] {:bar 42}))
      (is (= (<!! (get-in store [:baz :bar]))
             42))
      (<!! (update-in store [:baz :bar] inc))
      (is (= (<!! (get-in store [:baz :bar]))
             43))
      (<!! (update-in store [:baz :bar] + 2 3))
      (is (= (<!! (get-in store [:baz :bar]))
             48))
      (<!! (dissoc store :foo))
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (bassoc store :binbar (byte-array (range 10))))
      (<!! (bget store :binbar (fn [{:keys [input-stream]}]
                                 (go
                                   (is (= (map byte (slurp input-stream))
                                          (range 10))))))))))

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
          store  (<!! (new-fs-store folder))
          _      (<!! (bassoc store :binbar (byte-array (range 10))))
          binbar (atom nil)
          _ (<!! (bget store :binbar
                       (fn [{:keys [input-stream]}]
                         (go
                           (reset! binbar (map byte (slurp input-stream)))))))]
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
      (is (= (<!! (list-keys store))
             #{{:key :binbar, :format :binary}}))
      (is (= @binbar (range 10)))
      (delete-store folder)
      (let [store (<!! (new-fs-store folder))]
        (is (= (<!! (list-keys store))
               #{}))))))

