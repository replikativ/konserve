(ns konserve.core-test
  (:refer-clojure :exclude [get update-in assoc-in dissoc exists? keys])
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!! <! go chan put! close!] :as async]
            [konserve.core :refer :all]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.memory :refer [new-mem-store]]))

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




