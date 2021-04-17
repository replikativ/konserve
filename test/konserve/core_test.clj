(ns konserve.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.memory :refer [new-mem-store]]))

(deftest memory-store-compliance-test
  (compliance-test (<!! (new-mem-store))))

(deftest append-store-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-mem-store))]
      (<!! (k/append store :foo {:bar 42}))
      (<!! (k/append store :foo {:bar 43}))
      (is (= (<!! (k/log store :foo))
             '({:bar 42}
               {:bar 43})))
      (is (= (<!! (k/reduce-log store
                                :foo
                                (fn [acc elem]
                                  (conj acc elem))
                                []))
             [{:bar 42} {:bar 43}]))
      (let [{:keys [key type :konserve.core/timestamp]} (<!! (k/get-meta store :foo))]
        (are [x y] (= x y)
          :foo           key
          :append-log    type
          java.util.Date (clojure.core/type timestamp))))))




