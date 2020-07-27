(ns konserve.compliance-test
  (:refer-clojure :exclude [get update-in assoc-in dissoc exists? keys])
  (:require [clojure.core.async :refer [<!! go chan]]
            [konserve.core :refer :all]
            #?(:clj [clojure.test :refer :all]
               :cljs [cljs.test :refer :all :include-macros true])))

(defn compliance-test [store]
  (testing "Test the core API."
    (let [store (<!! store)]
      (is (= (<!! (get store :foo))
             nil))
      (<!! (assoc store :foo :bar))
      (is (= (<!! (get store :foo))
             :bar))
      (<!! (assoc-in store [:foo] :bar2))
      (is (= :bar2 (<!! (get store :foo))))
      (is (= :default
             (<!! (get-in store [:fuu] :default))))
      (is (= :bar2 (<!! (get store :foo))))
      (is (= :default
             (<!! (get-in store [:fuu] :default))))
      (<!! (update-in store [:foo] name))
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
                                          (range 10)))))))
      (let  [{:keys [key type :konserve.core/timestamp]} (first (<!! (keys store)))]
         (are [x y] (= x y)
           (type (java.util.Date.)) (type timestamp)
           true (contains? #{:foo :baz} key)
           true (contains? #{:edn} type))))))
