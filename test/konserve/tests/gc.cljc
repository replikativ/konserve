(ns konserve.tests.gc
  "gc/sweep! is async only"
  (:require [clojure.core.async :refer [go] :as a]
            [clojure.test :refer [deftest is testing]]
            #?(:cljs [fress.util :refer [byte-array]])
            [konserve.core :as k]
            [konserve.gc :as gc]
            [superv.async :refer [<?-]]))

(defn test-gc-async [store]
  (go
   (and
    (<?- (k/assoc store :foo :bar))
    (<?- (k/assoc-in store [:foo] :bar2))
    (<?- (k/assoc-in store [:foo2] :bar2))
    (<?- (k/assoc-in store [:foo3] :bar2))
    (let [_(a/<! (a/timeout 10))
          ts        #?(:cljs (js/Date.) :clj (java.util.Date.))
          whitelist #{:baz}]
      (a/<! (a/timeout 10))
      (and
       (is (= [:bar2 "bar2"]        (<?- (k/update-in store [:foo] name))))
       (is (= [nil {:bar 42}]       (<?- (k/assoc-in store  [:baz] {:bar 42}))))
       (is (= [{:bar 42} {:bar 43}] (<?- (k/update-in store [:baz :bar] inc))))
       (is (= [{:bar 43} {:bar 48}] (<?- (k/update-in store [:baz :bar] #(+ % 2 3)))))
       (is (true? (<?- (k/bassoc store :binbar (byte-array (into-array (range 10)))))))
       (is (= #{:foo :foo2 :foo3 :baz :binbar}
              (into #{} (map :key) (<?- (k/keys store)))))
       (is (= #{:foo2 :foo3} (<?- (gc/sweep! store whitelist ts))))
       (is (= #{:foo :baz :binbar}
              (into #{} (map :key) (<?- (k/keys store))))))))))
