(ns konserve.tests.gc
  "gc/sweep! is async only"
  (:require [clojure.core.async :refer [go] :as a]
            [clojure.test :refer [deftest is testing]]
            #?(:cljs [fress.util :refer [byte-array]])
            [konserve.core :as k]
            [konserve.gc :as gc]
            [konserve.utils :as utils]
            [superv.async :refer [<?-]]))

(defn test-gc-async [store]
  (go
    (and
     (<?- (k/assoc store :foo :bar))
     (<?- (k/assoc-in store [:foo] :bar2))
     (<?- (k/assoc-in store [:foo2] :bar2))
     (<?- (k/assoc-in store [:foo3] :bar2))
     ;; Cutoffs MUST come from the store's monotonic write clock (utils/now)
     ;; — during a wall-clock retreat the stamps hold ABOVE a raw (Date.)
     ;; read. The clock is non-strict (same-ms ties possible, spared by the
     ;; sweep — fail-safe), so let a beat pass before capturing the cutoff:
     ;; wall time advances past all prior stamps and the sweep below is
     ;; deterministic.
     (let [_         (a/<! (a/timeout 10))
           ts        (utils/now)
           whitelist #{:baz}]
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
               (into #{} (map :key) (<?- (k/keys store)))))

        (if (utils/multi-key-capable? store)
          (do
            (doseq [i (range 1 11)]
              (<?- (k/assoc store (keyword (str "test" i)) i)))
            (let [_ (a/<! (a/timeout 10))
                  ts2 (utils/now)]
              (and
               (is (= 13 (count (<?- (k/keys store)))))
               (is (= 10 (count (<?- (gc/sweep! store #{:foo :baz :binbar} ts2)))))
               (is (= #{:foo :baz :binbar}
                      (into #{} (map :key) (<?- (k/keys store)))))
               (let [_ (<?- (k/assoc store :batch1 1))
                     _ (<?- (k/assoc store :batch2 2))
                     _ (<?- (k/assoc store :batch3 3))
                     _ (<?- (k/assoc store :batch4 4))
                     _ (<?- (k/assoc store :batch5 5))
                     _ (a/<! (a/timeout 10))
                     ts3 (utils/now)]
                 (is (= 5 (count (<?- (gc/sweep! store #{:foo :baz :binbar} ts3 2)))))))))
          true))))))
