(ns konserve.gc-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc keys])
  (:require [konserve.gc :refer [sweep!]]
            [clojure.test :refer :all]
            [konserve.core :refer :all]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store delete-store]]
            [clojure.core.async :refer [<!! <! go chan put! close!] :as async]))

(deftest memory-store-gc-test
  (testing "Test the GC."
    (let [store (<!! (new-mem-store))]
      (<!! (assoc store :foo :bar))
      (<!! (assoc-in store [:foo] :bar2))
      (<!! (assoc-in store [:foo2] :bar2))
      (<!! (assoc-in store [:foo3] :bar2))
      (<!! (async/timeout 1))
      (let [ts        (java.util.Date.)
            whitelist #{:baz}]
        (<!! (update-in store [:foo] name))
        (<!! (assoc-in store [:baz] {:bar 42}))
        (<!! (update-in store [:baz :bar] inc))
        (<!! (update-in store [:baz :bar] + 2 3))
        (<!! (bassoc store :binbar (byte-array (range 10))))
        (is (= #{:foo2 :foo3} (<!! (sweep! store whitelist ts))))
        ;it seems clock of threads are not sync.
        #_(prn (.getTime ts) (map (fn [e] [(:key e) (-> e :konserve.core/timestamp .getTime)]) (<!! (keys store))))))))
 
(deftest file-store-gc-test
  (testing "Test the GC."
    (let [folder "/tmp/konserve-gc"
          _      (delete-store folder)
          store  (<!! (new-fs-store folder))]
      (<!! (assoc store :foo :bar))
      (<!! (assoc-in store [:foo] :bar2))
      (<!! (assoc-in store [:foo2] :bar2))
      (<!! (assoc-in store [:foo3] :bar2))
      (let [ts        (java.util.Date.)
            whitelist #{:baz}]
        (<!! (update-in store [:foo] name))
        (<!! (assoc-in store [:baz] {:bar 42}))
        (<!! (update-in store [:baz :bar] inc))
        (<!! (update-in store [:baz :bar] + 2 3))
        (<!! (bassoc store :binbar (byte-array (range 10))))
        (is (= #{:foo2 :foo3} (<!! (sweep! store whitelist ts))))))))

