(ns konserve.gc-test
  (:require [konserve.gc :refer [sweep!]]
            [clojure.test :refer :all]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [clojure.core.async :refer [<!! <! go chan put! close!] :as async]))

(deftest memory-store-gc-test
  (testing "Test the GC."
    (let [store (<!! (new-mem-store))]
      (<!! (k/assoc store :foo :bar))
      (<!! (k/assoc-in store [:foo] :bar2))
      (<!! (k/assoc-in store [:foo2] :bar2))
      (<!! (k/assoc-in store [:foo3] :bar2))
      (<!! (async/timeout 1))
      (let [ts        (java.util.Date.)
            whitelist #{:baz}]
        (<!! (k/update-in store [:foo] name))
        (<!! (k/assoc-in store [:baz] {:bar 42}))
        (<!! (k/update-in store [:baz :bar] inc))
        (<!! (k/update-in store [:baz :bar] #(+ % 2 3)))
        (<!! (k/bassoc store :binbar (byte-array (range 10))))
        (is (= #{:foo2 :foo3} (<!! (sweep! store whitelist ts))))
        ;it seems clock of threads are not sync.
        #_(prn (.getTime ts) (map (fn [e] [(:key e) (-> e :konserve.core/timestamp .getTime)]) (<!! (keys store))))))))

(deftest file-store-gc-test
  (testing "Test the GC."
    (let [folder "/tmp/konserve-gc"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))]
      (<!! (k/assoc store :foo :bar))
      (<!! (k/assoc-in store [:foo] :bar2))
      (<!! (k/assoc-in store [:foo2] :bar2))
      (<!! (k/assoc-in store [:foo3] :bar2))
      (let [ts        (java.util.Date.)
            whitelist #{:baz}]
        (<!! (k/update-in store [:foo] name))
        (<!! (k/assoc-in store [:baz] {:bar 42}))
        (<!! (k/update-in store [:baz :bar] inc))
        (<!! (k/update-in store [:baz :bar] #(+ % 2 3)))
        (<!! (k/bassoc store :binbar (byte-array (range 10))))
        (is (= #{:foo2 :foo3} (<!! (sweep! store whitelist ts))))))))

