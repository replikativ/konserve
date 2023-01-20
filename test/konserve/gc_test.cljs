(ns konserve.gc-test
  (:require [clojure.core.async :refer [<! go timeout]]
            [cljs.test :refer [deftest testing is async]]
            [konserve.gc :as gc]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.node-filestore :refer [connect-fs-store delete-store]]))

(deftest memory-store-gc-test
  (async done
         (go
           (testing "Test the GC."
             (let [store (<! (new-mem-store))]
               (<! (k/assoc store :foo :bar))
               (<! (k/assoc-in store [:foo] :bar2))
               (<! (k/assoc-in store [:foo2] :bar2))
               (<! (k/assoc-in store [:foo3] :bar2))
               (<! (timeout 1))
               (let [ts (js/Date.)
                     whitelist #{:baz}]
                 (<! (k/update-in store [:foo] name))
                 (<! (k/assoc-in store [:baz] {:bar 42}))
                 (<! (k/update-in store [:baz :bar] inc))
                 (<! (k/update-in store [:baz :bar] #(+ % 2 3)))
                 (<! (k/bassoc store :binbar (js/Buffer.from  (into-array (range 10)))))
                 (is (= #{:foo2 :foo3} (<! (gc/sweep! store whitelist ts))))))
             (done)))))

(deftest file-store-gc-test
  (async done
         (go
           (let [folder "/tmp/konserve-gc"
                 _ (delete-store folder)
                 store (<! (connect-fs-store folder))]
             (<! (k/assoc store :foo :bar))
             (<! (k/assoc-in store [:foo] :bar2))
             (<! (k/assoc-in store [:foo2] :bar2))
             (<! (k/assoc-in store [:foo3] :bar2))
             (let [ts        (js/Date.)
                   whitelist #{:baz}]
               (<! (k/update-in store [:foo] name))
               (<! (k/assoc-in store [:baz] {:bar 42}))
               (<! (k/update-in store [:baz :bar] inc))
               (<! (k/update-in store [:baz :bar] #(+ % 2 3)))
               (<! (k/bassoc store :binbar (js/Buffer.from  (into-array (range 10)))))
               (is (= #{:foo2 :foo3} (<! (gc/sweep! store whitelist ts))))))
           (done))))
