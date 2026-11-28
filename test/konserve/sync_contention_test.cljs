(ns konserve.sync-contention-test
  "ClojureScript cannot wait synchronously for a lock held by an asynchronous
   operation: the holder can only finish on the event loop a waiting caller
   would occupy."
  (:require [cljs.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.impl.mutex :as mutex]
            [konserve.memory :refer [new-mem-store]]))

(deftest a-taken-key-is-a-contention-error
  (let [store (new-mem-store (atom {}) {:sync? true})]
    (k/assoc store :k 1 {:sync? true})
    (let [lock (k/get-lock store :k)]
      (mutex/try-acquire! lock)                   ; held, as by a parked async op
      (try
        (is (= :konserve/sync-lock-contention
               (try (k/get store :k nil {:sync? true}) nil
                    (catch :default e (:type (ex-data e)))))
            "a synchronous operation on the taken key is refused")
        (is (mutex/held? lock) "and the holder keeps the lock")
        (finally
          (mutex/release! lock)
          (k/release-lock store :k))))
    (testing "once the key is free the synchronous operation runs"
      (is (= 1 (k/get store :k nil {:sync? true}))))))
