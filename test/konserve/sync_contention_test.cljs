(ns konserve.sync-contention-test
  "ClojureScript cannot wait synchronously for a lock held by an asynchronous
   operation: the holder can only finish on the event loop a waiting caller
   would occupy."
  (:require [cljs.core.async :refer [go <! timeout]]
            [cljs.test :refer [deftest is testing async]]
            [konserve.core :as k]
            [konserve.impl.mutex :as mutex]
            [konserve.memory :refer [new-mem-store]]
            [konserve.tiered :as tiered]))

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

(deftest a-write-through-write-during-a-fill-keeps-the-cache-coherent
  (testing "a synchronous write-through write commits to the backend, then
            updates the frontend under the store's cache lock. If an async fill
            holds that lock, the write cannot wait for it: it applies the update
            and evicts the key again once the holder is done, in case the
            holder writes the old value back when it resumes"
    (async done
           (let [frontend (new-mem-store (atom {}) {:sync? true})
                 backend (new-mem-store (atom {}) {:sync? true})
                 store (tiered/connect-tiered-store frontend backend :opts {:sync? true})
                 lock (:cache-lock store)]
             (k/assoc store :k 1 {:sync? true})
             (is (= 1 (k/get frontend :k nil {:sync? true})) "cached")
             (mutex/try-acquire! lock)                ; held, as by a parked async fill
             (is (some? (k/assoc store :k 2 {:sync? true})) "the write succeeds")
             (is (= 2 (k/get backend :k nil {:sync? true})))
             (is (= 2 (k/get store :k nil {:sync? true})) "and is read back at once")
             ;; the parked fill resumes and writes the value it read earlier
             (k/assoc frontend :k 1 {:sync? true})
             (mutex/release! lock)
             (go
               (<! (timeout 50))
               (is (= 2 (k/get store :k nil {:sync? true}))
                   "the deferred eviction removed the fill's stale value")
               (is (not (mutex/held? lock)))
               (done))))))
