(ns konserve.tiered-dispatch-test
  "A burst of cold reads on a tiered store while its cache lock is held must
   neither stall core.async nor disturb the lock."
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.tiered :as tiered]))

(defn- tokens
  "How many lock tokens the cache lock holds right now, as 0, 1 or 2 (two or
   more). Leaves at most one behind and never blocks."
  [lock]
  (let [a (a/poll! lock)
        b (when a (a/poll! lock))]
    (when a (a/offer! lock :unlocked))
    (cond b 2 a 1 :else 0)))

(defn- dispatch-threads-blocked-on-the-lock
  "core.async dispatch threads whose stack is inside a blocking cache-lock take."
  []
  (count (for [[^Thread t frames] (Thread/getAllStackTraces)
               :when (.startsWith (.getName t) "async-dispatch")
               :when (some #(.contains (str %) "take_cache_lock_sync") frames)]
           t)))

(deftest a-detached-fill-does-not-block-a-dispatch-thread
  (testing "a cold read starts a detached cache fill. For a synchronous read
            that fill took the cache lock with a blocking take inside its go
            block, so while an asynchronous holder kept the lock it pinned a
            core.async dispatch thread, and a burst of cold reads stalled the
            whole pool. Checked by looking at the dispatch threads rather than
            by stalling the pool, which would strand the rest of the suite."
    (doseq [sync? [true false]]
      (let [frontend (new-mem-store (atom {}) {:sync? true})
            backend (new-mem-store (atom {}) {:sync? true})
            store (tiered/connect-tiered-store frontend backend :opts {:sync? true})
            lock (:cache-lock store)
            ks (mapv #(keyword (str "cold-" %)) (range 3))]
        (doseq [kk ks] (k/assoc backend kk kk {:sync? true}))
        ;; the lock, held as an asynchronous operation would hold it
        (a/<!! lock)
        (try
          (doseq [kk ks]
            (is (= kk (if sync?
                        (k/get store kk nil {:sync? true})
                        (a/<!! (k/get store kk nil {:sync? false})))))
            "the read itself answers")
          (Thread/sleep 300)
          (is (zero? (dispatch-threads-blocked-on-the-lock))
              (str ":sync? " sync? ": no dispatch thread waits for the lock"))
          (finally (a/>!! lock :unlocked)))
        (let [deadline (+ (System/currentTimeMillis) 5000)]
          (while (and (< (count (filter #(k/exists? frontend % {:sync? true}) ks)) (count ks))
                      (< (System/currentTimeMillis) deadline))
            (Thread/sleep 20)))
        (is (= (count ks) (count (filter #(k/exists? frontend % {:sync? true}) ks)))
            "the fills complete once the lock is free")
        (Thread/sleep 100)
        (is (= 1 (tokens lock)) "and leave exactly one lock token")))))

(deftest a-frontend-hook-may-write-to-the-store
  (testing "a detached fill runs the frontend's write hooks after releasing the
            cache lock; a hook that writes to the tiered store must not wait
            for a lock its own fill holds"
    (let [frontend (new-mem-store (atom {}) {:sync? true})
          backend (new-mem-store (atom {}) {:sync? true})
          store (tiered/connect-tiered-store frontend backend :opts {:sync? true})
          done (promise)]
      (k/assoc backend :cold 1 {:sync? true})
      (k/add-write-hook! frontend ::audit
                         (fn [event]
                           (when (= :cold (:key event))
                             (k/assoc store :audit 2 {:sync? true})
                             (deliver done :written))))
      (is (= 1 (k/get store :cold nil {:sync? true})))
      (is (= :written (deref done 2000 :blocked)))
      (is (= 2 (k/get backend :audit nil {:sync? true})))
      (is (= 1 (tokens (:cache-lock store)))))))
