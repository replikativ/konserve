(ns konserve.tiered-dispatch-test
  "A burst of synchronous cold reads on a tiered store must not take the
   core.async dispatch pool down with it."
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.tiered :as tiered]))

(def ^:private pool-size
  (Long/parseLong (System/getProperty "clojure.core.async.pool-size" "8")))

(deftest cold-sync-reads-do-not-block-the-dispatch-pool
  (testing "every synchronous read that misses the frontend starts a detached
            cache fill. In sync mode that fill took the cache lock with a
            blocking take inside a go block, so while an asynchronous holder
            kept the lock, each cold read pinned one dispatch thread; once
            they were all pinned, nothing else could run, including the
            holder's own release"
    (let [frontend (new-mem-store (atom {}) {:sync? true})
          backend (new-mem-store (atom {}) {:sync? true})
          store (tiered/connect-tiered-store frontend backend :opts {:sync? true})
          n (* 4 pool-size)
          ks (mapv #(keyword (str "cold-" %)) (range n))]
      (doseq [kk ks] (k/assoc backend kk kk {:sync? true}))
      ;; the lock, held as an asynchronous operation would hold it
      (a/<!! (:cache-lock store))
      (doseq [kk ks]
        (is (= kk (k/get store kk nil {:sync? true})) "the read itself answers"))
      (is (= :ran (deref (future (a/<!! (a/go :ran))) 2000 :blocked))
          "an unrelated go block still runs")
      (a/>!! (:cache-lock store) :unlocked)
      (let [deadline (+ (System/currentTimeMillis) 5000)]
        (while (and (< (count (filter #(k/exists? frontend % {:sync? true}) ks)) n)
                    (< (System/currentTimeMillis) deadline))
          (Thread/sleep 20)))
      (is (= n (count (filter #(k/exists? frontend % {:sync? true}) ks)))
          "and the fills complete once the lock is free"))))
