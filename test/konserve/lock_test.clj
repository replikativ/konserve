(ns konserve.lock-test
  "Regression tests for konserve.core in-process per-key lock lifecycle.

   Before the refcount fix, `konserve.core/get-lock` assoc'd a persistent lock
   channel per key into the store's `:locks` atom and NEVER removed it, so
   `:locks` grew one core.async ManyToManyChannel per DISTINCT key for the whole
   lifetime of the store (unbounded heap retention — acutely visible with
   datahike's content-addressed key space, which produced ~82k retained
   channels on a single long-lived store)."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!! <! timeout]]
            [konserve.core :refer [go-locked get-lock release-lock]]
            [konserve.memory :refer [new-mem-store]]))

(deftest locks-reclaimed-after-use
  (testing "go-locked reclaims its :locks entry instead of leaking it"
    (let [store (<!! (new-mem-store))
          locks (:locks store)]
      (is (zero? (count @locks)) "baseline: empty lock registry")

      (testing "single op reclaims its lock entry"
        (<!! (go-locked store :k :work))
        (is (zero? (count @locks))))

      (testing "repeated ops on ONE key do not accumulate"
        (dotimes [_ 100] (<!! (go-locked store :k :work)))
        (is (zero? (count @locks))))

      (testing "many DISTINCT keys do not accumulate (the heap-leak shape)"
        (dotimes [i 500] (<!! (go-locked store (str "key-" i) :work)))
        (is (zero? (count @locks)))))))

(deftest get-release-lock-refcount
  (testing "explicit get-lock/release-lock bracket reclaims only at refcount 0"
    (let [store (<!! (new-mem-store))
          locks (:locks store)]
      (get-lock store :k)
      (get-lock store :k)                       ;; two concurrent holders of :k
      (is (= 1 (count @locks)))
      (is (= 2 (:n (get @locks :k))))
      (release-lock store :k)
      (is (= 1 (count @locks)) "entry survives while one holder remains")
      (is (= 1 (:n (get @locks :k))))
      (release-lock store :k)
      (is (zero? (count @locks)) "entry reclaimed when the last holder releases")
      (release-lock store :k)                   ;; over-release is a safe no-op
      (is (zero? (count @locks))))))

(deftest mutual-exclusion-preserved-under-contention
  (testing "refcounted lock still serializes concurrent ops on the same key"
    (let [store   (<!! (new-mem-store))
          locks   (:locks store)
          counter (atom 0)
          n       200
          ;; Each op does a non-atomic read-modify-write while holding the lock,
          ;; PARKING (<! timeout) inside the critical section to widen the race
          ;; window. If a premature dissoc let two ops hold :shared at once,
          ;; updates would be lost and the final count would be < n.
          ops (mapv (fn [_]
                      (go-locked store :shared
                                 (let [v @counter]
                                   (<! (timeout 0))
                                   (reset! counter (inc v)))))
                    (range n))]
      (doseq [op ops] (<!! op))
      (is (= n @counter) "no lost updates: mutual exclusion held under contention")
      (is (zero? (count @locks)) "all locks reclaimed after contention"))))
