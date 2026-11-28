(ns konserve.lock-test
  "Regression tests for konserve.core in-process per-key lock lifecycle.

   Before the refcount fix, `konserve.core/get-lock` assoc'd a persistent lock
   channel per key into the store's `:locks` atom and NEVER removed it, so
   `:locks` grew one core.async ManyToManyChannel per DISTINCT key for the whole
   lifetime of the store (unbounded heap retention — acutely visible with
   datahike's content-addressed key space, which produced ~82k retained
   channels on a single long-lived store)."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!! <! go timeout alts!! poll! offer!]]
            [konserve.core :refer [go-locked get-lock release-lock locked]]
            [konserve.impl.mutex :as mutex]
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

(deftest last-holder-can-release-without-a-waiter
  (let [store (<!! (new-mem-store))
        lock (get-lock store :k)]
    (try
      (is (true? (mutex/try-acquire! lock)))
      (is (nil? (mutex/release! lock)))
      (is (not (mutex/held? lock)))
      (is (thrown? clojure.lang.ExceptionInfo (mutex/release! lock))
          "a lock that is not held cannot be released")
      (finally (release-lock store :k)))))

(deftest more-than-1024-async-waiters-on-one-key
  (testing "the lock was a core.async channel, which refuses more than 1024
            pending takes; the 1025th go-locked waiter threw inside the go
            block and released a lock it never took"
    (let [store (<!! (new-mem-store))
          lock (get-lock store :k)
          inside (atom 0)
          most (atom 0)
          n 3000]
      (try
        (is (true? (mutex/try-acquire! lock)))
        (let [chs (doall (for [_ (range n)]
                           (go-locked store :k
                                      (swap! most max (swap! inside inc))
                                      (<! (timeout 0))
                                      (swap! inside dec)
                                      :done)))]
          (let [deadline (+ (System/currentTimeMillis) 10000)]
            (while (and (< (count (:waiters @lock)) n) (< (System/currentTimeMillis) deadline))
              (Thread/sleep 5)))
          (is (= n (count (:waiters @lock))) "all of them wait at once")
          (mutex/release! lock)
          ;; one overall deadline: a lost handoff fails in seconds
          (let [[v _] (alts!! [(clojure.core.async/into [] (clojure.core.async/merge chs))
                               (timeout 30000)])]
            (is (= (repeat n :done) v)))
          (is (= 1 @most) "one holder at a time"))
        (finally (release-lock store :k))))))

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

;; -----------------------------------------------------------------------------
;; Randomized concurrency stress.
;;
;; The reclaim mechanism is konserve's per-key isolation (the "I" in ACID, given a
;; single-writer runtime). A regression could surface three ways, and this test is
;; built to make each one observable:
;;
;;   1. Lost mutual exclusion — a reclaimed entry hands a fresh channel to a 2nd
;;      writer while the key is still held → two writers in one CS at once.
;;   2. Premature reclaim / lost wakeup — a channel a waiter is parked on gets
;;      discarded → that waiter never wakes → deadlock.
;;   3. The original leak — the registry never shrinks.
;;
;; Oracles (each DETERMINISTIC for a correct impl under ANY interleaving, so a
;; correct impl never flakes; a broken one fails with high probability):
;;   - per-key MAX OCCUPANCY must be 1            — direct witness of (1)
;;   - a NON-ATOMIC per-key counter must equal    — any single lost update fails;
;;     the op count                                 a sensitive amplifier of (1)
;;   - every op must finish within a wall budget  — catches (2) instead of hanging
;;   - :locks must be empty at quiescence         — catches (3) and over-reclaim
;;
;; Fault-detection is maximized by: short critical sections that PARK mid-way to
;; widen the interleaving window; staggered arrivals (seeded jitter) so a key
;; oscillates idle↔busy across the n=0 reclaim boundary; contention regimes from
;; one hot key to a spray of single-use keys; and BOTH acquire paths (async
;; `go-locked` parking + sync `locked` spinning) on the same keys.

(defn- stress-round
  "Run one randomized round against a fresh `store`. Returns a result map of the
   four oracles. `sync-every` n>0 routes every n-th op through the sync `locked`
   path (real thread, spin-acquire); 0 = all async."
  [store {:keys [keys-n per-key sync-every round-timeout-ms seed]}]
  (let [locks    (:locks store)
        rng      (java.util.Random. (long seed))
        jit      (fn [] (.nextInt rng 2))                 ; 0..1 ms, seeded
        occ      (atom {})                                ; key -> # in CS now
        max-occ  (atom 0)
        counter  (atom {})                                ; key -> non-atomic count
        enter    (fn [k] (swap! max-occ max (get (swap! occ update k (fnil inc 0)) k)))
        leave    (fn [k] (swap! occ update k (fnil dec 0)))
        ks       (mapv #(str "k-" %) (range keys-n))
        idx      (atom -1)
        results  (vec
                  (for [k ks _ (range per-key)]
                    (let [i     (swap! idx inc)
                          sync? (and (pos? (long (or sync-every 0)))
                                     (zero? (mod i (long sync-every))))
                          pre   (jit)]                     ; arrival stagger (seeded)
                      (if sync?
                        {:future
                         (future
                           (Thread/sleep (long pre))
                           (locked store k
                                   (enter k)
                                   (let [v (get @counter k 0)] ; non-atomic RMW:
                                     (Thread/sleep (long (jit)))
                                     (swap! counter assoc k (inc v))) ; ...stale write
                                   (leave k)))}
                        {:chan
                         (go
                           (<! (timeout pre))             ; stagger OUTSIDE the lock
                           (<! (go-locked store k
                                          (enter k)
                                          (let [v (get @counter k 0)]
                                            (<! (timeout (jit)))
                                            (swap! counter assoc k (inc v)))
                                          (leave k))))}))))
        chans    (keep :chan results)
        futs     (keep :future results)
        ;; shared wall budget so a deadlock FAILS the round instead of hanging
        deadline (timeout round-timeout-ms)
        ok-chans (every? (fn [c] (let [[_ port] (alts!! [c deadline])] (= port c))) chans)
        end      (+ (System/currentTimeMillis) round-timeout-ms)
        ok-futs  (every? (fn [f] (not= ::timeout
                                       (deref f (max 0 (- end (System/currentTimeMillis)))
                                              ::timeout)))
                         futs)]
    {:max-occ      @max-occ
     :counts-ok    (= (zipmap ks (repeat per-key)) @counter)
     :no-deadlock  (and ok-chans ok-futs)
     :leaked       (count @locks)
     :occ-residual (reduce + 0 (vals @occ))}))

(deftest lock-stress
  (testing "randomized stress: isolation holds, locks reclaim, no deadlock"
    (let [regimes [{:label "hot key (async)"          :keys-n 1   :per-key 300 :sync-every 0}
                   {:label "few keys oscillating"     :keys-n 6   :per-key 80  :sync-every 4}
                   {:label "many keys churn boundary" :keys-n 150 :per-key 3   :sync-every 5}
                   {:label "spray (leak shape)"       :keys-n 400 :per-key 1   :sync-every 0}
                   {:label "mixed sync+async"         :keys-n 3   :per-key 60  :sync-every 2}]
          rounds  3]
      (doseq [[ri regime] (map-indexed vector regimes)
              r           (range rounds)]
        (let [store (<!! (new-mem-store))
              res   (stress-round store (assoc regime
                                               :round-timeout-ms 20000
                                               :seed (+ (* 100 (inc ri)) r)))
              tag   (str (:label regime) " / round " r " -> " res)]
          (is (= 1 (:max-occ res))        (str "two writers entered one key's CS: " tag))
          (is (:counts-ok res)            (str "lost update — mutual exclusion broke: " tag))
          (is (:no-deadlock res)          (str "deadlock / lost wakeup: " tag))
          (is (zero? (:leaked res))       (str "registry not reclaimed: " tag))
          (is (zero? (:occ-residual res)) (str "occupancy residual: " tag)))))))

(deftest a-throw-right-after-acquiring-releases-the-key
  (testing "everything after the lock is taken runs inside the bracket that
            releases it: a throwing metrics sink or logger must not leave the
            key locked for the operation queued behind it"
    (let [store (<!! (new-mem-store))
          calls (atom 0)
          real konserve.metrics/finish!
          lock (get-lock store :k)
          queued (fn [n] (let [deadline (+ (System/currentTimeMillis) 5000)]
                           (while (and (< (count (:waiters @lock)) n)
                                       (< (System/currentTimeMillis) deadline))
                             (Thread/sleep 5))))]
      (with-redefs [konserve.metrics/finish!
                    (fn [& args]
                      (if (= 1 (swap! calls inc))
                        (throw (ex-info "sink failed" {}))
                        (apply real args)))]
        (try
          (mutex/try-acquire! lock)
          (let [first-op (future (try (locked store :k :first) (catch Exception e e)))
                _ (queued 1)
                second-op (go-locked store :k :second)]
            (queued 2)
            (is (= 2 (count (:waiters @lock))) "both operations wait for the key")
            (mutex/release! lock)
            (is (instance? Exception (deref first-op 5000 :blocked)) "the first operation throws")
            (is (= :second (first (alts!! [second-op (timeout 5000)])))
                "and the one queued behind it still gets the key"))
          (finally (release-lock store :k))))
      (is (zero? (count @(:locks store)))))))
