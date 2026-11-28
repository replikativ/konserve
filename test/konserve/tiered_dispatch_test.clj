(ns konserve.tiered-dispatch-test
  "A burst of cold reads on a tiered store while its cache lock is held must
   neither stall core.async nor disturb the lock."
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.tiered :as tiered]))

(def ^:private pool-size
  (Long/parseLong (System/getProperty "clojure.core.async.pool-size" "8")))

(defn- go-block-runs?
  "Whether a fresh go block gets a dispatch thread within two seconds. Waits on
   a promise: core.async runs even a blocking take's callback on the dispatch
   pool, so `alts!!` with a timeout would hang on a stalled pool too."
  []
  (let [p (promise)]
    (a/go (deliver p :ran))
    (= :ran (deref p 2000 :blocked))))

(defn- take-within
  "The value of channel `ch`, or ::timeout after two seconds; see `go-block-runs?`."
  [ch]
  (let [p (promise)]
    (a/go (deliver p (a/<! ch)))
    (deref p 2000 ::timeout)))

(defn- tokens
  "How many lock tokens the cache lock holds right now, as 0, 1 or 2 (two or
   more). Leaves at most one behind and never blocks."
  [lock]
  (let [a (a/poll! lock)
        b (when a (a/poll! lock))]
    (when a (a/offer! lock :unlocked))
    (cond b 2 a 1 :else 0)))

(deftest cold-reads-while-the-cache-lock-is-held
  ;; async first: a stalled pool (the defect) stays stalled for the rest of
  ;; the JVM, so the phase that can cause it runs last
  (doseq [sync? [false true]]
    (testing (str ":sync? " sync? ": every read that misses the frontend starts
                  a detached cache fill. A synchronous fill used to wait for the
                  lock with a blocking take inside its go block, so while an
                  asynchronous holder kept the lock each cold read pinned a
                  dispatch thread, until the pool, and the holder's release
                  with it, stalled.")
      (let [frontend (new-mem-store (atom {}) {:sync? true})
            backend (new-mem-store (atom {}) {:sync? true})
            store (tiered/connect-tiered-store frontend backend :opts {:sync? true})
            opts {:sync? sync?}
            ;; bounded, so a stalled pool fails the test instead of hanging it
            read (fn [kk] (if sync?
                            (k/get store kk nil opts)
                            (take-within (k/get store kk nil opts))))
            ;; past the pool, below the fill budget (512)
            n (min (* 4 pool-size) 256)
            ks (mapv #(keyword (str "cold-" %)) (range n))]
        (doseq [kk ks] (k/assoc backend kk kk {:sync? true}))
        ;; the lock, held as an asynchronous operation would hold it
        (a/<!! (:cache-lock store))
        (let [live? (try
                      (doseq [kk ks]
                        (is (= kk (read kk)) "the read itself answers"))
                      (is (go-block-runs?) "an unrelated go block still runs")
                      (finally
                        ;; released whatever happened, so a failure here does
                        ;; not leave the lock held for later tests
                        (a/put! (:cache-lock store) :unlocked)))]
        ;; stop here if the pool is stalled; anything further would hang
          (when live?
            (is (go-block-runs?))
            (testing "the parked fills complete once the lock is free, and leave
                  exactly one lock token"
              (let [deadline (+ (System/currentTimeMillis) 5000)]
                (while (and (< (count (filter #(k/exists? frontend % {:sync? true}) ks)) n)
                            (< (System/currentTimeMillis) deadline))
                  (Thread/sleep 20)))
              (is (= n (count (filter #(k/exists? frontend % {:sync? true}) ks))))
              (Thread/sleep 100)
              (is (= 1 (tokens (:cache-lock store)))))))))))

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

(deftest an-interrupted-waiter-does-not-take-the-lock-with-it
  (testing "a synchronous caller waiting for the cache lock is interrupted. Its
            wait must not swallow the token the holder releases later"
    (let [frontend (new-mem-store (atom {}) {:sync? true})
          backend (new-mem-store (atom {}) {:sync? true})
          store (tiered/connect-tiered-store frontend backend :opts {:sync? true})
          lock (:cache-lock store)
          outcome (promise)]
      (k/assoc backend :cold 1 {:sync? true})
      (a/<!! lock)
      (let [t (Thread. #(deliver outcome
                                 (try (k/get store :cold nil {:sync? true :await-read-through? true})
                                      (catch Throwable e e))))]
        (.start t)
        (Thread/sleep 200)
        (.interrupt t)
        (is (instance? Throwable (deref outcome 2000 :blocked)) "the waiter gave up"))
      (a/>!! lock :unlocked)
      (Thread/sleep 200)
      (is (= 1 (tokens lock)) "the released token is still there")
      (testing "and the lock still works"
        ;; bounded: with the token lost this read would wait forever
        (is (= 1 (deref (future (k/get store :cold nil {:sync? true :await-read-through? true}))
                        2000 :blocked)))
        (is (k/exists? frontend :cold {:sync? true}))))))
