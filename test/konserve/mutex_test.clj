(ns konserve.mutex-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [konserve.impl.mutex :as mutex]))

(defn- queued [m] (count (:waiters @m)))

(defn- await-queued
  "Wait until `n` waiters are queued on `m`, so a test exercises what it says."
  [m n]
  (let [deadline (+ (System/currentTimeMillis) 10000)]
    (while (and (< (queued m) n) (< (System/currentTimeMillis) deadline))
      (Thread/sleep 5))
    (queued m)))

(defn- take-within [ch]
  (let [p (promise)]
    (a/take! ch #(deliver p %))
    (deref p 10000 ::timeout)))

(deftest one-holder-at-a-time
  (testing "async and sync acquirers mixed, on dispatch and plain threads"
    (let [m (mutex/mutex)
          inside (atom 0)
          most (atom 0)
          work (fn [] (swap! most max (swap! inside inc)) (Thread/sleep 1) (swap! inside dec))
          gos (doall (for [_ (range 200)]
                       (a/go (a/<! (mutex/wait-chan (mutex/acquire m)))
                             (try (work) (finally (mutex/release! m)))
                             :go)))
          threads (doall (for [_ (range 8)]
                           (future (dotimes [_ 20]
                                     (mutex/acquire-sync! m)
                                     (try (work) (finally (mutex/release! m))))
                                   :thread)))]
      (is (every? #{:go} (map take-within gos)))
      (is (every? #{:thread} (map #(deref % 10000 ::timeout) threads)))
      (is (= 1 @most))
      (is (not (mutex/held? m))))))

(deftest waiters-are-served-in-arrival-order
  (let [m (mutex/mutex)
        order (atom [])]
    (mutex/try-acquire! m)
    (let [chs (doall (for [i (range 10)]
                       (let [c (mutex/wait-chan (mutex/acquire m))]
                         (a/go (a/<! c) (swap! order conj i) (mutex/release! m)))))]
      (is (= 10 (await-queued m 10)))
      (mutex/release! m)
      (run! take-within chs)
      (is (= (range 10) @order)))))

(deftest no-limit-on-waiters
  (testing "a core.async channel refuses more than 1024 pending takes"
    (let [m (mutex/mutex)]
      (mutex/try-acquire! m)
      (let [chs (doall (for [_ (range 5000)]
                         (a/go (a/<! (mutex/wait-chan (mutex/acquire m))) (mutex/release! m) :ok)))]
        (is (= 5000 (await-queued m 5000)) "all of them wait at once")
        (mutex/release! m)
        ;; one overall deadline: a lost handoff fails in seconds, not hours
        (let [all (a/into [] (a/merge chs))
              [v _] (a/alts!! [all (a/timeout 30000)])]
          (is (= 5000 (count (filter #{:ok} v)))))))))

(deftest an-abandoned-wait-leaves-the-lock-intact
  (testing "abandoned while still queued: the waiter is removed"
    (let [m (mutex/mutex)]
      (mutex/try-acquire! m)
      (let [w (mutex/acquire m)]
        (is (= 1 (queued m)))
        (mutex/abandon! m w)
        (is (zero? (queued m))))
      (mutex/release! m)
      (is (not (mutex/held? m)))))
  (testing "abandoned after the lock was handed to it: the lock passes on"
    (let [m (mutex/mutex)]
      (mutex/try-acquire! m)
      (let [w1 (mutex/acquire m)
            w2 (mutex/acquire m)]
        (mutex/release! m)                        ; handed to w1
        (is (true? (take-within (mutex/wait-chan w1))))
        (mutex/abandon! m w1)                     ; w1 gave up anyway
        (is (true? (take-within (mutex/wait-chan w2))) "the next waiter gets it")
        (mutex/release! m)
        (is (not (mutex/held? m))))))
  (testing "a blocked thread whose wait ends in an exception (interrupt)"
    (let [m (mutex/mutex)
          outcome (promise)]
      (mutex/try-acquire! m)
      (let [t (Thread. #(deliver outcome (try (mutex/acquire-sync! m) (catch Throwable e e))))]
        (.start t)
        (is (= 1 (await-queued m 1)))
        (.interrupt t)
        (is (instance? InterruptedException (deref outcome 5000 :blocked))))
      (is (zero? (queued m)))
      (mutex/release! m)
      (is (not (mutex/held? m)))
      (is (true? (mutex/try-acquire! m)))
      (mutex/release! m))))

(deftest abandoning-twice-does-nothing
  (let [m (mutex/mutex)]
    (mutex/try-acquire! m)
    (let [w (mutex/acquire m)]
      (mutex/abandon! m w)
      (mutex/abandon! m w))
    (is (mutex/held? m) "the holder still holds it")
    (is (false? (mutex/try-acquire! m)) "and nobody else got in")
    (mutex/release! m)
    (is (not (mutex/held? m)))))

(deftest a-release-skips-a-waiter-that-gave-up
  (let [m (mutex/mutex)]
    (mutex/try-acquire! m)
    (let [w1 (mutex/acquire m)
          w2 (mutex/acquire m)]
      ;; w1 gives up without leaving the queue yet: mark only, as a race would
      (reset! (:state w1) :abandoned)
      (mutex/release! m)
      (is (true? (take-within (mutex/wait-chan w2))) "w2 gets it, not the abandoned w1")
      (mutex/release! m)
      (is (not (mutex/held? m))))))

(deftest go-checking-only-for-a-call-that-would-block
  (testing "with core.async's go-checking on, a synchronous acquire inside a go
            block throws only when it would block, as `<!!` does; a free lock
            is taken"
    (let [m (mutex/mutex)]
      (System/setProperty "clojure.core.async.go-checking" "true")
      (try
        (is (true? (take-within (a/go (mutex/acquire-sync! m)))) "free: taken")
        (is (instance? IllegalStateException
                       (take-within (a/go (try (mutex/acquire-sync! m) (catch Throwable e e)))))
            "held: refused")
        (is (zero? (queued m)) "and the refused call left no waiter behind")
        (finally
          (System/clearProperty "clojure.core.async.go-checking")
          (mutex/release! m))))))

(deftest releasing-a-free-lock-is-an-error
  (let [m (mutex/mutex)]
    (is (thrown? clojure.lang.ExceptionInfo (mutex/release! m)))
    (is (not (mutex/held? m)))))
