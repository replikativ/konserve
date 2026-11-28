(ns konserve.mutex-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [konserve.impl.mutex :as mutex]))

(deftest one-holder-at-a-time
  (testing "async and sync acquirers mixed, on dispatch and plain threads"
    (let [m (mutex/mutex)
          inside (atom 0)
          most (atom 0)
          work (fn [] (swap! most max (swap! inside inc)) (Thread/sleep 1) (swap! inside dec))
          gos (doall (for [_ (range 200)]
                       (a/go (a/<! (mutex/acquire m))
                             (try (work) (finally (mutex/release! m)))
                             :go)))
          threads (doall (for [_ (range 8)]
                           (future (dotimes [_ 20]
                                     (mutex/acquire-sync! m)
                                     (try (work) (finally (mutex/release! m))))
                                   :thread)))]
      (is (every? #{:go} (map a/<!! gos)))
      (is (every? #{:thread} (map deref threads)))
      (is (= 1 @most))
      (is (not (mutex/held? m))))))

(deftest waiters-are-served-in-arrival-order
  (let [m (mutex/mutex)
        order (atom [])]
    (mutex/try-acquire! m)
    (let [chs (doall (for [i (range 10)]
                       (let [c (mutex/acquire m)]
                         (a/go (a/<! c) (swap! order conj i) (mutex/release! m)))))]
      (mutex/release! m)
      (run! a/<!! chs)
      (is (= (range 10) @order)))))

(deftest no-limit-on-waiters
  (testing "a core.async channel refuses more than 1024 pending takes"
    (let [m (mutex/mutex)]
      (mutex/try-acquire! m)
      (let [chs (doall (for [_ (range 5000)]
                         (a/go (a/<! (mutex/acquire m)) (mutex/release! m) :ok)))]
        (mutex/release! m)
        (is (= 5000 (count (filter #{:ok} (map a/<!! chs)))))))))

(deftest an-interrupted-waiter-leaves-the-lock-intact
  (testing "a waiter interrupted while queued withdraws; the holder's release
            then frees the lock instead of handing it to the abandoned waiter"
    (let [m (mutex/mutex)
          outcome (promise)]
      (mutex/try-acquire! m)
      (let [t (Thread. #(deliver outcome (try (mutex/acquire-sync! m) (catch InterruptedException e e))))]
        (.start t)
        (Thread/sleep 100)
        (.interrupt t)
        (is (instance? InterruptedException (deref outcome 2000 :blocked))))
      (mutex/release! m)
      (is (not (mutex/held? m)))
      (is (true? (mutex/try-acquire! m)))
      (mutex/release! m)))
  (testing "a waiter interrupted just as the lock is handed to it releases it"
    (let [m (mutex/mutex)
          granted (promise)
          outcome (promise)]
      (mutex/try-acquire! m)
      (let [t (Thread. #(deliver outcome
                                 (try (mutex/acquire-sync! m) :acquired
                                      (catch InterruptedException e e))))]
        (.start t)
        (Thread/sleep 100)
        ;; hand over and interrupt as close together as the test can
        (.interrupt t)
        (mutex/release! m)
        (let [r (deref outcome 2000 :blocked)]
          (when (= :acquired r) (mutex/release! m))
          (is (not= :blocked r))))
      (Thread/sleep 50)
      (is (not (mutex/held? m)) "either way the lock ends up free"))))

(deftest releasing-a-free-lock-is-an-error
  (let [m (mutex/mutex)]
    (is (thrown? clojure.lang.ExceptionInfo (mutex/release! m)))
    (is (not (mutex/held? m)))))
