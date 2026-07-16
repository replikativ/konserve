(ns konserve.monotonic-clock-test
  "Pins the monotonic write-stamp clock (utils/monotonic-now-ms, utils/now).

   Why it exists: `:last-write` stamps are the currency of `konserve.gc/sweep!`
   and of external collectors (datahike's gc-guard safe-point). The sweep's
   safety argument — every object a guarded write sequence produces carries a
   stamp ≥ the sequence's start stamp — silently assumed the machine clock is
   monotonic. It is not: NTP step-backs, VM suspend/resume and manual clock
   sets move `System/currentTimeMillis` backwards, and under the old raw
   `(Date.)` stamping a live object could be stamped BEFORE the cutoff that
   guards it and be swept — store corruption. These tests make the clock
   contract executable: strictly increasing stamps, retreat tolerance, and
   `meta-update` actually consuming the monotonic source."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.utils :as utils]))

(deftest stamps-strictly-increase
  (testing "consecutive stamps are strictly increasing (no ties)"
    (let [stamps (repeatedly 1000 utils/monotonic-now-ms)]
      (is (every? true? (map < stamps (rest stamps)))
          "two stamps must never be equal — ties made sweep boundaries ambiguous"))))

(deftest clock-retreat-does-not-reverse-stamps
  (testing "a wall-clock step-back cannot produce a smaller stamp"
    (let [t0 (utils/monotonic-now-ms)]
      (binding [utils/*wall-clock-ms* (fn [] (- t0 60000))] ;; clock steps back 60s
        (let [t1 (utils/monotonic-now-ms)
              t2 (utils/monotonic-now-ms)]
          (is (> t1 t0) "stamp after retreat still advances")
          (is (> t2 t1) "and keeps advancing while the clock lags")))
      ;; recovery: once the wall clock is ahead again, stamps track it
      (let [t3 (utils/monotonic-now-ms)]
        (is (> t3 t0))))))

(deftest meta-update-uses-monotonic-stamps
  (testing "last-write stamps from meta-update are strictly ordered even
            under clock retreat"
    (let [m1 (utils/meta-update :k :edn nil)
          t (.getTime ^#?(:clj java.util.Date :cljs js/Date) (:last-write m1))]
      (binding [utils/*wall-clock-ms* (fn [] (- t 5000))]
        (let [m2 (utils/meta-update :k :edn m1)]
          (is (> (.getTime ^#?(:clj java.util.Date :cljs js/Date) (:last-write m2))
                 t)
              "a write during clock retreat must still stamp AFTER the previous write"))))))
