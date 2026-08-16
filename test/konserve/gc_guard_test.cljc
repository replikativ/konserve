(ns konserve.gc-guard-test
  "Pins the store's SAFE POINT against the race it exists to close.

   A persistent index on konserve writes every value the new state references
   and only THEN the pointer that makes them reachable. Inside that window the
   fresh values are reachable from nothing while already being older than `now`,
   so a sweep with a `now` cutoff deletes them and the pointer lands on holes.

   The guard's own logic is platform-independent and tested on both. The tests
   that assert the CONSEQUENCE — a sweep eating or sparing real values — drive
   a store with blocking takes and so are JVM-only; `konserve.gc/sweep!` is
   async-only, and expressing these as cljs async tests would obscure what they
   are demonstrating without covering anything the guard does differently there."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string]
            [konserve.gc-guard :as guard]
            [konserve.utils :as utils]
            #?@(:clj [[clojure.core.async :refer [<!!]]
                      [konserve.core :as k]
                      [konserve.gc :as gc]
                      [konserve.store :as ks]
                      [konserve.memory :refer [new-mem-store]]])))

;; =============================================================================
;; Guard logic — both platforms
;; =============================================================================

(deftest safe-point-is-now-when-nothing-in-flight
  (testing "no sequence open => the mark's verdict on everything written so far
            is final, so the cutoff is the caller's own instant"
    (let [sid (random-uuid)
          before (utils/now)
          sp (guard/safe-point sid)]
      (is (not (guard/in-flight? sid)))
      (is (>= (.getTime sp) (.getTime before))))))

(deftest safe-point-retreats-to-the-oldest-open-sequence
  (testing "the cutoff is the START of the oldest in-flight sequence, so every
            object that sequence writes lands at or after it and is spared"
    ;; Drive the clock rather than waiting on wall time: the stamps are pinned to
    ;; wall time, so consecutive reads land in the same millisecond and could not
    ;; distinguish a retreating safe point from a stationary one.
    ;;
    ;; The high-water mark is process-global and `max`-based, so driving it
    ;; forward here would leave every LATER test stamping ahead of wall time —
    ;; which silently breaks any test that compares a write stamp against a
    ;; cutoff (including this file's own sweep tests). Restore it.
    (let [sid (random-uuid)
          stamp-atom @#'utils/last-stamp
          saved @stamp-atom
          clock (atom (+ 1000 (utils/monotonic-now-ms)))]
      (try
        (binding [utils/*wall-clock-ms* #(deref clock)]
          (let [t1 (guard/writing! sid)
                _ (swap! clock + 1000)
                t2 (guard/writing! sid)
                _ (swap! clock + 1000)
                sp (guard/safe-point sid)
                latest (utils/now)]
            (is (guard/in-flight? sid))
            (is (< (.getTime sp) (.getTime latest))
                "safe point must not be `now` while a sequence is open")
            (guard/done! sid t2)
            (is (= (.getTime sp) (.getTime (guard/safe-point sid)))
                "closing the YOUNGER sequence must not advance the safe point")
            (guard/done! sid t1)
            (is (not (guard/in-flight? sid)))
            (is (>= (.getTime (guard/safe-point sid)) (.getTime latest))
                "with all sequences closed the safe point returns to now")))
        (finally (reset! stamp-atom saved))))))

(deftest cutoff-never-runs-ahead-of-the-collection-start
  (testing "`cutoff` takes the min, so a caller cannot collect into the future
            by passing a later instant — and cannot miss an open sequence"
    (let [sid (random-uuid)
          started (utils/now)]
      (is (= (.getTime started) (.getTime (guard/cutoff sid started)))
          "idle: the collection's own instant stands")
      (let [t (guard/writing! sid)]
        (try
          (is (<= (.getTime (guard/cutoff sid started)) (.getTime started))
              "in flight: the cutoff retreats, never advances")
          (finally (guard/done! sid t)))))))

#?(:clj
   (deftest with-unreferenced-writes-closes-on-throw
     (testing "a sequence that dies mid-way drops its entry — its objects are
               unreachable, i.e. garbage, and a later cycle collects them"
       (let [sid (random-uuid)]
         (is (thrown? Exception
                      (guard/with-unreferenced-writes sid
                        (is (guard/in-flight? sid))
                        (throw (ex-info "torn write" {})))))
         (is (not (guard/in-flight? sid))
             "the guard must not leak and wedge GC forever")))))

;; =============================================================================
;; Consequence for a real sweep — JVM only (sweep! is async-only)
;; =============================================================================

#?(:clj
   (defn- fresh-store [] (<!! (new-mem-store))))

#?(:clj
   (defn- cutoff-after-writes
     "A collection-start instant strictly greater than every stamp issued so far.

      Derived rather than observed: the write clock is pinned to wall time, so a
      cutoff read straight after a write lands in the SAME millisecond about as
      often as not, and the sweep spares ties (fail-safe). Sleeping to let the
      millisecond turn over would make the test slow and still probabilistic; the
      successor of the current high-water mark is exact."
     []
     (java.util.Date. (inc (utils/monotonic-now-ms)))))

#?(:clj
   (deftest sweep-without-the-guard-eats-an-unrooted-write
     (testing "the bug this guard exists for: values written but not yet pointed at
               are older than the collection's start, so a `now` cutoff deletes them"
       (let [store (fresh-store)]
         ;; values land, pointer has NOT yet been written
         (<!! (k/assoc store :value-1 {:node :data} {:sync? false}))
         (<!! (k/assoc store :value-2 {:node :data} {:sync? false}))
         (let [started (cutoff-after-writes)
               ;; whitelist holds only the (still old) pointer, as a real mark would
               deleted (<!! (gc/sweep! store #{:root} started 1000 {}))]
           (is (contains? (set deleted) :value-1))
           (is (contains? (set deleted) :value-2))
           (is (nil? (<!! (k/get store :value-1 nil {:sync? false})))
               "unguarded: the sweep destroyed values the pointer was about to reference"))))))

#?(:clj
   (deftest sweep-with-the-guard-spares-the-unrooted-write
     (testing "same sequence, but the CALLER derives the cutoff from the guard
               before marking: min(ts, safe-point) sits at or before the open
               sequence, so its values are never older than the cutoff"
       (let [store (fresh-store)
             sid (random-uuid)
             token (guard/writing! sid)]
         (<!! (k/assoc store :value-1 {:node :data} {:sync? false}))
         (<!! (k/assoc store :value-2 {:node :data} {:sync? false}))
         (let [cutoff (guard/cutoff sid (cutoff-after-writes))
               deleted (<!! (gc/sweep! store #{:root} cutoff))]
           (is (empty? deleted) "nothing may be collected while the sequence is open")
           (is (some? (<!! (k/get store :value-1 nil {:sync? false}))))
           (is (some? (<!! (k/get store :value-2 nil {:sync? false})))))
         (guard/done! sid token)
         (is (not (guard/in-flight? sid)))))))

#?(:clj
   (deftest the-guard-must-be-read-before-the-mark
     (testing "THE ordering property, and why `sweep!` refuses to read the guard
               itself: it is handed an already-computed whitelist, so any reading
               it did would necessarily be AFTER the caller's mark.

               A sequence open at the start, whose pointer lands between the mark
               and the sweep, is invisible to a cutoff taken after the mark — by
               then the guard is closed, so the cutoff snaps back to the
               collection start, while the mark walked roots that did not name
               the values yet.

               The clock is driven rather than waited on: stamps are pinned to
               wall time at millisecond granularity and the sweep spares ties, so
               a real-time version of this test spares the values by accident
               about as often as it proves anything. The high-water mark is
               process-global and `max`-based, so it is restored afterwards."
       (let [stamp-atom @#'utils/last-stamp
             saved @stamp-atom]
         (try
           ;; WRONG ORDER: mark, then read the guard.
           (let [store (fresh-store)
                 sid (random-uuid)
                 clock (atom (+ 1000 (utils/monotonic-now-ms)))]
             (binding [utils/*wall-clock-ms* #(deref clock)]
               (let [token (guard/writing! sid)]
                 (swap! clock + 1000)
                 (<!! (k/assoc store :root {:points-to []} {:sync? false}))
                 (<!! (k/assoc store :v1 {:node :data} {:sync? false}))
                 (swap! clock + 1000)
                 (let [ts (utils/now)
                       whitelist #{:root}]      ; mark: :root does not name :v1 yet
                   (swap! clock + 1000)
                   ;; the pointer lands and the sequence closes, after the mark
                   (<!! (k/assoc store :root {:points-to [:v1]} {:sync? false}))
                   (guard/done! sid token)
                   (let [cutoff (guard/cutoff sid ts)   ; read too late
                         deleted (<!! (gc/sweep! store whitelist cutoff))]
                     (is (contains? (set deleted) :v1)
                         "a guard read after the mark cannot spare :v1")
                     (is (nil? (<!! (k/get store :v1 nil {:sync? false})))
                         "so :root is left dangling"))))))
           ;; RIGHT ORDER: read the guard, then mark.
           (let [store (fresh-store)
                 sid (random-uuid)
                 clock (atom (+ 1000 (utils/monotonic-now-ms)))]
             (binding [utils/*wall-clock-ms* #(deref clock)]
               (let [token (guard/writing! sid)]
                 (swap! clock + 1000)
                 (<!! (k/assoc store :root {:points-to []} {:sync? false}))
                 (<!! (k/assoc store :v1 {:node :data} {:sync? false}))
                 (swap! clock + 1000)
                 (let [cutoff (guard/cutoff sid (utils/now))  ; guard first, still open
                       whitelist #{:root}]                    ; then mark
                   (swap! clock + 1000)
                   (<!! (k/assoc store :root {:points-to [:v1]} {:sync? false}))
                   (guard/done! sid token)
                   (let [deleted (<!! (gc/sweep! store whitelist cutoff))]
                     (is (empty? deleted)
                         "the open sequence pinned the cutoff before its own writes")
                     (is (some? (<!! (k/get store :v1 nil {:sync? false})))
                         "so :root's target survives"))))))
           (finally (reset! stamp-atom saved)))))))

#?(:clj
   (deftest guard-is-shared-across-independent-writers-on-one-store
     (testing "the reason this lives in konserve: two index structures sharing a
               store must see each other's in-flight sequences, because ONE sweep
               covers both"
       (let [store (fresh-store)
             sid (random-uuid)
             ;; writer A (say a datahike commit) opens a sequence
             token-a (guard/writing! sid)]
         (<!! (k/assoc store :a-value {:from :a} {:sync? false}))
         ;; writer B (say a scriptum manifest sync) triggers a collection, and
         ;; derives its cutoff from the SAME store id
         (let [cutoff (guard/cutoff sid (cutoff-after-writes))
               deleted (<!! (gc/sweep! store #{} cutoff))]
           (is (empty? deleted)
               "B's sweep must not collect A's in-flight values")
           (is (some? (<!! (k/get store :a-value nil {:sync? false})))))
         (guard/done! sid token-a)))))

#?(:clj
   (deftest a-store-names-itself-so-callers-cannot-disagree
     (testing "the id a caller feeds the guard should come off the store, not be
               passed alongside it — disagreement there is invisible until a
               collection deletes something.

               Clock driven, and the high-water mark restored: the second sweep
               has to actually collect, and against wall time its cutoff ties
               with the write it is meant to be younger than about half the time."
       (let [id (random-uuid)
             store (ks/create-store {:backend :memory :id id} {:sync? true})
             stamp-atom @#'utils/last-stamp
             saved @stamp-atom
             clock (atom (+ 1000 (utils/monotonic-now-ms)))]
         (is (= id (ks/store-id store)) "the store must carry its own id")
         (try
           (binding [utils/*wall-clock-ms* #(deref clock)]
             (let [sid (ks/store-id store)
                   token (guard/writing! sid)]
               (swap! clock + 1000)
               (<!! (k/assoc store :value-1 {:node :data} {:sync? false}))
               (swap! clock + 1000)
               (let [cutoff (guard/cutoff sid (utils/now))
                     deleted (<!! (gc/sweep! store #{} cutoff))]
                 (is (empty? deleted)
                     "the id taken off the store must reach the guard")
                 (is (some? (<!! (k/get store :value-1 nil {:sync? false})))))
               (guard/done! sid token)
               ;; Sequence closed: the same sweep now collects, which proves the
               ;; emptiness above came from the guard and not from a sweep that
               ;; simply never ran.
               (swap! clock + 1000)
               (let [cutoff (guard/cutoff sid (utils/now))
                     deleted (<!! (gc/sweep! store #{} cutoff))]
                 (is (contains? (set deleted) :value-1)))))
           (finally (reset! stamp-atom saved)))))))

#?(:clj
   (deftest a-constructor-built-store-has-no-id-of-its-own
     (testing "a store built through a backend constructor never took a config,
               so it cannot name itself and the caller must supply the id it
               guards with"
       (let [store (fresh-store)
             sid (random-uuid)]
         (is (nil? (ks/store-id store)))
         (let [token (guard/writing! sid)]
           (<!! (k/assoc store :value-1 {:node :data} {:sync? false}))
           (is (empty? (<!! (gc/sweep! store #{}
                                       (guard/cutoff sid (cutoff-after-writes))))))
           (guard/done! sid token))))))

#?(:clj
   (deftest attached-config-identifies-without-carrying-secrets
     (testing "the store reports the config it was connected with"
       (let [id (random-uuid)
             store (ks/create-store {:backend :memory :id id} {:sync? true})]
         (is (= id (ks/store-id store)))
         (is (= :memory (:backend (ks/store-config store))))))

     (testing "credentials are stripped: a config a caller passes once may hold
               an S3 secret or a JDBC password, and a store is long-lived and
               printable — identity survives, secrets do not"
       (let [id (random-uuid)
             store (ks/create-store {:backend :memory :id id
                                     :access-key "AKIAEXAMPLE"
                                     :secret "shhh"
                                     :password "hunter2"
                                     :jdbcUrl "jdbc://user:pw@host/db"}
                                    {:sync? true})
             cfg (ks/store-config store)
             printed (pr-str cfg)]
         (is (= id (:id cfg)) "identity survives")
         (doseq [k [:access-key :secret :password :jdbcUrl]]
           (is (not (contains? cfg k)) (str k " must not be retained")))
         (doseq [leak ["AKIAEXAMPLE" "shhh" "hunter2" "hunter2" "user:pw"]]
           (is (not (clojure.string/includes? printed leak))
               "no secret may survive into a printable form"))))))
