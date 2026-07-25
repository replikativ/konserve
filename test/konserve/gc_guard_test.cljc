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
     (testing "same sequence, guard held: sweep! derives min(ts, safe-point) from
               :store-id and leaves the in-flight values alone"
       (let [store (fresh-store)
             sid (random-uuid)
             token (guard/writing! sid)]
         (<!! (k/assoc store :value-1 {:node :data} {:sync? false}))
         (<!! (k/assoc store :value-2 {:node :data} {:sync? false}))
         (let [started (cutoff-after-writes)
               deleted (<!! (gc/sweep! store #{:root} started 1000 {:store-id sid}))]
           (is (empty? deleted) "nothing may be collected while the sequence is open")
           (is (some? (<!! (k/get store :value-1 nil {:sync? false}))))
           (is (some? (<!! (k/get store :value-2 nil {:sync? false})))))
         (guard/done! sid token)
         (is (not (guard/in-flight? sid)))))))

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
         ;; writer B (say a scriptum manifest sync) triggers a collection
         (let [started (cutoff-after-writes)
               deleted (<!! (gc/sweep! store #{} started 1000 {:store-id sid}))]
           (is (empty? deleted)
               "B's sweep must not collect A's in-flight values")
           (is (some? (<!! (k/get store :a-value nil {:sync? false})))))
         (guard/done! sid token-a)))))

#?(:clj
   (deftest sweep-consults-the-guard-without-being-told-the-store-id
     (testing "a store connected through konserve.store names itself, so the
               guard applies with no :store-id argument at all — the case where
               a caller and a writer could otherwise disagree about the name"
       (let [id (random-uuid)
             store (ks/create-store {:backend :memory :id id} {:sync? true})]
         (is (= id (ks/store-id store)) "the store must carry its own id")
         (let [token (guard/writing! id)]
           (<!! (k/assoc store :value-1 {:node :data} {:sync? false}))
           ;; No :store-id passed. The sweep has to find it on the store.
           (let [deleted (<!! (gc/sweep! store #{} (cutoff-after-writes) 1000 {}))]
             (is (empty? deleted)
                 "the derived id must reach the guard and spare the in-flight write")
             (is (some? (<!! (k/get store :value-1 nil {:sync? false})))))
           (guard/done! id token))
         ;; With the sequence closed the same sweep collects normally, which
         ;; proves the emptiness above came from the guard and not from a sweep
         ;; that simply never ran.
         (let [deleted (<!! (gc/sweep! store #{} (cutoff-after-writes) 1000 {}))]
           (is (contains? (set deleted) :value-1)))))))

#?(:clj
   (deftest an-explicit-store-id-still-wins
     (testing "a store built through a backend constructor never took an id, so
               the caller must still be able to supply one"
       (let [store (fresh-store)
             sid (random-uuid)]
         (is (nil? (ks/store-id store)))
         (let [token (guard/writing! sid)]
           (<!! (k/assoc store :value-1 {:node :data} {:sync? false}))
           (is (empty? (<!! (gc/sweep! store #{} (cutoff-after-writes) 1000
                                       {:store-id sid}))))
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
