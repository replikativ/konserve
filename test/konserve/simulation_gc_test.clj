(ns konserve.simulation-gc-test
  "Tests for konserve GC (garbage collection) with crash simulation.

   Verifies that sweep! correctly handles:
   - Whitelist preservation
   - Timestamp-based filtering
   - Crash recovery during sweep
   - Concurrent operations during GC"
  (:require [clojure.test :refer :all]
            [konserve.simulation.crash :as crash]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as sl]
            [konserve.serializers :as ser]
            [konserve.core :as k]
            [konserve.gc :as gc]
            [clojure.core.async :as a])
  (:import [java.util Date]))

;; =============================================================================
;; Test Fixtures and Helpers
;; =============================================================================

(defn create-test-store
  "Create a DefaultStore backed by CrashAwareBackingStore for testing."
  []
  (let [{:keys [backing state-atom crash-point-atom history-atom]} (crash/create-crash-aware-store)
        _ (sl/-create-store backing {:sync? true})
        store (defaults/connect-default-store
               backing
               {:opts {:sync? true}
                :config {:default-serializer :FressianSerializer
                         :serializers {:FressianSerializer (ser/fressian-serializer)}}
                :buffer-size 8192})]
    {:store store
     :backing backing
     :state-atom state-atom
     :crash-point-atom crash-point-atom
     :history-atom history-atom}))

(defn sync-assoc!
  "Synchronously assoc a value."
  [store key value]
  (k/assoc store key value {:sync? true}))

(defn sync-get
  "Synchronously get a value."
  [store key]
  (k/get store key nil {:sync? true}))

(defn sync-keys
  "Synchronously get all keys."
  [store]
  (a/<!! (k/keys store)))

(defn future-timestamp
  "Return a timestamp in the future (1 hour from now).
   Use this to DELETE keys - sweep deletes keys with last-write < ts."
  []
  (Date. (+ (System/currentTimeMillis) 3600000)))

(defn past-timestamp
  "Return a timestamp in the past (1 hour ago).
   Use this to PRESERVE keys by timestamp - sweep keeps keys with last-write >= ts."
  []
  (Date. (- (System/currentTimeMillis) 3600000)))

;; =============================================================================
;; Basic GC Tests
;; =============================================================================

(deftest basic-sweep-test
  (testing "sweep! deletes non-whitelisted keys"
    (let [{:keys [store]} (create-test-store)]
      ;; Create several keys
      (sync-assoc! store :keep1 {:data 1})
      (sync-assoc! store :keep2 {:data 2})
      (sync-assoc! store :delete1 {:data 3})
      (sync-assoc! store :delete2 {:data 4})

      ;; Verify all exist
      (is (= 4 (count (sync-keys store))))

      ;; Sweep with whitelist - use future timestamp so all are eligible for deletion
      (a/<!! (gc/sweep! store #{:keep1 :keep2} (future-timestamp)))

      ;; Verify only whitelisted keys remain
      (is (= #{:keep1 :keep2} (set (map :key (sync-keys store)))))
      (is (= {:data 1} (sync-get store :keep1)))
      (is (= {:data 2} (sync-get store :keep2)))
      (is (nil? (sync-get store :delete1)))
      (is (nil? (sync-get store :delete2))))))

(deftest empty-whitelist-sweep-test
  (testing "sweep! with empty whitelist deletes everything"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :key1 {:data 1})
      (sync-assoc! store :key2 {:data 2})
      (sync-assoc! store :key3 {:data 3})

      (a/<!! (gc/sweep! store #{} (future-timestamp)))

      (is (empty? (sync-keys store))))))

(deftest full-whitelist-sweep-test
  (testing "sweep! with full whitelist deletes nothing"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :key1 {:data 1})
      (sync-assoc! store :key2 {:data 2})

      (a/<!! (gc/sweep! store #{:key1 :key2} (future-timestamp)))

      (is (= 2 (count (sync-keys store))))
      (is (= {:data 1} (sync-get store :key1)))
      (is (= {:data 2} (sync-get store :key2))))))

;; =============================================================================
;; Timestamp-based Filtering Tests
;; =============================================================================

(deftest timestamp-preserves-recent-writes-test
  (testing "sweep! preserves keys written after timestamp"
    (let [{:keys [store]} (create-test-store)]
      ;; Create keys - all will have recent timestamps
      (sync-assoc! store :key1 {:data 1})
      (sync-assoc! store :key2 {:data 2})
      (sync-assoc! store :key3 {:data 3})

      ;; Sweep with past timestamp and empty whitelist
      ;; Keys are preserved because last-write >= ts (all keys written now > past)
      (a/<!! (gc/sweep! store #{} (past-timestamp)))

      ;; All keys should still exist (written after the past timestamp)
      (is (= 3 (count (sync-keys store)))))))

(deftest timestamp-and-whitelist-combined-test
  (testing "sweep! respects both timestamp and whitelist"
    (let [{:keys [store]} (create-test-store)]
      ;; Create keys
      (sync-assoc! store :whitelist-key {:data 1})
      (sync-assoc! store :not-whitelisted {:data 2})

      ;; Sweep with future timestamp - makes all keys eligible for deletion
      ;; Only whitelist-key is protected by whitelist
      (a/<!! (gc/sweep! store #{:whitelist-key} (future-timestamp)))

      ;; Only whitelisted key survives
      (is (= 1 (count (sync-keys store))))
      (is (= {:data 1} (sync-get store :whitelist-key)))
      (is (nil? (sync-get store :not-whitelisted))))))

;; =============================================================================
;; Crash During GC Tests
;; =============================================================================

(deftest crash-during-sweep-delete-test
  (testing "crash during sweep! delete leaves store consistent"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      ;; Create keys
      (sync-assoc! store :keep {:data "keep"})
      (sync-assoc! store :delete1 {:data "delete1"})
      (sync-assoc! store :delete2 {:data "delete2"})
      (sync-assoc! store :delete3 {:data "delete3"})

      ;; Set crash point during delete operation
      (crash/set-crash-point! crash-point-atom :after-write-value)

      ;; Attempt sweep - may crash during deletion
      (try
        (a/<!! (gc/sweep! store #{:keep} (future-timestamp)))
        (catch Exception _))

      ;; Simulate crash
      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; Verify store is consistent
      ;; :keep must exist, others may or may not depending on crash timing
      (is (= {:data "keep"} (sync-get store :keep))
          "Whitelisted key must survive crash during GC")

      ;; All remaining keys should be readable (no corruption)
      (doseq [{:keys [key]} (sync-keys store)]
        (is (some? (sync-get store key))
            (str "Key " key " should be readable after crash"))))))

(deftest crash-recovery-gc-idempotent-test
  (testing "GC can be re-run after crash without data loss"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      ;; Create keys
      (sync-assoc! store :important1 {:critical "data1"})
      (sync-assoc! store :important2 {:critical "data2"})
      (sync-assoc! store :garbage {:trash true})

      ;; First GC attempt crashes
      (crash/set-crash-point! crash-point-atom :after-sync)
      (try
        (a/<!! (gc/sweep! store #{:important1 :important2} (future-timestamp)))
        (catch Exception _))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; Re-run GC after recovery
      (a/<!! (gc/sweep! store #{:important1 :important2} (future-timestamp)))

      ;; Important keys must survive
      (is (= {:critical "data1"} (sync-get store :important1)))
      (is (= {:critical "data2"} (sync-get store :important2)))
      ;; Garbage should be gone (either from first partial GC or second complete GC)
      (is (nil? (sync-get store :garbage))))))

;; =============================================================================
;; Concurrent Operations During GC Tests
;; =============================================================================

(deftest concurrent-write-during-gc-test
  (testing "writes during GC are preserved"
    (let [{:keys [store]} (create-test-store)
          gc-started (promise)
          gc-done (promise)]
      ;; Create initial keys
      (sync-assoc! store :existing {:data "existing"})
      (sync-assoc! store :garbage {:data "garbage"})

      ;; Start GC in background (will use batch-size 1 to slow it down)
      (future
        (deliver gc-started true)
        (a/<!! (gc/sweep! store #{:existing :new-during-gc} (future-timestamp) 1))
        (deliver gc-done true))

      @gc-started

      ;; Write new key during GC
      (sync-assoc! store :new-during-gc {:data "new"})

      @gc-done

      ;; New key should survive (in whitelist)
      (is (= {:data "new"} (sync-get store :new-during-gc)))
      (is (= {:data "existing"} (sync-get store :existing)))
      ;; Garbage may or may not be deleted depending on timing
      )))

(deftest gc-with-ongoing-reads-test
  (testing "reads during GC return consistent data"
    (let [{:keys [store]} (create-test-store)
          read-results (atom [])
          read-count 100]
      ;; Create keys
      (sync-assoc! store :stable {:data "stable"})
      (sync-assoc! store :garbage {:data "garbage"})

      ;; Start concurrent reads
      (let [readers (doall
                     (for [_ (range read-count)]
                       (future
                          ;; long: Thread/sleep has no (int) overload, and
                          ;; konserve builds on Clojure 1.11 where this is a
                          ;; reflective call.
                         (Thread/sleep (long (rand-int 10)))
                         (let [v (sync-get store :stable)]
                           (swap! read-results conj v)
                           v))))]

        ;; Run GC while reads are happening
        (a/<!! (gc/sweep! store #{:stable} (future-timestamp)))

        ;; Wait for all reads
        (doseq [r readers] @r))

      ;; All reads should have gotten the correct value
      (is (every? #(= {:data "stable"} %) @read-results)
          "All concurrent reads should see consistent data"))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest gc-empty-store-test
  (testing "sweep! on empty store is safe"
    (let [{:keys [store]} (create-test-store)]
      (a/<!! (gc/sweep! store #{} (future-timestamp)))
      (is (empty? (sync-keys store))))))

(deftest gc-with-nested-data-test
  (testing "sweep! correctly handles complex nested values"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :complex {:nested {:deeply {:data [1 2 3]}}
                                   :list [1 2 3 4 5]
                                   :set #{:a :b :c}})
      (sync-assoc! store :simple {:x 1})

      (a/<!! (gc/sweep! store #{:complex} (future-timestamp)))

      (is (= {:nested {:deeply {:data [1 2 3]}}
              :list [1 2 3 4 5]
              :set #{:a :b :c}}
             (sync-get store :complex)))
      (is (nil? (sync-get store :simple))))))

(deftest gc-preserves-nil-values-test
  (testing "sweep! preserves keys with nil values when whitelisted"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :nil-val nil)
      (sync-assoc! store :delete-me {:data 1})

      (a/<!! (gc/sweep! store #{:nil-val} (future-timestamp)))

      ;; nil-val key should exist (even though value is nil)
      (is (contains? (set (map :key (sync-keys store))) :nil-val))
      (is (nil? (sync-get store :delete-me))))))

(deftest repeated-gc-cycles-test
  (testing "multiple GC cycles work correctly"
    (let [{:keys [store]} (create-test-store)]
      ;; Cycle 1: Create and GC
      (sync-assoc! store :persistent {:cycle 1})
      (sync-assoc! store :temp1 {:temp true})
      (a/<!! (gc/sweep! store #{:persistent} (future-timestamp)))

      (is (= {:cycle 1} (sync-get store :persistent)))
      (is (nil? (sync-get store :temp1)))

      ;; Cycle 2: Add more and GC again
      (sync-assoc! store :persistent {:cycle 2})  ; Update
      (sync-assoc! store :temp2 {:temp true})
      (a/<!! (gc/sweep! store #{:persistent} (future-timestamp)))

      (is (= {:cycle 2} (sync-get store :persistent)))
      (is (nil? (sync-get store :temp2)))

      ;; Cycle 3: Final check
      (sync-assoc! store :temp3 {:temp true})
      (a/<!! (gc/sweep! store #{:persistent} (future-timestamp)))

      (is (= 1 (count (sync-keys store))))
      (is (= {:cycle 2} (sync-get store :persistent))))))

;; =============================================================================
;; Batch Size Tests
;; =============================================================================

(deftest gc-batch-size-test
  (testing "sweep! with different batch sizes works correctly"
    (doseq [batch-size [1 5 10 100]]
      (let [{:keys [store]} (create-test-store)]
        ;; Create 20 keys
        (doseq [i (range 20)]
          (sync-assoc! store (keyword (str "key" i)) {:index i}))

        ;; Keep only even-numbered keys
        (let [whitelist (set (for [i (range 0 20 2)]
                               (keyword (str "key" i))))]
          (a/<!! (gc/sweep! store whitelist (future-timestamp) batch-size))

          ;; Verify only even keys remain
          (is (= 10 (count (sync-keys store)))
              (str "With batch-size " batch-size ", should have 10 keys"))
          (doseq [i (range 0 20 2)]
            (is (= {:index i} (sync-get store (keyword (str "key" i))))
                (str "Key" i " should exist"))))))))
