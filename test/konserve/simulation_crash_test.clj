(ns konserve.simulation-crash-test
  "Tests for CrashAwareBackingStore - verifying crash simulation with sync-point tracking.

   Based on CrashMonkey research: Most crash bugs reproduce with 3 operations or fewer,
   and 100% of reported bugs occurred after fsync-like calls."
  (:require [clojure.test :refer :all]
            [konserve.simulation.crash :as crash]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as sl]
            [konserve.serializers :as ser]
            [konserve.core :as k])
  (:import [clojure.lang ExceptionInfo]))

;; =============================================================================
;; Test Fixtures and Helpers
;; =============================================================================

(defn create-test-store
  "Create a DefaultStore backed by CrashAwareBackingStore for testing."
  []
  (let [{:keys [backing state-atom crash-point-atom history-atom]} (crash/create-crash-aware-store)
        ;; Create the store directory
        _ (sl/-create-store backing {:sync? true})
        ;; Connect DefaultStore with sync mode for easier testing
        ;; In sync mode, connect-default-store returns the store directly
        store (defaults/connect-default-store
               backing
               {:opts {:sync? true}  ; Use sync mode
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

(defn sync-dissoc!
  "Synchronously dissoc a key."
  [store key]
  (k/dissoc store key {:sync? true}))

;; =============================================================================
;; Basic Operation Tests (No Crashes)
;; =============================================================================

(deftest basic-operations-test
  (testing "Basic operations work without crash injection"
    (let [{:keys [store state-atom]} (create-test-store)]
      ;; Write
      (sync-assoc! store :test-key {:value 1})

      ;; Read back
      (is (= {:value 1} (sync-get store :test-key)))

      ;; Verify data is in synced state
      (is (seq (crash/get-synced-data state-atom))
          "Data should be in synced state after successful write"))))

(deftest multiple-writes-test
  (testing "Multiple writes work correctly"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :key1 {:a 1})
      (sync-assoc! store :key2 {:b 2})
      (sync-assoc! store :key3 {:c 3})

      (is (= {:a 1} (sync-get store :key1)))
      (is (= {:b 2} (sync-get store :key2)))
      (is (= {:c 3} (sync-get store :key3))))))

(deftest overwrite-test
  (testing "Overwriting a key preserves new value"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :key {:version 1})
      (is (= {:version 1} (sync-get store :key)))

      (sync-assoc! store :key {:version 2})
      (is (= {:version 2} (sync-get store :key))))))

;; =============================================================================
;; Crash Point Tests
;; =============================================================================

(deftest crash-after-write-header-test
  (testing "Crash after write-header leaves no partial data"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      ;; First write succeeds
      (sync-assoc! store :key {:before-crash true})
      (is (= {:before-crash true} (sync-get store :key)))

      ;; Set crash point
      (crash/set-crash-point! crash-point-atom :after-write-header)

      ;; Attempt write - should crash
      (is (thrown-with-msg?
           ExceptionInfo
           #"Simulated crash at after-write-header"
           (sync-assoc! store :key {:after-crash true})))

      ;; Simulate the crash (discard pending data)
      (crash/simulate-crash! state-atom)

      ;; Original value should be preserved
      (crash/clear-crash-point! crash-point-atom)
      (is (= {:before-crash true} (sync-get store :key))
          "Original value should be preserved after crash"))))

(deftest crash-after-write-meta-test
  (testing "Crash after write-meta leaves no partial data"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (sync-assoc! store :key {:original true})

      (crash/set-crash-point! crash-point-atom :after-write-meta)

      (is (thrown-with-msg?
           ExceptionInfo
           #"Simulated crash at after-write-meta"
           (sync-assoc! store :key {:new-value true})))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      (is (= {:original true} (sync-get store :key))))))

(deftest crash-after-write-value-test
  (testing "Crash after write-value (before sync) loses the write"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (sync-assoc! store :key {:original true})

      (crash/set-crash-point! crash-point-atom :after-write-value)

      (is (thrown-with-msg?
           ExceptionInfo
           #"Simulated crash at after-write-value"
           (sync-assoc! store :key {:new-value true})))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      (is (= {:original true} (sync-get store :key))
          "Value written but not synced should be lost"))))

(deftest crash-after-sync-test
  (testing "Crash after sync (before atomic-move) loses the write"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (sync-assoc! store :key {:original true})

      (crash/set-crash-point! crash-point-atom :after-sync)

      (is (thrown-with-msg?
           ExceptionInfo
           #"Simulated crash at after-sync"
           (sync-assoc! store :key {:new-value true})))

      ;; At this point, .new file exists but original is unchanged
      (is (seq (crash/get-pending-new-files state-atom))
          "Pending .new file should exist")

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; After crash, .new file is discarded
      (is (empty? (crash/get-pending-new-files state-atom))
          "Pending .new file should be discarded after crash")

      (is (= {:original true} (sync-get store :key))))))

(deftest crash-after-atomic-move-test
  (testing "Crash after atomic-move (before sync-store) - write may or may not survive"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (sync-assoc! store :key {:original true})

      (crash/set-crash-point! crash-point-atom :after-atomic-move)

      (is (thrown-with-msg?
           ExceptionInfo
           #"Simulated crash at after-atomic-move"
           (sync-assoc! store :key {:new-value true})))

      ;; After atomic-move but before sync-store, data may be visible
      ;; but on real crash, it could be lost if filesystem doesn't persist directory entries
      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; In our simulation, we revert pending moves
      (is (= {:original true} (sync-get store :key))
          "In strict crash model, pending moves are reverted"))))

(deftest crash-after-sync-store-test
  (testing "Crash after sync-store - write survives (fully durable)"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (sync-assoc! store :key {:original true})

      ;; Note: This crash happens after sync-store completes
      ;; so the write IS durable, crash just prevents backup deletion
      (crash/set-crash-point! crash-point-atom :after-sync-store)

      ;; This should actually succeed since sync-store completed
      ;; but then crash - the backup file just won't be deleted
      (is (thrown-with-msg?
           ExceptionInfo
           #"Simulated crash at after-sync-store"
           (sync-assoc! store :key {:new-value true})))

      (crash/clear-crash-point! crash-point-atom)

      ;; After sync-store, the write is durable
      ;; Note: Our crash simulation doesn't revert after sync-store
      (is (= {:new-value true} (sync-get store :key))
          "After sync-store, write is durable even if crash follows"))))

;; =============================================================================
;; Recovery Tests
;; =============================================================================

(deftest recovery-after-crash-test
  (testing "Store can recover after crash simulation"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      ;; Write some initial data
      (sync-assoc! store :key1 {:data 1})
      (sync-assoc! store :key2 {:data 2})

      ;; Crash during a write
      (crash/set-crash-point! crash-point-atom :after-write-value)
      (try
        (sync-assoc! store :key3 {:data 3})
        (catch ExceptionInfo _))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; Verify recovery
      (is (= {:data 1} (sync-get store :key1)) "key1 should survive")
      (is (= {:data 2} (sync-get store :key2)) "key2 should survive")
      (is (nil? (sync-get store :key3)) "key3 should not exist (crash during write)"))))

(deftest multiple-crash-recovery-test
  (testing "Store survives multiple crashes"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      ;; First write
      (sync-assoc! store :key {:version 1})

      ;; First crash
      (crash/set-crash-point! crash-point-atom :after-write-value)
      (try
        (sync-assoc! store :key {:version 2})
        (catch ExceptionInfo _))
      (crash/simulate-crash! state-atom)

      ;; Second write succeeds
      (crash/clear-crash-point! crash-point-atom)
      (sync-assoc! store :key {:version 3})

      ;; Second crash
      (crash/set-crash-point! crash-point-atom :after-write-meta)
      (try
        (sync-assoc! store :key {:version 4})
        (catch ExceptionInfo _))
      (crash/simulate-crash! state-atom)

      ;; Verify final state
      (crash/clear-crash-point! crash-point-atom)
      (is (= {:version 3} (sync-get store :key))
          "Should have version 3 (last successful write)"))))

;; =============================================================================
;; Invariant Verification Tests
;; =============================================================================

(deftest no-partial-data-invariant-test
  (testing "No partial data is ever visible to reads"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      ;; Write a value
      (sync-assoc! store :key {:complete-value "with lots of data"})

      ;; Try crash at each point and verify no partial reads
      (doseq [crash-point [:after-write-header
                           :after-write-meta
                           :after-write-value
                           :after-sync]]
        (crash/set-crash-point! crash-point-atom crash-point)
        (try
          (sync-assoc! store :key {:new-value "should not be visible"})
          (catch ExceptionInfo _))

        (crash/simulate-crash! state-atom)
        (crash/clear-crash-point! crash-point-atom)

        ;; Verify we get either old value or nil, never partial
        (let [value (sync-get store :key)]
          (is (or (= {:complete-value "with lots of data"} value)
                  (nil? value))
              (str "At crash point " crash-point ", got unexpected: " value)))))))

(deftest atomicity-invariant-test
  (testing "Writes are atomic - either fully applied or not at all"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (sync-assoc! store :key {:before true})

      (doseq [crash-point [:after-write-header
                           :after-write-meta
                           :after-write-value
                           :after-sync
                           :after-atomic-move]]
        (let [before-state @state-atom]
          (crash/set-crash-point! crash-point-atom crash-point)
          (try
            (sync-assoc! store :key {:after true :crash-point crash-point})
            (catch ExceptionInfo _))

          (crash/simulate-crash! state-atom)
          (crash/clear-crash-point! crash-point-atom)

          (let [after-state @state-atom]
            ;; Synced data should either be unchanged or completely updated
            (is (crash/verify-atomicity before-state after-state
                                        (first (keys (:synced-blobs before-state))))
                (str "Atomicity violated at " crash-point))))))))

;; =============================================================================
;; Concurrent Operations Tests
;; =============================================================================

(deftest concurrent-write-with-crash-test
  (testing "Concurrent writes handle crash correctly"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      ;; Write two different keys
      (sync-assoc! store :key1 {:data 1})

      ;; Crash during second write
      (crash/set-crash-point! crash-point-atom :after-write-value)
      (try
        (sync-assoc! store :key2 {:data 2})
        (catch ExceptionInfo _))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; key1 survives, key2 doesn't
      (is (= {:data 1} (sync-get store :key1)))
      (is (nil? (sync-get store :key2))))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest empty-value-crash-test
  (testing "Crash during write of empty/nil value"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (sync-assoc! store :key {:non-empty "value"})

      (crash/set-crash-point! crash-point-atom :after-write-value)
      (try
        (sync-assoc! store :key nil)
        (catch ExceptionInfo _))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; Original value preserved
      (is (= {:non-empty "value"} (sync-get store :key))))))

(deftest large-value-crash-test
  (testing "Crash during write of large value"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)
          large-value (vec (repeat 10000 "x"))]
      (sync-assoc! store :key {:small "value"})

      (crash/set-crash-point! crash-point-atom :after-write-value)
      (try
        (sync-assoc! store :key {:large large-value})
        (catch ExceptionInfo _))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      (is (= {:small "value"} (sync-get store :key))))))

(deftest new-key-crash-test
  (testing "Crash during write of new key (not overwrite)"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (crash/set-crash-point! crash-point-atom :after-write-value)

      (try
        (sync-assoc! store :new-key {:data 1})
        (catch ExceptionInfo _))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; Key should not exist
      (is (nil? (sync-get store :new-key))))))

;; =============================================================================
;; API Tests
;; =============================================================================

(deftest invalid-crash-point-test
  (testing "Invalid crash point throws error"
    (let [{:keys [crash-point-atom]} (crash/create-crash-aware-store)]
      (is (thrown-with-msg?
           ExceptionInfo
           #"Invalid crash point"
           (crash/set-crash-point! crash-point-atom :invalid-point))))))

(deftest crash-point-clearing-test
  (testing "Clearing crash point allows normal operation"
    (let [{:keys [store crash-point-atom]} (create-test-store)]
      (crash/set-crash-point! crash-point-atom :after-write-value)
      (crash/clear-crash-point! crash-point-atom)

      ;; Should work normally
      (sync-assoc! store :key {:value 1})
      (is (= {:value 1} (sync-get store :key))))))

(deftest state-inspection-test
  (testing "State inspection APIs work correctly"
    (let [{:keys [store state-atom]} (create-test-store)]
      ;; Initial state
      (is (empty? (crash/get-synced-data state-atom)))
      (is (empty? (crash/get-pending-data state-atom)))

      ;; After write
      (sync-assoc! store :key {:value 1})
      (is (seq (crash/get-synced-data state-atom)))
      (is (empty? (crash/get-pending-data state-atom))
          "No pending data after successful sync"))))
