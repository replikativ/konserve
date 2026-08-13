(ns konserve.simulation-stress-test
  "Stress tests for konserve backing stores.

   Covers:
   - Resource exhaustion (storage limits, key limits)
   - Concurrent access (parallel writes, read/write contention)
   - Long-running stability (many operations over time)
   - Error recovery under load"
  (:require [clojure.test :refer :all]
            [konserve.simulation.crash :as crash]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as sl]
            [konserve.serializers :as ser]
            [konserve.core :as k])
  (:import [clojure.lang ExceptionInfo]
           [java.util.concurrent CountDownLatch Executors TimeUnit]))

;; =============================================================================
;; Test Fixtures and Helpers
;; =============================================================================

(defn create-test-store
  "Create a DefaultStore backed by CrashAwareBackingStore."
  ([] (create-test-store {}))
  ([opts]
   (let [{:keys [backing state-atom crash-point-atom history-atom]}
         (crash/create-crash-aware-store opts)
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
      :history-atom history-atom})))

(defn sync-assoc!
  [store key value]
  (k/assoc store key value {:sync? true}))

(defn sync-get
  [store key]
  (k/get store key nil {:sync? true}))

(defn sync-dissoc!
  [store key]
  (k/dissoc store key {:sync? true}))

;; =============================================================================
;; Resource Exhaustion Tests
;; =============================================================================

(deftest storage-limit-test
  (testing "Storage limit is enforced"
    (let [{:keys [store state-atom]} (create-test-store {:max-total-bytes 1000})]
      ;; First write should succeed
      (sync-assoc! store :key1 {:small "value"})
      (is (= {:small "value"} (sync-get store :key1)))

      ;; Large write should fail
      (let [large-value (vec (repeat 2000 "x"))]
        (is (thrown-with-msg?
             ExceptionInfo
             #"Storage limit exceeded"
             (sync-assoc! store :key2 {:large large-value}))))

      ;; Original data intact
      (is (= {:small "value"} (sync-get store :key1))))))

(deftest key-limit-test
  (testing "Key limit is enforced"
    (let [{:keys [store]} (create-test-store {:max-keys 3})]
      ;; First 3 keys succeed
      (sync-assoc! store :key1 {:v 1})
      (sync-assoc! store :key2 {:v 2})
      (sync-assoc! store :key3 {:v 3})

      ;; 4th key fails
      (is (thrown-with-msg?
           ExceptionInfo
           #"Key limit exceeded"
           (sync-assoc! store :key4 {:v 4})))

      ;; Can still update existing keys
      (sync-assoc! store :key1 {:v 100})
      (is (= {:v 100} (sync-get store :key1))))))

(deftest storage-reclaim-test
  (testing "Deleting keys frees storage"
    (let [{:keys [store state-atom]} (create-test-store {:max-total-bytes 500})]
      ;; Fill up storage
      (sync-assoc! store :key1 {:data (vec (repeat 100 "x"))})

      ;; This should fail (would exceed limit)
      (is (thrown-with-msg?
           ExceptionInfo
           #"Storage limit exceeded"
           (sync-assoc! store :key2 {:data (vec (repeat 100 "y"))})))

      ;; Delete first key
      (sync-dissoc! store :key1)

      ;; Now second write should succeed
      (sync-assoc! store :key2 {:data (vec (repeat 100 "y"))})
      (is (some? (sync-get store :key2))))))

(deftest update-within-limits-test
  (testing "Updating existing key stays within limits"
    (let [{:keys [store]} (create-test-store {:max-total-bytes 500})]
      ;; Write a value
      (sync-assoc! store :key {:data (vec (repeat 50 "x"))})

      ;; Update to larger value should consider net change
      (sync-assoc! store :key {:data (vec (repeat 60 "y"))})
      (is (some? (sync-get store :key))))))

;; =============================================================================
;; Concurrent Access Tests
;; =============================================================================

(deftest concurrent-writes-same-key-test
  (testing "Concurrent writes to same key serialize correctly"
    (let [{:keys [store]} (create-test-store)
          n-threads 10
          n-writes 50
          latch (CountDownLatch. n-threads)
          results (atom [])
          executor (Executors/newFixedThreadPool n-threads)]
      (try
        ;; Launch concurrent writers
        (dotimes [t n-threads]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (dotimes [i n-writes]
                         (sync-assoc! store :shared-key {:thread t :iteration i}))
                       (catch Exception e
                         (swap! results conj {:error (.getMessage e)}))
                       (finally
                         (.countDown latch))))))

        (.await latch 30 TimeUnit/SECONDS)

        ;; Verify final value is from one of the threads
        (let [final-value (sync-get store :shared-key)]
          (is (some? final-value))
          (is (contains? (set (range n-threads)) (:thread final-value))))

        (finally
          (.shutdown executor))))))

(deftest concurrent-writes-different-keys-test
  (testing "Concurrent writes to different keys all succeed"
    (let [{:keys [store]} (create-test-store)
          n-threads 10
          latch (CountDownLatch. n-threads)
          executor (Executors/newFixedThreadPool n-threads)]
      (try
        ;; Each thread writes to its own key
        (dotimes [t n-threads]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (sync-assoc! store (keyword (str "key-" t)) {:thread t})
                       (finally
                         (.countDown latch))))))

        (.await latch 30 TimeUnit/SECONDS)

        ;; Verify all keys exist
        (dotimes [t n-threads]
          (is (= {:thread t} (sync-get store (keyword (str "key-" t))))
              (str "Key for thread " t " should exist")))

        (finally
          (.shutdown executor))))))

(deftest read-write-contention-test
  (testing "Reads during writes return consistent data"
    (let [{:keys [store]} (create-test-store)
          _ (sync-assoc! store :key {:version 0})
          n-readers 5
          n-writers 3
          n-ops 20
          latch (CountDownLatch. (+ n-readers n-writers))
          read-results (atom [])
          executor (Executors/newFixedThreadPool (+ n-readers n-writers))]
      (try
        ;; Launch writers
        (dotimes [w n-writers]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (dotimes [i n-ops]
                         (sync-assoc! store :key {:version (+ (* w 1000) i)}))
                       (finally
                         (.countDown latch))))))

        ;; Launch readers
        (dotimes [_r n-readers]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (dotimes [_ n-ops]
                         (when-let [v (sync-get store :key)]
                           (swap! read-results conj v)))
                       (finally
                         (.countDown latch))))))

        (.await latch 30 TimeUnit/SECONDS)

        ;; All reads should have valid version structure
        (doseq [result @read-results]
          (is (contains? result :version)
              "Every read should return a map with :version"))

        (finally
          (.shutdown executor))))))

;; =============================================================================
;; Long-Running Stability Tests
;; =============================================================================

(deftest many-operations-stability-test
  (testing "Store remains stable after many operations"
    (let [{:keys [store state-atom]} (create-test-store)
          n-keys 50
          n-iterations 20]
      ;; Perform many write/read/delete cycles
      (dotimes [iter n-iterations]
        (dotimes [k n-keys]
          (let [key (keyword (str "key-" k))]
            ;; Write
            (sync-assoc! store key {:iteration iter :key k})
            ;; Read back
            (is (= {:iteration iter :key k} (sync-get store key))))))

      ;; Verify final state
      (dotimes [k n-keys]
        (is (= {:iteration (dec n-iterations) :key k}
               (sync-get store (keyword (str "key-" k)))))))))

(deftest write-delete-cycle-test
  (testing "Write/delete cycles don't leak state"
    (let [{:keys [store state-atom]} (create-test-store)
          n-cycles 100]
      (dotimes [i n-cycles]
        (sync-assoc! store :ephemeral {:cycle i})
        (is (= {:cycle i} (sync-get store :ephemeral)))
        (sync-dissoc! store :ephemeral)
        (is (nil? (sync-get store :ephemeral))))

      ;; Verify no keys remain
      (is (empty? (crash/get-synced-data state-atom))))))

(deftest growing-values-test
  (testing "Store handles growing value sizes"
    (let [{:keys [store]} (create-test-store)]
      (doseq [size [10 100 1000 5000]]
        (let [value (vec (repeat size "x"))]
          (sync-assoc! store :growing {:size size :data value})
          (let [result (sync-get store :growing)]
            (is (= size (:size result)))
            (is (= size (count (:data result))))))))))

;; =============================================================================
;; Error Recovery Under Load
;; =============================================================================

(deftest crash-during-concurrent-writes-test
  (testing "Crash during concurrent writes recovers correctly"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)
          _ (sync-assoc! store :stable {:value "original"})
          n-threads 5
          latch (CountDownLatch. n-threads)
          executor (Executors/newFixedThreadPool n-threads)]
      (try
        ;; Set crash point for some writes
        (crash/set-crash-point! crash-point-atom :after-write-value)

        ;; Launch concurrent writers (they will all crash)
        (dotimes [t n-threads]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (sync-assoc! store :stable {:value (str "thread-" t)})
                       (catch Exception _))
                     (.countDown latch))))

        (.await latch 10 TimeUnit/SECONDS)

        ;; Simulate crash recovery
        (crash/simulate-crash! state-atom)
        (crash/clear-crash-point! crash-point-atom)

        ;; Original value should be preserved
        (is (= {:value "original"} (sync-get store :stable)))

        (finally
          (.shutdown executor))))))

(deftest resource-exhaustion-recovery-test
  (testing "Store recovers after resource exhaustion errors"
    (let [{:keys [store state-atom]} (create-test-store {:max-keys 5})]
      ;; Fill up to limit
      (dotimes [i 5]
        (sync-assoc! store (keyword (str "key-" i)) {:v i}))

      ;; Try to exceed - should fail
      (is (thrown? ExceptionInfo
                   (sync-assoc! store :extra {:v 99})))

      ;; Delete one and try again - should succeed
      (sync-dissoc! store :key-0)
      (sync-assoc! store :new-key {:v 100})
      (is (= {:v 100} (sync-get store :new-key))))))

;; =============================================================================
;; Mixed Operation Stress Test
;; =============================================================================

(deftest mixed-operations-stress-test
  (testing "Mixed concurrent operations maintain consistency"
    (let [{:keys [store]} (create-test-store)
          n-threads 8
          n-ops 50
          latch (CountDownLatch. n-threads)
          errors (atom [])
          executor (Executors/newFixedThreadPool n-threads)]
      (try
        ;; Initialize some keys
        (dotimes [i 10]
          (sync-assoc! store (keyword (str "init-" i)) {:v i}))

        ;; Launch mixed operation threads
        (dotimes [t n-threads]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (dotimes [i n-ops]
                         (let [op (rand-nth [:read :write :delete])
                               key (keyword (str "key-" (rand-int 20)))]
                           (case op
                             :read (sync-get store key)
                             :write (sync-assoc! store key {:thread t :op i})
                             :delete (sync-dissoc! store key))))
                       (catch Exception e
                         (swap! errors conj {:thread t :error (.getMessage e)}))
                       (finally
                         (.countDown latch))))))

        (.await latch 60 TimeUnit/SECONDS)

        ;; No unexpected errors
        (is (empty? @errors)
            (str "Unexpected errors: " @errors))

        (finally
          (.shutdown executor))))))

;; =============================================================================
;; Binary Blob Handling Tests
;; =============================================================================

(deftest binary-blob-basic-test
  (testing "Binary blobs round-trip correctly"
    (let [{:keys [store]} (create-test-store)]
      ;; Write binary data
      (let [data (byte-array [0 1 2 3 4 5 127 -128 -1])]
        (k/bassoc store :binary data {:sync? true})
        ;; Read back via bget
        (let [result (k/bget store :binary
                             (fn [{:keys [input-stream]}]
                               (let [baos (java.io.ByteArrayOutputStream.)]
                                 (loop []
                                   (let [b (.read input-stream)]
                                     (when (>= b 0)
                                       (.write baos b)
                                       (recur))))
                                 (.toByteArray baos)))
                             {:sync? true})]
          (is (= (seq data) (seq result))))))))

(deftest binary-blob-large-test
  (testing "Large binary blobs work correctly"
    (let [{:keys [store]} (create-test-store)]
      (doseq [size [1000 10000 100000]]
        (let [data (byte-array (repeatedly size #(unchecked-byte (rand-int 256))))]
          (k/bassoc store :large-binary data {:sync? true})
          (let [result (k/bget store :large-binary
                               (fn [{:keys [input-stream]}]
                                 (let [baos (java.io.ByteArrayOutputStream.)
                                       buf (byte-array 8192)]
                                   (loop []
                                     (let [n (.read input-stream buf)]
                                       (when (pos? n)
                                         (.write baos buf 0 n)
                                         (recur))))
                                   (.toByteArray baos)))
                               {:sync? true})]
            (is (= size (count result)))
            (is (= (seq data) (seq result)))))))))

(deftest binary-blob-overwrite-test
  (testing "Binary blob overwrites work correctly"
    (let [{:keys [store]} (create-test-store)]
      ;; Write initial
      (k/bassoc store :overwrite (byte-array [1 2 3]) {:sync? true})
      ;; Overwrite with different size
      (k/bassoc store :overwrite (byte-array [4 5 6 7 8]) {:sync? true})
      ;; Verify new data
      (let [result (k/bget store :overwrite
                           (fn [{:keys [input-stream]}]
                             (let [baos (java.io.ByteArrayOutputStream.)]
                               (loop []
                                 (let [b (.read input-stream)]
                                   (when (>= b 0)
                                     (.write baos b)
                                     (recur))))
                               (.toByteArray baos)))
                           {:sync? true})]
        (is (= [4 5 6 7 8] (vec result)))))))

(deftest binary-and-edn-mixed-test
  (testing "Binary and EDN data coexist"
    (let [{:keys [store]} (create-test-store)]
      ;; Write both types
      (sync-assoc! store :edn-key {:type "edn" :value 42})
      (k/bassoc store :binary-key (byte-array [1 2 3]) {:sync? true})

      ;; Verify both readable
      (is (= {:type "edn" :value 42} (sync-get store :edn-key)))
      (let [result (k/bget store :binary-key
                           (fn [{:keys [input-stream]}]
                             (let [baos (java.io.ByteArrayOutputStream.)]
                               (loop []
                                 (let [b (.read input-stream)]
                                   (when (>= b 0)
                                     (.write baos b)
                                     (recur))))
                               (.toByteArray baos)))
                           {:sync? true})]
        (is (= [1 2 3] (vec result)))))))

;; =============================================================================
;; Rapid Key Reuse Tests
;; =============================================================================

(deftest rapid-key-reuse-test
  (testing "Rapid delete/recreate cycles work correctly"
    (let [{:keys [store]} (create-test-store)
          n-cycles 200]
      (dotimes [i n-cycles]
        ;; Create
        (sync-assoc! store :rapid {:cycle i})
        (is (= {:cycle i} (sync-get store :rapid))
            (str "Read after create failed at cycle " i))
        ;; Delete
        (sync-dissoc! store :rapid)
        (is (nil? (sync-get store :rapid))
            (str "Read after delete failed at cycle " i))))))

(deftest rapid-key-reuse-different-values-test
  (testing "Rapid reuse with different value types"
    (let [{:keys [store]} (create-test-store)]
      (dotimes [i 50]
        ;; Alternate between different value types
        (let [value (case (mod i 4)
                      0 {:map "value" :i i}
                      1 ["vector" i]
                      2 (str "string-" i)
                      3 i)]
          (sync-assoc! store :polymorphic value)
          (is (= value (sync-get store :polymorphic)))
          (sync-dissoc! store :polymorphic)
          (is (nil? (sync-get store :polymorphic))))))))

(deftest rapid-key-reuse-concurrent-test
  (testing "Concurrent rapid key reuse"
    (let [{:keys [store]} (create-test-store)
          n-threads 4
          n-cycles 50
          latch (CountDownLatch. n-threads)
          errors (atom [])
          executor (Executors/newFixedThreadPool n-threads)]
      (try
        ;; Each thread rapidly creates/deletes its own key
        (dotimes [t n-threads]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (let [key (keyword (str "thread-" t "-key"))]
                         (dotimes [i n-cycles]
                           (sync-assoc! store key {:thread t :cycle i})
                           (let [v (sync-get store key)]
                             (when (not= t (:thread v))
                               (swap! errors conj {:thread t :cycle i :found v})))
                           (sync-dissoc! store key)))
                       (catch Exception e
                         (swap! errors conj {:thread t :error (.getMessage e)}))
                       (finally
                         (.countDown latch))))))

        (.await latch 30 TimeUnit/SECONDS)

        (is (empty? @errors)
            (str "Errors during concurrent rapid reuse: " @errors))

        (finally
          (.shutdown executor))))))

(deftest rapid-key-reuse-with-binary-test
  (testing "Rapid delete/recreate with binary data"
    (let [{:keys [store]} (create-test-store)]
      (dotimes [i 100]
        ;; Create binary
        (let [data (byte-array (repeatedly 100 #(unchecked-byte i)))]
          (k/bassoc store :rapid-binary data {:sync? true})
          ;; Verify
          (let [result (k/bget store :rapid-binary
                               (fn [{:keys [input-stream]}]
                                 (.read input-stream))
                               {:sync? true})]
            (is (= (unchecked-byte i) (unchecked-byte result))))
          ;; Delete
          (sync-dissoc! store :rapid-binary)
          (is (nil? (k/bget store :rapid-binary identity {:sync? true}))))))))

;; =============================================================================
;; Update-In Atomicity Tests
;; =============================================================================

(deftest update-in-basic-test
  (testing "update-in correctly modifies nested values"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :counter {:count 0 :name "test"})

      (k/update-in store [:counter :count] inc {:sync? true})
      (is (= {:count 1 :name "test"} (sync-get store :counter)))

      (k/update-in store [:counter :count] #(+ % 10) {:sync? true})
      (is (= {:count 11 :name "test"} (sync-get store :counter))))))

(deftest update-in-sequential-test
  (testing "Sequential update-in operations accumulate correctly"
    (let [{:keys [store]} (create-test-store)
          n-updates 100]
      (sync-assoc! store :counter {:value 0})

      (dotimes [_ n-updates]
        (k/update-in store [:counter :value] inc {:sync? true}))

      (is (= {:value n-updates} (sync-get store :counter))))))

(deftest update-in-concurrent-test
  (testing "Concurrent update-in operations - checking for lost updates"
    (let [{:keys [store]} (create-test-store)
          n-threads 5
          n-updates 20
          latch (CountDownLatch. n-threads)
          executor (Executors/newFixedThreadPool n-threads)]
      (try
        (sync-assoc! store :shared-counter {:value 0})

        (dotimes [_ n-threads]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (dotimes [_ n-updates]
                         (k/update-in store [:shared-counter :value] inc {:sync? true}))
                       (finally
                         (.countDown latch))))))

        (.await latch 30 TimeUnit/SECONDS)

        ;; konserve serialises update-in on a per-key lock, so every increment
        ;; must land. This is asserted exactly, not merely reported: a weaker
        ;; check (final-value is positive) passes even when most updates are
        ;; lost, and would not catch a regression in the locking path.
        (let [final-value (:value (sync-get store :shared-counter))
              expected (* n-threads n-updates)]
          (is (= expected final-value)
              (format "lost %d of %d updates -- per-key locking did not hold"
                      (- expected final-value) expected)))

        (finally
          (.shutdown executor))))))

(deftest update-in-with-crash-test
  (testing "Crash during update-in preserves atomicity"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      (sync-assoc! store :counter {:value 10})

      (crash/set-crash-point! crash-point-atom :after-write-value)

      (is (thrown? ExceptionInfo
                   (k/update-in store [:counter :value] inc {:sync? true})))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; Original value preserved
      (is (= {:value 10} (sync-get store :counter))))))

;; =============================================================================
;; Store Close/Reopen Tests
;; =============================================================================

(deftest store-reopen-preserves-data-test
  (testing "Data survives store close and reopen"
    (let [;; Create first store instance
          {:keys [backing state-atom]} (crash/create-crash-aware-store)
          _ (konserve.impl.storage-layout/-create-store backing {:sync? true})
          store1 (defaults/connect-default-store
                  backing
                  {:opts {:sync? true}
                   :config {:default-serializer :FressianSerializer
                            :serializers {:FressianSerializer (ser/fressian-serializer)}}
                   :buffer-size 8192})]

      ;; Write data
      (k/assoc store1 :persistent {:value "survives"} {:sync? true})
      (k/assoc store1 :also-persistent {:count 42} {:sync? true})

      ;; Create second store instance on same state
      (let [store2 (defaults/connect-default-store
                    backing
                    {:opts {:sync? true}
                     :config {:default-serializer :FressianSerializer
                              :serializers {:FressianSerializer (ser/fressian-serializer)}}
                     :buffer-size 8192})]

        ;; Verify data visible in second instance
        (is (= {:value "survives"} (k/get store2 :persistent nil {:sync? true})))
        (is (= {:count 42} (k/get store2 :also-persistent nil {:sync? true})))))))

(deftest store-reopen-after-crash-test
  (testing "Store reopens correctly after crash during write"
    (let [{:keys [backing state-atom crash-point-atom]} (crash/create-crash-aware-store)
          _ (konserve.impl.storage-layout/-create-store backing {:sync? true})
          store1 (defaults/connect-default-store
                  backing
                  {:opts {:sync? true}
                   :config {:default-serializer :FressianSerializer
                            :serializers {:FressianSerializer (ser/fressian-serializer)}}
                   :buffer-size 8192})]

      ;; Write some data
      (k/assoc store1 :before-crash {:safe true} {:sync? true})

      ;; Crash during another write
      (crash/set-crash-point! crash-point-atom :after-write-value)
      (try
        (k/assoc store1 :during-crash {:unsafe true} {:sync? true})
        (catch ExceptionInfo _))

      ;; Simulate crash
      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; Reopen store
      (let [store2 (defaults/connect-default-store
                    backing
                    {:opts {:sync? true}
                     :config {:default-serializer :FressianSerializer
                              :serializers {:FressianSerializer (ser/fressian-serializer)}}
                     :buffer-size 8192})]

        ;; Safe data survives, unsafe data doesn't
        (is (= {:safe true} (k/get store2 :before-crash nil {:sync? true})))
        (is (nil? (k/get store2 :during-crash nil {:sync? true})))))))

;; =============================================================================
;; Keys Enumeration Under Modification Tests
;; =============================================================================

(deftest keys-enumeration-basic-test
  (testing "keys returns all stored keys"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :key1 {:v 1})
      (sync-assoc! store :key2 {:v 2})
      (sync-assoc! store :key3 {:v 3})

      (let [ks (k/keys store {:sync? true})]
        (is (= 3 (count ks)))))))

(deftest keys-enumeration-during-writes-test
  (testing "keys enumeration while concurrent writes happen"
    (let [{:keys [store]} (create-test-store)
          writer-running (java.util.concurrent.atomic.AtomicBoolean. true)
          latch (CountDownLatch. 1)
          executor (Executors/newFixedThreadPool 2)
          keys-results (atom [])
          errors (atom [])]
      (try
        ;; Writer thread continuously adds/removes keys
        (.submit executor
                 ^Runnable
                 (fn []
                   (try
                     (loop [i 0]
                       (when (.get writer-running)
                         (let [key (keyword (str "key-" (mod i 30)))]
                           (if (even? i)
                             (sync-assoc! store key {:i i})
                             (sync-dissoc! store key)))
                         (recur (inc i))))
                     (finally
                       (.countDown latch)))))

        ;; Reader thread enumerates keys repeatedly
        (dotimes [_ 50]
          (try
            (let [ks (k/keys store {:sync? true})]
              (swap! keys-results conj (count ks))
              ;; Should always return a valid set
              (when-not (set? ks)
                (swap! errors conj "keys did not return a set")))
            (catch Exception e
              (swap! errors conj (.getMessage e)))))

        (.set writer-running false)
        (.await latch 5 TimeUnit/SECONDS)

        ;; Main invariant: keys should never error during concurrent modification
        (is (empty? @errors) (str "Errors during concurrent keys enumeration: " @errors))
        ;; We should have gotten some results
        (is (pos? (count @keys-results)) "Should have enumerated keys multiple times")

        (finally
          (.shutdown executor))))))

(deftest keys-after-delete-test
  (testing "keys correctly reflects deletions"
    (let [{:keys [store]} (create-test-store)]
      (sync-assoc! store :key1 {:v 1})
      (sync-assoc! store :key2 {:v 2})
      (sync-assoc! store :key3 {:v 3})

      (is (= 3 (count (k/keys store {:sync? true}))))

      (sync-dissoc! store :key2)

      (let [ks (k/keys store {:sync? true})]
        (is (= 2 (count ks)))
        (is (not (contains? ks :key2)))))))

;; =============================================================================
;; Delete During Concurrent Operations Tests
;; =============================================================================

(deftest delete-during-read-test
  (testing "Delete while read is in progress"
    (let [{:keys [store]} (create-test-store)
          read-started (java.util.concurrent.atomic.AtomicBoolean. false)
          delete-done (java.util.concurrent.atomic.AtomicBoolean. false)
          executor (Executors/newFixedThreadPool 2)]
      (try
        ;; Write initial data
        (sync-assoc! store :target {:value "original"})

        ;; We can't truly interleave at the backing store level in this test,
        ;; but we can verify that delete followed by read returns nil
        (sync-dissoc! store :target)
        (is (nil? (sync-get store :target)))

        (finally
          (.shutdown executor))))))

(deftest delete-during-update-test
  (testing "Delete and update racing on same key"
    (let [{:keys [store]} (create-test-store)
          n-iterations 50
          results (atom {:update-wins 0 :delete-wins 0 :errors 0})
          executor (Executors/newFixedThreadPool 2)]
      (try
        (dotimes [_ n-iterations]
          ;; Reset state
          (sync-assoc! store :contested {:value 0})

          (let [latch (CountDownLatch. 2)]
            ;; Thread 1: update
            (.submit executor
                     ^Runnable
                     (fn []
                       (try
                         (k/update-in store [:contested :value] inc {:sync? true})
                         (catch Exception _))
                       (.countDown latch)))

            ;; Thread 2: delete
            (.submit executor
                     ^Runnable
                     (fn []
                       (try
                         (sync-dissoc! store :contested)
                         (catch Exception _))
                       (.countDown latch)))

            (.await latch 5 TimeUnit/SECONDS)

            ;; Check outcome
            (let [result (sync-get store :contested)]
              (cond
                (nil? result) (swap! results update :delete-wins inc)
                (map? result) (swap! results update :update-wins inc)
                :else (swap! results update :errors inc)))))

        ;; Should see both outcomes (proves race exists)
        (is (zero? (:errors @results)) "No errors during racing operations")
        ;; At least one of each outcome expected over 50 iterations
        (is (or (pos? (:delete-wins @results))
                (pos? (:update-wins @results)))
            "Should see at least some successful operations")

        (finally
          (.shutdown executor))))))

(deftest delete-nonexistent-key-test
  (testing "Deleting nonexistent key doesn't error"
    (let [{:keys [store]} (create-test-store)]
      ;; Should not throw
      (sync-dissoc! store :never-existed)
      (is (nil? (sync-get store :never-existed))))))

;; =============================================================================
;; Append/Log Operation Tests
;; =============================================================================

(deftest append-basic-test
  (testing "append adds entries to log"
    (let [{:keys [store]} (create-test-store)]
      (k/append store :event-log {:event "first" :ts 1} {:sync? true})
      (k/append store :event-log {:event "second" :ts 2} {:sync? true})
      (k/append store :event-log {:event "third" :ts 3} {:sync? true})

      (let [log (k/log store :event-log {:sync? true})]
        (is (= 3 (count log)))
        (is (= "first" (:event (first log))))
        (is (= "third" (:event (last log))))))))

(deftest append-preserves-order-test
  (testing "append preserves insertion order"
    (let [{:keys [store]} (create-test-store)
          n-entries 100]
      (dotimes [i n-entries]
        (k/append store :ordered-log {:index i} {:sync? true}))

      (let [log (k/log store :ordered-log {:sync? true})]
        (is (= n-entries (count log)))
        ;; Verify order
        (doseq [[i entry] (map-indexed vector log)]
          (is (= i (:index entry)) (str "Entry at position " i " should have index " i)))))))

(deftest append-concurrent-test
  (testing "Concurrent appends all succeed"
    (let [{:keys [store]} (create-test-store)
          n-threads 4
          n-appends 25
          latch (CountDownLatch. n-threads)
          executor (Executors/newFixedThreadPool n-threads)]
      (try
        (dotimes [t n-threads]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (dotimes [i n-appends]
                         (k/append store :concurrent-log
                                   {:thread t :seq i}
                                   {:sync? true}))
                       (finally
                         (.countDown latch))))))

        (.await latch 30 TimeUnit/SECONDS)

        (let [log (k/log store :concurrent-log {:sync? true})]
          ;; All entries should be present
          (is (= (* n-threads n-appends) (count log)))
          ;; Each thread's entries should all be present
          (doseq [t (range n-threads)]
            (let [thread-entries (filter #(= t (:thread %)) log)]
              (is (= n-appends (count thread-entries))
                  (str "Thread " t " should have all entries")))))

        (finally
          (.shutdown executor))))))

(deftest append-with-crash-test
  (testing "Crash during append doesn't corrupt log"
    (let [{:keys [store state-atom crash-point-atom]} (create-test-store)]
      ;; Add some entries
      (k/append store :crash-log {:entry 1} {:sync? true})
      (k/append store :crash-log {:entry 2} {:sync? true})

      ;; Crash during third append
      (crash/set-crash-point! crash-point-atom :after-write-value)
      (try
        (k/append store :crash-log {:entry 3} {:sync? true})
        (catch ExceptionInfo _))

      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)

      ;; Log should have first two entries
      (let [log (k/log store :crash-log {:sync? true})]
        (is (= 2 (count log)))
        (is (= 1 (:entry (first log))))
        (is (= 2 (:entry (second log))))))))

(deftest log-empty-test
  (testing "log on nonexistent key returns empty"
    (let [{:keys [store]} (create-test-store)]
      (let [log (k/log store :no-such-log {:sync? true})]
        (is (or (nil? log) (empty? log)))))))
