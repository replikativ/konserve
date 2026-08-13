(ns konserve.simulation-backing-test
  "Tests for konserve error propagation and memory model.

   These tests verify:
   1. Error propagation - errors at PBackingStore level reach callers
   2. Memory model - atomicity and durability guarantees
   3. FileStore behavior - real filesystem operations"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!]]
            [clojure.java.io :as io]
            [konserve.simulation.backing :as backing]
            [konserve.simulation.memory :as mb]
            [konserve.core :as k]
            [konserve.filestore :as fs]
            [konserve.impl.defaults :refer [connect-default-store]])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def ^:dynamic *test-dir* nil)

(defn create-temp-dir []
  (let [path (Files/createTempDirectory "konserve-simulation-test"
                                        (into-array FileAttribute []))]
    (.toString path)))

(defn delete-dir-recursive [^File f]
  (when (.exists f)
    (when (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-dir-recursive child)))
    (.delete f)))

(defn temp-dir-fixture [f]
  (let [dir (create-temp-dir)]
    (try
      (binding [*test-dir* dir]
        (f))
      (finally
        (delete-dir-recursive (io/file dir))))))

(use-fixtures :each temp-dir-fixture)

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn create-filestore-backing
  "Create a raw BackingFilestore for wrapping."
  [path]
  ;; ephemeral? is a predicate for filtering files - use (constantly false) for no filtering
  ;; filesystem = nil means use default filesystem
  (fs/->BackingFilestore path nil (constantly false) nil))

(defn create-wrapped-store
  "Create a DefaultStore with SimulatedBackingStore wrapping FileStore."
  [path fault-config seed]
  (let [real-backing (create-filestore-backing path)
        rng (backing/rng seed)
        history-atom (atom [])
        simulated-backing (backing/wrap-backing-store real-backing fault-config rng history-atom)]
    {:store (<!! (connect-default-store simulated-backing
                                        {:opts {:sync? false}
                                         :config {:sync-blob? true}}))
     :backing simulated-backing
     :history history-atom}))

(defn create-sync-wrapped-store
  "Create a DefaultStore with SimulatedBackingStore (sync mode)."
  [path fault-config seed]
  (let [real-backing (create-filestore-backing path)
        rng (backing/rng seed)
        history-atom (atom [])
        simulated-backing (backing/wrap-backing-store real-backing fault-config rng history-atom)]
    {:store (connect-default-store simulated-backing
                                   {:opts {:sync? true}
                                    :config {:sync-blob? true}})
     :backing simulated-backing
     :history history-atom}))

;; =============================================================================
;; Error Propagation Tests
;; =============================================================================

(deftest error-propagation-write-header-test
  (testing "write-header fault propagates to assoc"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           (assoc backing/no-faults-config
                                  :write-header-fault-rate 1.0)
                           42)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Simulated write-header fault"
           (k/assoc store :test-key {:value 1} {:sync? true}))))))

(deftest error-propagation-write-meta-test
  (testing "write-meta fault propagates to assoc"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           (assoc backing/no-faults-config
                                  :write-meta-fault-rate 1.0)
                           42)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Simulated write-meta fault"
           (k/assoc store :test-key {:value 1} {:sync? true}))))))

(deftest error-propagation-write-value-test
  (testing "write-value fault propagates to assoc"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           (assoc backing/no-faults-config
                                  :write-value-fault-rate 1.0)
                           42)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Simulated write-value fault"
           (k/assoc store :test-key {:value 1} {:sync? true}))))))

(deftest error-propagation-read-header-test
  (testing "read-header fault propagates to get"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; First write successfully
      (k/assoc store :test-key {:value 1} {:sync? true})

      ;; Now create a new store with read faults
      (let [{:keys [store]} (create-sync-wrapped-store
                             *test-dir*
                             (assoc backing/no-faults-config
                                    :read-header-fault-rate 1.0)
                             43)]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Simulated read-header fault"
             (k/get store :test-key nil {:sync? true})))))))

(deftest error-propagation-read-value-test
  (testing "read-value fault propagates to get"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; First write successfully
      (k/assoc store :test-key {:value 1} {:sync? true})

      ;; Now create a new store with read faults
      (let [{:keys [store]} (create-sync-wrapped-store
                             *test-dir*
                             (assoc backing/no-faults-config
                                    :read-value-fault-rate 1.0)
                             43)]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Simulated read-value fault"
             (k/get store :test-key nil {:sync? true})))))))

(deftest error-propagation-atomic-move-test
  (testing "atomic-move fault propagates to assoc"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           (assoc backing/no-faults-config
                                  :atomic-move-fault-rate 1.0)
                           42)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Simulated atomic-move fault"
           (k/assoc store :test-key {:value 1} {:sync? true}))))))

(deftest error-propagation-delete-blob-test
  (testing "delete-blob fault propagates to dissoc"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; First write successfully
      (k/assoc store :test-key {:value 1} {:sync? true})

      ;; Now create a new store with delete faults
      (let [{:keys [store]} (create-sync-wrapped-store
                             *test-dir*
                             (assoc backing/no-faults-config
                                    :delete-blob-fault-rate 1.0)
                             43)]
        ;; Error propagates but is wrapped by DefaultStore with context
        ;; The wrapping adds {:key :test-key, :exception <our-fault>}
        (try
          (k/dissoc store :test-key {:sync? true})
          (is false "Should have thrown")
          (catch clojure.lang.ExceptionInfo e
            ;; DefaultStore wraps with context, nested exception is our fault
            (let [data (ex-data e)
                  nested-ex (:exception data)]
              (is (some? nested-ex) "Should have nested exception")
              (when nested-ex
                (is (re-find #"Simulated delete-blob fault"
                             (.getMessage ^Exception nested-ex)))))))))))

(deftest error-propagation-async-test
  (testing "errors propagate correctly in async mode"
    (let [{:keys [store]} (create-wrapped-store
                           *test-dir*
                           (assoc backing/no-faults-config
                                  :write-header-fault-rate 1.0)
                           42)
          result (<!! (k/assoc store :test-key {:value 1}))]
      ;; In async mode, errors are returned on the channel
      (is (instance? clojure.lang.ExceptionInfo result))
      (is (re-find #"Simulated write-header fault" (.getMessage ^Exception result))))))

;; =============================================================================
;; Memory Model Tests - Atomicity
;; =============================================================================

(deftest atomicity-write-failure-leaves-no-trace-test
  (testing "failed write does not leave partial data"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; First write successfully
      (k/assoc store :test-key {:value 1} {:sync? true})
      (is (= {:value 1} (k/get store :test-key nil {:sync? true})))

      ;; Now try to write with fault - should fail
      (let [{:keys [store]} (create-sync-wrapped-store
                             *test-dir*
                             (assoc backing/no-faults-config
                                    :write-value-fault-rate 1.0)
                             43)]
        (is (thrown? clojure.lang.ExceptionInfo
                     (k/assoc store :test-key {:value 2} {:sync? true}))))

      ;; Original value should still be readable
      (let [{:keys [store]} (create-sync-wrapped-store
                             *test-dir*
                             backing/no-faults-config
                             44)]
        (is (= {:value 1} (k/get store :test-key nil {:sync? true})))))))

(deftest atomicity-atomic-move-failure-test
  (testing "atomic-move failure leaves original intact"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; First write successfully
      (k/assoc store :test-key {:value 1} {:sync? true})
      (is (= {:value 1} (k/get store :test-key nil {:sync? true}))))

    ;; Now try to write with atomic-move fault
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           (assoc backing/no-faults-config
                                  :atomic-move-fault-rate 1.0)
                           43)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (k/assoc store :test-key {:value 2} {:sync? true}))))

    ;; Original value should still be readable
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           44)]
      (is (= {:value 1} (k/get store :test-key nil {:sync? true}))))))

;; =============================================================================
;; Memory Model Tests - Durability
;; =============================================================================

(deftest durability-successful-write-persists-test
  (testing "successful write is durable across store reopens"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; Write data
      (k/assoc store :key1 {:value 1} {:sync? true})
      (k/assoc store :key2 {:value 2} {:sync? true})
      (k/assoc store :key3 {:value 3} {:sync? true}))

    ;; Reopen store and verify data persists
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           43)]
      (is (= {:value 1} (k/get store :key1 nil {:sync? true})))
      (is (= {:value 2} (k/get store :key2 nil {:sync? true})))
      (is (= {:value 3} (k/get store :key3 nil {:sync? true}))))))

(deftest durability-exists-after-reopen-test
  (testing "exists? works after store reopen"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      (k/assoc store :test-key {:value 1} {:sync? true}))

    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           43)]
      (is (true? (k/exists? store :test-key {:sync? true})))
      (is (false? (k/exists? store :nonexistent {:sync? true}))))))

;; =============================================================================
;; FileStore Specific Tests
;; =============================================================================

(deftest filestore-orphan-new-file-test
  (testing ".new files are orphans from failed writes"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; Successful write
      (k/assoc store :test-key {:value 1} {:sync? true}))

    ;; Simulate crash before atomic-move by manually creating .new file
    ;; (In real scenario, this would be left behind by a crash)
    (let [new-file (io/file *test-dir* "test.ksv.new")]
      (spit new-file "orphan data"))

    ;; Store should still work, ignoring orphan
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           43)]
      (is (= {:value 1} (k/get store :test-key nil {:sync? true})))

      ;; New writes should work
      (k/assoc store :key2 {:value 2} {:sync? true})
      (is (= {:value 2} (k/get store :key2 nil {:sync? true}))))))

(deftest filestore-keys-ignores-new-and-backup-test
  (testing "keys operation ignores .new and .backup files"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; Write some data
      (k/assoc store :key1 {:value 1} {:sync? true})
      (k/assoc store :key2 {:value 2} {:sync? true})

      ;; Create orphan files
      (spit (io/file *test-dir* "orphan.ksv.new") "new file")
      (spit (io/file *test-dir* "orphan.ksv.backup") "backup file")

      ;; Keys should only return real keys
      (let [all-keys (k/keys store {:sync? true})]
        (is (= 2 (count all-keys)))
        (is (every? #(contains? #{:key1 :key2} (:key %)) all-keys))))))

;; =============================================================================
;; History Recording Tests
;; =============================================================================

(deftest history-records-operations-test
  (testing "SimulatedBackingStore records operation history"
    (let [{:keys [store backing]} (create-sync-wrapped-store
                                   *test-dir*
                                   backing/no-faults-config
                                   42)]
      ;; Perform some operations
      (k/assoc store :key1 {:value 1} {:sync? true})
      (k/get store :key1 nil {:sync? true})
      (k/dissoc store :key1 {:sync? true})

      ;; Check history
      (let [history (backing/get-history backing)]
        (is (pos? (count history)))

        ;; Should have invoke/ok pairs
        (is (some #(= :invoke (:op-type %)) history))
        (is (some #(= :ok (:op-type %)) history))

        ;; Should have various operations
        (is (some #(= :create-blob (:operation %)) history))
        (is (some #(= :write-header (:operation %)) history))
        (is (some #(= :atomic-move (:operation %)) history))))))

(deftest history-records-faults-test
  (testing "SimulatedBackingStore records fault injection"
    (let [{:keys [store backing]} (create-sync-wrapped-store
                                   *test-dir*
                                   (assoc backing/no-faults-config
                                          :write-header-fault-rate 1.0)
                                   42)]
      ;; Try to write (will fail)
      (try
        (k/assoc store :key1 {:value 1} {:sync? true})
        (catch Exception _))

      ;; Check history has fault recorded
      (let [history (backing/get-history backing)]
        (is (some #(= :fail (:op-type %)) history))
        (is (= 1 (backing/count-faults backing)))))))

;; =============================================================================
;; Stress Tests
;; =============================================================================

(deftest stress-chaos-mode-test
  (testing "store survives chaos mode without data loss"
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           42)]
      ;; Write initial data without faults
      (dotimes [i 10]
        (k/assoc store (keyword (str "key" i)) {:value i} {:sync? true})))

    ;; Now operate with chaos - some operations will fail
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/chaos-fault-config
                           43)
          successes (atom 0)
          failures (atom 0)]
      (dotimes [i 20]
        (try
          (k/assoc store (keyword (str "chaos" i)) {:value i} {:sync? true})
          (swap! successes inc)
          (catch Exception _
            (swap! failures inc))))

      ;; Some should succeed, some should fail
      (is (pos? @successes) "Some writes should succeed")
      (is (pos? @failures) "Some writes should fail due to chaos"))

    ;; Original data should still be intact
    (let [{:keys [store]} (create-sync-wrapped-store
                           *test-dir*
                           backing/no-faults-config
                           44)]
      (dotimes [i 10]
        (is (= {:value i} (k/get store (keyword (str "key" i)) nil {:sync? true})))))))

;; =============================================================================
;; MemoryBackingStore Tests (Fast, no filesystem)
;; =============================================================================

(deftest memory-backing-basic-operations-test
  (testing "MemoryBackingStore supports basic operations"
    (let [backing (mb/create-memory-backing)
          store (<!! (connect-default-store backing {:opts {:sync? false}}))]

      ;; Write
      (<!! (k/assoc store :key1 {:value 1}))
      (<!! (k/assoc store :key2 {:value 2}))

      ;; Read
      (is (= {:value 1} (<!! (k/get store :key1))))
      (is (= {:value 2} (<!! (k/get store :key2))))

      ;; Exists
      (is (true? (<!! (k/exists? store :key1))))
      (is (false? (<!! (k/exists? store :nonexistent))))

      ;; Delete
      (<!! (k/dissoc store :key1))
      (is (false? (<!! (k/exists? store :key1))))
      (is (= {:value 2} (<!! (k/get store :key2)))))))

(deftest memory-backing-sync-mode-test
  (testing "MemoryBackingStore works in sync mode"
    (let [backing (mb/create-memory-backing)
          store (connect-default-store backing {:opts {:sync? true}})]

      ;; Write
      (k/assoc store :key1 {:value 1} {:sync? true})

      ;; Read
      (is (= {:value 1} (k/get store :key1 nil {:sync? true})))

      ;; Exists
      (is (true? (k/exists? store :key1 {:sync? true}))))))

(deftest memory-backing-update-test
  (testing "MemoryBackingStore supports update-in"
    (let [backing (mb/create-memory-backing)
          store (<!! (connect-default-store backing {:opts {:sync? false}}))]

      (<!! (k/assoc store :counter 0))
      (<!! (k/update store :counter inc))
      (<!! (k/update store :counter inc))
      (<!! (k/update store :counter inc))

      (is (= 3 (<!! (k/get store :counter)))))))

(deftest memory-backing-with-fault-injection-test
  (testing "MemoryBackingStore + SimulatedBackingStore for fast fault testing"
    (let [real-backing (mb/create-memory-backing)
          rng (backing/rng 42)
          history-atom (atom [])
          simulated-backing (backing/wrap-backing-store
                             real-backing
                             backing/no-faults-config
                             rng
                             history-atom)
          store (<!! (connect-default-store simulated-backing {:opts {:sync? false}}))]

      ;; Operations work through simulated layer
      (<!! (k/assoc store :key1 {:value 1}))
      (is (= {:value 1} (<!! (k/get store :key1))))

      ;; History is recorded
      (let [history (backing/get-history simulated-backing)]
        (is (pos? (count history)))
        (is (some #(= :create-blob (:operation %)) history))))))

(deftest memory-backing-fault-propagation-test
  (testing "Faults propagate from MemoryBackingStore through DefaultStore"
    (let [real-backing (mb/create-memory-backing)
          rng (backing/rng 42)
          simulated-backing (backing/wrap-backing-store
                             real-backing
                             (assoc backing/no-faults-config
                                    :write-header-fault-rate 1.0)
                             rng)
          store (connect-default-store simulated-backing {:opts {:sync? true}})]

      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Simulated write-header fault"
           (k/assoc store :test-key {:value 1} {:sync? true}))))))

(deftest memory-backing-keys-test
  (testing "MemoryBackingStore keys operation works"
    (let [backing (mb/create-memory-backing)
          store (<!! (connect-default-store backing {:opts {:sync? false}}))]

      ;; Write multiple keys
      (<!! (k/assoc store :key1 {:value 1}))
      (<!! (k/assoc store :key2 {:value 2}))
      (<!! (k/assoc store :key3 {:value 3}))

      ;; Keys should return all
      (let [all-keys (<!! (k/keys store))]
        (is (= 3 (count all-keys)))))))

(comment
  ;; Run tests
  (clojure.test/run-tests 'konserve.simulation-backing-test)

  ;; Run single test
  (clojure.test/test-var #'error-propagation-write-header-test)
  (clojure.test/test-var #'memory-backing-basic-operations-test))
