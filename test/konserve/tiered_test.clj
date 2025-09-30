(ns konserve.tiered-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!! go chan put! close! <!] :as async]
            [konserve.core :refer [bassoc bget keys get-in assoc-in dissoc exists? get]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.tiered :refer [connect-tiered-store sync-on-connect populate-missing-strategy]]
            [konserve.memory :as memory]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.tests.gc :as gct]))

(deftest tiered-store-compliance-test
  (let [folder "/tmp/konserve-tiered-comp-test"
        _ (delete-store folder)
        frontend-store (<!! (memory/new-mem-store))
        backend-store (<!! (connect-fs-store folder))
        store (<!! (connect-tiered-store frontend-store backend-store))]
    (testing "Compliance test with default config."
      (compliance-test store))
    (delete-store folder)))

(deftest tiered-store-write-policies-test
  (testing "Write policy behaviors"
    (let [folder "/tmp/konserve-tiered-write-test"
          _ (delete-store folder)]

      (testing "Write-through policy"
        (let [frontend-store (<!! (memory/new-mem-store))
              backend-store (<!! (connect-fs-store folder))
              store (<!! (connect-tiered-store frontend-store backend-store
                                               :write-policy :write-through))]
          (<!! (assoc-in store [:test-key] {:value 42}))

          ;; Both stores should have the value immediately
          (is (= {:value 42} (<!! (get-in frontend-store [:test-key]))))
          (is (= {:value 42} (<!! (get-in backend-store [:test-key]))))
          (delete-store folder)))

      (testing "Write-around policy"
        (let [frontend-store (<!! (memory/new-mem-store))
              backend-store (<!! (connect-fs-store folder))
              store (<!! (connect-tiered-store frontend-store backend-store
                                               :write-policy :write-around))]
          (<!! (assoc-in store [:test-key] {:value 44}))

          ;; Only backend should have the value
          (is (nil? (<!! (get-in frontend-store [:test-key]))))
          (is (= {:value 44} (<!! (get-in backend-store [:test-key]))))
          ;; Value should be available through the tiered store
          (is (= {:value 44} (<!! (get-in store [:test-key]))))
          (delete-store folder))))))

(deftest tiered-store-read-policies-test
  (testing "Read policy behaviors"
    (let [folder "/tmp/konserve-tiered-read-test"
          _ (delete-store folder)]

      (testing "Frontend-first policy"
        (let [frontend-store (<!! (memory/new-mem-store))
              backend-store (<!! (connect-fs-store folder))
              store (<!! (connect-tiered-store frontend-store backend-store
                                               :read-policy :frontend-first))]
          ;; Put different values in each store
          (<!! (assoc-in frontend-store [:test-key] {:source "frontend"}))
          (<!! (assoc-in backend-store [:test-key] {:source "backend"}))

          ;; Should read from frontend first
          (is (= {:source "frontend"} (<!! (get-in store [:test-key]))))
          (delete-store folder)))

      (testing "Frontend-only policy"
        (let [frontend-store (<!! (memory/new-mem-store))
              backend-store (<!! (connect-fs-store folder))
              store (<!! (connect-tiered-store frontend-store backend-store
                                               :read-policy :frontend-only))]

          (<!! (assoc-in backend-store [:test-key] {:source "backend"}))

          ;; Should read from frontend only
          (is (nil? (<!! (get-in store [:test-key]))))
          (delete-store folder))))))

(deftest tiered-store-keys-operations-test
  (testing "Key operations"
    (let [folder "/tmp/konserve-tiered-keys-test"
          _ (delete-store folder)
          frontend-store (<!! (memory/new-mem-store))
          backend-store (<!! (connect-fs-store folder))
          store (<!! (connect-tiered-store frontend-store backend-store))]

      ;; Add keys to both stores
      (<!! (assoc-in frontend-store [:frontend-key] {:value "frontend"}))
      (<!! (assoc-in backend-store [:backend-key] {:value "backend"}))
      (<!! (assoc-in store [:tiered-key] {:value "tiered"}))

      ;; Keys should include all keys from both stores
      (let [all-keys (set (map :key (<!! (keys store))))]
        ;; Only when a key is in the backend store it should appear in the tiered store
        (is (not (contains? all-keys :frontend-key)))
        (is (contains? all-keys :backend-key))
        (is (contains? all-keys :tiered-key)))

      ;; Test exists?
      (is (<!! (exists? store :frontend-key)))
      (is (<!! (exists? store :backend-key)))
      (is (<!! (exists? store :tiered-key)))
      (is (not (<!! (exists? store :nonexistent-key))))

      ;; Test dissoc
      (<!! (dissoc store :tiered-key))
      (is (not (<!! (exists? store :tiered-key))))

      (delete-store folder))))

(deftest tiered-store-binary-operations-test
  (testing "Binary operations"
    (let [folder "/tmp/konserve-tiered-binary-test"
          _ (delete-store folder)
          frontend-store (<!! (memory/new-mem-store))
          backend-store (<!! (connect-fs-store folder))
          store (<!! (connect-tiered-store frontend-store backend-store
                                           :write-policy :write-through))]

      (testing "Binary storage and retrieval"
        (let [test-bytes (byte-array (range 10))
              res-ch (chan)]

          ;; Store binary data
          (is (<!! (bassoc store :binary-key test-bytes)))

          ;; Retrieve binary data
          (is (<!! (bget store :binary-key
                         (fn [{:keys [input-stream]}]
                           (go
                             (put! res-ch (mapv byte (slurp input-stream))))))))

          ;; Verify data integrity
          (is (= (mapv byte test-bytes) (<!! res-ch)))
          (close! res-ch)))

      (delete-store folder))))

(deftest tiered-store-error-handling-test
  (testing "Error handling"
    (let [folder "/tmp/konserve-tiered-error-test"
          _ (delete-store folder)]

      (testing "Invalid write policy"
        (is (thrown? Exception
                     (<!! (connect-tiered-store
                           (<!! (memory/new-mem-store))
                           (<!! (connect-fs-store folder))
                           :write-policy :invalid-policy)))))

      (testing "Invalid read policy"
        (is (thrown? Exception
                     (<!! (connect-tiered-store
                           (<!! (memory/new-mem-store))
                           (<!! (connect-fs-store folder))
                           :read-policy :invalid-policy)))))

      (delete-store folder))))

(deftest tiered-store-sync-on-connect-test
  (testing "Sync on connect functionality"
    (let [folder "/tmp/konserve-tiered-sync-test"
          _ (delete-store folder)
          backend-store (<!! (connect-fs-store folder))
          _ (<!! (assoc-in backend-store [:existing-key-1] {:value "existing1"}))
          _ (<!! (assoc-in backend-store [:existing-key-2] {:value "existing2"}))

          frontend-store (<!! (memory/new-mem-store))
          tiered-store (<!! (connect-tiered-store frontend-store backend-store))

          ;; Perform sync on connect
          sync-result (<!! (sync-on-connect tiered-store
                                            populate-missing-strategy
                                            {:sync? false}))]

      ;; Check sync result
      (is (= 2 (:synced-keys sync-result)))
      (is (= 0 (:frontend-keys sync-result)))
      (is (= 2 (:backend-keys sync-result)))

      ;; Verify frontend now has the backend keys after sync
      (is (= {:value "existing1"} (<!! (get-in frontend-store [:existing-key-1]))))
      (is (= {:value "existing2"} (<!! (get-in frontend-store [:existing-key-2]))))

      ;; Verify tiered store can access the keys
      (is (= {:value "existing1"} (<!! (get-in tiered-store [:existing-key-1]))))
      (is (= {:value "existing2"} (<!! (get-in tiered-store [:existing-key-2]))))

      (delete-store folder))))
