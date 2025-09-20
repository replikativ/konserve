(ns konserve.tiered-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!! go chan put! close! <!] :as async]
            [konserve.core :refer [bassoc bget keys get-in assoc-in dissoc exists? get]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.tiered :refer [connect-tiered-store
                                     populate-missing-strategy full-sync-strategy
                                     connect-memory-tiered-store
                                     perform-sync]]
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

      (testing "Backend-first policy"
        (let [frontend-store (<!! (memory/new-mem-store))
              backend-store (<!! (connect-fs-store folder))
              store (<!! (connect-tiered-store frontend-store backend-store
                                               :read-policy :backend-first))]
          ;; Put different values in each store
          (<!! (assoc-in frontend-store [:test-key] {:source "frontend"}))
          (<!! (assoc-in backend-store [:test-key] {:source "backend"}))

          ;; Should read from backend first
          (is (= {:source "backend"} (<!! (get-in store [:test-key]))))
          (delete-store folder))))))

(deftest tiered-store-sync-test
  (testing "Sync functionality"
    (let [folder "/tmp/konserve-tiered-sync-test"
          _ (delete-store folder)]

      (testing "Populate missing strategy"
        (let [frontend-store (<!! (memory/new-mem-store))
              backend-store (<!! (connect-fs-store folder))]

          ;; Add some data to backend only
          (<!! (assoc-in backend-store [:backend-only-1] {:value "backend1"}))
          (<!! (assoc-in backend-store [:backend-only-2] {:value "backend2"}))
          (<!! (assoc-in backend-store [:shared-key] {:value "backend-shared"}))

          ;; Add some data to frontend only
          (<!! (assoc-in frontend-store [:frontend-only] {:value "frontend"}))
          (<!! (assoc-in frontend-store [:shared-key] {:value "frontend-shared"}))

          ;; Sync with populate-missing strategy
          (let [result (<!! (perform-sync frontend-store backend-store populate-missing-strategy {}))]
            (is (= 2 (:synced-keys result))) ; Should sync only missing keys

            ;; Check that missing keys were added to frontend
            (is (= {:value "backend1"} (<!! (get-in frontend-store [:backend-only-1]))))
            (is (= {:value "backend2"} (<!! (get-in frontend-store [:backend-only-2]))))

            ;; Check that existing keys were not overwritten
            (is (= {:value "frontend-shared"} (<!! (get-in frontend-store [:shared-key]))))
            (is (= {:value "frontend"} (<!! (get-in frontend-store [:frontend-only])))))
          (delete-store folder)))

      (testing "Full sync strategy"
        (let [frontend-store (<!! (memory/new-mem-store))
              backend-store (<!! (connect-fs-store folder))]

          ;; Add some data to backend
          (<!! (assoc-in backend-store [:backend-key-1] {:value "backend1"}))
          (<!! (assoc-in backend-store [:backend-key-2] {:value "backend2"}))

          ;; Add some data to frontend 
          (<!! (assoc-in frontend-store [:frontend-key] {:value "frontend"}))

          ;; Sync with full-sync strategy
          (let [result (<!! (perform-sync frontend-store backend-store full-sync-strategy {}))]
            (is (= 2 (:synced-keys result))) ; Should sync all backend keys

            ;; Check that all backend keys were synced
            (is (= {:value "backend1"} (<!! (get-in frontend-store [:backend-key-1]))))
            (is (= {:value "backend2"} (<!! (get-in frontend-store [:backend-key-2]))))

            ;; Frontend-only keys should remain
            (is (= {:value "frontend"} (<!! (get-in frontend-store [:frontend-key])))))
          (delete-store folder))))))

(deftest tiered-store-convenience-constructors-test
  (testing "Convenience constructor functions"
    (let [folder "/tmp/konserve-tiered-convenience-test"
          _ (delete-store folder)]

      (testing "Memory-tiered store"
        (let [backend-store (<!! (connect-fs-store folder))
              store (<!! (connect-memory-tiered-store backend-store))]

          ;; Should work like a normal tiered store
          (<!! (assoc-in store [:test-key] {:value "test"}))
          (is (= {:value "test"} (<!! (get-in store [:test-key]))))))
      (delete-store folder))))

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

#!============
#! GC tests

(deftest tiered-async-gc-test
  (let [folder "/tmp/konserve-tiered-gc-test"
        _ (delete-store folder)
        frontend-store (<!! (memory/new-mem-store))
        backend-store (<!! (connect-fs-store folder))
        store (<!! (connect-tiered-store frontend-store backend-store))]
    (<!! (gct/test-gc-async store))
    (delete-store folder)))
