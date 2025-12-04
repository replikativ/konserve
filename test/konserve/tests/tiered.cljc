(ns konserve.tests.tiered
  (:require [clojure.core.async :refer [go <! timeout alts! promise-chan put! close!]]
            [clojure.test :refer [is testing]]
            [konserve.core :as k]
            [konserve.tiered :as tiered]
            [konserve.compliance-test :refer [async-compliance-test] :as ct]
            [superv.async :refer [<?-]]))

#?(:clj
   (defn test-tiered-compliance-sync [frontend-store backend-store]
     (let [store (clojure.core.async/<!! (tiered/connect-tiered-store frontend-store backend-store))]
       (ct/compliance-test store))))

(defn test-tiered-deep-async
  "Tests the deep read miss scenario in a tiered store, ensuring that a value
   read from the deepest backend propagates to the frontend and intermediate tiers,
   triggering write hooks where appropriate."
  [frontend-store middle-store deepest-store]
  (let [hook-promise (promise-chan)
        hook (fn [e] (put! hook-promise e))]
    (k/add-write-hook! middle-store :debug-hook hook)
    (go
      (let [tier-2-3  (<! (tiered/connect-tiered-store middle-store deepest-store))
            top-store (<! (tiered/connect-tiered-store frontend-store tier-2-3))]
        (and
         (is (= [nil "found-it"] (<! (k/assoc-in deepest-store [:deep-secret] "found-it"))))
         (is (false? (<! (k/exists? middle-store :deep-secret))) "Should not be in middle tier yet")
         (is (false? (<! (k/exists? frontend-store :deep-secret))) "Should not be in frontend yet")
         (is (= "found-it" (<! (k/get-in top-store [:deep-secret]))) "Should retrieve value from deepest store")
         (is (true? (<! (k/exists? frontend-store :deep-secret))) "Value should now be in frontend store")
         ;; Verify write hook on middle tier
         (let [[val port] (alts! [hook-promise (timeout 2000)])]
           (and
            (is (identical? port hook-promise) "Write hook should have fired on middle tier")
            (is (= {:api-op :assoc-in, :key :deep-secret, :key-vec [:deep-secret], :value "found-it"} val))))
         (is (true? (<! (k/exists? middle-store :deep-secret))) "Value should now be in middle tier"))))))

(defn test-tiered-compliance-async
  [frontend-store backend-store]
  (go
    (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store))]
      (<! (async-compliance-test store)))))

(defn test-write-policies-async [frontend-store backend-store]
  (go
    (testing "Write-through policy"
      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :write-policy :write-through))]
        (<?- (k/assoc-in store [:test-key] {:value 42}))
        (is (= {:value 42} (<?- (k/get-in frontend-store [:test-key]))))
        (is (= {:value 42} (<?- (k/get-in backend-store [:test-key]))))))

    (testing "Write-around policy"
      ;; Clean up test key from previous test if stores are reused, though usually new stores are passed
      (<?- (k/dissoc frontend-store :test-key))
      (<?- (k/dissoc backend-store :test-key))
      
      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :write-policy :write-around))]
        (<?- (k/assoc-in store [:test-key] {:value 44}))
        (is (nil? (<?- (k/get-in frontend-store [:test-key]))))
        (is (= {:value 44} (<?- (k/get-in backend-store [:test-key]))))
        (is (= {:value 44} (<?- (k/get-in store [:test-key]))))))))

(defn test-read-policies-async [frontend-store backend-store]
  (go
    (testing "Frontend-first policy"
      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :read-policy :frontend-first))]
        (<?- (k/assoc-in frontend-store [:test-key] {:source "frontend"}))
        (<?- (k/assoc-in backend-store [:test-key] {:source "backend"}))
        (is (= {:source "frontend"} (<?- (k/get-in store [:test-key]))))))

    (testing "Frontend-only policy"
      (<?- (k/dissoc frontend-store :test-key))
      (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                    :read-policy :frontend-only))]
        (<?- (k/assoc-in backend-store [:test-key] {:source "backend"}))
        (is (nil? (<?- (k/get-in store [:test-key]))))))))

(defn test-key-operations-async [frontend-store backend-store]
  (go
    (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store))]
      (<?- (k/assoc-in frontend-store [:frontend-key] {:value "frontend"}))
      (<?- (k/assoc-in backend-store [:backend-key] {:value "backend"}))
      (<?- (k/assoc-in store [:tiered-key] {:value "tiered"}))

      (let [all-keys (set (map :key (<?- (k/keys store))))]
        (is (not (contains? all-keys :frontend-key)))
        (is (contains? all-keys :backend-key))
        (is (contains? all-keys :tiered-key)))

      (is (true? (<?- (k/exists? store :frontend-key))))
      (is (true? (<?- (k/exists? store :backend-key))))
      (is (true? (<?- (k/exists? store :tiered-key))))
      (is (false? (<?- (k/exists? store :nonexistent-key))))

      (<?- (k/dissoc store :tiered-key))
      (is (false? (<?- (k/exists? store :tiered-key)))))))

(defn test-binary-operations-async [frontend-store backend-store]
  (go
    (let [store (<?- (tiered/connect-tiered-store frontend-store backend-store
                                                  :write-policy :write-through))
          test-bytes #?(:clj (byte-array (range 10))
                        :cljs #js [0 1 2 3 4 5 6 7 8 9])]

      (is (true? (<?- (k/bassoc store :binary-key test-bytes))))

      (let [read-val (<?- (k/bget store :binary-key
                                  (fn [{:keys [input-stream]}]
                                    (go input-stream))))]
        (is (= (vec (seq test-bytes)) (vec (seq read-val))))))))

(defn test-sync-on-connect-async [frontend-store backend-store]
  (go
    (<?- (k/assoc-in backend-store [:existing-key-1] {:value "existing1"}))
    (<?- (k/assoc-in backend-store [:existing-key-2] {:value "existing2"}))

    (let [tiered-store (<?- (tiered/connect-tiered-store frontend-store backend-store))
          sync-result (<?- (tiered/sync-on-connect tiered-store
                                                  tiered/populate-missing-strategy
                                                  {:sync? false}))]

      (is (= 2 (:synced-keys sync-result)))
      (is (= 0 (:frontend-keys sync-result)))
      (is (= 2 (:backend-keys sync-result)))

      (is (= {:value "existing1"} (<?- (k/get-in frontend-store [:existing-key-1]))))
      (is (= {:value "existing2"} (<?- (k/get-in frontend-store [:existing-key-2]))))

      (is (= {:value "existing1"} (<?- (k/get-in tiered-store [:existing-key-1]))))
      (is (= {:value "existing2"} (<?- (k/get-in tiered-store [:existing-key-2])))))))

(defn test-error-handling [frontend-store backend-store]
  (testing "Error handling"
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (tiered/connect-tiered-store frontend-store backend-store :write-policy :invalid-policy)))
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (tiered/connect-tiered-store frontend-store backend-store :read-policy :invalid-policy)))))
