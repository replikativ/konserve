(ns konserve.store-test
  (:require [clojure.core.async :as a :refer [go <! take! #?(:clj <!!)]]
            #?(:clj [clojure.core.async.impl.protocols])
            [clojure.test :refer [deftest #?(:cljs async) is testing]]
            [konserve.store :as store]
            [konserve.core :as k]))

;; =============================================================================
;; Memory Backend Tests
;; =============================================================================

#?(:clj
   (deftest memory-store-connect-sync
     (testing "connect-store :memory with sync mode"
       (let [id #uuid "550e8400-e29b-41d4-a716-446655440001"
             _ (store/create-store {:backend :memory :id id} {:sync? true})
             store-inst (store/connect-store {:backend :memory :id id} {:sync? true})]
         (is (some? store-inst))
         ;; Basic operations should work
         (k/assoc-in store-inst ["test"] 42 {:sync? true})
         (let [val (k/get-in store-inst ["test"] nil {:sync? true})]
           (is (= val 42)))
         (store/delete-store {:backend :memory :id id})))))

#?(:clj
   (deftest memory-store-connect-async
     (testing "create-store :memory with async mode"
       (let [id #uuid "550e8400-e29b-41d4-a716-446655440002"
             ch (store/create-store {:backend :memory :id id} {:sync? false})]
         (let [store-inst (<!! ch)]
           (is (some? store-inst))
           ;; Basic operations should work asynchronously
           (<!! (k/assoc-in store-inst ["test"] 42 {:sync? false}))
           (let [val (<!! (k/get-in store-inst ["test"] nil {:sync? false}))]
             (is (= val 42)))
           (store/delete-store {:backend :memory :id id}))))))

#?(:clj
   (deftest memory-store-create-sync
     (testing "create-store :memory creates new store"
       (let [id #uuid "550e8400-e29b-41d4-a716-446655440003"
             store-inst (store/create-store {:backend :memory :id id} {:sync? true})]
         (is (some? store-inst))
         (store/delete-store {:backend :memory :id id})))))

#?(:clj
   (deftest memory-store-delete
     (testing "delete-store :memory removes the store from the registry"
       ;; NB: this used to assert `(nil? (store/delete-store {...}))` with no opts —
       ;; i.e. it asserted that the ASYNC path returns a plain nil. That was the bug
       ;; written down as a test. delete-store defaults to {:sync? false} and must
       ;; return a channel; the value is delivered on it.
       (let [id #uuid "550e8400-e29b-41d4-a716-446655440004"
             cfg {:backend :memory :id id}]
         (store/create-store cfg {:sync? true})
         (is (nil? (<!! (store/delete-store cfg))))
         (is (false? (store/store-exists? cfg {:sync? true})))))))

#?(:clj
   (deftest memory-store-release
     (testing "release-store :memory does nothing"
       (let [id #uuid "550e8400-e29b-41d4-a716-446655440005"]
         ;; Memory store has nothing to release
         (is (nil? (store/release-store {:backend :memory :id id} nil {:sync? true})))))))

;; =============================================================================
;; File Backend Tests (JVM only)
;; =============================================================================

#?(:clj
   (deftest file-store-connect-sync
     (testing "connect-store :file with sync mode"
       (let [tmpdir (System/getProperty "java.io.tmpdir")
             path (str tmpdir "/konserve-store-test-" (System/currentTimeMillis))
             id #uuid "550e8400-e29b-41d4-a716-446655440006"
             store-inst (store/create-store {:backend :file :path path :id id} {:sync? true})]
         (is (some? store-inst))
         ;; Cleanup
         (store/delete-store {:backend :file :path path :id id})))))

#?(:clj
   (deftest file-store-connect-async
     (testing "create-store :file with async mode"
       (let [tmpdir (System/getProperty "java.io.tmpdir")
             path (str tmpdir "/konserve-store-test-" (System/currentTimeMillis))
             id #uuid "550e8400-e29b-41d4-a716-446655440007"
             ch (store/create-store {:backend :file :path path :id id} {:sync? false})]
         (let [store-inst (<!! ch)]
           (is (some? store-inst)))
         ;; Cleanup
         (store/delete-store {:backend :file :path path :id id})))))

;; =============================================================================
;; Unsupported Backend Error Handling
;; =============================================================================

#?(:clj
   (deftest unsupported-backend-connect
     (testing "connect-store with unsupported backend throws"
       (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                             #"Unsupported store backend: :unknown"
                             (store/connect-store {:backend :unknown :id #uuid "550e8400-e29b-41d4-a716-446655440008"}))))))

#?(:clj
   (deftest unsupported-backend-create
     (testing "create-store with unsupported backend throws"
       (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                             #"Unsupported store backend: :unknown"
                             (store/create-store {:backend :unknown :id #uuid "550e8400-e29b-41d4-a716-446655440009"}))))))

#?(:clj
   (deftest unsupported-backend-delete
     (testing "delete-store with unsupported backend throws"
       (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                             #"Unsupported store backend: :unknown"
                             (store/delete-store {:backend :unknown :id #uuid "550e8400-e29b-41d4-a716-44665544000a"}))))))

;; =============================================================================
;; UUID Validation Tests
;; =============================================================================

#?(:clj
   (deftest missing-id-validation
     (testing "Stores require :id field"
       (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                             #"Store :id is required"
                             (store/create-store {:backend :memory} {:sync? true}))))))

#?(:clj
   (deftest invalid-uuid-type-validation
     (testing "Store :id must be UUID type, not string"
       (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                             #"Store :id must be a UUID type"
                             (store/create-store {:backend :memory
                                                  :id "550e8400-e29b-41d4-a716-446655440000"}
                                                 {:sync? true}))))))

#?(:clj
   (deftest valid-uuid-accepted
     (testing "Valid UUID type is accepted"
       (let [id #uuid "550e8400-e29b-41d4-a716-44665544000b"
             store (store/create-store {:backend :memory :id id} {:sync? true})]
         (is (some? store))
         (store/delete-store {:backend :memory :id id})))))

;; =============================================================================
;; Backend Registration Pattern Tests
;; =============================================================================

#?(:clj
   (deftest async-sync-polymorphism-memory
     (testing "async/sync polymorphism works with memory store"
       ;; Sync mode
       (let [id #uuid "550e8400-e29b-41d4-a716-44665544000c"
             store-sync (store/create-store {:backend :memory :id id} {:sync? true})]
         (k/assoc-in store-sync ["key1"] "value1" {:sync? true})
         (let [result (k/get-in store-sync ["key1"] nil {:sync? true})]
           (is (= result "value1"))))

       ;; Async mode
       (let [id #uuid "550e8400-e29b-41d4-a716-44665544000d"
             store-async (<!! (store/create-store {:backend :memory :id id} {:sync? false}))]
         (<!! (k/assoc-in store-async ["key2"] "value2" {:sync? false}))
         (let [result (<!! (k/get-in store-async ["key2"] nil {:sync? false}))]
           (is (= result "value2")))))))

#?(:clj
   (deftest async-sync-polymorphism-file
     (testing "async/sync polymorphism works with file store"
       (let [tmpdir (System/getProperty "java.io.tmpdir")
             path (str tmpdir "/konserve-store-test-async-sync-" (System/currentTimeMillis))
             id #uuid "550e8400-e29b-41d4-a716-44665544000e"]
         ;; Sync mode
         (let [store-sync (store/create-store {:backend :file :path path :id id} {:sync? true})]
           (k/assoc-in store-sync ["key1"] "value1" {:sync? true})
           (let [result (k/get-in store-sync ["key1"] nil {:sync? true})]
             (is (= result "value1"))))

         ;; Async mode
         (let [store-async (<!! (store/connect-store {:backend :file :path path :id id} {:sync? false}))]
           (<!! (k/assoc-in store-async ["key2"] "value2" {:sync? false}))
           (let [result (<!! (k/get-in store-async ["key2"] nil {:sync? false}))]
             (is (= result "value2"))))

         ;; Cleanup
         (store/delete-store {:backend :file :path path :id id})))))

;; =============================================================================
;; -delete-store contract
;; =============================================================================
;; Regression: -delete-store was the ONE store method that ignored `opts`.
;; :memory and :file returned a plain value whatever :sync? said (so an async caller
;; could not await the deletion), and :tiered went further — it called
;; `(delete-store backend-config)` with no opts, i.e. the {:sync? false} DEFAULT, and
;; then dropped the returned channel, so deleting a tiered store over an async backend
;; removed nothing at all, silently.
;;
;; The polymorphism tests above never exercised delete-store; they only used it (with
;; no opts, un-awaited) as cleanup. Hence: assert the contract explicitly.

#?(:clj
   (deftest delete-store-async-returns-a-channel-and-completes
     (testing ":memory — async delete-store returns a channel; store is gone when it delivers"
       (let [id #uuid "550e8400-e29b-41d4-a716-4466554400a1"
             cfg {:backend :memory :id id}]
         (store/create-store cfg {:sync? true})
         (is (true? (store/store-exists? cfg {:sync? true})))
         (let [res (store/delete-store cfg)]      ;; default {:sync? false}
           (is (satisfies? clojure.core.async.impl.protocols/ReadPort res)
               "async delete-store must return a channel, not a plain value")
           (<!! res))
         (is (false? (store/store-exists? cfg {:sync? true}))
             "store must be gone once the async delete-store channel delivers")))

     (testing ":memory — sync delete-store returns a value, not a channel"
       (let [id #uuid "550e8400-e29b-41d4-a716-4466554400a2"
             cfg {:backend :memory :id id}]
         (store/create-store cfg {:sync? true})
         (let [res (store/delete-store cfg {:sync? true})]
           (is (not (satisfies? clojure.core.async.impl.protocols/ReadPort res))
               "sync delete-store must return a value, not a channel"))
         (is (false? (store/store-exists? cfg {:sync? true})))))

     (testing ":file — async delete-store returns a channel; store is gone when it delivers"
       (let [path (str (System/getProperty "java.io.tmpdir")
                       "/konserve-delete-contract-" (System/currentTimeMillis))
             id #uuid "550e8400-e29b-41d4-a716-4466554400a3"
             cfg {:backend :file :path path :id id}]
         (store/create-store cfg {:sync? true})
         (is (true? (store/store-exists? cfg {:sync? true})))
         (let [res (store/delete-store cfg)]
           (is (satisfies? clojure.core.async.impl.protocols/ReadPort res)
               "async delete-store must return a channel, not a plain value")
           (<!! res))
         (is (false? (store/store-exists? cfg {:sync? true}))
             "store must be gone once the async delete-store channel delivers")))))

#?(:clj
   (deftest delete-store-tiered-actually-deletes-the-backend
     ;; The nasty one: :tiered called (delete-store backend-config) with NO opts —
     ;; the async default — and never awaited it, so on an async backend it deleted
     ;; nothing while reporting success.
     (testing ":tiered — deleting removes BOTH tiers, sync and async"
       (let [mk (fn [n]
                  {:backend :tiered
                   :id #uuid "550e8400-e29b-41d4-a716-4466554400b0"
                   :frontend-config {:backend :file
                                     :path (str (System/getProperty "java.io.tmpdir")
                                                "/konserve-tiered-del-f" n "-"
                                                (System/currentTimeMillis))
                                     :id #uuid "550e8400-e29b-41d4-a716-4466554400b0"}
                   :backend-config  {:backend :file
                                     :path (str (System/getProperty "java.io.tmpdir")
                                                "/konserve-tiered-del-b" n "-"
                                                (System/currentTimeMillis))
                                     :id #uuid "550e8400-e29b-41d4-a716-4466554400b0"}})]
         ;; sync
         (let [cfg (mk "s")]
           (store/create-store cfg {:sync? true})
           (is (true? (store/store-exists? cfg {:sync? true})))
           (store/delete-store cfg {:sync? true})
           (is (false? (store/store-exists? cfg {:sync? true})))
           (is (false? (store/store-exists? (:backend-config cfg) {:sync? true}))
               "tiered delete must delete the authoritative BACKEND")
           (is (false? (store/store-exists? (:frontend-config cfg) {:sync? true}))
               "tiered delete must delete a persistent frontend"))
         ;; async
         (let [cfg (mk "a")
               _   (store/create-store cfg {:sync? true})
               res (store/delete-store cfg)]
           (is (satisfies? clojure.core.async.impl.protocols/ReadPort res)
               "async tiered delete-store must return a channel")
           (<!! res)
           (is (false? (store/store-exists? (:backend-config cfg) {:sync? true}))
               "async tiered delete must have deleted the BACKEND by the time it delivers")
           (is (false? (store/store-exists? (:frontend-config cfg) {:sync? true}))
               "async tiered delete must have deleted the FRONTEND by the time it delivers"))))))

#?(:clj
   (deftest delete-store-frontend-only-must-not-delete-the-backend
     ;; :frontend-only means this store is a read-through CACHE over a backend that
     ;; someone else OWNS and that this peer must never write. Deleting is the most
     ;; destructive write there is: a cache peer deleting the shared backend would take
     ;; the authoritative data with it — exactly the invariant the policy exists to
     ;; enforce. So a :frontend-only delete removes only the cache.
     (testing ":frontend-only — delete removes the cache, NOT the shared backend"
       (let [id  #uuid "550e8400-e29b-41d4-a716-4466554400c0"
             tmp (System/getProperty "java.io.tmpdir")
             be  {:backend :file :id id :path (str tmp "/konserve-fo-backend-" (System/currentTimeMillis))}
             fe  {:backend :file :id id :path (str tmp "/konserve-fo-frontend-" (System/currentTimeMillis))}
             cfg {:backend :tiered :id id
                  :write-policy :frontend-only
                  :read-policy  :frontend-first
                  :frontend-config fe
                  :backend-config  be}]
         (store/create-store cfg {:sync? true})
         (is (true? (store/store-exists? be {:sync? true})) "backend exists to begin with")

         (<!! (store/delete-store cfg))

         (is (true? (store/store-exists? be {:sync? true}))
             "a :frontend-only cache must NOT delete the shared, writer-owned backend")
         (is (false? (store/store-exists? fe {:sync? true}))
             "it must delete its own cache")
         (store/delete-store be {:sync? true})))))

;; =============================================================================
;; ClojureScript IndexedDB Tests
;; =============================================================================
;; Note: IndexedDB tests are in indexeddb_test.cljs which only runs in browsers.
;; IndexedDB is now treated as an external backend that must be explicitly required.
