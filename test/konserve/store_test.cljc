(ns konserve.store-test
  (:require [clojure.core.async :as a :refer [go <! take! #?(:clj <!!)]]
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
     (testing "delete-store :memory does nothing"
       (let [id #uuid "550e8400-e29b-41d4-a716-446655440004"]
         ;; Memory store doesn't have persistent storage to delete
         (is (nil? (store/delete-store {:backend :memory :id id})))))))

#?(:clj
   (deftest memory-store-release
     (testing "release-store :memory does nothing"
       (let [id #uuid "550e8400-e29b-41d4-a716-446655440005"]
         ;; Memory store has nothing to release
         (is (nil? (store/release-store {:backend :memory :id id} nil)))))))

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
;; ClojureScript IndexedDB Tests
;; =============================================================================
;; Note: IndexedDB tests are in indexeddb_test.cljs which only runs in browsers.
;; IndexedDB is now treated as an external backend that must be explicitly required.
