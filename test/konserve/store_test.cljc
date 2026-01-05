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
       (let [store-inst (store/connect-store {:backend :memory :opts {:sync? true}})]
         (is (some? store-inst))
         ;; Basic operations should work
         (k/assoc-in store-inst ["test"] 42 {:sync? true})
         (let [val (k/get-in store-inst ["test"] nil {:sync? true})]
           (is (= val 42)))))))

#?(:clj
   (deftest memory-store-connect-async
     (testing "connect-store :memory with async mode"
       (let [ch (store/connect-store {:backend :memory :opts {:sync? false}})]
         (let [store-inst (<!! ch)]
           (is (some? store-inst))
           ;; Basic operations should work asynchronously
           (<!! (k/assoc-in store-inst ["test"] 42 {:sync? false}))
           (let [val (<!! (k/get-in store-inst ["test"] nil {:sync? false}))]
             (is (= val 42))))))))

#?(:clj
   (deftest memory-store-empty-sync
     (testing "empty-store :memory creates new store"
       (let [store-inst (store/empty-store {:backend :memory :opts {:sync? true}})]
         (is (some? store-inst))))))

#?(:clj
   (deftest memory-store-delete
     (testing "delete-store :memory does nothing"
       ;; Memory store doesn't have persistent storage to delete
       (is (nil? (store/delete-store {:backend :memory}))))))

#?(:clj
   (deftest memory-store-release
     (testing "release-store :memory does nothing"
       ;; Memory store has nothing to release
       (is (nil? (store/release-store {:backend :memory} nil))))))

;; =============================================================================
;; File Backend Tests (JVM only)
;; =============================================================================

#?(:clj
   (deftest file-store-connect-sync
     (testing "connect-store :file with sync mode"
       (let [tmpdir (System/getProperty "java.io.tmpdir")
             path (str tmpdir "/konserve-store-test-" (System/currentTimeMillis))
             store-inst (store/connect-store {:backend :file :path path :opts {:sync? true}})]
         (is (some? store-inst))
         ;; Cleanup
         (store/delete-store {:backend :file :path path})))))

#?(:clj
   (deftest file-store-connect-async
     (testing "connect-store :file with async mode"
       (let [tmpdir (System/getProperty "java.io.tmpdir")
             path (str tmpdir "/konserve-store-test-" (System/currentTimeMillis))
             ch (store/connect-store {:backend :file :path path :opts {:sync? false}})]
         (let [store-inst (<!! ch)]
           (is (some? store-inst)))
         ;; Cleanup
         (store/delete-store {:backend :file :path path})))))

;; =============================================================================
;; Unsupported Backend Error Handling
;; =============================================================================

#?(:clj
   (deftest unsupported-backend-connect
     (testing "connect-store with unsupported backend throws"
       (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                             #"Unsupported store backend: :unknown"
                             (store/connect-store {:backend :unknown}))))))

#?(:clj
   (deftest unsupported-backend-empty
     (testing "empty-store with unsupported backend throws"
       (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                             #"Unsupported store backend: :unknown"
                             (store/empty-store {:backend :unknown}))))))

#?(:clj
   (deftest unsupported-backend-delete
     (testing "delete-store with unsupported backend throws"
       (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                             #"Unsupported store backend: :unknown"
                             (store/delete-store {:backend :unknown}))))))

;; =============================================================================
;; Backend Registration Pattern Tests
;; =============================================================================

#?(:clj
   (deftest async-sync-polymorphism-memory
     (testing "async/sync polymorphism works with memory store"
       ;; Sync mode
       (let [store-sync (store/connect-store {:backend :memory :opts {:sync? true}})]
         (k/assoc-in store-sync ["key1"] "value1" {:sync? true})
         (let [result (k/get-in store-sync ["key1"] nil {:sync? true})]
           (is (= result "value1"))))

       ;; Async mode
       (let [store-async (<!! (store/connect-store {:backend :memory :opts {:sync? false}}))]
         (<!! (k/assoc-in store-async ["key2"] "value2" {:sync? false}))
         (let [result (<!! (k/get-in store-async ["key2"] nil {:sync? false}))]
           (is (= result "value2")))))))

#?(:clj
   (deftest async-sync-polymorphism-file
     (testing "async/sync polymorphism works with file store"
       (let [tmpdir (System/getProperty "java.io.tmpdir")
             path (str tmpdir "/konserve-store-test-async-sync-" (System/currentTimeMillis))]
         ;; Sync mode
         (let [store-sync (store/connect-store {:backend :file :path path :opts {:sync? true}})]
           (k/assoc-in store-sync ["key1"] "value1" {:sync? true})
           (let [result (k/get-in store-sync ["key1"] nil {:sync? true})]
             (is (= result "value1"))))

         ;; Async mode
         (let [store-async (<!! (store/connect-store {:backend :file :path path :opts {:sync? false}}))]
           (<!! (k/assoc-in store-async ["key2"] "value2" {:sync? false}))
           (let [result (<!! (k/get-in store-async ["key2"] nil {:sync? false}))]
             (is (= result "value2"))))

         ;; Cleanup
         (store/delete-store {:backend :file :path path})))))

;; =============================================================================
;; ClojureScript IndexedDB Tests
;; =============================================================================
;; Note: IndexedDB tests are in indexeddb_test.cljs which only runs in browsers.
;; IndexedDB is now treated as an external backend that must be explicitly required.
