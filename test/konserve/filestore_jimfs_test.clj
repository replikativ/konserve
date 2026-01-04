(ns konserve.filestore-jimfs-test
  "Tests for FileStore using Jimfs - Google's in-memory NIO filesystem.

   Purpose: Verify correct usage of Java NIO filesystem APIs.

   Jimfs enables testing scenarios that are hard on real filesystems:
   - Multiple store instances on same path (file locking behavior)
   - Rapid store lifecycle without OS cleanup delays
   - Isolated filesystem instances (no cross-contamination)
   - Path handling edge cases

   Note: Jimfs only supports synchronous FileChannel, not AsynchronousFileChannel,
   so all tests use sync mode."
  (:require [clojure.test :refer :all]
            [konserve.filestore :as fs]
            [konserve.core :as k])
  (:import [com.google.common.jimfs Jimfs Configuration]
           [java.nio.file Files FileSystem]
           [java.util.concurrent CountDownLatch Executors TimeUnit CyclicBarrier]
           [java.util.concurrent.atomic AtomicInteger AtomicBoolean]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(defn create-jimfs
  "Create a new Jimfs filesystem with Unix configuration."
  ^FileSystem []
  (Jimfs/newFileSystem (Configuration/unix)))

(defn create-store-path
  "Create a directory in Jimfs and return a connected store."
  [^FileSystem jimfs ^String path]
  (Files/createDirectories
   (.getPath jimfs path (into-array String []))
   (into-array java.nio.file.attribute.FileAttribute []))
  (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true}))

(defmacro with-jimfs-store
  "Execute body with a FileStore backed by Jimfs."
  [[store-sym] & body]
  `(let [jimfs# (create-jimfs)
         ~store-sym (create-store-path jimfs# "/test-store")]
     (try
       ~@body
       (finally
         (.close jimfs#)))))

;; =============================================================================
;; Basic NIO Path Handling
;; =============================================================================

(deftest jimfs-basic-roundtrip-test
  (testing "Basic CRUD roundtrip verifies path operations work"
    (with-jimfs-store [store]
      (k/assoc store :key {:value 1} {:sync? true})
      (is (= {:value 1} (k/get store :key nil {:sync? true})))
      (k/dissoc store :key {:sync? true})
      (is (nil? (k/get store :key nil {:sync? true}))))))

(deftest jimfs-binary-roundtrip-test
  (testing "Binary operations use correct NIO channel APIs"
    (with-jimfs-store [store]
      (let [data (byte-array (range 256))]
        (k/bassoc store :binary data {:sync? true})
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

;; =============================================================================
;; Multiple Store Instances - Same Path (File Locking)
;; =============================================================================

(deftest multiple-stores-same-path-test
  (testing "Multiple store instances can access same path (tests file locking)"
    (let [jimfs (create-jimfs)
          path "/shared-store"]
      (try
        (Files/createDirectories
         (.getPath jimfs path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        (let [store1 (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})
              store2 (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]

          ;; Write from store1, read from store2
          (k/assoc store1 :from-store1 {:source 1} {:sync? true})
          (is (= {:source 1} (k/get store2 :from-store1 nil {:sync? true})))

          ;; Write from store2, read from store1
          (k/assoc store2 :from-store2 {:source 2} {:sync? true})
          (is (= {:source 2} (k/get store1 :from-store2 nil {:sync? true})))

          ;; Both stores see all keys
          (is (= 2 (count (k/keys store1 {:sync? true}))))
          (is (= 2 (count (k/keys store2 {:sync? true})))))

        (finally
          (.close jimfs))))))

(deftest concurrent-stores-same-key-test
  (testing "Multiple stores writing to same key tests atomic move"
    (let [jimfs (create-jimfs)
          path "/concurrent-store"
          n-stores 3
          n-writes 50
          latch (CountDownLatch. n-stores)
          executor (Executors/newFixedThreadPool n-stores)
          successful-writes (AtomicInteger. 0)]
      (try
        (Files/createDirectories
         (.getPath jimfs path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        (dotimes [store-id n-stores]
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (let [store (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]
                         (dotimes [i n-writes]
                           (try
                             (k/assoc store :contested-key {:store store-id :write i} {:sync? true})
                             (.incrementAndGet successful-writes)
                             (catch Exception _e nil))))
                       (finally
                         (.countDown latch))))))

        (.await latch 30 TimeUnit/SECONDS)

        (let [store (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})
              v (k/get store :contested-key nil {:sync? true})]
          (is (some? v) "Should have a value after concurrent writes")
          (is (contains? (set (range n-stores)) (:store v))))

        (finally
          (.shutdown executor)
          (.close jimfs))))))

;; =============================================================================
;; Filesystem Isolation
;; =============================================================================

(deftest separate-jimfs-instances-isolated-test
  (testing "Separate Jimfs instances are completely isolated"
    (let [jimfs1 (create-jimfs)
          jimfs2 (create-jimfs)
          path "/isolated-store"]
      (try
        (Files/createDirectories
         (.getPath jimfs1 path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))
        (Files/createDirectories
         (.getPath jimfs2 path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        (let [store1 (fs/connect-fs-store path :filesystem jimfs1 :opts {:sync? true})
              store2 (fs/connect-fs-store path :filesystem jimfs2 :opts {:sync? true})]

          (k/assoc store1 :key {:from "jimfs1"} {:sync? true})
          (k/assoc store2 :key {:from "jimfs2"} {:sync? true})

          ;; Each sees only its own data
          (is (= {:from "jimfs1"} (k/get store1 :key nil {:sync? true})))
          (is (= {:from "jimfs2"} (k/get store2 :key nil {:sync? true}))))

        (finally
          (.close jimfs1)
          (.close jimfs2))))))

;; =============================================================================
;; Rapid Store Lifecycle (Tests Path Creation/Deletion)
;; =============================================================================

(deftest rapid-store-creation-test
  (testing "Rapidly creating many stores tests path operations"
    (let [jimfs (create-jimfs)
          n-stores 20]
      (try
        (doseq [i (range n-stores)]
          (let [path (str "/store-" i)]
            (Files/createDirectories
             (.getPath jimfs path (into-array String []))
             (into-array java.nio.file.attribute.FileAttribute []))
            (let [store (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]
              (k/assoc store :key {:store i} {:sync? true})
              (is (= {:store i} (k/get store :key nil {:sync? true}))))))

        ;; Verify all stores are independent
        (doseq [i (range n-stores)]
          (let [path (str "/store-" i)
                store (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]
            (is (= {:store i} (k/get store :key nil {:sync? true})))))

        (finally
          (.close jimfs))))))

(deftest store-reopen-persistence-test
  (testing "Data persists when store is reopened (tests file persistence)"
    (let [jimfs (create-jimfs)
          path "/reopen-store"]
      (try
        (Files/createDirectories
         (.getPath jimfs path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        ;; First store instance
        (let [store1 (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]
          (k/assoc store1 :persistent {:value "survives"} {:sync? true}))

        ;; Second store instance should see data
        (let [store2 (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]
          (is (= {:value "survives"} (k/get store2 :persistent nil {:sync? true}))))

        (finally
          (.close jimfs))))))

(deftest rapid-key-create-delete-cycle-test
  (testing "Rapidly creating/deleting keys tests file create/delete operations"
    (with-jimfs-store [store]
      (dotimes [i 100]
        (k/assoc store :cycling-key {:cycle i} {:sync? true})
        (is (= {:cycle i} (k/get store :cycling-key nil {:sync? true})))
        (k/dissoc store :cycling-key {:sync? true})
        (is (nil? (k/get store :cycling-key nil {:sync? true})))))))

;; =============================================================================
;; Concurrent Read-Write (Tests File Channel Behavior)
;; =============================================================================

(deftest concurrent-read-write-same-key-test
  (testing "Concurrent reads and writes test channel locking"
    (with-jimfs-store [store]
      (let [n-readers 3
            n-writers 2
            n-operations 30
            barrier (CyclicBarrier. (+ n-readers n-writers))
            latch (CountDownLatch. (+ n-readers n-writers))
            executor (Executors/newFixedThreadPool (+ n-readers n-writers))
            read-count (AtomicInteger. 0)
            write-count (AtomicInteger. 0)]

        (try
          (k/assoc store :rw-key {:initial true} {:sync? true})

          (dotimes [_ n-readers]
            (.submit executor
                     ^Runnable
                     (fn []
                       (try
                         (.await barrier)
                         (dotimes [_ n-operations]
                           (try
                             (when (k/get store :rw-key nil {:sync? true})
                               (.incrementAndGet read-count))
                             (catch Exception _e nil)))
                         (finally
                           (.countDown latch))))))

          (dotimes [w n-writers]
            (.submit executor
                     ^Runnable
                     (fn []
                       (try
                         (.await barrier)
                         (dotimes [i n-operations]
                           (try
                             (k/assoc store :rw-key {:writer w :op i} {:sync? true})
                             (.incrementAndGet write-count)
                             (catch Exception _e nil)))
                         (finally
                           (.countDown latch))))))

          (.await latch 30 TimeUnit/SECONDS)

          (is (pos? (.get read-count)) "Should have successful reads")
          (is (pos? (.get write-count)) "Should have successful writes")
          (is (some? (k/get store :rw-key nil {:sync? true})) "Key should exist")

          (finally
            (.shutdown executor)))))))

(deftest concurrent-keys-enumeration-test
  (testing "Enumerating keys while writing tests directory stream handling"
    (with-jimfs-store [store]
      (let [writer-running (AtomicBoolean. true)
            latch (CountDownLatch. 1)
            executor (Executors/newFixedThreadPool 2)]

        (try
          (.submit executor
                   ^Runnable
                   (fn []
                     (try
                       (loop [i 0]
                         (when (.get writer-running)
                           (let [key (keyword (str "key-" (mod i 50)))]
                             (if (even? i)
                               (k/assoc store key {:i i} {:sync? true})
                               (k/dissoc store key {:sync? true})))
                           (recur (inc i))))
                       (finally
                         (.countDown latch)))))

          ;; Enumerate keys while writer is active
          (dotimes [_ 20]
            (let [ks (k/keys store {:sync? true})]
              (is (set? ks) "keys should return a set")))

          (.set writer-running false)
          (.await latch 5 TimeUnit/SECONDS)

          (finally
            (.shutdown executor)))))))

;; =============================================================================
;; Store Lifecycle (Tests delete-store, store-exists?)
;; =============================================================================

(deftest store-exists-test
  (testing "store-exists? correctly checks path existence"
    (let [jimfs (create-jimfs)
          path "/exists-test"]
      (try
        (is (not (fs/store-exists? jimfs path)))

        (Files/createDirectories
         (.getPath jimfs path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        (is (fs/store-exists? jimfs path))

        (finally
          (.close jimfs))))))

(deftest delete-store-test
  (testing "delete-store removes directory and contents"
    (let [jimfs (create-jimfs)
          path "/delete-test"]
      (try
        (Files/createDirectories
         (.getPath jimfs path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        (let [store (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]
          (k/assoc store :key {:value 1} {:sync? true}))

        (fs/delete-store jimfs path)
        (is (not (fs/store-exists? jimfs path)))

        (finally
          (.close jimfs))))))

(deftest delete-recreate-store-test
  (testing "Deleting and recreating store clears old data"
    (let [jimfs (create-jimfs)
          path "/delete-recreate"]
      (try
        (Files/createDirectories
         (.getPath jimfs path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        (let [store (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]
          (k/assoc store :key {:value 1} {:sync? true}))

        (fs/delete-store jimfs path)

        (Files/createDirectories
         (.getPath jimfs path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        (let [store (fs/connect-fs-store path :filesystem jimfs :opts {:sync? true})]
          (is (nil? (k/get store :key nil {:sync? true})))
          (is (= 0 (count (k/keys store {:sync? true})))))

        (finally
          (.close jimfs))))))

;; =============================================================================
;; Many Keys (Tests Directory Listing at Scale)
;; =============================================================================

(deftest many-keys-directory-listing-test
  (testing "Store handles many keys (tests directory stream at scale)"
    (with-jimfs-store [store]
      (let [n-keys 200]
        (dotimes [i n-keys]
          (k/assoc store (keyword (str "key-" i)) {:index i} {:sync? true}))

        (is (= n-keys (count (k/keys store {:sync? true}))))))))
