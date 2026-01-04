(ns konserve.filestore-jimfs-test
  "Tests for FileStore using Jimfs - Google's in-memory NIO filesystem.

   Jimfs enables testing FileStore's NIO path handling, concurrent access,
   and error scenarios without touching the real filesystem.

   Note: Jimfs only supports synchronous FileChannel, not AsynchronousFileChannel,
   so all tests use sync mode."
  (:require [clojure.test :refer :all]
            [konserve.filestore :as fs]
            [konserve.core :as k])
  (:import [com.google.common.jimfs Jimfs Configuration]
           [java.nio.file Files FileSystem]
           [java.util.concurrent CountDownLatch Executors TimeUnit]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(defn create-jimfs
  "Create a new Jimfs filesystem with Unix configuration."
  ^FileSystem []
  (Jimfs/newFileSystem (Configuration/unix)))

(defmacro with-jimfs-store
  "Execute body with a FileStore backed by Jimfs.
   Jimfs only supports synchronous FileChannel, not AsynchronousFileChannel,
   so we always use sync mode.
   Bindings: [store-sym opts?]"
  [[store-sym & [opts]] & body]
  `(let [jimfs# (create-jimfs)
         base-path# "/test-store"
         ;; Create the base directory in Jimfs
         _# (Files/createDirectories
             (.getPath jimfs# base-path# (into-array String []))
             (into-array java.nio.file.attribute.FileAttribute []))
         ;; Note: Jimfs doesn't support AsynchronousFileChannel, so always use sync
         ~store-sym (fs/connect-fs-store
                     base-path#
                     :filesystem jimfs#
                     :opts {:sync? true}
                     ~@(mapcat identity opts))]
     (try
       ~@body
       (finally
         (.close jimfs#)))))

;; =============================================================================
;; Basic Operation Tests
;; =============================================================================

(deftest jimfs-basic-operations-test
  (testing "Basic CRUD operations work on Jimfs"
    (with-jimfs-store [store]
      ;; Create
      (k/assoc store :key {:value 1} {:sync? true})
      (is (= {:value 1} (k/get store :key nil {:sync? true})))

      ;; Update
      (k/assoc store :key {:value 2} {:sync? true})
      (is (= {:value 2} (k/get store :key nil {:sync? true})))

      ;; Delete
      (k/dissoc store :key {:sync? true})
      (is (nil? (k/get store :key nil {:sync? true}))))))

(deftest jimfs-multiple-keys-test
  (testing "Multiple keys work correctly on Jimfs"
    (with-jimfs-store [store]
      ;; Write multiple keys
      (doseq [i (range 10)]
        (k/assoc store (keyword (str "key-" i)) {:index i} {:sync? true}))

      ;; Read all back
      (doseq [i (range 10)]
        (is (= {:index i} (k/get store (keyword (str "key-" i)) nil {:sync? true})))))))

(deftest jimfs-sync-operations-test
  (testing "Sync operations work on Jimfs"
    (let [jimfs (create-jimfs)
          base-path "/sync-store"
          _ (Files/createDirectories
             (.getPath jimfs base-path (into-array String []))
             (into-array java.nio.file.attribute.FileAttribute []))
          store (fs/connect-fs-store
                 base-path
                 :filesystem jimfs
                 :opts {:sync? true})]
      (try
        ;; Sync write and read
        (k/assoc store :sync-key {:sync true} {:sync? true})
        (is (= {:sync true} (k/get store :sync-key nil {:sync? true})))
        (finally
          (.close jimfs))))))

;; =============================================================================
;; Binary Data Tests
;; =============================================================================

(deftest jimfs-binary-operations-test
  (testing "Binary operations work on Jimfs"
    (with-jimfs-store [store]
      (let [data (byte-array [1 2 3 4 5])]
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

(deftest jimfs-large-binary-test
  (testing "Large binary data works on Jimfs"
    (with-jimfs-store [store]
      (let [size 100000
            data (byte-array (repeatedly size #(unchecked-byte (rand-int 256))))]
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
          (is (= (seq data) (seq result))))))))

;; =============================================================================
;; Concurrent Access Tests
;; =============================================================================

(deftest jimfs-concurrent-writes-test
  (testing "Concurrent writes work correctly on Jimfs"
    (with-jimfs-store [store]
      (let [n-threads 5
            n-writes 20
            latch (CountDownLatch. n-threads)
            executor (Executors/newFixedThreadPool n-threads)]
        (try
          ;; Each thread writes to its own key
          (dotimes [t n-threads]
            (.submit executor
                     ^Runnable
                     (fn []
                       (try
                         (dotimes [i n-writes]
                           (k/assoc store
                                    (keyword (str "thread-" t "-key"))
                                    {:thread t :iteration i}
                                    {:sync? true}))
                         (finally
                           (.countDown latch))))))

          (.await latch 30 TimeUnit/SECONDS)

          ;; Verify all threads' final values exist
          (dotimes [t n-threads]
            (let [v (k/get store (keyword (str "thread-" t "-key")) nil {:sync? true})]
              (is (= t (:thread v)))))

          (finally
            (.shutdown executor)))))))

(deftest jimfs-concurrent-same-key-test
  (testing "Concurrent writes to same key serialize correctly on Jimfs"
    (with-jimfs-store [store]
      (let [n-threads 5
            n-writes 10
            latch (CountDownLatch. n-threads)
            executor (Executors/newFixedThreadPool n-threads)]
        (try
          ;; All threads write to same key
          (dotimes [t n-threads]
            (.submit executor
                     ^Runnable
                     (fn []
                       (try
                         (dotimes [i n-writes]
                           (k/assoc store :shared {:thread t :iteration i} {:sync? true}))
                         (finally
                           (.countDown latch))))))

          (.await latch 30 TimeUnit/SECONDS)

          ;; Final value should be from one of the threads
          (let [v (k/get store :shared nil {:sync? true})]
            (is (some? v))
            (is (contains? (set (range n-threads)) (:thread v))))

          (finally
            (.shutdown executor)))))))

;; =============================================================================
;; Keys Listing Tests
;; =============================================================================

(deftest jimfs-keys-listing-test
  (testing "Keys listing works on Jimfs"
    (with-jimfs-store [store]
      ;; Write some keys
      (k/assoc store :key1 {:v 1} {:sync? true})
      (k/assoc store :key2 {:v 2} {:sync? true})
      (k/assoc store :key3 {:v 3} {:sync? true})

      ;; List keys
      (let [keys-list (k/keys store {:sync? true})]
        (is (= 3 (count keys-list)))))))

;; =============================================================================
;; Path Edge Cases
;; =============================================================================

(deftest jimfs-special-key-names-test
  (testing "Special key names work on Jimfs"
    (with-jimfs-store [store]
      ;; Keys with various characters (hashed, so should work)
      (let [keys [:simple
                  :with-dash
                  :with_underscore
                  (keyword "with space")
                  :CamelCase
                  :123numeric]]
        (doseq [k keys]
          (k/assoc store k {:key k} {:sync? true})
          (is (= {:key k} (k/get store k nil {:sync? true}))
              (str "Key " k " should work")))))))

(deftest jimfs-nested-data-test
  (testing "Deeply nested data works on Jimfs"
    (with-jimfs-store [store]
      (let [nested-data {:level1
                         {:level2
                          {:level3
                           {:level4
                            {:value "deep"}}}}}]
        (k/assoc store :nested nested-data {:sync? true})
        (is (= nested-data (k/get store :nested nil {:sync? true})))))))

;; =============================================================================
;; Store Lifecycle Tests
;; =============================================================================

(deftest jimfs-store-exists-test
  (testing "Store exists check works on Jimfs"
    (let [jimfs (create-jimfs)
          base-path "/exists-test"]
      (try
        ;; Initially doesn't exist
        (is (not (fs/store-exists? jimfs base-path)))

        ;; Create and connect
        (Files/createDirectories
         (.getPath jimfs base-path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        ;; Now exists
        (is (fs/store-exists? jimfs base-path))

        (finally
          (.close jimfs))))))

(deftest jimfs-delete-store-test
  (testing "Delete store works on Jimfs"
    (let [jimfs (create-jimfs)
          base-path "/delete-test"]
      (try
        ;; Create store
        (Files/createDirectories
         (.getPath jimfs base-path (into-array String []))
         (into-array java.nio.file.attribute.FileAttribute []))

        (let [store (fs/connect-fs-store
                     base-path
                     :filesystem jimfs
                     :opts {:sync? true})]
          ;; Write some data
          (k/assoc store :key {:v 1} {:sync? true})

          ;; Delete store
          (fs/delete-store jimfs base-path)

          ;; Verify deleted
          (is (not (fs/store-exists? jimfs base-path))))

        (finally
          (.close jimfs))))))
