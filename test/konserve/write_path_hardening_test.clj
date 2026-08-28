(ns konserve.write-path-hardening-test
  "One test per finding of the write-path review, each written to FAIL on the
   code it was written against."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.binary :as kb]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]])
  (:import [java.io InputStream ByteArrayInputStream]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- fresh-path []
  (str (Files/createTempDirectory "konserve-hardening-" (make-array FileAttribute 0)) "/store"))

(defn- short-reads
  "An InputStream that hands out at most `max` bytes per read — what a socket,
   a pipe, or any stream that is not a byte array does routinely."
  ^InputStream [^bytes data ^long max]
  (let [inner (ByteArrayInputStream. data)]
    (proxy [InputStream] []
      (read
        ([] (.read inner))
        ([^bytes buf] (.read ^InputStream this buf 0 (alength buf)))
        ([^bytes buf off len] (.read inner buf off (min (long len) max)))))))

(defn- read-bytes [store key opts]
  (let [r (k/bget store key (kb/to-bytes opts) opts)]
    (if (:sync? opts) r (<!! r))))

(deftest short-reads-do-not-shift-the-value
  (testing "a binary written from a stream that returns short reads is intact (sync and async)"
    (doseq [sync? [true false]]
      (let [path  (fresh-path)
            ;; Connect synchronously — an async connect yields a channel — and
            ;; exercise the async path per operation, as the binary tests do.
            store (connect-fs-store path :opts {:sync? true})
            data  (byte-array (map #(mod % 251) (range 100000)))
            opts  {:sync? sync?}]
        (if sync?
          (k/bassoc store :k (short-reads data 777) opts)
          (<!! (k/bassoc store :k (short-reads data 777) opts)))
        (is (= (seq data) (seq (read-bytes store :k opts)))
            (str "sync? " sync? ": a fixed stride over short reads leaves holes and shifts the tail"))
        (delete-store path)))))

(deftest shorter-write-does-not-keep-the-old-tail
  (testing "a shorter value over a longer one is exactly the shorter value"
    (doseq [in-place? [false true]]
      (let [path  (fresh-path)
            store (connect-fs-store path :config {:in-place? in-place?} :opts {:sync? true})]
        (k/bassoc store :k (byte-array 100 (byte 1)) {:sync? true})
        (k/bassoc store :k (byte-array 3 (byte 2)) {:sync? true})
        (is (= [2 2 2] (seq (read-bytes store :k {:sync? true})))
            (str "in-place? " in-place? ": the file must be truncated to the new value"))
        (delete-store path)))))

(deftest dissoc-is-ordered-against-a-fenced-write
  (testing "a delete on a fenceable key takes the sidecar lock, so it cannot slip inside a fenced write"
    (let [path (fresh-path)
          ;; Two stores on one path: separate lock registries, like two processes.
          a    (connect-fs-store path :opts {:sync? true})
          b    (connect-fs-store path :opts {:sync? true})]
      (k/assoc-in a [:k] :v0 {:sync? true})
      (let [[_ rev] (k/get-in a [:k] nil {:sync? true :with-revision? true})
            writer  (future
                      ;; Holds the sidecar lock for the whole fenced write.
                      (k/update-in a [:k] (fn [_] (Thread/sleep 400) :from-a)
                                   {:sync? true :expected-revision rev}))]
        (Thread/sleep 100)
        ;; Arrives while A is mid-write. Must WAIT for A, then delete.
        (k/dissoc b :k {:sync? true})
        @writer
        (is (nil? (k/get-in a [:k] nil {:sync? true}))
            "the delete ran after the fenced write it was ordered behind; the key must not reappear"))
      (delete-store path))))
