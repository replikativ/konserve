(ns konserve.write-path-hardening-test
  "One test per finding of the write-path review, each written to FAIL on the
   code it was written against."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.binary :as kb]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.impl.defaults :as d])
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
  (testing "in-place mode: a shorter value over a longer one is exactly the shorter value"
    ;; In-place only — a rename-mode write always lands in a fresh staging
    ;; file, so it cannot keep a tail and would pass this without truncating.
    (doseq [sync? [true false]]
      (let [path  (fresh-path)
            store (connect-fs-store path :config {:in-place? true} :opts {:sync? true})
            opts  {:sync? sync?}
            run   (fn [op] (if sync? (op) (<!! (op))))]
        (run #(k/bassoc store :bin (byte-array 100 (byte 1)) opts))
        (run #(k/bassoc store :bin (byte-array 3 (byte 2)) opts))
        (is (= [2 2 2] (seq (read-bytes store :bin opts)))
            (str "sync? " sync? ": binary — the file must be truncated to the new value"))
        (run #(k/assoc-in store [:edn] (apply str (repeat 5000 "x")) opts))
        (run #(k/assoc-in store [:edn] "short" opts))
        (is (= "short" (run #(k/get-in store [:edn] nil opts)))
            (str "sync? " sync? ": edn — the value segment is truncated too"))
        (delete-store path)))))

(deftest reader-input-keeps-surrogate-pairs
  (testing "a Reader whose chunks split UTF-16 surrogate pairs still encodes them as UTF-8"
    ;; Tiny buffers force one char per chunk, so every emoji straddles a
    ;; boundary; 8 gives two, 1024 the ordinary case.
    (doseq [buffer-size [4 5 8 1024]]
      (let [path  (fresh-path)
            store (connect-fs-store path :buffer-size buffer-size :opts {:sync? true})
            s     (apply str (repeat 40 "a😀b"))]
        (is (= buffer-size (:buffer-size store)) "the buffer size under test took effect")
        (k/bassoc store :k (java.io.StringReader. s) {:sync? true})
        (is (= (seq (.getBytes s "UTF-8")) (seq (read-bytes store :k {:sync? true})))
            (str "buffer-size " buffer-size ": pairs split across chunks must not become replacement bytes"))
        (delete-store path)))))

(deftest dissoc-is-ordered-against-a-fenced-write
  (testing "a delete on a fenceable key takes the sidecar lock, so it cannot slip inside a fenced write"
    ;; Both ways a caller obtains a token. `k/revision` is the one that used to
    ;; hand it out WITHOUT creating the sidecar, so the delete found nothing to
    ;; lock and slipped inside the fenced write.
    (doseq [[token-source token] [[:with-revision? #(second (k/get-in % [:k] nil {:sync? true :with-revision? true}))]
                                  [:k/revision      #(k/revision % :k {:sync? true})]]]
      (let [path (fresh-path)
          ;; Two stores on one path: separate lock registries, like two processes.
            a    (connect-fs-store path :opts {:sync? true})
            b    (connect-fs-store path :opts {:sync? true})]
        (k/assoc-in a [:k] :v0 {:sync? true})
        (let [rev      (token a)
              _        (is (.exists (java.io.File. path (str (d/key->store-key :k) d/cas-lock-suffix)))
                           (str token-source ": handing out a revision token makes the key fenceable — "
                                "the sidecar must exist before any fenced write, or an unconditional "
                                "writer or delete that probes first finds nothing to be ordered by"))
              entered  (promise)   ; the writer is inside its locked update
              proceed  (promise)   ; ... and stays there until we say so
              writer   (future
                         (k/update-in a [:k] (fn [_] (deliver entered true) @proceed :from-a)
                                      {:sync? true :expected-revision rev}))]
          @entered
        ;; A is provably inside the fenced write, holding the sidecar lock.
          (let [deleter (future (k/dissoc b :k {:sync? true}) :deleted)]
            (is (= ::blocked (deref deleter 200 ::blocked))
                (str token-source ": the delete must wait for the fenced write, not slip inside it"))
            (deliver proceed true)
            (is (= :from-a (second @writer)) "the fenced write completes: update-in yields [old new]")
            (is (= :deleted (deref deleter 5000 ::stuck)) "then the delete runs")
            (is (nil? (k/get-in a [:k] nil {:sync? true}))
                (str token-source ": delete after write — the key is gone and does not reappear"))))
        (delete-store path)))))
