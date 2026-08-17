(ns konserve.filestore-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [clojure.core.async :refer [<!! go chan put! close! <!] :as async]
            [konserve.core :refer [bassoc bget keys] :as k]
            [konserve.protocols]
            [konserve.compliance-test :refer [compliance-test conditional-write-compliance-test]]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.tests.cache :as ct]
            [konserve.tests.encryptor :as et]
            [konserve.tests.gc :as gct]
            [konserve.tests.serializers :as st]
            [konserve.tests.tiered :as tiered-tests]
            [konserve.memory :as memory]
            [konserve.tiered :as tiered])
  (:import [java.nio.channels FileChannel]
           [java.nio.file Paths StandardOpenOption]))

(deftest filestore-conditional-write-test
  ;; WIRED IN deliberately. The conditional-write contract shipped once with a
  ;; compliance test that nothing called — and when it was finally run by hand it
  ;; failed three of its own assertions. A contract test that is never invoked is
  ;; worth less than no test, because it reads as coverage.
  (let [folder "/tmp/konserve-fs-cas-test"
        _      (delete-store folder)
        store  (<!! (connect-fs-store folder))]
    (conditional-write-compliance-test store)
    (delete-store folder)))

(deftest every-writer-to-a-fenceable-key-takes-the-sidecar
  (testing "the fence must exclude UNCONDITIONAL writers too, or it is not a
            fence. A plain write renames a new inode over the key; a fenced write
            that opened the old inode before locking it then reads the pre-write
            value through a DETACHED file, compares the revision against that,
            passes, and renames its own result over the top. Not merely a failure
            to exclude — a false pass that loses a committed write. (S3 has no
            such hole: If-Match is evaluated at write time, so an intervening
            unconditional PUT correctly rejects.)

            A key becomes fenceable when a conditional write or a
            revision-bearing read creates its sidecar; from then on every writer
            takes it. Keys that are never fenced pay one `exists?` probe and get
            no extra file, which is what keeps the cost on mutable pointers
            rather than on the content-addressed bulk of a store."
    (let [folder "/tmp/konserve-fs-sidecar-scope"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))
          cas-of (fn [] (filter #(str/ends-with? % ".cas") (map str (.list (java.io.File. folder)))))]
      (k/assoc store :head {:v 1} {:sync? true})
      (k/assoc store :plain {:v 1} {:sync? true})
      (is (empty? (cas-of)) "no key is fenceable until something asks for a revision")

      (k/get store :head nil {:sync? true :with-revision? true})
      (is (= 1 (count (cas-of)))
          "a revision-bearing read makes exactly the key it read fenceable")

      (dotimes [i 20] (k/assoc store (keyword (str "bulk" i)) {:v i} {:sync? true}))
      (is (= 1 (count (cas-of)))
          "and ordinary writes to other keys still cost no extra file")

      ;; Hold the sidecar and watch who contends for it. Within one JVM the
      ;; overlap raises rather than blocking, which is enough to tell whether the
      ;; lock was taken at all; across processes it blocks, which is the point.
      (let [cas (first (cas-of))
            ch  (FileChannel/open (Paths/get (str folder "/" cas) (into-array String []))
                                  (into-array StandardOpenOption
                                              [StandardOpenOption/CREATE StandardOpenOption/WRITE]))
            l   (.lock ch)]
        (try
          (is (= :wrote (deref (future (try (k/assoc store :plain {:v 2} {:sync? true}) :wrote
                                            (catch Throwable _ :contended)))
                               10000 :timed-out))
              "a write to a key that is not fenceable must not take the sidecar")
          (is (= :contended (deref (future (try (k/assoc store :head {:v 2} {:sync? true}) :wrote
                                                (catch Throwable _ :contended)))
                                   10000 :timed-out))
              "but an UNCONDITIONAL write to the fenceable key must")
          (finally (.release l) (.close ch))))
      (delete-store folder))))

(deftest an-unknown-domain-is-refused-rather-than-ranked
  (testing "`conditional-write?` compares against a REQUIRED domain, and a name
            that is not a domain is a mistake in the caller. It used to default
            to rank 0 — `:process`, the weakest — so a typo, a string, or a nil
            out of a config compared as satisfied by every store. The one
            function whose job is to stop a caller believing they are fenced
            answered true for a memory store."
    (let [m (<!! (memory/new-mem-store))]
      (is (= :process (k/conditional-write-domain m)))
      (is (true? (k/conditional-write? m :process)))
      (is (false? (k/conditional-write? m :machine)) "and does not overstate its reach")
      (doseq [bad [:machien "machine" nil :planet]]
        (is (thrown? clojure.lang.ExceptionInfo (k/conditional-write? m bad))
            (str "must refuse " (pr-str bad)))))))

(deftest filestore-cached-conflict-coherence-test
  (delete-store "/tmp/cache-conflict-store")
  (let [store (connect-fs-store "/tmp/cache-conflict-store" :opts {:sync? true})]
    (ct/test-cached-conflict-coherence-sync store)
    (delete-store "/tmp/cache-conflict-store")))

(deftest a-domain-survives-lock-blob-only-if-it-rests-on-the-lock
  (testing "`:lock-blob? false` revokes a LOCK-BASED claim: `io-operation`'s
            compare-and-write is one step only while it holds the lock, so
            without one the filestore's :machine claim is void however sincerely
            it is made"
    (let [folder "/tmp/konserve-fs-nolock-test"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder :config {:lock-blob? false}))]
      (is (nil? (k/conditional-write-domain store))
          "no lock, no lock-based domain")
      (is (thrown? clojure.lang.ExceptionInfo
                   (<!! (k/assoc store :k 1 {:expected-revision :anything})))
          "and the option is refused rather than silently ignored")
      (delete-store folder)))

  (testing "but a :global claim does NOT rest on that lock — it is the storage
            layer's own compare (konserve-s3: If-Match, evaluated by S3), and
            konserve-s3's `-get-lock` is a no-op that passes today only because it
            happens to set `:lock-blob?`. A flag about a lock nobody uses must not
            silently turn the guarantee off.

            Stubbed rather than run against S3 so the RULE is tested here; the
            behaviour it enables is covered by konserve-s3's own suite."
    (let [folder "/tmp/konserve-fs-global-test"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder :config {:lock-blob? false}))
          ;; Same store, with a backing that fences in its own storage layer.
          global (clojure.core/assoc store :backing
                                     (reify konserve.protocols/PConditionalWrite
                                       (-conditional-write? [_] :global)))]
      (is (= :global (k/conditional-write-domain global))
          "a storage-layer fencer keeps its domain without konserve's lock")
      (delete-store folder))))

(deftest filestore-compliance-test
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (<!! (connect-fs-store folder))]
    (testing "Compliance test with default config."
      (compliance-test store))))

(deftest filestore-compliance-test-no-fsync
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (connect-fs-store folder :opts {:sync? true} :config {:sync-blob? false})]
    (testing "Compliance test without syncing."
      (compliance-test store))))

(deftest filestore-compliance-test-no-file-lock
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (<!! (connect-fs-store folder :config {:lock-blob? false}))]
    (testing "Compliance test without file locking."
      (compliance-test store))))

(defn create-tiered-stores [folder]
  (delete-store folder)
  {:frontend (<!! (memory/new-mem-store))
   :backend (<!! (connect-fs-store folder))})

(deftest tiered-store-filestore-backend-test
  (testing "Tiered Store with Filestore Backend"
    (let [folder "/tmp/konserve-tiered-fs-test"]

      (testing "Compliance (Async)"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-tiered-compliance-async frontend backend))
          (delete-store folder)))

      (testing "Compliance (Sync)"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (tiered-tests/test-tiered-compliance-sync frontend backend)
          (delete-store folder)))

      (testing "Write Policies"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-write-policies-async frontend backend))
          (delete-store folder)))

      (testing "Read Policies"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-read-policies-async frontend backend))
          (delete-store folder)))

      (testing "Key Operations"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-key-operations-async frontend backend))
          (delete-store folder)))

      (testing "Binary Operations"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-binary-operations-async frontend backend))
          (delete-store folder)))

      (testing "Sync on Connect"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-sync-on-connect-async frontend backend))
          (delete-store folder)))

      (testing "Error Handling"
        (tiered-tests/test-error-handling nil nil)))))

(deftest binary-polymorhism-test
  (testing "Test storage of different binary input formats."
    (let [folder "/tmp/konserve-fs-test"
          _      (spit "/tmp/foo" (range 1 10))
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))]
      (testing "Binary"
        (testing "ByteArray"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :byte-array (byte-array (range 10))))))
            (is (= true (<!! (bget store :byte-array
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (mapv byte (slurp input-stream)))))))))
            (is (=  (mapv byte (byte-array (range 10))) (<!! res-ch)))
            (close! res-ch)))
        (testing "CharArray"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :char-array (char-array "foo")))))
            (is (= true (<!! (bget store :char-array
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  "foo" (<!! res-ch)))))
        (testing "File Inputstream"
          (let [res-ch (chan)]
            (spit "/tmp/foo" (range 1 10))
            (is (= true (<!! (bassoc store :file-input-stream (java.io.FileInputStream. "/tmp/foo")))))
            (is (= true (<!! (bget store :file-input-stream
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  (str (range 1 10)) (<!! res-ch)))))
        (testing "Byte Array Inputstream"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :input-stream (java.io.ByteArrayInputStream. (byte-array (range 10)))))))
            (is (= true (<!! (bget store :input-stream
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (map byte (slurp input-stream)))))))))
            (is (=  (map byte (byte-array (range 10))) (<!! res-ch)))
            (close! res-ch)))
        (testing "String"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :string "foo bar"))))
            (is (= true (<!! (bget store :string
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (= "foo bar" (<!! res-ch)))))
        (testing "Reader"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :reader (java.io.StringReader. "foo bar")))))
            (is (= true (<!! (bget store :reader
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  "foo bar" (<!! res-ch))))))
      (delete-store folder)
      (let [store (<!! (connect-fs-store folder))]
        (is (= (<!! (keys store))
               #{}))))))

(deftest binary-file-reads-are-streaming
  (let [folder "/tmp/konserve-streaming-read-test"
        source (java.io.File/createTempFile "konserve-stream" ".bin")
        size (+ (* 2 1024 1024) 17)]
    (try
      (with-open [out (java.io.BufferedOutputStream.
                       (java.io.FileOutputStream. source))]
        (let [block (byte-array (map unchecked-byte (range 251)))]
          (loop [remaining size]
            (when (pos? remaining)
              (let [n (min remaining (alength block))]
                (.write out block 0 n)
                (recur (- remaining n)))))))
      (delete-store folder)
      (let [store (connect-fs-store folder :opts {:sync? true})]
        (is (true? (bassoc store :large (java.io.FileInputStream. source)
                           {:sync? true})))
        (is (= size
               (bget store :large
                     (fn [{:keys [input-stream size]}]
                       (is (not (instance? java.io.ByteArrayInputStream input-stream)))
                       (is (= size (.available ^java.io.InputStream input-stream)))
                       (loop [total 0]
                         (let [n (.read ^java.io.InputStream input-stream
                                        (byte-array 8192))]
                           (if (neg? n) total (recur (+ total n))))))
                     {:sync? true :streaming? true}))))
      (finally
        (.delete source)
        (delete-store folder)))))

#!============
#! Cache tests

(deftest cache-PEDNKeyValueStore-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})]
    (<!! (ct/test-cached-PEDNKeyValueStore-async store))))

(deftest filestore-cached-revision-test
  (delete-store "/tmp/cache-revision-store")
  (let [store (connect-fs-store "/tmp/cache-revision-store" :opts {:sync? true})]
    (ct/test-cached-revision-sync store)))

(deftest cache-PKeyIterable-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})]
    (<!! (ct/test-cached-PKeyIterable-async store))))

(deftest cache-PBin-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})
        f (fn [{:keys [input-stream]}]
            (async/to-chan! [input-stream]))]
    (<!! (ct/test-cached-PBin-async store f))))

#!============
#! GC tests

(deftest async-gc-test
  (delete-store "/tmp/gc-store")
  (let [store (connect-fs-store "/tmp/gc-store" :opts {:sync? true})]
    (<!! (gct/test-gc-async store))))

#!==================
#! Serializers tests

(deftest fressian-serializer-test
  (<!! (st/test-fressian-serializers-async "/tmp/serializers-test"
                                           connect-fs-store
                                           (fn [p] (go (delete-store p)))
                                           (fn [{:keys [input-stream]}]
                                             (async/to-chan! [input-stream])))))

(deftest CBOR-serializer-test
  (st/cbor-serializer-test "/tmp/konserve-fs-cbor-test"
                           connect-fs-store
                           (fn [p] (go (delete-store p)))))

#!==================
#! Encryptor tests

(deftest encryptor-sync-test
  (et/sync-encryptor-test "/tmp/encryptor-test"
                          connect-fs-store
                          delete-store))

(deftest encryptor-async-test
  (<!! (et/async-encryptor-test "/tmp/encryptor-test"
                                connect-fs-store
                                (fn [p] (go (delete-store p))))))
