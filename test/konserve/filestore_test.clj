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

(deftest a-rejected-fenced-write-creates-nothing-to-clean-up
  (testing "a fenced write to a key that does not exist must not create the key's
            blob. There is nothing to read — the check either fails for want of a
            revision, or it is a create and `update-blob` makes its own blob — so
            opening it only produces an empty file that may never be written.

            That empty file was the ghost, and the cleanup written to remove it
            deleted BY PATH. On a backing that takes no sidecar (every `:global`
            one) the cleanup ran unlocked and unlinked whatever was at the path,
            which in an ordinary create-if-absent race is the WINNER's value:
            reproduced against MinIO as 10 of 10 keys, one peer told its fenced
            write succeeded and the key then missing. No ghost means no cleanup
            means that whole class is gone, so this asserts the absence of the
            file rather than the behaviour of a collector."
    (let [folder "/tmp/konserve-fs-no-ghost"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))
          files  #(set (map str (.list (java.io.File. folder))))]
      (k/assoc store :other {:v 1} {:sync? true})
      (let [blobs #(set (remove (fn [f] (str/ends-with? f ".cas")) (files)))
            before (blobs)]
        (is (thrown? clojure.lang.ExceptionInfo
                     (k/assoc store :missing {:v :no}
                              {:sync? true :expected-revision "a-revision-that-never-existed"}))
            "the write is rejected")
        (is (= before (blobs))
            "and left no BLOB behind — nothing for a collector to have to remove")
        ;; The sidecar it does leave is deliberate: the key is fenceable now, so
        ;; the next attempt on it is protected from the first one's race.
        (is (= 1 (count (filter #(str/ends-with? % ".cas") (files))))
            "only the sidecar, which is what makes the key fenceable")
        (is (false? (k/exists? store :missing {:sync? true})))
        (is (= 1 (count (k/keys store {:sync? true}))) "enumeration is unaffected"))
      (delete-store folder))))

(deftest a-transient-failure-must-not-leak-the-sidecar-lock
  (testing "the sidecar is a JVM-wide `FileLock`, so leaking one locks every
            process on the machine out of that key until this JVM exits. It used
            to be acquired in a `let` binding ABOVE the `try` that releases it, so
            anything throwing in between leaked it — an IOException opening the
            value blob, the existence probe, or `get-lock` giving up after about a
            second of contention. Reads take the sidecar too, so a branch head
            would simply stop responding, for good, after one blip.

            The blip here is real rather than injected: hold the value blob's lock
            so konserve cannot take it, which is the `:file-lock-acquisition-error`
            path."
    (let [folder "/tmp/konserve-fs-lock-leak"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))]
      (k/assoc store :head {:v 1} {:sync? true})
      (k/get store :head nil {:sync? true :with-revision? true})   ;; fenceable
      (let [ksv (first (filter #(str/ends-with? % ".ksv")
                               (map str (.list (java.io.File. folder)))))
            ch  (FileChannel/open (Paths/get (str folder "/" ksv) (into-array String []))
                                  (into-array StandardOpenOption
                                              [StandardOpenOption/CREATE StandardOpenOption/WRITE]))
            l   (.lock ch)]
        (try
          (is (thrown? clojure.lang.ExceptionInfo (k/assoc store :head {:v 2} {:sync? true}))
              "the contended write fails, which is the transient error")
          (finally (.release l) (.close ch))))
      ;; The assertions that matter: the sidecar was released on the way out.
      (is (= :wrote (deref (future (try (k/assoc store :head {:v 3} {:sync? true}) :wrote
                                        (catch Throwable e (ex-message e))))
                           20000 :timed-out-holding-the-lock))
          "the key must still be writable")
      (is (= {:v 3} (deref (future (try (k/get store :head nil {:sync? true})
                                        (catch Throwable e (ex-message e))))
                           20000 :timed-out-holding-the-lock))
          "and still readable")
      (delete-store folder))))

(deftest a-failed-revision-read-must-not-leak-the-sidecar-lock
  (testing "a revision-bearing READ takes the sidecar before it takes the value
            blob lock. If the latter acquisition fails, both the sidecar lock and
            its channel still have to be released; otherwise one failed head read
            wedges the mutable pointer for the lifetime of this JVM."
    (let [folder "/tmp/konserve-fs-revision-read-lock-leak"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))]
      (k/assoc store :head {:v 1} {:sync? true})
      (k/get store :head nil {:sync? true :with-revision? true})
      (let [ksv (first (filter #(and (str/ends-with? % ".ksv")
                                     (not (str/ends-with? % ".ksv.cas")))
                               (map str (.list (java.io.File. folder)))))
            ch  (FileChannel/open (Paths/get (str folder "/" ksv) (into-array String []))
                                  (into-array StandardOpenOption
                                              [StandardOpenOption/CREATE StandardOpenOption/WRITE]))
            l   (.lock ch)]
        (try
          (is (thrown? clojure.lang.ExceptionInfo
                       (k/get store :head nil {:sync? true :with-revision? true}))
              "the contended revision read reaches the transient failure path")
          (finally (.release l) (.close ch))))
      (let [[value revision]
            (k/get store :head nil {:sync? true :with-revision? true})]
        (is (= {:v 1} value)
            "the sidecar was released and a subsequent fenced read succeeds")
        (is (some? revision) "and still returns its fencing token"))
      (is (= :wrote (deref (future (try (k/assoc store :head {:v 2} {:sync? true})
                                        :wrote
                                        (catch Throwable e (ex-message e))))
                           20000 :timed-out-holding-the-lock))
          "the key remains writable too")
      (delete-store folder))))

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

  (testing "but a claim that does NOT rest on that lock survives it, and which
            claims those are is DECLARED, not inferred from the domain.

            konserve used to test the domain for `:global`, on the reasoning that
            nothing local can reach that far. True, and useless in the other
            direction: RocksDB fences itself with a write batch at `:process`,
            LMDB with an ACID transaction at `:machine`, and a JDBC backend with
            one statement that is `:global` on Postgres and `:machine` on SQLite.
            The old test would have disarmed every one of them — and handed them
            konserve's sidecar, writing a phantom `.cas` row into the table being
            fenced."
    (let [folder "/tmp/konserve-fs-selffence-test"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder :config {:lock-blob? false}))
          ;; `:in-place? true` alongside the swapped backing, because that is the
          ;; configuration a self-fencing backend actually ships — and since
          ;; konserve#176 it is part of what makes the claim valid. The filestore
          ;; itself defaults to `:in-place? false`, which is fine for ITS fence
          ;; (konserve holds the lock across the write and the rename) but not for
          ;; a backing that evaluates the condition inside `-sync`: there the
          ;; condition would be evaluated against `<key>.new` and the rename would
          ;; compare nothing. What this test is about — `:lock-blob?` must not
          ;; disarm a self-fencing backing — is unchanged.
          self   (fn [domain]
                   (clojure.core/assoc store
                                       :config (clojure.core/assoc (:config store) :in-place? true)
                                       :backing
                                       (reify konserve.protocols/PConditionalWrite
                                         (-conditional-write-domain [_] domain)
                                         konserve.protocols/PSelfConditionalWrite)))]
      (doseq [d [:process :machine :global]]
        (is (= d (k/conditional-write-domain (self d)))
            (str "a self-fencing backing keeps " d " without konserve's lock")))
      (delete-store folder)))

  (testing "and a self-fencing backing is not handed the sidecar either, whatever
            its reach — the storage layer already evaluates the condition, so the
            extra blob and its round trips would buy nothing"
    (let [folder "/tmp/konserve-fs-selffence-sidecar"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))
          cas-of #(filter (fn [f] (str/ends-with? f ".cas"))
                          (map str (.list (java.io.File. folder))))]
      ;; the real filestore does NOT self-fence, so it gets one
      (k/assoc store :fenced {:v 1} {:sync? true})
      (k/get store :fenced nil {:sync? true :with-revision? true})
      (is (= 1 (count (cas-of))) "konserve-lock backing: sidecar")
      ;; the same store with a self-fencing backing gets none
      (let [self-store (clojure.core/assoc store
                                           :config (clojure.core/assoc (:config store) :in-place? true)
                                           :backing
                                           (reify konserve.protocols/PConditionalWrite
                                             (-conditional-write-domain [_] :machine)
                                             konserve.protocols/PSelfConditionalWrite))]
        (is (= :machine (k/conditional-write-domain self-store)))
        (is (false? (boolean (#'konserve.impl.defaults/internal-artifact? "x.ksv")))
            "sanity: a value blob is not an artifact"))
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
