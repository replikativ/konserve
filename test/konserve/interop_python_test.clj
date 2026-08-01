(ns konserve.interop-python-test
  "Proof that a konserve store is readable without Clojure.

  `interop/read_konserve_blob.py` is documentation, and documentation about a
  binary format rots the moment nobody executes it. This writes a real store
  with the CBOR serializer and the zstd compressor, runs that script over the
  blob it produced, and compares the result to the Clojure value. A change to
  the header layout, the compressor id, or the serializer id breaks it here.

  Skips -- loudly -- when python3, cbor2/zstandard, or zstd-jni are missing,
  so a contributor without them is not blocked. `bin/run-unittests` runs the
  suite twice, once with zstd-jni and once without, so the pass that matters
  here is the one that has it."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]))

(def ^:private zstd?
  "com.github.luben/zstd-jni is optional; `bin/run-unittests` supplies it on
  the second pass. Without it there is no zstd blob to hand the reader."
  (try (Class/forName "com.github.luben.zstd.Zstd") true (catch Throwable _ false)))

(defn- python-ready? []
  (try (zero? (:exit (sh "python3" "-c" "import cbor2, zstandard")))
       (catch Exception _ false)))

(defn- blob-file [dir]
  (->> (file-seq (io/file dir))
       (filter #(.isFile ^java.io.File %))
       (filter #(str/ends-with? (.getName ^java.io.File %) ".ksv"))
       first))

(deftest python-reads-a-konserve-blob
  (if-not (and zstd? (python-ready?))
    (println (str "  SKIPPED konserve.interop-python-test — "
                  (if zstd? "pip install cbor2 zstandard"
                      "run with -M:zstd:test")))
    (let [dir (str (System/getProperty "java.io.tmpdir") "/konserve-interop-python")
          value {:title "kabel"
                 :tags #{:cbor :zstd}
                 :counts [1 2 3]
                 :nested {:a {:b "deep"}}
                 :bytes? true}]
      (delete-store dir)
      (try
        (let [store (connect-fs-store dir
                                      :opts {:sync? true}
                                      :default-serializer :BoringSerializer
                                      :config {:compressor {:type :zstd}})]
          (k/assoc store "doc" value {:sync? true})
          (is (= value (k/get store "doc" nil {:sync? true}))
              "Clojure reads its own blob back")

          (let [f (blob-file dir)
                _ (is (some? f) "a .ksv blob was written")
                {:keys [exit out err]} (sh "python3"
                                           "interop/read_konserve_blob.py"
                                           (.getPath ^java.io.File f))]
            (is (zero? exit) (str "python reader failed:\n" err out))
            (testing "the header the script parsed is the header konserve wrote"
              (is (str/includes? out "serializer=BoringSerializer"))
              (is (str/includes? out "compressor=zstd")))
            (testing "and the VALUE arrives, not merely a header and a shrug.
                      Asserted field by field: a substring check against the
                      whole repr would pass on a partially decoded map."
              (is (str/includes? out ":title: 'kabel'"))
              (is (str/includes? out ":counts: [1, 2, 3]"))
              (is (str/includes? out ":nested: {:a: {:b: 'deep'}}")
                  "a keyword key prints as :a, NOT as the string 'a' -- the two
                   are different map keys in Clojure and flattening them merges
                   entries")
              (is (str/includes? out ":cbor") "set members survive tag 258"))
            (testing "metadata is decoded from its own compressed segment"
              (is (str/includes? out ":key")))))
        (finally (delete-store dir))))))
