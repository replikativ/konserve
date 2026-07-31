(ns konserve.compressor-test
  "Compressors, through real bytes and a real store.

  Two things here are durable-format assertions rather than behaviour tests:
  the compressor id lives in every blob header, and LZ4 byte 1 must keep
  meaning exactly what it meant before its writer switched to the high
  compressor."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest testing is]]
            [konserve.compressor :as c]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.protocols :as p]
            [konserve.serializers :as ser])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [net.jpountz.lz4 LZ4FrameInputStream LZ4FrameOutputStream]))

(defn- round-trip
  "Serialize through `compressor` and read the bytes back."
  [compressor v]
  (let [baos (ByteArrayOutputStream.)]
    (p/-serialize compressor baos (atom {}) v)
    [(count (.toByteArray baos))
     (p/-deserialize compressor (atom {})
                     (ByteArrayInputStream. (.toByteArray baos)))]))

(def ^:private zstd?
  "Whether com.github.luben/zstd-jni is on the classpath. It is an OPTIONAL
  dependency, so both branches are real and both are asserted below -- a test
  that silently skipped when it was absent would make `clj -M:test` green while
  covering nothing. Run the full matrix with `clj -M:zstd:test`."
  (try (Class/forName "com.github.luben.zstd.Zstd") true (catch Throwable _ false)))

(def ^:private payload
  ;; Repetitive the way serialized data actually is: the same keys over and
  ;; over. A random payload would measure nothing about a compressor.
  (vec (for [i (range 500)]
         {:person/name (str "person-" i)
          :person/city "Berlin"
          :person/role :engineer
          :person/tx (+ 536870912 i)})))

(deftest compressor-ids-are-durable
  (testing "the id is written into every blob header, so it must be stable and
            a new compressor must take the next free id rather than reuse one"
    (is (= 3 (count c/byte->compressor)))
    (is (= c/null-compressor (get c/byte->compressor 0)))
    (is (some? (get c/byte->compressor 1)) "LZ4 keeps byte 1")
    (is (= c/zstd-compressor (get c/byte->compressor 2)) "zstd is byte 2, not 1")))

(deftest lz4-resolves-on-graalvm
  (testing "konserve's native-image? test checked only whether
            org.graalvm.nativeimage.ImageInfo was on the classpath. That class
            ships with every GraalVM JDK, not just image builds, so on GraalVM
            byte 1 resolved to the UNSUPPORTED lz4 stub -- writing threw an NPE
            from create-header (nil compressor id) and reading an existing lz4
            blob threw outright. LZ4 was broken in both directions on a whole
            JDK family, silently.

            Assert the round trip through the id map, which is what actually
            broke, rather than that the constructor exists."
    (is (= 1 (get c/compressor->byte (c/get-compressor :lz4)))
        "the lz4 constructor must be findable in the inverted id map")
    (is (= 2 (get c/compressor->byte (c/get-compressor :zstd))))
    (is (= 0 (get c/compressor->byte (c/get-compressor :none))))))

(deftest lz4-byte-1-still-means-lz4-frame
  (testing "byte 1 is LZ4 Frame and must stay readable across versions. Asserted
            against a frame built by the convenience constructor rather than by
            round-tripping our own writer, so a change to how we WRITE cannot
            quietly make older blobs unreadable."
    (let [raw (.getBytes (apply str (repeat 500 "repeated-chunk ")))
          old-frame (let [baos (ByteArrayOutputStream.)
                          o (LZ4FrameOutputStream. baos)]
                      (.write o raw) (.close o) (.toByteArray baos))
          ;; fressian, not string: the string serializer binds *out* and needs a
          ;; Writer, while a compressor hands its wrapped OutputStream through.
          via-konserve (let [baos (ByteArrayOutputStream.)]
                         (p/-serialize (c/lz4-compressor (ser/fressian-serializer))
                                       baos (atom {}) "x")
                         (.toByteArray baos))]
      (testing "a frame written the OLD way still decompresses"
        (let [in (LZ4FrameInputStream. (ByteArrayInputStream. old-frame))
              out (ByteArrayOutputStream.)]
          (.transferTo in out)
          (is (= (seq raw) (seq (.toByteArray out))))))
      (testing "and what konserve writes now is readable by the same reader"
        (is (= "x" (p/-deserialize (c/lz4-compressor (ser/fressian-serializer))
                                   (atom {})
                                   (ByteArrayInputStream. via-konserve))))))))

(deftest zstd-degrades-to-a-typed-error-when-absent
  (testing "an optional dependency must fail with something actionable, not a
            ClassNotFoundError at namespace load"
    (if zstd?
      (is (= 2 (get c/compressor->byte (c/get-compressor :zstd)))
          "present: resolves to byte 2")
      (let [e (try (round-trip (c/zstd-compressor (ser/fressian-serializer)) 1)
                   (catch clojure.lang.ExceptionInfo e e))]
        (is (= :konserve/missing-optional-dependency (:type (ex-data e))))
        (is (= 'com.github.luben/zstd-jni (:dependency (ex-data e)))
            "the message names the dependency to add")))))

(deftest compressors-round-trip-and-actually-compress
  (doseq [[label compressor] (cond-> [["null" (c/null-compressor (ser/fressian-serializer))]
                                      ["lz4"  (c/lz4-compressor (ser/fressian-serializer))]]
                               zstd? (conj ["zstd" (c/zstd-compressor (ser/fressian-serializer))]))]
    (testing label
      (let [[n back] (round-trip compressor payload)]
        (is (= payload back) "value survives")
        (when-not (= "null" label)
          (let [[raw _] (round-trip (c/null-compressor (ser/fressian-serializer)) payload)]
            (is (< n (* 0.5 raw))
                (format "%s: %d B vs %d B uncompressed" label n raw))))))))

(deftest zstd-beats-lz4-on-serialized-data
  (testing "the reason byte 2 exists. zstd-3 is both smaller and faster than
            LZ4 here -- on one 512-datom blob, 2507 B in 69 us against lz4-hc's
            4767 B in 1602 us. That measurement is why :lz4 stayed the FAST
            compressor: whoever picks it wants speed, and ratio is what :zstd
            is for."
    (when zstd?
      (let [ser (ser/fressian-serializer)
          [lz4-n _] (round-trip (c/lz4-compressor ser) payload)
            [zstd-n _] (round-trip (c/zstd-compressor ser) payload)]
        (is (<= zstd-n lz4-n)
            (format "zstd %d B vs lz4-hc %d B" zstd-n lz4-n))))))

(deftest a-filestore-writes-the-compressor-id-it-was-given
  (testing "end to end: the id in the header on disk is the one selected, and
            the value reads back through it"
    ;; The selection path is :config {:compressor {:type kw}}, resolved through
    ;; konserve.compressor/get-compressor. Two earlier spellings of this test
    ;; silently produced header byte 0 -- the default null compressor -- and
    ;; still "passed" the round trip, which is why the header byte is asserted
    ;; rather than just the value coming back.
    (doseq [[label kw expected-byte] (cond-> [["lz4" :lz4 1]]
                                       zstd? (conj ["zstd" :zstd 2]))]
      (let [dir (str (System/getProperty "java.io.tmpdir") "/konserve-compressor-" label)]
        (delete-store dir)
        (try
          (let [store (connect-fs-store dir :opts {:sync? true}
                                        :config {:compressor {:type kw}})]
            (k/assoc store "k" payload {:sync? true})
            (is (= payload (k/get store "k" nil {:sync? true})) label)
            (let [f (->> (file-seq (io/file dir))
                         (filter #(.isFile ^java.io.File %))
                         (filter #(re-find #"\.ksv$" (.getName ^java.io.File %)))
                         first)
                  head (byte-array 4)]
              (is (some? f) "a blob was written")
              (with-open [in (io/input-stream f)] (.read in head))
              (is (= expected-byte (aget head 2))
                  (str label ": header byte 2 is the compressor id"))))
          (finally (delete-store dir)))))))
