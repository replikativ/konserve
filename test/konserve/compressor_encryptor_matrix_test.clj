(ns konserve.compressor-encryptor-matrix-test
  "Every compressor x encryptor x serializer combination, through a real store.

  Two bugs lived here undetected because nothing combined a non-null compressor
  with a non-null encryptor:

  1. The read path nested the wrappers the other way round from the write path,
     so it tried to DECOMPRESS CIPHERTEXT. Every compressor+encryptor pair
     failed. Both defaults are null, and null is identity, so order did not
     matter in any configuration the tests used -- while the README documents
     compression and encryption configured together.

  2. The LZ4 compressor called `.flush` rather than `.close`, so the frame's
     EndMark was never written. Fressian never noticed: it stops reading at the
     end of a value. The CBOR serializer reads to EOF and hit the truncation --
     a compressor bug only one serializer could see, which is the argument for
     running this matrix over more than one.

  Skips zstd when zstd-jni is absent; `bin/run-unittests` supplies it on the
  second pass."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.java.io :as io]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]))

(def ^:private zstd?
  (try (Class/forName "com.github.luben.zstd.Zstd") true (catch Throwable _ false)))

(def ^:private value
  {:title "matrix" :tags #{:a :b} :counts (vec (range 200))
   :nested {:deep {:deeper "value"}}})

(defn- round-trip [serializer config]
  (let [dir (str (System/getProperty "java.io.tmpdir")
                 "/konserve-matrix-" (Math/abs (hash [serializer config])))]
    (delete-store dir)
    (try
      (let [store (connect-fs-store dir :opts {:sync? true}
                                    :default-serializer serializer
                                    :config config)]
        (k/assoc store "k" value {:sync? true})
        (k/get store "k" nil {:sync? true}))
      (finally (delete-store dir)))))

(deftest every-compressor-encryptor-combination-round-trips
  (doseq [serializer [:BoringSerializer :FressianSerializer]
          compressor (cond-> [nil {:type :lz4}] zstd? (conj {:type :zstd}))
          encryptor  [nil {:type :aes :key "s3cr3t"}]]
    (let [config (cond-> {}
                   compressor (assoc :compressor compressor)
                   encryptor  (assoc :encryptor encryptor))
          label  (str serializer " / " (or (:type compressor) "none")
                      " / " (or (:type encryptor) "none"))]
      (testing label
        (is (= value (round-trip serializer config)) label)))))

(deftest compression-happens-before-encryption
  (testing "the order is not arbitrary: compressing ciphertext accomplishes
            nothing, so a compressed+encrypted blob must be smaller than an
            encrypted-only one. This also pins the order the read path has to
            mirror."
    (when zstd?
      (let [size (fn [config]
                   (let [dir (str (System/getProperty "java.io.tmpdir")
                                  "/konserve-order-" (Math/abs (hash config)))]
                     (delete-store dir)
                     (try
                       (let [store (connect-fs-store dir :opts {:sync? true}
                                                     :default-serializer :BoringSerializer
                                                     :config config)]
                         (k/assoc store "k" value {:sync? true})
                         (->> (file-seq (io/file dir))
                              (filter #(.isFile ^java.io.File %))
                              (filter #(re-find #"\.ksv$" (.getName ^java.io.File %)))
                              first .length))
                       (finally (delete-store dir)))))
            enc-only (size {:encryptor {:type :aes :key "s3cr3t"}})
            both     (size {:compressor {:type :zstd}
                            :encryptor {:type :aes :key "s3cr3t"}})]
        (is (< both enc-only)
            (str "zstd+aes " both " B should be smaller than aes alone "
                 enc-only " B"))))))
