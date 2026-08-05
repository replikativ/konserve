(ns konserve.encoding-config-test
  "`:config :encoding` — the canonical home for everything about turning a
  value into bytes, and the old spellings that must keep working.

  Every assertion here is on the BLOB HEADER rather than the config map,
  because the header is what a later reader dispatches on. Every bug in this
  family was invisible precisely because nothing checked the resulting bytes."
  (:require [clojure.test :refer [deftest testing is]]
            [konserve.core :as k]
            [konserve.store :as st]
            [konserve.filestore :refer [connect-fs-store]]
            [konserve.impl.defaults :refer [normalize-store-config
                                            assert-encoding-supported!]])
  (:import [java.io File FileInputStream]))

(defn- fresh-dir [n]
  (let [d (str (System/getProperty "java.io.tmpdir") "/konserve-enc-" n)]
    (when (.exists (File. d))
      (run! #(.delete ^File %) (reverse (file-seq (File. d)))))
    d))

(defn- header-of
  "[version serializer compressor encryptor] of the store's only blob."
  [dir]
  (let [^File f (first (filter #(.isFile ^File %) (file-seq (File. dir))))]
    (with-open [in (FileInputStream. f)]
      (let [b (byte-array 4)] (.read in b)
           (vec (map #(bit-and % 0xff) b))))))

(defn- write! [dir cfg]
  (let [store (st/create-store (merge {:backend :file :path dir
                                       :id (java.util.UUID/randomUUID)}
                                      cfg)
                               {:sync? true})]
    (k/assoc store "k" {:a (vec (range 100))} {:sync? true})
    (header-of dir)))

(deftest encoding-drives-the-header
  (testing "the canonical shape reaches all three header bytes"
    (is (= [1 1 0 0] (write! (fresh-dir "default") {})))
    (is (= [1 3 0 0] (write! (fresh-dir "boring")
                             {:config {:encoding {:serializer :BoringSerializer}}})))
    (is (= [1 3 1 0] (write! (fresh-dir "both")
                             {:config {:encoding {:serializer :BoringSerializer
                                                  :compressor {:type :lz4}}}})))))

(deftest old-spellings-still-work
  (testing "`:default-serializer` at the top level and `:compressor` directly
            under `:config` are the pre-:encoding spellings. They must keep
            producing the same bytes -- konserve is pre-1.0 but silently
            changing what a caller's config means is the failure this whole
            shape exists to prevent."
    (is (= [1 3 1 0] (write! (fresh-dir "old")
                             {:default-serializer :BoringSerializer
                              :config {:compressor {:type :lz4}}})))))

(deftest canonical-wins-over-the-old-spelling
  (testing "so a caller migrating one key at a time never gets a surprise from
            the leftover"
    (is (= [1 3 0 0] (write! (fresh-dir "both-spellings")
                             {:default-serializer :FressianSerializer
                              :config {:encoding {:serializer :BoringSerializer}}})))))

(deftest an-unknown-encoding-key-is-refused
  (testing "this is what stops the family recurring: a typo or a wrong-shaped
            value fails loudly instead of being silently ignored, which is how
            every bug here started"
    (is (= :store-configuration-error
           (:type (ex-data (try (normalize-store-config
                                 {:config {:encoding {:compresor {:type :lz4}}}})
                                (catch Exception e e))))))))

(deftest normalize-is-idempotent
  (testing "a config that is already canonical must pass through unchanged --
            otherwise normalising twice, which the filestore and
            connect-default-store both do, would drift"
    (let [c {:config {:encoding {:serializer :BoringSerializer}
                      :sync-blob? true}}]
      (is (= (normalize-store-config c)
             (normalize-store-config (normalize-store-config c)))))))

(deftest backends-can-refuse-an-encoding-they-cannot-honour
  (testing "konserve-lmdb uses its own buffer format and cannot honour an
            arbitrary serializer; others hardcoded one for years. A shared
            helper makes the refusal read the same everywhere, instead of
            being spelled five ways -- or, as it was, not at all."
    (let [cfg {:config {:encoding {:serializer :BoringSerializer}}}]
      (testing "an unsupported serializer is refused, naming what is supported"
        (is (= :store-configuration-error
               (:type (ex-data (try (assert-encoding-supported!
                                     "konserve-lmdb" cfg
                                     {:serializers #{:FressianSerializer}})
                                    (catch Exception e e)))))))
      (testing "a supported one passes through"
        (is (= cfg (assert-encoding-supported!
                    "konserve-x" cfg
                    {:serializers #{:BoringSerializer :FressianSerializer}}))))
      (testing "nil means the backend does not care"
        (is (= cfg (assert-encoding-supported! "konserve-x" cfg {}))))
      (testing "compression is named by :type, with :none for absent"
        (is (= :store-configuration-error
               (:type (ex-data (try (assert-encoding-supported!
                                     "konserve-y"
                                     {:config {:encoding {:compressor {:type :lz4}}}}
                                     {:compressors #{:none}})
                                    (catch Exception e e))))))
        (is (some? (assert-encoding-supported! "konserve-y" {} {:compressors #{:none}})))))))
