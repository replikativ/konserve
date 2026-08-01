(ns konserve.cbor-pss-test
  "The vertical: persistent-sorted-set index nodes stored through konserve's
  boring serializer (byte 3), in CBOR.

  This is the integration the whole boring effort exists for, and it is the one
  the clj-cbor serializer (byte 2) could not do at all — it throws on any
  handler, so a PSS node could never reach it.

  Lives in test-pss/ rather than test/ so the default `clj -M:test` run does
  not need persistent-sorted-set on the classpath. Run it with:

    clj -M:pss:test -d test-pss"
  (:require [boring.core :as boring]
            [clojure.test :refer [deftest testing is]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.serializers :as ser]
            [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.cbor :as pss-cbor])
  (:import [org.replikativ.persistent_sorted_set IStorage]))

(def ^:private test-dir
  (str (System/getProperty "java.io.tmpdir") "/konserve-boring-pss-test"))

(defn- vertical
  "Build a konserve-backed IStorage whose blobs are boring-encoded PSS nodes.

  Note the resolvers are LATE-BOUND through an atom. The PSS root reader needs
  the live IStorage; the storage needs the konserve store; the store needs the
  serializer; the serializer needs the registry the resolvers live in. Since a
  boring registry is an immutable value it cannot be patched afterwards, so the
  cycle is broken with an atom the resolver closes over — the same shape
  datahike uses for its storage registry."
  [dir]
  (let [storage-box (atom nil)
        reads (atom 0)
        registry (pss-cbor/install (boring/tag-registry)
                             {:default-bf 512
                              :resolve-storage (fn [_] @storage-box)
                              :resolve-cmp (fn [_] compare)})
        store (connect-fs-store
               dir :opts {:sync? true}
               :serializers {:BoringSerializer
                             (ser/boring-serializer registry {:shapes true})}
               :default-serializer :BoringSerializer)
        n (atom 0)
        storage (reify IStorage
                  (store [_ node]
                    (let [addr (str "node-" (swap! n inc))]
                      (k/assoc store addr node {:sync? true})
                      addr))
                  (restore [_ addr]
                    (swap! reads inc)
                    (k/get store addr nil {:sync? true}))
                  (accessed [_ _] nil))]
    (reset! storage-box storage)
    {:store store :storage storage :writes n :reads reads}))

(deftest pss-index-nodes-round-trip-through-konserve
  (delete-store test-dir)
  (try
    (let [{:keys [store storage writes reads]} (vertical test-dir)
          ;; Big enough to force a multi-level tree: a small set produces a
          ;; single leaf and never exercises the branch path at all.
          s (into (pss/sorted-set-by compare) (range 50000))]
      (pss/store s storage)
      (testing "nodes were written as konserve blobs"
        (is (pos? @writes))
        (is (> @writes 1) "must be a multi-level tree, not one leaf"))

      (k/assoc store "root" s {:sync? true})
      (let [before @reads
            back (k/get store "root" nil {:sync? true})]
        (testing "the root comes back as a real PersistentSortedSet"
          (is (= org.replikativ.persistent_sorted_set.PersistentSortedSet (type back))))
        (testing "and its contents are intact"
          (is (= 50000 (count back)))
          (is (= (vec s) (vec back))))
        (testing "having actually restored nodes from konserve, not memory"
          (is (> @reads before)))))
    (finally (delete-store test-dir))))
