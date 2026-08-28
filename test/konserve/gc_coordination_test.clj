(ns konserve.gc-coordination-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.gc :as gc]
            [konserve.gc-coordination :as coord]
            [konserve.memory :refer [new-mem-store]])
  (:import [java.util Date]))

(defn- take!! [x]
  (let [v (<!! x)]
    (if (instance? Throwable v) (throw v) v)))

(defn- thrown-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(deftest publication-and-collection-exclude-one-another
  (let [backing (atom {})
        writer-store (new-mem-store backing {:sync? true})
        collector-store (new-mem-store backing {:sync? true})]
    (testing "a publisher visible through another store handle fences collection"
      (let [publication (take!! (coord/begin-publication! writer-store))]
        (is (= :konserve/gc-publication-active
               (:type (thrown-data
                       #(take!! (coord/begin-collection! collector-store))))))
        (take!! (coord/end-publication! writer-store publication))))

    (testing "the collector fences old-root publication until release"
      (let [collection (take!! (coord/begin-collection! collector-store))]
        (is (= :konserve/gc-collection-active
               (:type (thrown-data
                       #(take!! (coord/begin-publication! writer-store))))))
        (is (= (select-keys collection [:id :epoch])
               (select-keys (:collector (take!! (coord/state writer-store)))
                            [:id :epoch]))
            "the durable state exposes the owning id and epoch")
        (take!! (coord/end-collection! collector-store collection))
        (let [publication (take!! (coord/begin-publication! writer-store))]
          (take!! (coord/end-publication! writer-store publication)))))

    (is (nil? (:collector (take!! (coord/state collector-store)))))
    (is (empty? (:publishers (take!! (coord/state collector-store)))))))

(deftest only-the-owning-collector-token-can-release-the-fence
  (let [store (new-mem-store (atom {}) {:sync? true})
        token (take!! (coord/begin-collection! store))
        impostor (assoc token :id (random-uuid))]
    (is (= :konserve/gc-collection-token-mismatch
           (:type (thrown-data
                   #(take!! (coord/end-collection! store impostor))))))
    (is (some? (:collector (take!! (coord/state store))))
        "the failed release left the real fence intact")
    (take!! (coord/end-collection! store token))
    (take!! (coord/end-collection! store token))
    (is (nil? (:collector (take!! (coord/state store)))))))

(deftest requested-coordination-domain-is-enforced
  (let [store (new-mem-store (atom {}) {:sync? true})]
    (is (= :process (k/conditional-write-domain store)))
    (is (= :konserve/gc-coordination-domain-insufficient
           (:type
            (thrown-data
             #(take!!
               (coord/begin-collection! store {:required-domain :machine}))))))))

(deftest synchronous-api-and-concurrent-publishers
  (let [store (new-mem-store (atom {}) {:sync? true})
        tokens (doall (map deref
                           (repeatedly
                            20
                            #(future
                               (coord/begin-publication! store {:sync? true})))))]
    (is (= 20 (count (:publishers (coord/state store {:sync? true})))))
    (doseq [token tokens]
      (coord/end-publication! store token {:sync? true}))
    (let [collection (coord/begin-collection! store {:sync? true})]
      (is (= collection
             (coord/assert-collection! store collection {:sync? true})))
      (coord/end-collection! store collection {:sync? true}))
    (is (empty? (:publishers (coord/state store {:sync? true}))))))

(deftest sweep-preserves-coordination-and-validates-the-token
  (let [store (new-mem-store (atom {}) {:sync? true})
        cutoff (Date. Long/MAX_VALUE)]
    (k/assoc store :keep :live {:sync? true})
    (k/assoc store :garbage :dead {:sync? true})
    (let [collection (take!! (coord/begin-collection! store))]
      (is (= #{:garbage}
             (take!! (gc/sweep! store #{:keep} cutoff 1000
                                {:coordination-token collection}))))
      (is (map? (k/get store coord/coordination-key nil {:sync? true})))
      (take!! (coord/end-collection! store collection))
      (k/assoc store :later-garbage :dead {:sync? true})
      (is (= :konserve/gc-collection-token-lost
             (:type
              (thrown-data
               #(take!! (gc/sweep! store #{} cutoff 1000
                                   {:coordination-token collection}))))))
      (is (= :dead (k/get store :later-garbage nil {:sync? true}))
          "a lost token aborts before deleting anything"))))

(deftest filestore-handles-share-the-machine-domain-fence
  (let [folder (str "/tmp/konserve-gc-coordination-" (random-uuid))]
    (try
      (let [writer-store (take!! (connect-fs-store folder))
            collector-store (take!! (connect-fs-store folder))
            publication
            (take!! (coord/begin-publication!
                     writer-store {:required-domain :machine}))]
        (is (= :machine (k/conditional-write-domain writer-store)))
        (is (= :konserve/gc-publication-active
               (:type
                (thrown-data
                 #(take!!
                   (coord/begin-collection!
                    collector-store {:required-domain :machine}))))))
        (take!! (coord/end-publication!
                 writer-store publication {:required-domain :machine}))
        (let [collection
              (take!! (coord/begin-collection!
                       collector-store {:required-domain :machine}))]
          (is (= collection
                 (take!! (coord/assert-collection! writer-store collection))))
          (take!! (coord/end-collection!
                   collector-store collection {:required-domain :machine}))))
      (finally (delete-store folder)))))
