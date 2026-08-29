(ns konserve.gc-coordination-test
  (:require [clojure.core.async :as async :refer [<!! >!!]]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.gc :as gc]
            [konserve.gc-coordination :as coord]
            [konserve.memory :refer [new-mem-store]]
            [konserve.protocols :as kp])
  (:import [java.util Date]
           [java.util.concurrent CountDownLatch]))

(defn- take!! [x]
  (let [v (<!! x)]
    (if (instance? Throwable v) (throw v) v)))

(defn- thrown-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- delivered [value]
  (let [ch (async/promise-chan)]
    (async/put! ch value)
    ch))

(deftest acquisition-requires-explicit-quiescent-activation
  (let [store (new-mem-store (atom {}) {:sync? true})]
    (is (false? (take!! (coord/active? store))))
    (is (= :konserve/gc-coordination-not-active
           (:type (thrown-data
                   #(take!! (coord/begin-publication! store))))))
    (is (= :konserve/gc-coordination-not-active
           (:type (thrown-data
                   #(take!! (coord/begin-collection! store))))))
    (take!! (coord/activate! store))
    (is (true? (take!! (coord/active? store))))))

(deftest publication-and-collection-exclude-one-another
  (let [backing (atom {})
        writer-store (new-mem-store backing {:sync? true})
        collector-store (new-mem-store backing {:sync? true})]
    (take!! (coord/activate! writer-store))
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
        _ (take!! (coord/activate! store))
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
        _ (coord/activate! store {:sync? true})
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
    (take!! (coord/activate! store))
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

(deftest activated-coordination-cannot-be-bypassed-by-tokenless-sweep
  (let [store (new-mem-store (atom {}) {:sync? true})
        cutoff (Date. Long/MAX_VALUE)]
    (take!! (coord/activate! store))
    (k/assoc store :candidate :unpublished {:sync? true})
    (is (= :konserve/gc-coordination-token-required
           (:type (thrown-data
                   #(take!! (gc/sweep! store #{} cutoff))))))
    (is (= :unpublished (k/get store :candidate nil {:sync? true})))
    (let [collection (take!! (coord/begin-collection! store))]
      (is (= #{:candidate}
             (take!! (gc/sweep! store #{} cutoff 1000
                                {:coordination-token collection}))))
      (take!! (coord/end-collection! store collection)))))

(deftest acquire-is-idempotent-by-caller-operation-id
  (let [store (new-mem-store (atom {}) {:sync? true})
        publication-id (random-uuid)
        collection-id (random-uuid)
        publication-opts {:id publication-id :owner {:job :writer}}]
    (take!! (coord/activate! store))
    (let [first-token (take!! (coord/begin-publication! store publication-opts))
          retry-token (take!! (coord/begin-publication! store publication-opts))]
      (is (= first-token retry-token))
      (is (= 1 (count (:publishers (take!! (coord/state store)))))
          "a lost response can be retried without leaking another slot")
      (take!! (coord/assert-publication! store first-token))
      (take!! (coord/end-publication! store first-token)))
    (let [opts {:id collection-id :owner {:job :collector}}
          first-token (take!! (coord/begin-collection! store opts))
          retry-token (take!! (coord/begin-collection! store opts))]
      (is (= first-token retry-token))
      (take!! (coord/end-collection! store first-token)))))

(deftest tokens-are-bound-to-store-identity
  (let [id-a (random-uuid)
        id-b (random-uuid)
        store-a (assoc (new-mem-store (atom {}) {:sync? true})
                       kp/store-config-key {:id id-a})
        store-b (assoc (new-mem-store (atom {}) {:sync? true})
                       kp/store-config-key {:id id-b})
        _ (take!! (coord/activate! store-a))
        token (take!! (coord/begin-publication! store-a))]
    (is (= :konserve/gc-coordination-store-mismatch
           (:type (thrown-data
                   #(take!! (coord/assert-publication! store-b token))))))
    (take!! (coord/end-publication! store-a token))))

(deftest managed-async-collection-releases-only-after-work-delivers
  (let [store (new-mem-store (atom {}) {:sync? true})
        started (async/promise-chan)
        finish (async/promise-chan)
        _ (take!! (coord/activate! store))
        result (coord/run-collection!
                store
                (fn [token _]
                  (async/put! started token)
                  finish))]
    (is (= :collection (:kind (<!! started))))
    (is (= :konserve/gc-collection-active
           (:type (thrown-data
                   #(take!! (coord/begin-publication! store))))))
    (>!! finish :finished)
    (is (= :finished (take!! result)))
    (let [publication (take!! (coord/begin-publication! store))]
      (take!! (coord/end-publication! store publication)))))

(deftest managed-async-publication-releases-only-after-work-delivers
  (let [store (new-mem-store (atom {}) {:sync? true})
        started (async/promise-chan)
        finish (async/promise-chan)
        _ (take!! (coord/activate! store))
        result (coord/run-publication!
                store
                (fn [token _]
                  (async/put! started token)
                  finish))]
    (is (= :publication (:kind (<!! started))))
    (is (= :konserve/gc-publication-active
           (:type (thrown-data
                   #(take!! (coord/begin-collection! store))))))
    (>!! finish :published)
    (is (= :published (take!! result)))
    (let [collection (take!! (coord/begin-collection! store))]
      (take!! (coord/end-collection! store collection)))))

(deftest managed-collection-releases-after-callback-failures
  (let [store (new-mem-store (atom {}) {:sync? true})
        _ (take!! (coord/activate! store))
        thrown (ex-info "callback threw" {:case :throw})
        delivered-error (ex-info "callback delivered" {:case :deliver})]
    (is (= :throw
           (:case (ex-data
                   (<!! (coord/run-collection!
                         store (fn [_ _] (throw thrown))))))))
    (is (nil? (:collector (take!! (coord/state store)))))
    (is (= :deliver
           (:case (ex-data
                   (<!! (coord/run-collection!
                         store (fn [_ _] (delivered delivered-error))))))))
    (is (nil? (:collector (take!! (coord/state store)))))))

(deftest managed-collection-reports-work-and-release-failures
  (let [store (new-mem-store (atom {}) {:sync? true})
        _ (take!! (coord/activate! store))
        work-error (ex-info "work failed" {:case :work})
        release-error (ex-info "release failed" {:case :release})]
    (with-redefs [coord/end-collection!
                  (fn [_ _ _] (delivered release-error))]
      (let [result (<!! (coord/run-collection!
                         store (fn [_ _] (delivered work-error))))]
        (is (= :konserve/gc-coordination-operation-and-release-failed
               (:type (ex-data result))))
        (is (= work-error (:work-error (ex-data result))))
        (is (= release-error (:release-error (ex-data result))))))))

(deftest managed-brackets-reject-wrong-callback-shapes-and-release
  (doseq [[label run! active-key]
          [[:publication coord/run-publication! :publishers]
           [:collection coord/run-collection! :collector]]]
    (let [store (new-mem-store (atom {}) {:sync? true})
          _ (take!! (coord/activate! store))
          async-result (<!! (run! store (fn [_ _] :plain-value)))]
      (is (= :konserve/gc-coordination-callback-shape
             (:type (ex-data async-result)))
          (str label " rejects a plain value in async mode"))
      (is (empty? (get (take!! (coord/state store)) active-key))
          (str label " async shape failure releases its fence"))
      (is (= :konserve/gc-coordination-callback-shape
             (:type
              (thrown-data
               #(run! store (fn [_ _] (delivered :later)) {:sync? true}))))
          (str label " rejects a channel in sync mode"))
      (is (empty? (get (take!! (coord/state store)) active-key))
          (str label " sync shape failure releases its fence")))))

(deftest filestore-handles-share-the-machine-domain-fence
  (let [folder (str "/tmp/konserve-gc-coordination-" (random-uuid))]
    (try
      (let [writer-store (take!! (connect-fs-store folder))
            collector-store (take!! (connect-fs-store folder))
            _ (take!! (coord/activate!
                       writer-store {:required-domain :machine}))
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

(deftest filestore-cas-preserves-concurrent-publishers-across-handles
  (let [folder (str "/tmp/konserve-gc-coordination-cas-" (random-uuid))]
    (try
      (let [store-a (take!! (connect-fs-store folder))
            store-b (take!! (connect-fs-store folder))
            n 24
            ready (CountDownLatch. n)
            start (CountDownLatch. 1)
            _ (coord/activate! store-a {:sync? true :required-domain :machine})
            futures
            (mapv (fn [i]
                    (future
                      (.countDown ready)
                      (.await start)
                      (let [store (if (even? i) store-a store-b)]
                        [store
                         (coord/begin-publication!
                          store {:sync? true
                                 :required-domain :machine
                                 :id (random-uuid)
                                 :owner {:worker i}})])))
                  (range n))]
        (.await ready)
        (.countDown start)
        (let [acquired (mapv deref futures)]
          (is (= n (count (:publishers
                           (coord/state store-a {:sync? true}))))
              "revision CAS must not lose a publisher registered through another handle")
          (doseq [[store token] acquired]
            (coord/end-publication!
             store token {:sync? true :required-domain :machine}))))
      (finally (delete-store folder)))))
