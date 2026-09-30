(ns konserve.batch-gc-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :as a]
            [konserve.core :as k]
            [konserve.gc :as gc]
            [konserve.memory :as mem]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as layout]
            [konserve.filestore :as fs]
            [konserve.protocols :as p]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [superv.async :refer [go-try- <?-]])
  (:import [java.util Date]))

(defrecord TestStore [delegate locks pages requests failed malformed native? hooks active peak fail-key]
  p/PWriteHookStore
  (-get-write-hooks [_] hooks)
  (-set-write-hooks! [this h] (assoc this :hooks h))
  p/PEDNKeyValueStore
  (-exists? [_ key opts] (p/-exists? delegate key opts))
  (-get-meta [_ key opts] (p/-get-meta delegate key opts))
  (-get-in [_ key not-found opts] (p/-get-in delegate key not-found opts))
  (-assoc-in [_ key mfn value opts] (p/-assoc-in delegate key mfn value opts))
  (-update-in [_ key mfn f opts] (p/-update-in delegate key mfn f opts))
  (-dissoc [_ key opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [n (swap! active inc)]
                   (swap! peak max n)
                   (try
                     (when (= key fail-key) (throw (ex-info "Delete failed" {:type :test/delete-failed})))
                     (<?- (p/-dissoc delegate key opts))
                     (finally (swap! active dec)))))))
  p/PKeyIterable
  (-keys [_ opts]
    (swap! requests conj :full-list)
    (p/-keys delegate opts))
  p/PKeyPage
  (-supports-key-pages? [_] (some? pages))
  (-key-page [_ cursor opts]
    (swap! requests conj [:page cursor])
    (async+sync (:sync? opts) *default-sync-translation* (go-try- (get pages cursor))))
  p/PBatchDelete
  (-supports-batch-delete? [_] native?)
  (-batch-delete [_ keys opts]
    (swap! requests conj [:delete (vec keys)])
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [deleted (set (remove failed keys))]
                   (doseq [key deleted] (<?- (k/dissoc delegate key opts)))
                   (if malformed {:deleted #{} :failed {}}
                       {:deleted deleted :failed (into {} (map (fn [k] [k {:code :denied}])) (filter failed keys))}))))))

(defn- fixture [options]
  (let [delegate (mem/new-mem-store (atom {}) {:sync? true})]
    (map->TestStore (merge {:delegate delegate :locks (atom {}) :requests (atom [])
                            :failed #{} :native? true :hooks (atom {})
                            :active (atom 0) :peak (atom 0)} options))))

(defn- take-result [value sync?]
  (let [result (if sync? value (a/<!! value))]
    (if (instance? Throwable result) (throw result) result)))

(deftest native-batches-are-independent-of-atomic-multi-key-writes
  (doseq [sync? [true false]]
    (let [store (fixture {})]
      (is (k/batch-delete-capable? store))
      (is (not (p/-supports-multi-key? store)))
      (doseq [key [:a :b]] (k/assoc (:delegate store) key key {:sync? true}))
      (is (= #{:a :b :missing} (take-result (k/batch-dissoc store [:a :b :missing :a] {:sync? sync?}) sync?)))
      (is (= [[:delete [:a :b :missing]]] @(:requests store)))
      (is (empty? @(:locks store)) "batch locks released on success")
      (is (= #{} (take-result (k/batch-dissoc store [] {:sync? sync?}) sync?))))))

(deftest partial-and-malformed-results-do-not-report-success
  (doseq [sync? [true false]]
    (let [store (fixture {:failed #{:b}})
          seen (atom [])]
      (k/add-write-hook! store :test #(swap! seen conj %))
      (doseq [key [:a :b]] (k/assoc (:delegate store) key key {:sync? true}))
      (let [error (try (take-result (k/batch-dissoc store [:a :b] {:sync? sync?}) sync?)
                       (catch Exception e e))]
        (is (= :konserve/batch-delete-incomplete (:type (ex-data error))))
        (is (= #{:a} (:deleted (ex-data error))))
        (is (= {:b {:code :denied}} (:failed (ex-data error))))
        (is (= [:a] (mapv :key @seen)))
        (is (= :b (k/get (:delegate store) :b nil {:sync? true})))
        (is (empty? @(:locks store)))))
    (let [store (fixture {:malformed true})]
      (is (= :konserve/batch-delete-invalid-result
             (try (take-result (k/batch-dissoc store [:a] {:sync? sync?}) sync?)
                  (catch Exception e (:type (ex-data e))))))
      (is (empty? @(:locks store))))))

(deftest paginated-sweep-bounds-results-and-obeys-logical-timestamps
  (doseq [sync? [true false]]
    (let [old (Date. 10) cutoff (Date. 20)
          metadata (fn [key ts] {:key key :last-write ts})
          store (fixture {:pages {nil {:keys [(metadata :a old) (metadata :keep old)] :cursor :p2}
                                  :p2 {:keys [] :cursor :p3}
                                  :p3 {:keys [(metadata :b old) (metadata :young cutoff)] :cursor nil}}})]
      (doseq [key [:a :b :keep :young]] (k/assoc (:delegate store) key key {:sync? true}))
      (is (= {:deleted-count 2 :batches 2}
             (take-result (gc/sweep! store #{:keep} cutoff 2 {:sync? sync? :page-size 2 :collect-deleted? false}) sync?)))
      (is (= [:keep :young] (sort (map :key (k/keys (:delegate store) {:sync? true})))))
      (is (= [[:page nil] [:delete [:a]] [:page :p2] [:page :p3] [:delete [:b]]] @(:requests store)))
      (is (not-any? #{:full-list} @(:requests store))))))

(deftest fallback-deletions-propagate-errors-and-drain-launched-work
  (doseq [sync? [true false]]
    (let [store (fixture {:native? false :fail-key :b
                          :pages {nil {:keys (mapv #(hash-map :key % :last-write (Date. 1)) [:a :b :c]) :cursor nil}}})]
      (is (= :test/delete-failed
             (try (take-result (gc/sweep! store #{} (Date. 10) 3 {:sync? sync? :concurrency 2}) sync?)
                  (catch Exception e
                    ;; futures retain the original cause inside ExecutionException.
                    (or (:type (ex-data e)) (some-> e .getCause ex-data :type))))))
      (is (zero? @(:active store)))
      (is (<= @(:peak store) 2))
      (is (empty? @(:locks store))))))

(deftest cancellation-and-invalid-limits-stop-before-deletion
  (let [store (fixture {:pages {nil {:keys [] :cursor nil}}})]
    (is (= :konserve/sweep-cancelled
           (try (gc/sweep! store #{} (Date.) 1 {:sync? true :cancel? (constantly true)})
                (catch Exception e (:type (ex-data e))))))
    (is (empty? @(:requests store)))
    (doseq [opts [{:concurrency 0} {:page-size -1}]]
      (is (= :konserve/sweep-invalid-limit
             (try (gc/sweep! store #{} (Date.) 1 opts)
                  (catch Exception e (:type (ex-data e)))))))))

(deftest conditional-deletion-and-stalled-pages-are-rejected
  (let [store (fixture {:pages {:same {:keys [] :cursor :same}}})]
    (is (thrown? Exception (k/batch-dissoc store [:a] {:expected-revision :token})))
    (is (empty? @(:requests store)))
    (is (= :konserve/key-page-invalid-result
           (try (k/key-page store :same {:sync? true})
                (catch Exception e (:type (ex-data e))))))))

(deftest default-store-validates-physical-batch-accounting
  (doseq [sync? [true false]]
    (let [requested (atom nil)
          result (atom nil)
          backing (reify layout/PBatchDeleteBackingStore
                    (-batch-delete-blobs [_ keys env]
                      (reset! requested (set keys))
                      (async+sync (:sync? env) *default-sync-translation*
                                  (go-try- (or @result {:deleted (set keys) :failed {}})))))
          store (defaults/map->DefaultStore {:backing backing :config {:in-place? true}
                                             :locks (atom {})})]
      (is (= #{nil :a} (take-result (k/batch-dissoc store [nil :a] {:sync? sync?}) sync?)))
      (is (= (set (map defaults/key->store-key [nil :a])) @requested))
      (is (not (k/batch-delete-capable? (assoc store :config {:in-place? false}))))
      ;; Unknown physical keys previously translated to logical nil and could
      ;; masquerade as successful deletion of a requested nil key.
      (doseq [bad [{:deleted #{"foreign.ksv"} :failed {}}
                   {:deleted [(defaults/key->store-key nil)] :failed {}}
                   {:deleted #{} :failed nil}]]
        (reset! result bad)
        (is (= :konserve/batch-delete-invalid-result
               (try (take-result (k/batch-dissoc store [nil] {:sync? sync?}) sync?)
                    (catch Exception e (:type (ex-data e))))))))))

(deftest default-store-validates-and-reads-physical-metadata-pages
  (let [path (str "/tmp/konserve-page-" (random-uuid))
        store (fs/connect-fs-store path :opts {:sync? true})
        raw (:backing store)
        page (atom nil)
        backing (reify layout/PKeyPageBackingStore
                  (-key-page-blobs [_ _ env]
                    (async+sync (:sync? env) *default-sync-translation* (go-try- @page)))
                  layout/PBackingStore
                  (-create-blob [_ key env] (layout/-create-blob raw key env)))
        paged (assoc store :backing backing)]
    (try
      (k/assoc store :a :payload {:sync? true})
      (let [expected (p/-get-meta store :a {:sync? true})]
        (reset! page {:keys [(defaults/key->store-key :a)] :cursor nil})
        (doseq [sync? [true false]]
          (is (= {:keys [expected] :cursor nil}
                 (take-result (k/key-page paged nil {:sync? sync? :limit 1}) sync?)))))
      (doseq [bad [nil {} {:keys nil :cursor nil}
                   {:keys ["a" "b"] :cursor nil}]]
        (reset! page bad)
        (is (= :konserve/key-page-invalid-result
               (try (k/key-page paged nil {:sync? true :limit 1})
                    (catch Exception e (:type (ex-data e)))))))
      (finally (fs/delete-store path)))))
