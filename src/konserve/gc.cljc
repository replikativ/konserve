(ns konserve.gc
  (:require [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.protocols :as p]
            #?(:clj [konserve.utils :as utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :as utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            [superv.async :refer [go-try- <?-]])
  #?(:clj (:import [java.util Date])))

(defn- epoch-ms ^long [d]
  #?(:clj (.getTime ^Date d) :cljs (.getTime d)))

(defn- cancelled! [opts]
  (when (and (:cancel? opts) ((:cancel? opts)))
    (throw (ex-info "Sweep cancelled; completed deletions are not rolled back."
                    {:type :konserve/sweep-cancelled}))))

(defn- delete-individually! [store keys concurrency opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (doseq [group (partition-all concurrency keys)]
                 (cancelled! opts)
                 (if (:sync? opts)
                   #?(:clj
                      (let [pending (mapv (fn [key]
                                            (future (k/dissoc store key {:sync? true :ignore-existence? true}))) group)]
                        ;; Wait for every dispatched delete before propagating an
                        ;; error, so no work is left running after sweep returns.
                        (let [results (mapv (fn [f] (try @f (catch Throwable e e))) pending)]
                          (doseq [r results] (when (instance? Throwable r) (throw r)))))
                      :cljs
                      (doseq [key group] (k/dissoc store key {:sync? true :ignore-existence? true})))
                   (let [pending (mapv #(k/dissoc store % {:ignore-existence? true}) group)
                         results (<?- (async/into [] (async/merge pending)))]
                     ;; merge treats exceptions as ordinary values. Explicitly
                     ;; surface them after draining all dispatched channels.
                     (doseq [r results]
                       (when (instance? #?(:clj Throwable :cljs js/Error) r) (throw r)))))))))

(defn- sweep-batches! [store candidates batch-size opts report]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (loop [batches (seq (partition-all batch-size candidates)) report report]
                 (if-let [batch (first batches)]
                   (let [_ (cancelled! opts)
                         keys (mapv :key batch)
                         _ (cond
                             (k/batch-delete-capable? store) (<?- (k/batch-dissoc store keys (select-keys opts [:sync?])))
                             (utils/multi-key-capable? store) (<?- (k/multi-dissoc store keys (select-keys opts [:sync?])))
                             :else (<?- (delete-individually! store keys (:concurrency opts 32) opts)))
                         report (if (:collect-deleted? opts true)
                                  (into report keys)
                                  (-> report (update :deleted-count + (count keys)) (update :batches inc)))]
                     (recur (next batches) report))
                   report)))))

(defn sweep!
  "Delete keys outside whitelist whose logical last-write precedes cutoff.

   Capture the guard cutoff BEFORE marking. This primitive does not serialize
   root publication with deletion; the caller owns that coordination contract.
   Age floors do not protect old objects promoted into roots after the mark.

   Options: :sync?, :concurrency (default 32, bounds individual deletes),
   :page-size (default 1000), :collect-deleted? (default true, returns a set;
   false returns {:deleted-count n :batches n}), and :cancel? (zero-arg predicate).
   Cancellation/failure may follow partial deletion and never rolls it back.
   Paginated stores are scanned one metadata page at a time. Other stores retain
   their existing keys enumeration; statistics-only results do not bound that
   backend's listing memory. Native batch deletion is non-atomic."
  ([store whitelist cutoff] (sweep! store whitelist cutoff 1000 {}))
  ([store whitelist cutoff batch-size] (sweep! store whitelist cutoff batch-size {}))
  ([store whitelist cutoff batch-size opts]
   (doseq [[k v] [[:batch-size batch-size] [:concurrency (:concurrency opts 32)] [:page-size (:page-size opts 1000)]]]
     (when-not (pos-int? v)
       (throw (ex-info "Sweep limits must be positive integers." {:type :konserve/sweep-invalid-limit :option k :value v}))))
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [candidate? (fn [{:keys [key last-write] :as meta}]
                                   (and (not (contains? whitelist key))
                                        (< (epoch-ms (or last-write (:konserve.core/timestamp meta))) (epoch-ms cutoff))))
                      initial (if (:collect-deleted? opts true) #{} {:deleted-count 0 :batches 0})]
                  (if (k/paged-keys? store)
                    (loop [cursor nil report initial]
                      (cancelled! opts)
                      (let [page (<?- (k/key-page store cursor (assoc (select-keys opts [:sync?]) :limit (:page-size opts 1000))))
                            report (<?- (sweep-batches! store (filter candidate? (:keys page)) batch-size opts report))]
                        (if-let [next-cursor (:cursor page)]
                          (recur next-cursor report)
                          report)))
                    (let [_ (cancelled! opts)
                          keys (<?- (k/keys store (select-keys opts [:sync?])))]
                      (<?- (sweep-batches! store (filter candidate? keys) batch-size opts initial)))))))))
