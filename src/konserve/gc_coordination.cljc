(ns konserve.gc-coordination
  "Durable coordination between root publication and mark/sweep collection.

   `konserve.gc-guard` protects newly written values until their pointer lands,
   but only inside one process. This namespace provides the durable equivalent
   and also protects the opposite transition: publishing a pointer to OLD
   objects after a collector has already marked roots. A timestamp cutoff cannot
   protect old objects because their writes may be arbitrarily old.

   The protocol is a durable reader/writer fence in one conditional-write
   domain. Publishers hold publication tokens across their unreachable-value
   interval; one collector holds the exclusive token from its authoritative
   root snapshot through sweep.

   A publisher that writes new values MUST acquire its token before the first
   value becomes visible in the store and retain it through pointer publication.
   A publisher that only names an already-complete old graph may acquire it just
   before publishing the pointer. This lets one fence cover both GC races.

   Tokens deliberately do not expire. Konserve cannot currently make a delete
   batch conditional on this coordination record, so automatically stealing an
   expired collector token would be unsafe: a paused old collector could resume
   between checking its token and deleting a batch. A crashed publisher OR
   collector therefore fails closed and needs operator recovery after proving it
   cannot resume."
  (:require [konserve.core :as k]
            [konserve.protocols :as kp]
            #?(:clj [clojure.core.async :as async]
               :cljs [clojure.core.async :as async :include-macros true])
            #?(:clj [clojure.core.async.impl.protocols :as async-proto]
               :cljs [cljs.core.async.impl.protocols :as async-proto])
            #?(:clj [konserve.utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            #?(:clj [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer-macros [go-try- <?-]]))
  #?(:clj (:import [java.util Date])))

(def coordination-key :konserve/gc-coordination)

(def coordination-root-keys
  "Konserve-internal keys a collector must never sweep."
  #{coordination-key})

(defn- now [] #?(:clj (Date.) :cljs (js/Date.)))

(defn- new-id [] #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid)))

(defn- error-value? [x]
  (instance? #?(:clj Throwable :cljs js/Error) x))

(defn- read-port? [x]
  (satisfies? async-proto/ReadPort x))

(defn- callback-shape-error [expected actual]
  (ex-info (str "Managed GC callback must return " expected ".")
           {:type :konserve/gc-coordination-callback-shape
            :expected expected
            :actual-type (type actual)}))

(defn- revision-mismatch? [x]
  (and (error-value? x)
       (= :konserve/revision-mismatch (:type (ex-data x)))))

(defn- initial-state []
  {:format-version 1
   :epoch 0
   :collector nil
   :publishers {}})

(defn- validate-state [state]
  (when-not (and (map? state)
                 (= 1 (:format-version state))
                 (nat-int? (:epoch state))
                 (map? (:publishers state))
                 (every? (fn [[id publisher]]
                           (and (uuid? id)
                                (map? publisher)
                                (= id (:id publisher))))
                         (:publishers state))
                 (or (nil? (:collector state))
                     (let [collector (:collector state)]
                       (and (map? collector)
                            (uuid? (:id collector))
                            (nat-int? (:epoch collector))))))
    (throw (ex-info "Invalid durable GC coordination state."
                    {:type :konserve/invalid-gc-coordination-state
                     :state state})))
  state)

(defn- require-domain! [store required-domain]
  (when-not (k/conditional-write? store required-domain)
    (throw
     (ex-info
      (str "Durable GC coordination requires conditional writes reaching "
           (pr-str required-domain) ".")
      {:type :konserve/gc-coordination-domain-insufficient
       :required-domain required-domain
       :actual-domain (k/conditional-write-domain store)}))))

(defn- token-context [store required-domain]
  {:required-domain required-domain
   :store-id (kp/store-id store)})

(defn- require-token-context! [store token]
  (let [required-domain (:required-domain token)
        actual-store-id (kp/store-id store)]
    (when-not required-domain
      (throw (ex-info "A GC coordination token is missing its required domain."
                      {:type :konserve/invalid-gc-coordination-token
                       :token token})))
    (require-domain! store required-domain)
    (when-not (= (:store-id token) actual-store-id)
      (throw (ex-info "A GC coordination token belongs to a different store."
                      {:type :konserve/gc-coordination-store-mismatch
                       :token-store-id (:store-id token)
                       :actual-store-id actual-store-id})))
    token))

(defn- apply-state-update [f stored require-active?]
  (try
    (when (and require-active? (nil? stored))
      (throw
       (ex-info "Durable GC coordination has not been activated for this store."
                {:type :konserve/gc-coordination-not-active})))
    {:value (-> (or stored (initial-state))
                validate-state
                f
                validate-state)}
    (catch #?(:clj Throwable :cljs js/Error) e
      {:error e})))

(defn- contention-error [attempt]
  (ex-info "Durable GC coordination did not converge."
           {:type :konserve/gc-coordination-contention
            :attempts (inc attempt)}))

(defn- update-state!
  [store f {:keys [required-domain require-active?] :as opts}]
  (let [required-domain (or required-domain :process)
        io-opts (dissoc opts :required-domain :require-active? :owner :id)]
    (require-domain! store required-domain)
    ;; Plain `k/update` is atomic only within one store handle. The durable fence
    ;; must serialize handles/processes at the backing's DECLARED domain, which
    ;; is exactly the revision-bearing read + expected-revision write contract.
    ;; Keep explicit sync/async loops: wrapping the catch/recur form in
    ;; `async+sync` hits a core.async CLJS macroexpansion bug.
    (if (:sync? io-opts)
      (loop [attempt 0]
        (let [[stored revision]
              (k/get store coordination-key nil
                     (assoc io-opts :with-revision? true))
              {:keys [value error]} (apply-state-update f stored require-active?)]
          (if error
            (throw error)
            (let [write-result
                  (try
                    (k/assoc store coordination-key value
                             (assoc io-opts :expected-revision revision))
                    nil
                    (catch #?(:clj Throwable :cljs js/Error) e e))]
              (cond
                (revision-mismatch? write-result)
                (if (< attempt 63)
                  (recur (inc attempt))
                  (throw (contention-error attempt)))

                (error-value? write-result) (throw write-result)
                :else value)))))
      (async/go-loop [attempt 0]
        (let [read-result
              (async/<! (k/get store coordination-key nil
                               (assoc io-opts :with-revision? true)))]
          (if (error-value? read-result)
            read-result
            (let [[stored revision] read-result
                  {:keys [value error]} (apply-state-update f stored require-active?)]
              (if error
                error
                (let [write-result
                      (async/<! (k/assoc
                                 store coordination-key value
                                 (assoc io-opts :expected-revision revision)))]
                  (cond
                    (revision-mismatch? write-result)
                    (if (< attempt 63)
                      (recur (inc attempt))
                      (contention-error attempt))

                    (error-value? write-result) write-result
                    :else value))))))))))

(defn state
  "Read the durable coordination state, or the empty initial state."
  ([store] (state store {:sync? false}))
  ([store opts]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (validate-state
      (or (<?- (k/get store coordination-key nil opts))
          (initial-state)))))))

(defn active?
  "Has this store completed the explicit coordination bootstrap? Unlike
   [[state]], this distinguishes an absent record from an active empty state."
  ([store] (active? store {:sync? false}))
  ([store opts]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (boolean (<?- (k/get store coordination-key nil opts)))))))

(defn activate!
  "Persist the empty coordination record and make coordinated collection
   mandatory for this store.

   Bootstrap this once while no legacy uncoordinated sweep is running. A
   tokenless `konserve.gc/sweep!` remains available only while this record is
   absent, so old stores can be migrated deliberately rather than changing
   semantics merely by upgrading a dependency."
  ([store] (activate! store {}))
  ([store opts]
   (update-state! store identity opts)))

(defn begin-publication!
  "Acquire a root-publication token.

   The store must first be initialized with [[activate!]] during a quiescent
   migration. Throws `:konserve/gc-collection-active` instead of waiting when a
   collector owns the fence. The caller may retry.

   If the operation writes NEW unreachable values, acquire this token BEFORE
   the first such write and retain it until the branch head, pin, alias, or
   equivalent root has landed. Preparing values before the token recreates the
   cross-process values-then-pointer sweep race this protocol closes. If the
   operation only publishes a pointer to an already-complete OLD object graph,
   acquiring immediately before the pointer write is sufficient. Expensive CPU
   preparation that writes nothing may happen before acquisition."
  ([store] (begin-publication! store {}))
  ([store {:keys [owner] :as opts}]
   (let [id (or (:id opts) (new-id))
         required-domain (or (:required-domain opts) :process)
         context (token-context store required-domain)
         token (merge {:kind :publication :id id} context)]
     (when-not (uuid? id)
       (throw (ex-info "A GC publication id must be a UUID."
                       {:type :konserve/invalid-gc-coordination-token
                        :id id})))
     (async+sync
      (:sync? opts) *default-sync-translation*
      (go-try-
       (<?- (update-state!
             store
             (fn [state]
               (when-let [collector (:collector state)]
                 (throw
                  (ex-info "A GC collection currently fences root publication."
                           {:type :konserve/gc-collection-active
                            :collector collector})))
               (if-let [publisher (get-in state [:publishers id])]
                 (do
                   (when-not (= (merge {:owner owner} context)
                                (select-keys publisher
                                             [:owner :required-domain :store-id]))
                     (throw
                      (ex-info "A GC publication id is already owned by another operation."
                               {:type :konserve/gc-coordination-id-collision
                                :id id :publisher publisher})))
                   state)
                 (assoc-in state [:publishers id]
                           (merge {:id id :owner owner :started-at (now)}
                                  context))))
             (assoc opts :require-active? true)))
       token)))))

(defn assert-publication!
  "Raise unless `token` still owns a durable publication slot. Long-running
   publishers must call this immediately before landing the root pointer."
  ([store token] (assert-publication! store token {:sync? false}))
  ([store {:keys [kind id] :as token} opts]
   (when-not (and (= :publication kind) (uuid? id))
     (throw (ex-info "Invalid GC publication token."
                     {:type :konserve/invalid-gc-coordination-token
                      :token token})))
   (require-token-context! store token)
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (let [publisher (get-in (<?- (state store opts)) [:publishers id])]
       (when-not (= (select-keys token [:required-domain :store-id])
                    (select-keys publisher [:required-domain :store-id]))
         (throw
          (ex-info "The GC publication token no longer owns a durable slot."
                   {:type :konserve/gc-publication-token-lost
                    :token token :publisher publisher}))))
     token))))

(defn end-publication!
  "Release a publication token. Idempotent after the token has disappeared."
  ([store token] (end-publication! store token {}))
  ([store {:keys [kind id] :as token} opts]
   (when-not (and (= :publication kind) (uuid? id))
     (throw (ex-info "Invalid GC publication token."
                     {:type :konserve/invalid-gc-coordination-token
                      :token token})))
   (require-token-context! store token)
   (update-state! store #(update % :publishers dissoc id)
                  (assoc opts
                         :required-domain (:required-domain token)
                         :require-active? true))))

(defn begin-collection!
  "Acquire the exclusive collector token.

   The store must first be initialized with [[activate!]] during a quiescent
   migration. Throws when a publisher or collector is active. After acquisition,
   take the authoritative root snapshot, mark, sweep, then call
   `end-collection!`. Prefer [[run-collection!]] so asynchronous work is awaited.
   The returned epoch is diagnostic and will support backend-fenced deletion
   later; it is not presently a recoverable lease."
  ([store] (begin-collection! store {}))
  ([store {:keys [owner] :as opts}]
   (let [id (or (:id opts) (new-id))
         required-domain (or (:required-domain opts) :process)
         context (token-context store required-domain)
         token* (atom nil)]
     (when-not (uuid? id)
       (throw (ex-info "A GC collection id must be a UUID."
                       {:type :konserve/invalid-gc-coordination-token
                        :id id})))
     (async+sync
      (:sync? opts) *default-sync-translation*
      (go-try-
       (<?- (update-state!
             store
             (fn [state]
               (if-let [collector (:collector state)]
                 (if (= id (:id collector))
                   (do
                     (when-not (= (merge {:owner owner} context)
                                  (select-keys collector
                                               [:owner :required-domain :store-id]))
                       (throw
                        (ex-info "A GC collection id is already owned by another operation."
                                 {:type :konserve/gc-coordination-id-collision
                                  :id id :collector collector})))
                     (reset! token* (merge {:kind :collection
                                            :id id
                                            :epoch (:epoch collector)}
                                           context))
                     state)
                   (throw
                    (ex-info "Another GC collection already owns the durable fence."
                             {:type :konserve/gc-collection-active
                              :collector collector})))
                 (do
                   (when (seq (:publishers state))
                     (throw
                      (ex-info "Root publication is active; collection must retry."
                               {:type :konserve/gc-publication-active
                                :publishers (vals (:publishers state))})))
                   (let [epoch (inc (:epoch state))
                         token (merge {:kind :collection :id id :epoch epoch}
                                      context)]
                     (reset! token* token)
                     (assoc state
                            :epoch epoch
                            :collector (merge {:id id :epoch epoch :owner owner
                                               :started-at (now)}
                                              context))))))
             (assoc opts :require-active? true)))
       @token*)))))

(defn end-collection!
  "Release the exclusive collector token. A different token cannot release it."
  ([store token] (end-collection! store token {}))
  ([store {:keys [kind id epoch] :as token} opts]
   (when-not (and (= :collection kind) (uuid? id) (nat-int? epoch))
     (throw (ex-info "Invalid GC collection token."
                     {:type :konserve/invalid-gc-coordination-token
                      :token token})))
   (require-token-context! store token)
   (update-state!
    store
    (fn [state]
      (if-let [collector (:collector state)]
        (if (= [id epoch] ((juxt :id :epoch) collector))
          (assoc state :collector nil)
          (throw
           (ex-info "A GC token cannot release another collector's fence."
                    {:type :konserve/gc-collection-token-mismatch
                     :token token :collector collector})))
        state))
    (assoc opts
           :required-domain (:required-domain token)
           :require-active? true))))

(defn assert-collection!
  "Raise unless `token` still owns the durable collection fence."
  ([store token] (assert-collection! store token {:sync? false}))
  ([store {:keys [kind id epoch] :as token} opts]
   (when-not (and (= :collection kind) (uuid? id) (nat-int? epoch))
     (throw (ex-info "Invalid GC collection token."
                     {:type :konserve/invalid-gc-coordination-token
                      :token token})))
   (require-token-context! store token)
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (let [collector (:collector (<?- (state store opts)))]
       (when-not (= [id epoch] ((juxt :id :epoch) collector))
         (throw
          (ex-info "The GC collection token no longer owns the durable fence."
                   {:type :konserve/gc-collection-token-lost
                    :token token :collector collector})))
       token)))))

(defn assert-sweep-authorized!
  "Authorize one destructive sweep step.

   A collection token is mandatory after [[activate!]]. Tokenless sweep remains
   available only for a legacy store whose coordination record is absent. This
   deliberately fails closed so an independent collector cannot bypass an
   active publisher.

   The first activation must be bootstrapped while no legacy sweep is running;
   no protocol layered above arbitrary delete operations can fence a deletion
   that began before the protocol existed."
  ([store token] (assert-sweep-authorized! store token {:sync? false}))
  ([store token opts]
   (if token
     (assert-collection! store token opts)
     (async+sync
      (:sync? opts) *default-sync-translation*
      (go-try-
       (when (<?- (k/get store coordination-key nil opts))
         (throw
          (ex-info "This store has durable GC coordination enabled; sweep requires a collection token."
                   {:type :konserve/gc-coordination-token-required})))
       nil)))))

(defn- invoke-managed [f token opts]
  (try
    (f token opts)
    (catch #?(:clj Throwable :cljs js/Error) e e)))

(defn- combined-lifecycle-error [work-error release-error]
  (ex-info "A managed GC operation and its fence release both failed."
           {:type :konserve/gc-coordination-operation-and-release-failed
            :work-error work-error
            :release-error release-error}))

(defn- finish-managed [result release-result]
  (cond
    (and (error-value? result) (error-value? release-result))
    (combined-lifecycle-error result release-result)

    (error-value? result) result
    (error-value? release-result) release-result
    :else result))

(defn- run-managed!
  [begin! end! store f opts]
  (if (:sync? opts)
    (let [token (begin! store opts)
          invoked (invoke-managed f token opts)
          result (if (and (not (error-value? invoked)) (read-port? invoked))
                   (callback-shape-error "a value in sync mode, not a channel" invoked)
                   invoked)
          release-result (try
                           (end! store token opts)
                           nil
                           (catch #?(:clj Throwable :cljs js/Error) e e))
          outcome (finish-managed result release-result)]
      (if (error-value? outcome) (throw outcome) outcome))
    (async/go
      (let [token-result (async/<! (begin! store opts))]
        (if (error-value? token-result)
          token-result
          (let [token token-result
                pending (invoke-managed f token opts)
                result (cond
                         (error-value? pending) pending
                         (not (read-port? pending))
                         (callback-shape-error "a core.async channel in async mode" pending)
                         :else (async/<! pending))
                release-result (async/<! (end! store token opts))]
            (finish-managed result release-result)))))))

(defn run-publication!
  "Acquire a publication token, run `f`, await its result, and then release.

   `f` receives `[token io-opts]`. In async mode it must return a channel; in
   sync mode it returns a value. The token spans the complete unreferenced-value
   interval, but `f` must still call [[assert-publication!]] immediately before
   landing its root pointer."
  ([store f] (run-publication! store f {}))
  ([store f opts]
   (run-managed! begin-publication! end-publication! store f opts)))

(defn run-collection!
  "Acquire the exclusive token, run `f`, await its result, and only then release.

   `f` receives `[token io-opts]`. In async mode it must return a channel; in
   sync mode it returns a value. Prefer this bracket to a caller-side
   `try/finally`: merely creating an async sweep channel is not completion, and
   releasing at that point reopens publication while deletes are still running."
  ([store f] (run-collection! store f {}))
  ([store f opts]
   (run-managed! begin-collection! end-collection! store f opts)))
