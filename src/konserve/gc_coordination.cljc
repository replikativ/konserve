(ns konserve.gc-coordination
  "Durable coordination between root publication and mark/sweep collection.

   `konserve.gc-guard` protects newly written values until their pointer lands.
   This namespace protects the opposite transition: publishing a pointer to
   OLD objects after a collector has already marked roots. A timestamp cutoff
   cannot protect those objects because their writes may be arbitrarily old.

   The protocol is a durable reader/writer fence in one conditional-write
   domain. Root publishers briefly hold publication tokens; one collector holds
   the exclusive token from its authoritative root snapshot through sweep.

   Tokens deliberately do not expire. Konserve cannot currently make a delete
   batch conditional on this coordination record, so automatically stealing an
   expired collector token would be unsafe: a paused old collector could resume
   between checking its token and deleting a batch. A crashed collector therefore
   fails closed and needs operator recovery after proving it cannot resume."
  (:require [konserve.core :as k]
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

(defn- revision-mismatch? [e]
  (= :konserve/revision-mismatch (:type (ex-data e))))

(defn- initial-state []
  {:format-version 1
   :epoch 0
   :collector nil
   :publishers {}})

(defn- validate-state [state]
  (when-not (and (map? state)
                 (= 1 (:format-version state))
                 (nat-int? (:epoch state))
                 (map? (:publishers state)))
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

(defn- update-state!
  [store f {:keys [required-domain] :as opts}]
  (let [required-domain (or required-domain :process)
        io-opts (dissoc opts :required-domain :owner)]
    (require-domain! store required-domain)
    (async+sync
     (:sync? io-opts) *default-sync-translation*
     (go-try-
      (loop [attempt 0]
        (let [[stored revision]
              (<?- (k/get store coordination-key nil
                          (assoc io-opts :with-revision? true)))
              state (validate-state (or stored (initial-state)))
              updated (validate-state (f state))
              result (try
                       (<?- (k/assoc store coordination-key updated
                                     (assoc io-opts :expected-revision revision)))
                       updated
                       (catch #?(:clj Throwable :cljs :default) e
                         (if (revision-mismatch? e) ::retry (throw e))))]
          (if (= ::retry result)
            (if (< attempt 63)
              (recur (inc attempt))
              (throw
               (ex-info "Durable GC coordination did not converge."
                        {:type :konserve/gc-coordination-contention
                         :attempts (inc attempt)})))
            result)))))))

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

(defn begin-publication!
  "Acquire a short-lived root-publication token.

   Throws `:konserve/gc-collection-active` instead of waiting when a collector
   owns the fence. The caller may retry. Hold the token only around publication
   of branch heads, pins, aliases, or other pointers that can name old objects;
   long value/build work happens before it."
  ([store] (begin-publication! store {}))
  ([store {:keys [owner] :as opts}]
   (let [id (new-id)
         token {:kind :publication :id id}]
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
               (assoc-in state [:publishers id]
                         {:id id :owner owner :started-at (now)}))
             opts))
       token)))))

(defn end-publication!
  "Release a publication token. Idempotent after the token has disappeared."
  ([store token] (end-publication! store token {}))
  ([store {:keys [kind id] :as token} opts]
   (when-not (and (= :publication kind) (some? id))
     (throw (ex-info "Invalid GC publication token."
                     {:type :konserve/invalid-gc-coordination-token
                      :token token})))
   (update-state! store #(update % :publishers dissoc id) opts)))

(defn begin-collection!
  "Acquire the exclusive collector token.

   Throws when a publisher or collector is active. After acquisition, take the
   authoritative root snapshot, mark, sweep, then call `end-collection!` in a
   `finally`. The returned epoch is diagnostic and will support backend-fenced
   deletion later; it is not presently a recoverable lease."
  ([store] (begin-collection! store {}))
  ([store {:keys [owner] :as opts}]
   (let [id (new-id)
         token* (atom nil)]
     (async+sync
      (:sync? opts) *default-sync-translation*
      (go-try-
       (<?- (update-state!
             store
             (fn [state]
               (when-let [collector (:collector state)]
                 (throw
                  (ex-info "Another GC collection already owns the durable fence."
                           {:type :konserve/gc-collection-active
                            :collector collector})))
               (when (seq (:publishers state))
                 (throw
                  (ex-info "Root publication is active; collection must retry."
                           {:type :konserve/gc-publication-active
                            :publishers (vals (:publishers state))})))
               (let [epoch (inc (:epoch state))
                     token {:kind :collection :id id :epoch epoch}]
                 (reset! token* token)
                 (assoc state
                        :epoch epoch
                        :collector {:id id :epoch epoch :owner owner
                                    :started-at (now)})))
             opts))
       @token*)))))

(defn end-collection!
  "Release the exclusive collector token. A different token cannot release it."
  ([store token] (end-collection! store token {}))
  ([store {:keys [kind id epoch] :as token} opts]
   (when-not (and (= :collection kind) (some? id) (nat-int? epoch))
     (throw (ex-info "Invalid GC collection token."
                     {:type :konserve/invalid-gc-coordination-token
                      :token token})))
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
    opts)))

(defn assert-collection!
  "Raise unless `token` still owns the durable collection fence."
  ([store token] (assert-collection! store token {:sync? false}))
  ([store {:keys [kind id epoch] :as token} opts]
   (when-not (and (= :collection kind) (some? id) (nat-int? epoch))
     (throw (ex-info "Invalid GC collection token."
                     {:type :konserve/invalid-gc-coordination-token
                      :token token})))
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
