(ns konserve.tiered
  "Tiered store implementation with frontend and backend storage layers."
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [clojure.core.async :refer [go] :as async]
            [clojure.set :as set]
            [konserve.memory :as memory]
            [konserve.protocols :as protocols :refer [-exists? -get-meta -get-in -assoc-in
                                                      -update-in -dissoc -bget -bassoc
                                                      -keys -multi-assoc -multi-dissoc -assoc-serializers
                                                      PEDNKeyValueStore PBinaryKeyValueStore
                                                      PKeyIterable PAssocSerializers PMultiKeySupport
                                                      PMultiKeyEDNValueStore]]
            [konserve.utils :refer [meta-update multi-key-capable? invoke-write-hooks! #?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async :refer [go-try- <?-]]
            [taoensso.timbre :refer [trace warn debug]]))

;; TODO add supervision or other mechanism to deal with stale exxceptions
;; TODO match metadata timestamps between frontend and backend

;; Write policies
(def write-policies #{:write-through :write-around})

;; Read policies
(def read-policies #{:frontend-first :frontend-only})

;; Default sync strategies
(defn populate-missing-strategy
  "Sync strategy that only adds keys missing from frontend."
  [frontend-keys backend-keys]
  (set/difference backend-keys frontend-keys))

(defn full-sync-strategy
  "Sync strategy that replaces entire frontend with backend."
  [_frontend-keys backend-keys]
  backend-keys)

;; Sync utilities
;; TODO abstract this to provide any sync between two stores
;; TODO load and write in parallel
(defn- sync-keys-to-frontend
  "Copy specified keys from backend to frontend."
  [frontend-store backend-store keys-to-sync opts]
  (async+sync (:sync? opts)
              *default-sync-translation*
              (go-try-
               (if (and (multi-key-capable? frontend-store)
                        (> (count keys-to-sync) 1))
                 ;; Use multi-assoc for efficiency
                 (let [kvs (loop [[k & r] keys-to-sync
                                  kvs {}]
                             (let [v (<?- (-get-in backend-store [k] nil opts))
                                   kvs (clojure.core/assoc kvs k v)]
                               (if-not (seq r) kvs (recur r kvs))))]
                   (<?- (-multi-assoc frontend-store kvs meta-update opts)))
                 ;; Fall back to individual operations
                 (doseq [key keys-to-sync]
                   (let [value (<?- (-get-in backend-store [key] ::not-found opts))]
                     (when (not= value ::not-found)
                       (<?- (-assoc-in frontend-store [key] (partial meta-update key :edn) value opts)))))))))

(defn perform-sync
  "Perform synchronization between frontend and backend stores."
  [frontend-store backend-store sync-strategy opts]
  (async+sync (:sync? opts)
              *default-sync-translation*
              (go-try-
               (let [backend-keys (<?- (-keys backend-store opts))
                     backend-key-set (set (map :key backend-keys))
                     frontend-keys (<?- (-keys frontend-store opts))
                     frontend-key-set (set (map :key frontend-keys))

                     ;; Determine which keys to sync
                     keys-to-sync (sync-strategy frontend-key-set backend-key-set)]

                 (debug "Sync operation" {:frontend-keys (count frontend-key-set)
                                          :backend-keys (count backend-key-set)
                                          :keys-to-sync (count keys-to-sync)})

                 (when (seq keys-to-sync)
                   (<?- (sync-keys-to-frontend frontend-store backend-store keys-to-sync opts)))

                 {:synced-keys (count keys-to-sync)
                  :frontend-keys (count frontend-key-set)
                  :backend-keys (count backend-key-set)}))))

(defn sync-on-connect
  "Optionally perform sync when connecting to store."
  [{:keys [frontend-store backend-store]} sync-strategy opts]
  (perform-sync frontend-store backend-store sync-strategy opts))

(defrecord TieredStore [frontend-store backend-store write-policy read-policy locks config]
  PEDNKeyValueStore
  (-exists? [_this key opts]
    (trace "tiered exists? on key" key)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case read-policy
                   :frontend-first
                   (let [frontend-exists? (<?- (-exists? frontend-store key opts))]
                     (if frontend-exists?
                       true
                       (<?- (-exists? backend-store key opts))))

                   :frontend-only
                   (<?- (-exists? frontend-store key opts))))))

  (-get-meta [_this key opts]
    (trace "tiered get-meta on key" key)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case read-policy
                   :frontend-first
                   (let [frontend-meta (<?- (-get-meta frontend-store key opts))]
                     (if (some? frontend-meta)
                       frontend-meta
                       (<?- (-get-meta backend-store key opts))))

                   :frontend-only
                   (<?- (-get-meta frontend-store key opts))))))

  (-get-in [_this key-vec not-found opts]
    (trace "tiered get-in on key" key-vec)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case read-policy
                   :frontend-first
                   (let [frontend-result (<?- (-get-in frontend-store key-vec ::missing opts))]
                     (if (not= frontend-result ::missing)
                       frontend-result  ;; Cache hit
                       (let [backend-result (<?- (-get-in backend-store key-vec ::missing opts))]
                         (when (not= backend-result ::missing)
                           ;; Populate frontend asynchronously (fire-and-forget)
                           (go (try
                                 (<?- (-assoc-in frontend-store key-vec (partial meta-update (first key-vec) :edn) backend-result opts))
                                 (invoke-write-hooks! frontend-store {:api-op :assoc-in
                                                                      :key (first key-vec)
                                                                      :key-vec key-vec
                                                                      :value backend-result})
                                 (catch #?(:clj Exception :cljs js/Error) e
                                   (debug "Async frontend population failed" {:key key-vec :error e})))))
                         (if (not= backend-result ::missing)
                           backend-result
                           not-found))))

                   :frontend-only
                   (<?- (-get-in frontend-store key-vec not-found opts))))))

  (-update-in [_this key-vec meta-up-fn up-fn opts]
    (trace "tiered update-in on key" key-vec)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case write-policy
                   :write-through
                   ;; Write to both stores - backend first for durability
                   (let [backend-result (<?- (-update-in backend-store key-vec meta-up-fn up-fn opts))]
                     (try
                       (<?- (-update-in frontend-store key-vec meta-up-fn up-fn opts))
                       (catch #?(:clj Exception :cljs js/Error) e
                         (warn "Frontend update failed in write-through" {:key key-vec :error e})))
                     backend-result)

                   :write-behind
                   ;; Write to backend first, then frontend asynchronously
                   (let [backend-result (<?- (-update-in backend-store key-vec meta-up-fn up-fn opts))]
                     (when-not (:skip-frontend-update? opts)
                       (go (try
                             (<?- (-update-in frontend-store key-vec meta-up-fn up-fn opts))
                             (catch #?(:clj Exception :cljs js/Error) e
                               (warn "Async frontend update failed" {:key key-vec :error e})))))
                     backend-result)

                   :write-around
                   ;; Write only to backend, invalidate frontend
                   (let [result (<?- (-update-in backend-store key-vec meta-up-fn up-fn opts))]
                     (go (try
                           (<?- (-dissoc frontend-store (first key-vec) opts))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (warn "Frontend invalidation failed" {:key (first key-vec) :error e}))))
                     result)))))

  (-assoc-in [_this key-vec meta-up-fn val opts]
    (trace "tiered assoc-in on key" key-vec)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case write-policy
                   :write-through
                   (let [backend-result (<?- (-assoc-in backend-store key-vec meta-up-fn val opts))]
                     (try
                       (<?- (-assoc-in frontend-store key-vec meta-up-fn val opts))
                       (catch #?(:clj Exception :cljs js/Error) e
                         (warn "Frontend assoc failed in write-through" {:key key-vec :error e})))
                     backend-result)

                   :write-behind
                   (let [backend-result (<?- (-assoc-in backend-store key-vec meta-up-fn val opts))]
                     (when-not (:skip-frontend-update? opts)
                       (go (try
                             (<?- (-assoc-in frontend-store key-vec meta-up-fn val opts))
                             (catch #?(:clj Exception :cljs js/Error) e
                               (warn "Async frontend assoc failed" {:key key-vec :error e})))))
                     backend-result)

                   :write-around
                   (let [result (<?- (-assoc-in backend-store key-vec meta-up-fn val opts))]
                     (go (try
                           (<?- (-dissoc frontend-store (first key-vec) opts))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (warn "Frontend invalidation failed" {:key (first key-vec) :error e}))))
                     result)))))

  (-dissoc [_this key opts]
    (trace "tiered dissoc on key" key)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 ;; Always remove from both stores
                 (let [backend-result (-dissoc backend-store key opts)
                       frontend-result (-dissoc frontend-store key opts)]
                   (<?- frontend-result)
                   (<?- backend-result)))))

  PBinaryKeyValueStore
  (-bget [_this key locked-cb opts]
    (trace "tiered bget on key" key)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case read-policy
                   :frontend-first
                   (if (<?- (-exists? frontend-store key opts))
                     (<?- (-bget frontend-store key locked-cb opts))
                     (<?- (-bget backend-store key locked-cb opts)))

                   :frontend-only
                   (<?- (-bget frontend-store key locked-cb opts))))))

  (-bassoc [_this key meta-up-fn val opts]
    (trace "tiered bassoc on key" key)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case write-policy
                   :write-through
                   (let [backend-result (<?- (-bassoc backend-store key meta-up-fn val opts))]
                     (try
                       (<?- (-bassoc frontend-store key meta-up-fn val opts))
                       (catch #?(:clj Exception :cljs js/Error) e
                         (warn "Frontend bassoc failed in write-through" {:key key :error e})))
                     backend-result)

                   :write-behind
                   (let [backend-result (<?- (-bassoc backend-store key meta-up-fn val opts))]
                     (when-not (:skip-frontend-update? opts)
                       (go (try
                             (<?- (-bassoc frontend-store key meta-up-fn val opts))
                             (catch #?(:clj Exception :cljs js/Error) e
                               (warn "Async frontend bassoc failed" {:key key :error e})))))
                     backend-result)

                   :write-around
                   (let [result (<?- (-bassoc backend-store key meta-up-fn val opts))]
                     (go (try
                           (<?- (-dissoc frontend-store key opts))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (warn "Frontend invalidation failed" {:key key :error e}))))
                     result)))))

  PAssocSerializers
  (-assoc-serializers [this serializers]
    (clojure.core/assoc this
                        :frontend-store (-assoc-serializers (:frontend-store this) serializers)
                        :backend-store  (-assoc-serializers (:backend-store  this) serializers)))

  PKeyIterable
  (-keys [_this opts]
    (trace "tiered keys")
    ;; Always get keys from backend (source of truth)
    (-keys backend-store opts))

  PMultiKeySupport
  (-supports-multi-key? [_this]
    ;; Only support multi-key if both stores support it
    (and (multi-key-capable? frontend-store)
         (multi-key-capable? backend-store)))

  PMultiKeyEDNValueStore
  (-multi-assoc [_this kvs meta-up-fn opts]
    (trace "tiered multi-assoc operation with" (count kvs) "keys")
    (when-not (and (multi-key-capable? frontend-store)
                   (multi-key-capable? backend-store))
      (throw (ex-info "Both stores must support multi-key operations for tiered multi-assoc"
                      {:frontend-supports (multi-key-capable? frontend-store)
                       :backend-supports (multi-key-capable? backend-store)})))
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case write-policy
                   :write-through
                   (let [backend-result (<?- (-multi-assoc backend-store kvs meta-up-fn opts))]
                     (try
                       (<?- (-multi-assoc frontend-store kvs meta-up-fn opts))
                       (catch #?(:clj Exception :cljs js/Error) e
                         (warn "Frontend multi-assoc failed in write-through" {:kvs-keys (clojure.core/keys kvs) :error e})))
                     backend-result)

                   :write-around
                   (let [result (<?- (-multi-assoc backend-store kvs meta-up-fn opts))]
                     ;; Invalidate all affected keys from frontend
                     (go (try
                           (doseq [k (clojure.core/keys kvs)]
                             (<?- (-dissoc frontend-store k opts)))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (warn "Frontend invalidation failed" {:kvs-keys (clojure.core/keys kvs) :error e}))))
                     result)))))

  (-multi-dissoc [_this keys-to-remove opts]
                 (trace "tiered multi-dissoc operation with" (count keys-to-remove) "keys")
                 (when-not (and (multi-key-capable? frontend-store)
                                (multi-key-capable? backend-store))
                   (throw (ex-info "Both stores must support multi-key operations for tiered multi-dissoc"
                                   {:frontend-supports (multi-key-capable? frontend-store)
                                    :backend-supports (multi-key-capable? backend-store)})))
                 (async+sync (:sync? opts)
                             *default-sync-translation*
                             (go-try-
                              (let [backend-result (<?- (-multi-dissoc backend-store keys-to-remove opts))]
                                (try
                                  (<?- (-multi-dissoc frontend-store keys-to-remove opts))
                                  (catch #?(:clj Exception :cljs js/Error) e
                                    (warn "Frontend multi-dissoc failed" {:keys keys-to-remove :error e})))
                                backend-result)))))

;; Constructor function following konserve patterns
(defn connect-tiered-store
  "Create a tiered store with frontend and backend stores.

   The backend store is the authoritative source of truth for durability.
   The frontend store acts as a performance cache layer.

   Options:
   - :write-policy      #{:write-through :write-around} (default :write-through)
   - :read-policy       #{:frontend-first :frontend-only} (default :frontend-first)
   - :sync?             Boolean for synchronous/asynchronous operation (default false)

   Write policies:
   - :write-through  Write to backend, then frontend synchronously
   - :write-around   Write only to backend, invalidate frontend

   Read policies:
   - :frontend-first Check frontend first, fallback to backend (populates frontend)
   - :frontend-only  Only read from frontend."
  [frontend-store backend-store & {:keys [write-policy read-policy opts]
                                   :or {write-policy :write-through
                                        read-policy :frontend-first
                                        opts {:sync? false}}
                                   :as params}]
  (when-not (contains? write-policies write-policy)
    (throw (ex-info "Invalid write policy" {:provided write-policy :valid write-policies})))
  (when-not (contains? read-policies read-policy)
    (throw (ex-info "Invalid read policy" {:provided read-policy :valid read-policies})))

  (let [store (map->TieredStore
               {:frontend-store frontend-store
                :backend-store backend-store
                :write-policy write-policy
                :read-policy read-policy
                :locks (atom {})
                :config params})]
    (if (:sync? opts) store (go store))))
