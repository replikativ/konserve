(ns konserve.tiered
  "Tiered store implementation with frontend and backend storage layers."
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [clojure.core.async :refer [go] :as async]
            [clojure.set :as set]
            [konserve.memory :as memory]
            [konserve.protocols :as protocols :refer [-exists? -get-meta -get-in -assoc-in
                                                      -update-in -dissoc -bget -bassoc
                                                      -keys -multi-get -multi-assoc -multi-dissoc -assoc-serializers
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
(def write-policies #{:write-through :write-behind :write-around})

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
                        (multi-key-capable? backend-store)
                        (> (count keys-to-sync) 1))
                 ;; Use multi-get + multi-assoc for maximum efficiency
                 (let [kvs (<?- (-multi-get backend-store keys-to-sync opts))]
                   (when (seq kvs)
                     (<?- (-multi-assoc frontend-store kvs meta-update opts))))
                 ;; Fall back to individual operations - use loop for proper async waiting
                 (loop [remaining-keys (seq keys-to-sync)]
                   (when remaining-keys
                     (let [key (first remaining-keys)
                           value (<?- (-get-in backend-store [key] ::not-found opts))]
                       (when (not= value ::not-found)
                         (<?- (-assoc-in frontend-store [key] (partial meta-update key :edn) value opts)))
                       (recur (next remaining-keys)))))))))

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

(defn sync-keys-to-frontend!
  "Public API for syncing specific keys from backend to frontend.
   Used by walk-based sync strategies that discover keys externally."
  [frontend-store backend-store keys-to-sync opts]
  (sync-keys-to-frontend frontend-store backend-store keys-to-sync opts))

(defn perform-walk-sync
  "Sync by walking from root key(s) and discovering reachable keys.

   Arguments:
   - frontend-store: The frontend (fast) store to sync to
   - backend-store: The backend (durable) store to sync from
   - root-keys: Collection of root keys to fetch and walk from
   - walk-fn: (fn [backend-store root-values opts] -> channel-of-keys)
              Given the root values, discovers all reachable keys asynchronously.
              Should return a core.async channel that yields the set of keys to sync.
   - opts: Options map, :sync? should be false for async backends

   Returns a channel with {:synced-keys count :root-keys count}

   This is useful for tree-structured data where you want to sync only
   reachable nodes rather than all keys in the store."
  [frontend-store backend-store root-keys walk-fn opts]
  (async+sync (:sync? opts)
              *default-sync-translation*
              (go-try-
               (let [;; 1. Fetch all root values from backend
                     root-values (loop [keys (seq root-keys)
                                        values {}]
                                   (if-not keys
                                     values
                                     (let [k (first keys)
                                           v (<?- (-get-in backend-store [k] nil opts))]
                                       (recur (next keys)
                                              (if v (clojure.core/assoc values k v) values)))))

                     ;; 2. Walk to discover reachable keys
                     reachable-keys (if (seq root-values)
                                      (<?- (walk-fn backend-store root-values opts))
                                      #{})

                     ;; 3. Combine root keys with reachable keys
                     all-keys-to-sync (into (set root-keys) reachable-keys)]

                 (when (seq all-keys-to-sync)
                   (<?- (sync-keys-to-frontend frontend-store backend-store all-keys-to-sync opts)))

                 {:synced-keys (count all-keys-to-sync)
                  :root-keys (count root-keys)
                  :reachable-keys (count reachable-keys)}))))

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
                   ;; Write to frontend first, then backend asynchronously (standard write-behind)
                   (let [frontend-result (<?- (-update-in frontend-store key-vec meta-up-fn up-fn opts))]
                     (go (try
                           (<?- (-update-in backend-store key-vec meta-up-fn up-fn opts))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (warn "Async backend update failed in write-behind" {:key key-vec :error e}))))
                     frontend-result)

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
                   ;; Write to frontend first, then backend asynchronously (standard write-behind)
                   (let [frontend-result (<?- (-assoc-in frontend-store key-vec meta-up-fn val opts))]
                     (go (try
                           (<?- (-assoc-in backend-store key-vec meta-up-fn val opts))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (warn "Async backend assoc failed in write-behind" {:key key-vec :error e}))))
                     frontend-result)

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
                   ;; Write to frontend first, then backend asynchronously (standard write-behind)
                   (let [frontend-result (<?- (-bassoc frontend-store key meta-up-fn val opts))]
                     (go (try
                           (<?- (-bassoc backend-store key meta-up-fn val opts))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (warn "Async backend bassoc failed in write-behind" {:key key :error e}))))
                     frontend-result)

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
    (trace "tiered keys, read-policy:" read-policy)
    ;; Respect read-policy: frontend-only returns frontend keys only
    ;; This is critical for performance when frontend has subset of backend keys
    (case read-policy
      :frontend-only (-keys frontend-store opts)
      :frontend-first (-keys backend-store opts)))

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

                   :write-behind
                   ;; Write to frontend first, then backend asynchronously (standard write-behind)
                   (let [frontend-result (<?- (-multi-assoc frontend-store kvs meta-up-fn opts))]
                     (go (try
                           (<?- (-multi-assoc backend-store kvs meta-up-fn opts))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (warn "Async backend multi-assoc failed in write-behind" {:kvs-keys (clojure.core/keys kvs) :error e}))))
                     frontend-result)

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
                   backend-result))))

  (-multi-get [_this keys opts]
    (trace "tiered multi-get operation with" (count keys) "keys")
    (when-not (and (multi-key-capable? frontend-store)
                   (multi-key-capable? backend-store))
      (throw (ex-info "Both stores must support multi-key operations for tiered multi-get"
                      {:frontend-supports (multi-key-capable? frontend-store)
                       :backend-supports (multi-key-capable? backend-store)})))
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case read-policy
                   :frontend-first
                   (let [frontend-result (<?- (-multi-get frontend-store keys opts))
                         ;; Find keys that were not in frontend (sparse map)
                         missing-keys (remove (set (clojure.core/keys frontend-result)) keys)]
                     (if (seq missing-keys)
                       ;; Some keys missing from frontend, fetch from backend
                       (let [backend-result (<?- (-multi-get backend-store missing-keys opts))]
                         ;; Populate frontend asynchronously with found backend values (fire-and-forget)
                         (when (seq backend-result)
                           (go (try
                                 (<?- (-multi-assoc frontend-store backend-result meta-update opts))
                                 (invoke-write-hooks! frontend-store {:api-op :multi-assoc
                                                                      :kvs backend-result})
                                 (catch #?(:clj Exception :cljs js/Error) e
                                   (debug "Async frontend population failed" {:keys (clojure.core/keys backend-result) :error e})))))
                         ;; Merge frontend and backend results
                         (merge frontend-result backend-result))
                       ;; All keys found in frontend
                       frontend-result))

                   :frontend-only
                   (<?- (-multi-get frontend-store keys opts)))))))

;; Constructor function following konserve patterns
(defn connect-tiered-store
  "Create a tiered store with frontend and backend stores.

   The backend store is the authoritative source of truth for durability.
   The frontend store acts as a performance cache layer.

   Options:
   - :write-policy      #{:write-through :write-behind :write-around} (default :write-through)
   - :read-policy       #{:frontend-first :frontend-only} (default :frontend-first)
   - :sync?             Boolean for synchronous/asynchronous operation (default false)

   Write policies:
   - :write-through  Write to backend, then frontend synchronously (strong consistency)
   - :write-behind   Write to frontend first, backend asynchronously (low latency, eventual durability)
   - :write-around   Write only to backend, invalidate frontend (bypass cache)

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
