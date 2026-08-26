(ns konserve.tiered
  "Tiered store implementation with frontend and backend storage layers."
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [clojure.core.async :refer [go] :as async]
            #?(:clj [konserve.nio-helpers :as nio])
            [clojure.set :as set]
            [konserve.memory :as memory]
            [konserve.protocols :as protocols :refer [PConditionalWrite -conditional-write-domain -revision
                                                      -exists? -get-meta -get-in -assoc-in
                                                      -update-in -dissoc -bget -bassoc
                                                      -keys -multi-get -multi-assoc -multi-dissoc -assoc-serializers
                                                      PEDNKeyValueStore PBinaryKeyValueStore
                                                      PKeyIterable PAssocSerializers PMultiKeySupport
                                                      PMultiKeyEDNValueStore]]
            [konserve.utils :refer [meta-update multi-key-capable? kv-keys invoke-write-hooks! #?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async :refer [go-try- <?-]]
            [replikativ.logging :as log]))

;; TODO add supervision or other mechanism to deal with stale exxceptions
;; TODO match metadata timestamps between frontend and backend

;; Write policies
(def write-policies #{:write-through :write-behind :write-around :frontend-only})
;; :frontend-only — a read-through CACHE over a read-only backend. Writes (and
;; deletes) go to the FRONTEND only; the backend is never mutated by this peer.
;; Combine with read-policy :frontend-first so reads still fall through to the
;; backend on a miss. `-keys` reports the frontend, so this store enumerates and
;; syncs as its local cache (e.g. a konserve-sync subscriber warming an LMDB
;; frontend over a shared, writer-owned S3 backend it must not write to).

;; Read policies
(def read-policies #{:frontend-first :frontend-only})

(def ^:private core-dissoc
  "`dissoc` from the host core, which this namespace shadows with the store op."
  #?(:clj clojure.core/dissoc :cljs cljs.core/dissoc))

(def ^:private write-behind-receipt-key ::write-behind-receipt)

(defn with-write-behind-receipt
  "Add an observable backend-completion receipt to a tiered write.

   Returns `{:opts opts' :receipt ch}`. Pass `opts'` to one write on a tiered
   store configured with `:write-policy :write-behind`. After that write has
   returned successfully, `ch` delivers one outcome:

   * `{:status :succeeded :result backend-result}` after the backend accepted
     the write.
   * `{:status :failed :error error}` when the asynchronous backend write
     failed.

   For example, a caller can await durability outside its latency-sensitive
   path before publishing a reference to the value:

   ```clojure
   (let [{:keys [opts receipt]} (with-write-behind-receipt)]
     (<! (konserve.core/bassoc store key bytes opts))
     (when (= :succeeded (:status (<! receipt)))
       (publish-reference! key)))
   ```

   The ordinary write still returns as soon as the frontend write completes;
   awaiting the receipt therefore does not add backend latency to the write's
   hot path. If the frontend write itself fails, the ordinary write reports
   that failure and no backend operation (or receipt outcome) exists.

   Receipts apply to the operations that are actually asynchronous under
   `:write-behind`: `assoc`, `assoc-in`, `update`, `update-in`, `bassoc`, and
   `multi-assoc`. A receipt must be used for exactly one write. Passing the
   returned opts to another write policy is an error.

   Arguments:
   - `opts`: The ordinary Konserve operation options map.

   Returns a map containing the augmented options and a core.async promise
   channel."
  ([]
   (with-write-behind-receipt {:sync? false}))
  ([opts]
   (let [receipt (async/promise-chan)]
     {:opts (clojure.core/assoc opts write-behind-receipt-key receipt)
      :receipt receipt})))

(defn- receipt [opts]
  (clojure.core/get opts write-behind-receipt-key))

(defn- store-opts [opts]
  (core-dissoc opts write-behind-receipt-key))

(defn- write-behind-opts [opts]
  ;; The backend operation runs in `go` even when the caller selected Konserve's
  ;; synchronous API for the frontend operation.
  (clojure.core/assoc (store-opts opts) :sync? false))

(defn- deliver-receipt! [receipt outcome]
  (when receipt
    (async/put! receipt outcome (fn [_] (async/close! receipt)))))

(defn- start-write-behind! [operation log-data receipt backend-write]
  (go
    (try
      (let [result (<?- (backend-write))]
        (deliver-receipt! receipt {:status :succeeded
                                   :result result}))
      (catch #?(:clj Exception :cljs js/Error) e
        (log/warn :konserve/tiered-backend-write-failed
                  (clojure.core/assoc log-data :operation operation :error e))
        (deliver-receipt! receipt {:status :failed
                                   :error e})))))

(defn- check-receipt-policy! [write-policy opts]
  (when (and (receipt opts) (not= :write-behind write-policy))
    (throw (ex-info "Write-behind receipts require :write-policy :write-behind."
                    {:type :konserve/write-behind-receipt-unsupported
                     :write-policy write-policy}))))

(defn owns-backend?
  "Does a tiered store with this write-policy OWN its backend, or is it merely a CACHE over
   a backend that another peer owns?

   `:frontend-only` is a read-through cache: the backend is read-only to this peer and must
   never be mutated by it (see the write-policy note above). Deleting is the most destructive
   mutation there is — a cache peer deleting the shared backend would take the authoritative
   data down with it — so `delete-store` on such a store removes only its own cache. Under
   every other policy the store owns both tiers and deleting it deletes both."
  [write-policy]
  (not= :frontend-only write-policy))

(def persistent-frontend-backends
  "Frontend backends with durable storage, i.e. something to delete. A `:memory` frontend is
   ephemeral: it goes away with the process and has no store to remove."
  #{:file :indexeddb :lmdb :rocksdb})

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

                 (log/debug :konserve/tiered-sync {:frontend-keys (count frontend-key-set)
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

(defn ^:private frontend-opts
  "`opts` with the fencing options removed, for a call to the FRONTEND store.

   A revision belongs to the store that minted it, and the two tiers mint their
   own. Forwarding `:expected-revision` made the frontend reject a write the
   backend had just accepted; the rejection was caught and logged, the caller was
   told the fenced write succeeded, and every later `:frontend-first` read
   returned the PRE-WRITE value indefinitely — using the fence created the
   incoherence it exists to prevent. `:with-revision?` goes too: its result shape
   is the caller's, and the frontend's token would be meaningless to them.

   The BACKEND keeps the caller's opts: it is the store whose revisions
   `-revision` reports and whose domain `-conditional-write-domain` advertises."
  [opts]
  ;; NOT `(dissoc opts ...)`: this namespace shadows `clojure.core/dissoc` with
  ;; the store operation, and `clojure.core/dissoc` cannot be written out in a
  ;; .cljc file because there it is `cljs.core/dissoc`.
  (core-dissoc opts :expected-revision :with-revision? write-behind-receipt-key))

(defn ^:private revision-sensitive?
  "Does this write participate in the backend's revision stream?

   `:expected-revision` is the conditional half; `:with-revision?` asks for the
   revision produced by an otherwise unconditional write so the caller can chain
   the next one. In both cases the backend is authoritative and the frontend must
   not independently replay the operation."
  [opts]
  (or (contains? opts :expected-revision)
      (:with-revision? opts)))

(defn ^:private make-cache-lock []
  (let [lock (async/chan 1)]
    (async/put! lock :unlocked)
    lock))

(defn ^:private take-cache-lock-sync! [cache-lock]
  #?(:clj
     (async/<!! cache-lock)
     :cljs
     (or (async/poll! cache-lock)
         (throw (ex-info "A synchronous tiered cache operation contended with an asynchronous one."
                         {:type :konserve/tiered-sync-cache-contention})))))

(defn ^:private populate-frontend
  "Run a read-through cache fill unless a backend write completed since its read.

   The generation check and the frontend mutation share `cache-lock` with
   post-write invalidation. Without both, a fire-and-forget fill can land after
   a newer backend write invalidated the frontend and resurrect the stale value."
  [cache-lock cache-generation observed-generation populate-fn opts]
  (if (:sync? opts)
    (try
      (take-cache-lock-sync! cache-lock)
      (if (= observed-generation @cache-generation)
        (do
          (populate-fn)
          true)
        false)
      (finally
        (async/put! cache-lock :unlocked)))
    (go-try-
     (try
       (<?- cache-lock)
       (if (= observed-generation @cache-generation)
         (do
           (<?- (populate-fn))
           true)
         false)
       (finally
         (async/put! cache-lock :unlocked))))))

(defn- complete-read-through
  "Populate one frontend read and run its write hook.

   Kept as an async+sync operation so a caller that needs a COMPLETE cache can
   await exactly the same generation-fenced fill that ordinary reads launch in
   the background. In a nested tier, `opts` reaches the backend read first, so
   `:await-read-through?` fills deepest-to-outermost."
  [frontend-store cache-lock cache-generation observed-generation
   populate-fn hook-event opts]
  (async+sync (:sync? opts)
              *default-sync-translation*
              (go-try-
               (when (<?- (populate-frontend cache-lock cache-generation
                                             observed-generation populate-fn opts))
                 (invoke-write-hooks! frontend-store hook-event)))))

(defn- start-read-through!
  "Await a cache fill when requested; otherwise preserve fire-and-forget reads."
  [frontend-store cache-lock cache-generation observed-generation
   populate-fn hook-event opts]
  (let [fill #(complete-read-through frontend-store cache-lock cache-generation
                                     observed-generation populate-fn hook-event opts)]
    (if (:await-read-through? opts)
      (fill)
      (do
        (go
          (try
            (<?- (fill))
            (catch #?(:clj Exception :cljs js/Error) e
              (log/debug :konserve/tiered-frontend-populate-failed
                         {:event hook-event :error e}))))
        ;; The caller uniformly awaits this function. Deliver an immediate
        ;; completion in ordinary mode while the detached fill continues.
        (async+sync (:sync? opts) *default-sync-translation* (go-try- true))))))

(defn ^:private update-frontend-after-backend
  "Order a frontend mutation after all older read-through cache fills."
  [cache-lock cache-generation update-fn opts]
  (if (:sync? opts)
    (try
      (take-cache-lock-sync! cache-lock)
      ;; Invalidate every fill that observed the backend before this write
      ;; committed, including fills still waiting for the lock.
      (swap! cache-generation inc)
      (update-fn)
      (finally
        (async/put! cache-lock :unlocked)))
    (go-try-
     (try
       (<?- cache-lock)
       (swap! cache-generation inc)
       (<?- (update-fn))
       (finally
         (async/put! cache-lock :unlocked))))))

(defn ^:private invalidate-frontend
  "Synchronously evict `key` from the frontend after the backend committed.

   A failed eviction is not a harmless cache error: a `:frontend-first` read
   could otherwise return the pre-write value indefinitely after the caller was
   told its durable write succeeded. Raise an explicit PARTIAL outcome instead
   of claiming coherence; the backend write has already landed."
  [frontend-store cache-lock cache-generation key opts]
  (async+sync (:sync? opts)
              *default-sync-translation*
              (go-try-
               (try
                 (<?- (update-frontend-after-backend
                       cache-lock cache-generation
                       #(-dissoc frontend-store key (frontend-opts opts))
                       opts))
                 (catch #?(:clj Exception :cljs js/Error) e
                   (throw (ex-info "The backend write committed, but the tiered frontend could not be invalidated."
                                   {:type               :konserve/tiered-cache-invalidation-failed
                                    :key                key
                                    :backend-committed? true}
                                   e)))))))

(defrecord TieredStore [frontend-store backend-store write-policy read-policy locks cache-lock cache-generation config]
  PConditionalWrite
  ;; Only `:write-through` can honour this, and only if the backend can.
  ;;
  ;; `:write-behind` cannot, for a reason no implementation effort would fix: it
  ;; returns to the caller once the FRONTEND has the value and writes the backend
  ;; in a `go` afterwards, so a rejected conditional write would be discovered
  ;; after the caller was told it succeeded. There is nobody left to report it to.
  ;;
  ;; `:frontend-only` is a read-through cache over a backend another peer owns; it
  ;; must not mutate it at all, let alone fence it.
  (-conditional-write-domain [_]
    ;; The BACKEND's domain, because the backend is what decides the write. A
    ;; memory frontend over an S3 backend is :global; reporting the frontend's
    ;; :process would be exactly backwards.
    (when (= :write-through write-policy)
      (when (satisfies? PConditionalWrite backend-store)
        (-conditional-write-domain backend-store))))
  ;; ALWAYS the backend, never the read-policy's choice. The two tiers keep
  ;; INDEPENDENT revision counters — they are separate stores — so a revision read
  ;; from the frontend and compared against the backend compares two unrelated
  ;; numbers. That would fail open or closed at random, which is worse than having
  ;; no fencing, because it looks like fencing.
  (-revision [_ key opts] (-revision backend-store key opts))

  PEDNKeyValueStore
  (-exists? [_this key opts]
    (log/trace :konserve/tiered-exists? {:key key})
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
    (log/trace :konserve/tiered-get-meta {:key key})
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
    (log/trace :konserve/tiered-get-in {:key-vec key-vec})
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (if (:with-revision? opts)
                   ;; STRAIGHT TO THE BACKEND, whatever the read-policy says. The
                   ;; revision must come from the tier that will EVALUATE the
                   ;; conditional write, and that is always the backend. Serving it
                   ;; from the frontend returns a token the backend has never seen —
                   ;; and because the frontend is usually a memory store, the value
                   ;; came back bare, so the caller fenced on nil and got an
                   ;; UNCONDITIONAL write from a store advertising :machine. A cache
                   ;; hit must not decide the identity of a durable value.
                   (<?- (-get-in backend-store key-vec not-found opts))
                   (case read-policy
                     :frontend-first
                     (let [frontend-result (<?- (-get-in frontend-store key-vec ::missing opts))]
                       (if (not= frontend-result ::missing)
                         frontend-result  ;; Cache hit
                         (let [observed-generation @cache-generation
                               backend-result (<?- (-get-in backend-store key-vec ::missing opts))]
                           (when (not= backend-result ::missing)
                             (<?- (start-read-through!
                                   frontend-store cache-lock cache-generation observed-generation
                                   #(-assoc-in frontend-store key-vec
                                               (partial meta-update (first key-vec) :edn)
                                               backend-result (frontend-opts opts))
                                   {:api-op :assoc-in
                                    :key (first key-vec)
                                    :key-vec key-vec
                                    :value backend-result}
                                   opts)))
                           (if (not= backend-result ::missing)
                             backend-result
                             not-found))))

                     :frontend-only
                     (<?- (-get-in frontend-store key-vec not-found opts)))))))

  (-update-in [_this key-vec meta-up-fn up-fn opts]
    (log/trace :konserve/tiered-update-in {:key-vec key-vec})
    (check-receipt-policy! write-policy opts)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case write-policy
                   :write-through
                   ;; Write to both stores - backend first for durability
                   (let [backend-result (<?- (-update-in backend-store key-vec meta-up-fn up-fn opts))]
                     (if (revision-sensitive? opts)
                       ;; The backend evaluated `up-fn` against the value whose
                       ;; revision the caller supplied. Re-running it against a
                       ;; stale frontend can compute a DIFFERENT value (and runs
                       ;; caller code twice). Evict the whole blob; the next read
                       ;; refills it from the authoritative backend.
                       (<?- (invalidate-frontend frontend-store cache-lock cache-generation (first key-vec) opts))
                       (try
                         (<?- (update-frontend-after-backend
                               cache-lock cache-generation
                               #(-update-in frontend-store key-vec meta-up-fn up-fn (frontend-opts opts))
                               opts))
                         (catch #?(:clj Exception :cljs js/Error) e
                           (log/warn :konserve/tiered-frontend-update-failed {:key key-vec :error e})
                           (<?- (invalidate-frontend frontend-store cache-lock cache-generation (first key-vec) opts)))))
                     backend-result)

                   :write-behind
                   ;; Write to frontend first, then backend asynchronously (standard write-behind)
                   (let [frontend-result (<?- (-update-in frontend-store key-vec meta-up-fn up-fn (frontend-opts opts)))]
                     (start-write-behind!
                      :update-in {:key key-vec} (receipt opts)
                      #(-update-in backend-store key-vec meta-up-fn up-fn (write-behind-opts opts)))
                     frontend-result)

                   :write-around
                   ;; Write only to backend, invalidate frontend
                   (let [result (<?- (-update-in backend-store key-vec meta-up-fn up-fn opts))]
                     (go (try
                           (<?- (-dissoc frontend-store (first key-vec) (frontend-opts opts)))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (log/warn :konserve/tiered-frontend-invalidation-failed {:key (first key-vec) :error e}))))
                     result)

                   :frontend-only
                   ;; Cache mode: write to the frontend only; never touch the backend.
                   (<?- (-update-in frontend-store key-vec meta-up-fn up-fn (frontend-opts opts)))))))

  (-assoc-in [_this key-vec meta-up-fn val opts]
    (log/trace :konserve/tiered-assoc-in {:key-vec key-vec})
    (check-receipt-policy! write-policy opts)
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (case write-policy
                   :write-through
                   (let [backend-result (<?- (-assoc-in backend-store key-vec meta-up-fn val opts))]
                     (if (revision-sensitive? opts)
                       ;; The frontend is a cache, not a second participant in the
                       ;; backend's revision stream. Eviction is also correct for a
                       ;; nested assoc: replaying it against a stale outer value
                       ;; would preserve fields the backend no longer has.
                       (<?- (invalidate-frontend frontend-store cache-lock cache-generation (first key-vec) opts))
                       (try
                         (<?- (update-frontend-after-backend
                               cache-lock cache-generation
                               #(-assoc-in frontend-store key-vec meta-up-fn val (frontend-opts opts))
                               opts))
                         (catch #?(:clj Exception :cljs js/Error) e
                           (log/warn :konserve/tiered-frontend-assoc-failed {:key key-vec :error e})
                           (<?- (invalidate-frontend frontend-store cache-lock cache-generation (first key-vec) opts)))))
                     backend-result)

                   :write-behind
                   ;; Write to frontend first, then backend asynchronously (standard write-behind)
                   (let [frontend-result (<?- (-assoc-in frontend-store key-vec meta-up-fn val (frontend-opts opts)))]
                     (start-write-behind!
                      :assoc-in {:key key-vec} (receipt opts)
                      #(-assoc-in backend-store key-vec meta-up-fn val (write-behind-opts opts)))
                     frontend-result)

                   :write-around
                   (let [result (<?- (-assoc-in backend-store key-vec meta-up-fn val opts))]
                     (go (try
                           (<?- (-dissoc frontend-store (first key-vec) (frontend-opts opts)))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (log/warn :konserve/tiered-frontend-invalidation-failed {:key (first key-vec) :error e}))))
                     result)

                   :frontend-only
                   (<?- (-assoc-in frontend-store key-vec meta-up-fn val (frontend-opts opts)))))))

  (-dissoc [_this key opts]
    (log/trace :konserve/tiered-dissoc {:key key})
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (if (= write-policy :frontend-only)
                   ;; Cache mode: delete from the frontend only; never touch the backend.
                   (<?- (-dissoc frontend-store key opts))
                   ;; Otherwise remove from both stores
                   (let [backend-result (-dissoc backend-store key opts)
                         frontend-result (-dissoc frontend-store key opts)]
                     (<?- frontend-result)
                     (<?- backend-result))))))

  PBinaryKeyValueStore
  (-bget [_this key locked-cb opts]
    (log/trace :konserve/tiered-bget {:key key})
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
    (log/trace :konserve/tiered-bassoc {:key key})
    (check-receipt-policy! write-policy opts)
    ;; Materialize ONCE, before fanning out. Both tiers receive the same `val`,
    ;; and an InputStream is exhausted by whichever writes first — the second
    ;; tier would then store nothing, silently. `bassoc` documents streams as
    ;; input, so this is the only place that can honour that for a two-tier
    ;; write.
    (let [val #?(:clj (nio/blob->bytes val) :cljs val)]
      (async+sync (:sync? opts)
                  *default-sync-translation*
                  (go-try-
                   (case write-policy
                     :write-through
                     (let [backend-result (<?- (-bassoc backend-store key meta-up-fn val opts))]
                       (try
                         (<?- (-bassoc frontend-store key meta-up-fn val opts))
                         (catch #?(:clj Exception :cljs js/Error) e
                           (log/warn :konserve/tiered-frontend-bassoc-failed {:key key :error e})))
                       backend-result)

                     :write-behind
                   ;; Write to frontend first, then backend asynchronously (standard write-behind)
                     (let [frontend-result (<?- (-bassoc frontend-store key meta-up-fn val (frontend-opts opts)))]
                       (start-write-behind!
                        :bassoc {:key key} (receipt opts)
                        #(-bassoc backend-store key meta-up-fn val (write-behind-opts opts)))
                       frontend-result)

                     :write-around
                     (let [result (<?- (-bassoc backend-store key meta-up-fn val opts))]
                       (go (try
                             (<?- (-dissoc frontend-store key opts))
                             (catch #?(:clj Exception :cljs js/Error) e
                               (log/warn :konserve/tiered-frontend-invalidation-failed {:key key :error e}))))
                       result)

                     :frontend-only
                     (<?- (-bassoc frontend-store key meta-up-fn val opts)))))))

  PAssocSerializers
  (-assoc-serializers [this serializers]
    (clojure.core/assoc this
                        :frontend-store (-assoc-serializers (:frontend-store this) serializers)
                        :backend-store  (-assoc-serializers (:backend-store  this) serializers)))

  PKeyIterable
  (-keys [_this opts]
    (log/trace :konserve/tiered-keys {:read-policy read-policy :write-policy write-policy})
    ;; A :frontend-only WRITE store (cache mode) enumerates/syncs as its local cache,
    ;; so `-keys` reports the frontend even though reads fall through to the backend.
    ;; Otherwise respect read-policy (frontend-only reads => frontend keys).
    (if (or (= write-policy :frontend-only) (= read-policy :frontend-only))
      (-keys frontend-store opts)
      (-keys backend-store opts)))

  PMultiKeySupport
  (-supports-multi-key? [_this]
    ;; Only support multi-key if both stores support it
    (and (multi-key-capable? frontend-store)
         (multi-key-capable? backend-store)))

  PMultiKeyEDNValueStore
  (-multi-assoc [_this kvs meta-up-fn opts]
    (log/trace :konserve/tiered-multi-assoc {:key-count (count kvs)})
    (check-receipt-policy! write-policy opts)
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
                         (log/warn :konserve/tiered-frontend-multi-assoc-failed {:kvs-keys (kv-keys kvs) :error e})))
                     backend-result)

                   :write-behind
                   ;; Write to frontend first, then backend asynchronously (standard write-behind)
                   (let [frontend-result (<?- (-multi-assoc frontend-store kvs meta-up-fn (frontend-opts opts)))]
                     (start-write-behind!
                      :multi-assoc {:kvs-keys (kv-keys kvs)} (receipt opts)
                      #(-multi-assoc backend-store kvs meta-up-fn (write-behind-opts opts)))
                     frontend-result)

                   :write-around
                   (let [result (<?- (-multi-assoc backend-store kvs meta-up-fn opts))]
                     ;; Invalidate all affected keys from frontend
                     (go (try
                           (doseq [k (kv-keys kvs)]
                             (<?- (-dissoc frontend-store k opts)))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (log/warn :konserve/tiered-frontend-invalidation-failed {:kvs-keys (kv-keys kvs) :error e}))))
                     result)

                   :frontend-only
                   (<?- (-multi-assoc frontend-store kvs meta-up-fn opts))))))

  (-multi-dissoc [_this keys-to-remove opts]
    (log/trace :konserve/tiered-multi-dissoc {:key-count (count keys-to-remove)})
    (when-not (and (multi-key-capable? frontend-store)
                   (multi-key-capable? backend-store))
      (throw (ex-info "Both stores must support multi-key operations for tiered multi-dissoc"
                      {:frontend-supports (multi-key-capable? frontend-store)
                       :backend-supports (multi-key-capable? backend-store)})))
    (async+sync (:sync? opts)
                *default-sync-translation*
                (go-try-
                 (if (= write-policy :frontend-only)
                   ;; Cache mode: delete from the frontend only; never touch the backend.
                   (<?- (-multi-dissoc frontend-store keys-to-remove opts))
                   (let [backend-result (<?- (-multi-dissoc backend-store keys-to-remove opts))]
                     (try
                       (<?- (-multi-dissoc frontend-store keys-to-remove opts))
                       (catch #?(:clj Exception :cljs js/Error) e
                         (log/warn :konserve/tiered-frontend-multi-dissoc-failed {:keys keys-to-remove :error e})))
                     backend-result)))))

  (-multi-get [_this keys opts]
    (log/trace :konserve/tiered-multi-get {:key-count (count keys)})
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
                       (let [observed-generation @cache-generation
                             backend-result (<?- (-multi-get backend-store missing-keys opts))]
                         (when (seq backend-result)
                           (<?- (start-read-through!
                                 frontend-store cache-lock cache-generation observed-generation
                                 #(-multi-assoc frontend-store backend-result meta-update
                                                (frontend-opts opts))
                                 {:api-op :multi-assoc :kvs backend-result}
                                 opts)))
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
                :cache-lock (make-cache-lock)
                :cache-generation (atom 0)
                :config params})]
    (if (:sync? opts) store (go store))))
