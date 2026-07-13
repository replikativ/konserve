(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [clojure.core.async :refer [chan put! poll!]]
            [hasch.core :as hasch]
            [konserve.encryptor :as encryptor]
            [konserve.protocols :as protocols :refer [-exists? -get-meta -get-in -assoc-in
                                                      -update-in -dissoc -bget -bassoc
                                                      -encrypt -decrypt
                                                      -keys -multi-get -multi-assoc -multi-dissoc
                                                      -assoc-serializers -get-write-hooks -lock-free?]]
            [konserve.utils :refer [meta-update multi-key-capable? invoke-write-hooks! #?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [konserve.impl.storage-layout :as storage-layout]
            [konserve.impl.defaults :as defaults]
            [konserve.store :as store]
            [superv.async :refer [go-try- <?-]]
            [replikativ.logging :as log])
  #?(:cljs (:require-macros [konserve.core :refer [go-locked locked maybe-go-locked maybe-locked]])))

;; ACID

;; atomic
;; consistent
;; isolated
;; durable

;; ============================================================================
;; Write Hooks - callbacks invoked after successful write operations
;; ============================================================================

(defn add-write-hook!
  "Register a write hook on the store. The hook-fn will be called after every
   successful write operation at the API layer (assoc-in, update-in, dissoc, etc.).

   Hook message format:
   {:api-op :assoc-in|:assoc|:update-in|:update|:dissoc|:bassoc|:multi-assoc
    :key <top-level key>
    :key-vec <full key path> (for assoc-in/update-in)
    :value <written value>
    :old-value <previous value> (for update operations)
    :kvs <the multi-assoc batch, forwarded VERBATIM — a map, or an ordered seq of
          [k v] pairs whose order is the apply order (see multi-assoc)>}

   Parameters:
   - store: A store implementing PWriteHookStore
   - hook-id: Unique identifier for the hook (keyword recommended)
   - hook-fn: Function of one argument (the write event map)

   Returns the store for chaining."
  [store hook-id hook-fn]
  (when-let [hooks (-get-write-hooks store)]
    (swap! hooks clojure.core/assoc hook-id hook-fn))
  store)

(defn remove-write-hook!
  "Remove a previously registered write hook by its id.

   Parameters:
   - store: A store implementing PWriteHookStore
   - hook-id: The id used when registering the hook

   Returns the store for chaining."
  [store hook-id]
  (when-let [hooks (-get-write-hooks store)]
    (swap! hooks clojure.core/dissoc hook-id))
  store)

;; ============================================================================
;; Locking utilities
;; ============================================================================

(defn lock-free?
  "Returns true if the store does not require application-level locking.
   MVCC stores like LMDB implement the PLockFreeStore protocol to indicate
   they handle concurrency internally."
  [store]
  (-lock-free? store))

;; --- In-process per-key lock registry ----------------------------------------
;; SOLID via Clojure protocols + records: callers (get-lock / release-lock and the
;; locked / go-locked macros) depend on the PLockRegistry abstraction (DIP), not on
;; the concrete map; the locking strategy is a substitutable record (LSP) and new
;; strategies are added without touching callers (OCP).

(defprotocol PLockRegistry
  "Per-key in-process lock lifecycle. A lock is a core.async channel used as a
   binary semaphore (holds one :unlocked token while free)."
  (-acquire-lock [this key]
    "Register intent on `key` and return its unlocked semaphore channel.")
  (-release-lock [this key]
    "Release `key`; reclaim the registry entry once no holder remains. Returns nil."))

;; Pure registry calculations — no I/O, trivially unit-testable ----------------

(defn- unlocked-chan
  "A fresh channel preloaded with one :unlocked semaphore token."
  []
  (let [c (chan)]
    (put! c :unlocked)
    c))

(defn- fresh-lock
  "A new refcounted registry entry: an unlocked lock held by one acquirer."
  []
  {:ch (unlocked-chan) :n 1})

(defn- acquire-entry
  "Bump the refcount of `key`'s lock in registry map `m`, inserting `entry` when
   absent. Pure."
  [m key entry]
  (if-let [e (clojure.core/get m key)]
    (clojure.core/assoc m key (clojure.core/update e :n inc))
    (clojure.core/assoc m key entry)))

(defn- release-entry
  "Drop one refcount for `key` in registry map `m`, removing the entry when the
   last holder releases. Pure."
  [m key]
  (if-let [e (clojure.core/get m key)]
    (if (<= (:n e) 1)
      (clojure.core/dissoc m key)
      (clojure.core/assoc m key (clojure.core/update e :n dec)))
    m))

;; Locking strategies — substitutable PLockRegistry records --------------------

(defrecord RefcountedLockRegistry [locks]
  ;; `locks`: atom of {key -> {:ch chan :n refcount}}. The refcount is bumped on
  ;; acquire (before the caller parks) and dropped on release, so the entry
  ;; survives every concurrent waiter and is reclaimed only at zero — bounding the
  ;; registry instead of leaking one channel per distinct key for the store's life.
  PLockRegistry
  (-acquire-lock [_ key]
    (:ch (clojure.core/get (swap! locks acquire-entry key (fresh-lock)) key)))
  (-release-lock [_ key]
    (swap! locks release-entry key)
    nil))

(defrecord LockFreeRegistry []
  ;; MVCC backends (LMDB, …) serialize internally, so locks need not be tracked:
  ;; hand out a throwaway unlocked channel and register nothing.
  PLockRegistry
  (-acquire-lock [_ _key] (unlocked-chan))
  (-release-lock [_ _key] nil))

(def ^:private lock-free-registry
  "Stateless singleton shared by every lock-free store."
  (->LockFreeRegistry))

(defn- store-lock-registry
  "Select `store`'s lock strategy (DIP: returns a PLockRegistry)."
  [store]
  (if (lock-free? store)
    lock-free-registry
    (->RefcountedLockRegistry (:locks store))))

;; Public API — single level of abstraction, delegate to the strategy ----------

(defn get-lock
  "Acquire `store`/`key`'s in-process lock channel, registering intent. MUST be
   paired with `release-lock` so the entry is reclaimed when the last holder
   releases — otherwise the registry grows one channel per distinct key for the
   store's lifetime (unbounded heap retention). Lock-free stores get a throwaway
   channel."
  [store key]
  (-acquire-lock (store-lock-registry store) key))

(defn release-lock
  "Release `store`/`key`'s in-process lock; reclaim the registry entry when no
   holder remains. No-op for lock-free stores / unregistered keys. Paired with
   `get-lock`."
  [store key]
  (-release-lock (store-lock-registry store) key))

(defn wait [lock]
  #?(:clj (while (not (poll! lock))
            (Thread/sleep (long (rand-int 20))))
     :cljs (when-not (some-> lock poll!)
             (log/debug :konserve/lock-not-active "WARNING: konserve lock is not active. Only use the synchronous variant with the memory store in JavaScript."))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defmacro locked [store key & code]
  `(let [s# ~store
         k# ~key
         l# (get-lock s# k#)]
     (try
       (wait l#)
       (log/trace :konserve/acquired-spin-lock {:key k#})
       ~@code
       (finally
         (log/trace :konserve/releasing-spin-lock {:key k#})
         (put! l# :unlocked)
         (release-lock s# k#)))))

(defmacro go-locked [store key & code]
  `(go-try-
    (let [s# ~store
          k# ~key
          l# (get-lock s# k#)]
      (try
        (<?- l#)
        (log/trace :konserve/acquired-go-lock {:key k#})
        ~@code
        (finally
          (log/trace :konserve/releasing-go-lock {:key k#})
          (put! l# :unlocked)
          (release-lock s# k#))))))

;; Optional locking macros - skip locking for lock-free stores (MVCC backends)
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defmacro maybe-locked
  "Like locked, but skips locking if store is lock-free."
  [store key & code]
  `(if (lock-free? ~store)
     (do ~@code)
     (locked ~store ~key ~@code)))

(defmacro maybe-go-locked
  "Like go-locked, but skips locking if store is lock-free."
  [store key & code]
  `(if (lock-free? ~store)
     (go-try- ~@code)
     (go-locked ~store ~key ~@code)))

(defn exists?
  "Checks whether value is in the store."
  ([store key]
   (exists? store key {:sync? false}))
  ([store key opts]
   (log/trace :konserve/exists? {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (maybe-go-locked
                store key
                (<?- (-exists? store key opts))))))

(defn get-in
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
   (get-in store key-vec not-found {:sync? false}))
  ([store key-vec not-found opts]
   (log/trace :konserve/get-in {:key-vec key-vec})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (maybe-go-locked
                store (first key-vec)
                (<?- (-get-in store key-vec not-found opts))))))

(defn get
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get store key nil))
  ([store key not-found]
   (get store key not-found {:sync? false}))
  ([store key not-found opts]
   (get-in store [key] not-found opts)))

(defn get-meta
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get-meta store key nil))
  ([store key not-found]
   (get-meta store key not-found {:sync? false}))
  ([store key not-found opts]
   (log/trace :konserve/get-meta {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (maybe-go-locked
                store key
                (let [a (<?- (-get-meta store key opts))]
                  (if (some? a)
                    a
                    not-found))))))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value).

  The optional `meta-up-fn` (5-arity, before the trailing `opts`) is
  `(fn [built-meta] -> meta)`, transforming the value's default metadata — the general
  metadata form (cf. `assoc`'s `meta` map). `opts` stays last."
  ([store key-vec up-fn]
   (update-in store key-vec up-fn nil {:sync? false}))
  ([store key-vec up-fn opts]
   (update-in store key-vec up-fn nil opts))
  ([store key-vec up-fn meta-up-fn opts]
   (log/trace :konserve/update-in {:key-vec key-vec})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (let [base (partial meta-update (first key-vec) :edn)
                      mfn  (if meta-up-fn (fn [old] (meta-up-fn (base old))) base)
                      [old-val new-val :as result] (<?- (-update-in store key-vec mfn up-fn opts))]
                  (invoke-write-hooks! store {:api-op :update-in
                                              :key (first key-vec)
                                              :key-vec key-vec
                                              :old-value old-val
                                              :value new-val})
                  result)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn update
  "Updates a position described by key by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  ([store key fn]
   (update store key fn nil {:sync? false}))
  ([store key fn opts]
   (update store key fn nil opts))
  ([store key fn meta-up-fn opts]
   (log/trace :konserve/update {:key key})
   (update-in store [key] fn meta-up-fn opts)))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created.

  The optional `meta-up-fn` (5-arity, before the trailing `opts`) is
  `(fn [built-meta] -> meta)` — it TRANSFORMS the value's default metadata (the built
  `{:key :type :last-write}`). This is the general form of `assoc`'s `meta` map (a map
  is just the merge-transform), exposed here because nested writes may derive metadata
  from the built fields. `opts` stays last and purely runtime."
  ([store key-vec val]
   (assoc-in store key-vec val nil {:sync? false}))
  ([store key-vec val opts]
   (assoc-in store key-vec val nil opts))
  ([store key-vec val meta-up-fn opts]
   (log/trace :konserve/assoc-in {:key-vec key-vec})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (let [base   (partial meta-update (first key-vec) :edn)
                      mfn    (if meta-up-fn (fn [old] (meta-up-fn (base old))) base)
                      result (<?- (-assoc-in store key-vec mfn val opts))]
                  (invoke-write-hooks! store {:api-op :assoc-in
                                              :key (first key-vec)
                                              :key-vec key-vec
                                              :value val})
                  result)))))

(defn assoc
  "Associates the key to the value. This is a simple top-level overwrite
   and does not require locking for MVCC stores. For nested paths, use assoc-in.

   The optional `meta` MAP (5-arity, before the trailing `opts`) is merged into the
   value's stored metadata — the built `{:key :type :last-write}` fields win on
   conflict, so `meta` is additive. Use `{:immutable? true}` to mark a content-
   addressed (write-once) value: it is recorded durably AND forwarded on the write-
   hook event, so a consumer (konserve-sync) can skip re-storing a value it already
   has. `opts` stays last and purely runtime."
  ([store key val]
   (assoc store key val nil {:sync? false}))
  ([store key val opts]
   (assoc store key val nil opts))
  ([store key val meta opts]
   (log/trace :konserve/assoc {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (maybe-go-locked
                store key
                (let [mfn    (if meta
                               (fn [old] (clojure.core/merge meta (meta-update key :edn old)))
                               (partial meta-update key :edn))
                      result (<?- (-assoc-in store [key] mfn val opts))]
                  (invoke-write-hooks! store (cond-> {:api-op :assoc :key key :value val}
                                               meta (clojure.core/assoc :meta meta)))
                  result)))))

(defn multi-get
  "Atomically retrieves multiple values by keys.
  Takes a collection of keys and returns a sparse map containing only found keys.
  Uses flat keys only (not key-vecs).

  Example:
  ```
  (multi-get store [:user1 :user2 :user3])
  ;; => {:user1 {:name \"Alice\"} :user3 {:name \"Charlie\"}}
  ;; (user2 was not found, so excluded from result)
  ```

  Returns a map {key -> value} for all found keys. Missing keys are excluded from result.
  Callers can use standard map lookup to handle missing keys:
  (get result :user2 :not-found) ;; => :not-found

  Throws an exception if the store doesn't support multi-key operations."
  ([store keys]
   (multi-get store keys {:sync? false}))
  ([store keys opts]
   (log/trace :konserve/multi-get {:key-count (count keys)})
   (when-not (multi-key-capable? store)
     (throw (#?(:clj ex-info :cljs js/Error.) "Store does not support multi-key operations"
                                              #?(:clj {:store-type (type store)
                                                       :reason "Store doesn't implement PMultiKeyEDNValueStore protocol or multi-key support is disabled"}))))
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (try
                  (<?- (-multi-get store keys opts))
                  (catch #?(:clj Exception :cljs js/Error) e
                    ;; Backend might throw an exception indicating it doesn't support multi-key operations
                    ;; even though the store implements the protocol
                    #?(:clj
                       (if (and (instance? clojure.lang.ExceptionInfo e)
                                (= :not-supported (:type (ex-data e))))
                         (throw (ex-info "Backing store does not support multi-key operations"
                                         {:store-type (type store)
                                          :cause e
                                          :reason (:reason (ex-data e))}))
                         (throw e))
                       :cljs (throw e))))))))

(defn uniform-meta
  "Build a per-key meta map applying the same `meta` to every key — a convenience
   for `multi-assoc`'s per-key `meta` when a whole batch shares one annotation:
   `(multi-assoc store nodes (uniform-meta nodes {:immutable? true}) opts)`.
   `kvs` may be a kv-map, an ordered seq of [k v] pairs, or a plain seq of keys.
   Note the result is a plain lookup map — per-key meta is unordered by nature;
   ordering lives in `kvs` (see `multi-assoc`)."
  [kvs meta]
  (let [ks (cond
             (map? kvs)                      (clojure.core/keys kvs)
             ;; seq of [k v] pairs (an ordered multi-assoc batch)
             (and (sequential? (first kvs))
                  (= 2 (count (first kvs))))  (clojure.core/map first kvs)
             :else                            kvs)]
    (zipmap ks (clojure.core/repeat meta))))

(defn multi-assoc
  "Associates multiple key-value pairs with flat keys, as one batch. Atomically where the
  backend can (IndexedDB); ordered everywhere (see below), which is the weaker guarantee
  that actually suffices.

  `kvs` is either a map, or — preferred when the batch has internal dependencies — an
  ORDERED sequence of `[k v]` pairs. **Sequence order is apply order**, and it is preserved
  end-to-end: through the backing store's writes and verbatim onto the `:multi-assoc`
  write-hook (so a sync layer can relay the batch in the same order). A map has no order,
  so a map batch makes no ordering promise.

  Why that matters: not every backend can write multiple keys atomically (S3, filesystems
  cannot; IndexedDB can). For a batch that writes a set of immutable, content-addressed
  values plus a MUTABLE pointer that makes them reachable, you do not need atomicity — you
  need order. Put the pointer LAST:

  ```
  (multi-assoc store [[node-a v] [node-b v] [:root {:refs [node-a node-b]}]]
               (uniform-meta [node-a node-b] {:immutable? true}))
  ```

  Then any prefix of the batch leaves the store consistent: the values are written but
  unreachable (harmless orphans, collectable), and the pointer flips only once everything it
  references exists. A torn batch can never produce a dangling pointer. This is the
  write-the-leaves-then-flip-the-root discipline, and it is what lets non-atomic backends be
  crash-safe.

  Atomic backends still apply the batch all-or-nothing, in which case the order is simply
  redundant — passing an ordered seq is always safe.

  Returns a map of keys to results (typically true for each key).

  The optional `meta` (4-arity, before the trailing `opts`) is a PER-KEY map
  `{key -> meta-map}`, pure data so the whole map is forwarded verbatim on the write-hook
  (a consumer like konserve-sync can relay/serialize it). Each written value's metadata is
  merged with `(get meta key)` (built `{:key :type :last-write}` fields win). Keys absent
  from `meta` get no extra metadata, so one atomic batch can mark some keys immutable
  (content-addressed nodes) and leave others (a mutable branch-head pointer) unmarked. Use
  `uniform-meta` for the all-keys-same case.

  Throws an exception if the store doesn't support multi-key operations."
  ([store kvs]
   (multi-assoc store kvs nil {:sync? false}))
  ([store kvs opts]
   (multi-assoc store kvs nil opts))
  ([store kvs meta opts]
   (log/trace :konserve/multi-assoc {:key-count (count kvs)})
   (when-not (multi-key-capable? store)
     (throw (#?(:clj ex-info :cljs js/Error.) "Store does not support multi-key operations"
                                              #?(:clj {:store-type (type store)
                                                       :reason "Store doesn't implement PMultiKeyEDNValueStore protocol or multi-key support is disabled"}))))
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [mfn    (if meta
                               ;; per-key meta map: `(get meta key)` (nil ⇒ just the built meta)
                               (fn [key type old] (clojure.core/merge (clojure.core/get meta key) (meta-update key type old)))
                               meta-update)
                      result (try
                               (<?- (-multi-assoc store kvs mfn opts))
                               (catch #?(:clj Exception :cljs js/Error) e
                                 ;; Backend might throw an exception indicating it doesn't support multi-key operations
                                 ;; even though the store implements the protocol
                                 #?(:clj
                                    (if (and (instance? clojure.lang.ExceptionInfo e)
                                             (= :not-supported (:type (ex-data e))))
                                      (throw (ex-info "Backing store does not support multi-key operations"
                                                      {:store-type (type store)
                                                       :cause e
                                                       :reason (:reason (ex-data e))}))
                                      (throw e))
                                    :cljs (throw e))))]
                  (invoke-write-hooks! store (cond-> {:api-op :multi-assoc :kvs kvs}
                                               ;; per-key meta map forwarded verbatim (pure data)
                                               meta (clojure.core/assoc :meta meta)))
                  result)))))

(defn dissoc
  "Removes an entry from the store. "
  ([store key]
   (dissoc store key {:sync? false}))
  ([store key opts]
   (log/trace :konserve/dissoc {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (maybe-go-locked
                store key
                (let [result (<?- (-dissoc store key opts))]
                  (when result
                    (invoke-write-hooks! store {:api-op :dissoc
                                                :key key}))
                  result)))))

(defn multi-dissoc
  "Atomically dissociates multiple keys with flat keys.
  Takes a collection of keys to remove and deletes them in a single atomic transaction.
  All operations must succeed or all must fail (all-or-nothing semantics).

  Example:
  ```
  (multi-dissoc store [:user1 :user2 :user3])
  ```

  Returns a map of keys to results (typically true for each key).

  Throws an exception if the store doesn't support multi-key operations."
  ([store keys]
   (multi-dissoc store keys {:sync? false}))
  ([store keys opts]
   (log/trace :konserve/multi-dissoc {:key-count (count keys)})
   (when-not (multi-key-capable? store)
     (throw (#?(:clj ex-info :cljs js/Error.) "Store does not support multi-key operations"
                                              #?(:clj {:store-type (type store)
                                                       :reason "Store doesn't implement PMultiKeyEDNValueStore protocol or multi-key support is disabled"}))))
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (try
                  (<?- (-multi-dissoc store keys opts))
                  (catch #?(:clj Exception :cljs js/Error) e
                    ;; Backend might throw an exception indicating it doesn't support multi-key operations
                    ;; even though the store implements the protocol
                    #?(:clj
                       (if (and (instance? clojure.lang.ExceptionInfo e)
                                (= :not-supported (:type (ex-data e))))
                         (throw (ex-info "Backing store does not support multi-key operations"
                                         {:store-type (type store)
                                          :cause e
                                          :reason (:reason (ex-data e))}))
                         (throw e))
                       :cljs (throw e))))))))

(defn append
  "Append the Element to the log at the given key or create a new append log there.
  This operation only needs to write the element and pointer to disk and hence is useful in write-heavy situations."
  ([store key elem]
   (append store key elem {:sync? false}))
  ([store key elem opts]
   (log/trace :konserve/append {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (let [head (<?- (-get-in store [key] nil opts))
                      [append-log? last-id first-id] head
                      new-elem {:next nil
                                :elem elem}
                      id (hasch/uuid)]
                  (when (and head (not= append-log? :append-log))
                    (throw (ex-info "This is not an append-log." {:key key})))
                  (<?- (-update-in store [id] (partial meta-update key :append-log) (fn [_] new-elem) opts))
                  (when first-id
                    (<?- (-update-in store [last-id :next] (partial meta-update key :append-log) (fn [_] id) opts)))
                  (<?- (-update-in store [key] (partial meta-update key :append-log) (fn [_] [:append-log id (or first-id id)]) opts))
                  [first-id id])))))

(defn log
  "Loads the whole append log stored at key."
  ([store key]
   (log store key {:sync? false}))
  ([store key opts]
   (log/trace :konserve/log {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [head (<?- (get store key nil opts))
                      [append-log? _last-id first-id] head]
                  (when (and head (not= append-log? :append-log))
                    (throw (ex-info "This is not an append-log." {:key key})))
                  (when first-id
                    (loop [{:keys [next elem]} (<?- (get store first-id nil opts))
                           hist []]
                      (if next
                        (recur (<?- (get store next nil opts))
                               (conj hist elem))
                        (conj hist elem)))))))))

(defn reduce-log
  "Loads the append log and applies reduce-fn over it."
  ([store key reduce-fn acc]
   (reduce-log store key reduce-fn acc {:sync? false}))
  ([store key reduce-fn acc opts]
   (log/trace :konserve/reduce-log {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [head (<?- (get store key nil opts))
                      [append-log? last-id first-id] head]
                  (when (and head (not= append-log? :append-log))
                    (throw (ex-info "This is not an append-log." {:key key})))
                  (if first-id
                    (loop [id first-id
                           acc acc]
                      (let [{:keys [next elem]} (<?- (get store id nil opts))]
                        (if (and next (not= id last-id))
                          (recur next (reduce-fn acc elem))
                          (reduce-fn acc elem))))
                    acc))))))

;; -----------------------------------------------------------------------------
;; Sealing raw bytes under the store's key
;; -----------------------------------------------------------------------------

(defn encrypted?
  "Does this store encrypt the values it writes? False for a store with no
  encryptor configured (and for stores that have no encryptor at all, like the
  in-memory one). Binary values are never encrypted regardless — see `bassoc`."
  [store]
  (let [{:keys [encryptor config]} store]
    (boolean (when encryptor
               (encryptor/encrypting? (encryptor (:encryptor config)))))))

(defn- store-cipher
  "The store's configured encryptor, or an error if it has none. `seal`/`unseal`
  exist to encrypt bytes the store itself will not touch, so an unencrypted store
  quietly returning the plaintext would defeat the point."
  [store]
  (when-not (encrypted? store)
    (throw (ex-info (str "This store has no encryptor configured, so there is no key "
                         "to seal with. Configure one, e.g. "
                         "{:encryptor {:type :aes-gcm :key ...}}.")
                    {:type :konserve/no-encryptor})))
  ((:encryptor store) (:encryptor (:config store))))

(defn seal
  "Encrypt `bytes` with this store's configured encryptor, bound to `key`.

  konserve does not encrypt binary values — `bassoc`/`bget` hand your bytes to
  storage untouched, so the format is yours. `seal`/`unseal` let you encrypt them
  under the store's own key without managing a second one, and bind the ciphertext
  to `key` and to the store's layout version: bytes sealed under one key will not
  unseal under another, and a tampered or truncated ciphertext is rejected rather
  than returned as garbage (with an AEAD cipher like :aes-gcm).

    (bassoc store :thumb (<? (seal store :thumb raw-bytes)) {:raw? true})
    (bget store :thumb
          (fn [{is :input-stream}]
            (go (<? (unseal store :thumb (slurp-bytes is)))))
          {:raw? true})

  `bytes` is a byte[] on the JVM and a Uint8Array on JavaScript. Returns the
  ciphertext, or a channel of it unless {:sync? true}."
  ([store key bytes]
   (seal store key bytes {:sync? false}))
  ([store key bytes opts]
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [enc (store-cipher store)
                      aad (encryptor/associated-data (:version store)
                                                     (defaults/key->store-key key)
                                                     :binary)]
                  (<?- (-encrypt enc bytes aad opts)))))))

(defn unseal
  "Decrypt bytes produced by `seal` under the same store and `key`. Raises if the
  ciphertext does not authenticate — wrong key, wrong store key, or tampering."
  ([store key bytes]
   (unseal store key bytes {:sync? false}))
  ([store key bytes opts]
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [enc (store-cipher store)
                      aad (encryptor/associated-data (:version store)
                                                     (defaults/key->store-key key)
                                                     :binary)]
                  (<?- (-decrypt enc bytes aad opts)))))))

(defn bget
  "Calls locked-cb with a platform specific binary representation inside
  the lock, e.g. wrapped InputStream on the JVM and Blob in
  JavaScript. You need to properly close/dispose the object when you
  are done!

  You have to do all work in locked-cb, e.g.

  (fn [{is :input-stream}]
    (let [tmp-file (io/file \"/tmp/my-private-copy\")]
      (io/copy is tmp-file)))

  When called asynchronously (by default or w/ {:sync? false}), the locked-cb
  must synchronously return a channel.

  Binary values are NOT encrypted: the bytes reach storage exactly as given. On a
  store with an encryptor configured this therefore requires {:raw? true}, an
  explicit statement that you own the format and its confidentiality. To encrypt
  under the store's key instead, see `seal` / `unseal`."
  ([store key locked-cb]
   (bget store key locked-cb {:sync? false}))
  ([store key locked-cb opts]
   (log/trace :konserve/bget {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (maybe-go-locked
                store key
                (<?- (-bget store key locked-cb opts))))))

(defn bassoc
  "Copies given value (InputStream, Reader, File, byte[] or String on
  JVM, Blob in JavaScript) under key in the store.

  The bytes are stored as given — konserve does not encrypt them. On a store with
  an encryptor configured this therefore requires {:raw? true}, an explicit
  statement that you own the format and its confidentiality. To encrypt under the
  store's key instead, see `seal` / `unseal`."
  ([store key val]
   (bassoc store key val {:sync? false}))
  ([store key val opts]
   (log/trace :konserve/bassoc {:key key})
   (async+sync (:sync? opts)
               *default-sync-translation*
               (maybe-go-locked
                store key
                (let [result (<?- (-bassoc store key (partial meta-update key :binary) val opts))]
                  (invoke-write-hooks! store {:api-op :bassoc
                                              :key key
                                              :value val})
                  result)))))

(defn keys
  "Return a channel that will yield all top-level keys currently in the store."
  ([store]
   (keys store {:sync? false}))
  ([store opts]
   (log/trace :konserve/keys "fetching keys")
   (-keys store opts)))

(defn assoc-serializers
  "Assoc the given serializers onto the store, taking effect immediately."
  [store serializers]
  (-assoc-serializers store serializers))

;; =============================================================================
;; Unified Store Interface (re-exported from konserve.store)
;; =============================================================================

(def connect-store
  "Connect to a konserve store based on :backend key in config.

   Dispatches to the appropriate backend implementation based on the :backend key.
   The second argument (opts) controls synchronous or asynchronous execution.

   Args:
     config - A map with :backend key and backend-specific configuration
     opts - Optional map with :sync? true/false (defaults to async {:sync? false})

   Built-in backends:
   - :memory - In-memory store (all platforms)
   - :file - File-based store (JVM only)

   External backends (require the module first):
   - :file - File-based store for Node.js (konserve.node-filestore)
   - :indexeddb - Browser IndexedDB (konserve.indexeddb - browser only)
   - :s3 - AWS S3 (konserve-s3)
   - :dynamodb - AWS DynamoDB (konserve-dynamodb)
   - :redis - Redis (konserve-redis)
   - :lmdb - LMDB (konserve-lmdb)
   - :rocksdb - RocksDB (konserve-rocksdb)

   Example:
     (connect-store {:backend :memory} {:sync? true})
     (connect-store {:backend :file :path \"/tmp/store\"} {:sync? true})
     (connect-store {:backend :s3 :bucket \"my-bucket\" :region \"us-east-1\"} {:sync? false})

   See konserve.store namespace for multimethod definitions and backend registration."
  store/connect-store)

(def create-store
  "Create a new store.

   Note: Most backends auto-create on connect-store, so this is often equivalent.
   Use this when you explicitly want to create a new store. Will error if store
   already exists.

   Args:
     config - A map with :backend key and backend-specific configuration
     opts - Optional map with :sync? true/false (defaults to async {:sync? false})

   Example:
     (create-store {:backend :memory} {:sync? true})
     (create-store {:backend :file :path \"/tmp/store\"} {:sync? true})

   See connect-store for available backends."
  store/create-store)

(def store-exists?
  "Check if a store exists at the given configuration.

   Args:
     config - A map with :backend key and backend-specific configuration
     opts - Optional map with :sync? true/false (defaults to async {:sync? false})

   Returns:
     true if store exists, false otherwise (or channel in async mode)

   Example:
     (store-exists? {:backend :memory :id \"my-store\"} {:sync? true})
     (store-exists? {:backend :file :path \"/tmp/store\"} {:sync? true})

   See connect-store for available backends."
  store/store-exists?)

(def delete-store
  "Delete/clean up an existing store (removes underlying storage).

   Args:
     config - The same config map used with connect-store
     opts - Optional map with :sync? true/false (defaults to async {:sync? false})

   Example:
     (delete-store {:backend :file :path \"/tmp/store\"} {:sync? true})
     (delete-store {:backend :s3 :bucket \"my-bucket\" :region \"us-east-1\"} {:sync? false})

   See connect-store for available backends."
  store/delete-store)

(def release-store
  "Release connections and resources held by a store.

   Args:
     config - The config map used to create the store
     store - The store instance to release
     opts - Optional map with :sync? true/false (defaults to async {:sync? false})

   Example:
     (release-store {:backend :file :path \"/tmp/store\"} store {:sync? true})
     (release-store {:backend :s3 :bucket \"my-bucket\" :region \"us-east-1\"} store)

   See connect-store for available backends."
  store/release-store)