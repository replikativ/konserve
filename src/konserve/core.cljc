(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [clojure.core.async :refer [chan put! poll!]]
            [hasch.core :as hasch]
            [konserve.protocols :as protocols :refer [PConditionalWrite -conditional-write-domain -revision
                                                      -exists? -get-meta -get-in -assoc-in
                                                      -update-in -dissoc -bget -bassoc
                                                      -keys -multi-get -multi-assoc -multi-dissoc
                                                      -assoc-serializers -get-write-hooks -lock-free?]]
            [konserve.utils :refer [meta-update multi-key-capable? kv-keys invoke-write-hooks! #?(:clj async+sync) *default-sync-translation*]
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
   is not present, or the not-found value if supplied.

   `opts` may carry **`:with-revision? true`**, which returns `[value revision]`
   instead of `value`. That is one call rather than two for a fencing caller, and
   not merely a convenience: reading the value and its revision separately can
   straddle another writer's commit, leaving you fencing on a revision that never
   belonged to the value you read. Pass the revision back as
   `:expected-revision`. Refused by stores that cannot compare-and-set, and by
   `konserve.cache` — a cached value carries no revision."
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
   is not present, or the not-found value if supplied.

   `opts` may carry **`:with-revision? true`**, which returns `[value revision]`
   instead of `value`. That is one call rather than two for a fencing caller, and
   not merely a convenience: reading the value and its revision separately can
   straddle another writer's commit, leaving you fencing on a revision that never
   belonged to the value you read. Pass the revision back as
   `:expected-revision`. Refused by stores that cannot compare-and-set, and by
   `konserve.cache` — a cached value carries no revision."
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

(def absent
  "The `:expected-revision` that means THE KEY MUST NOT EXIST — the create half of
   a conditional write.

   Re-exported from `konserve.impl.defaults`, which is where it is defined and
   where it stays for backends. Callers should not have to require an `impl`
   namespace to use a public contract, and every docstring here that tells you to
   pass it would otherwise be pointing you into konserve's internals."
  defaults/absent)

(def conditional-write-domains
  "Conditional-write reach, weakest first. See `PConditionalWrite`."
  [:process :machine :global])

(defn conditional-write-domain
  "How far this store's conditional writes reach — `:process`, `:machine`,
   `:global` — or nil if it has none."
  [store]
  (when (satisfies? PConditionalWrite store)
    (-conditional-write-domain store)))

(defn ^:private rank-domain!
  "Position of `domain` in `conditional-write-domains`, weakest first.

   RAISES on a domain that is not one of them rather than ranking it. The
   comparison below used a default of 0, i.e. `:process`, so a typo — `:machien`,
   a string `\"machine\"`, a nil out of a config — compared as the WEAKEST domain
   and every store satisfied it. The one function whose job is to stop a caller
   believing they are fenced answered true for a memory store."
  [domain]
  ;; The map is CALLED, not `get`-ed: this namespace shadows `clojure.core/get`
  ;; with the store read, so `(get m k)` here is a konserve GET that returns a
  ;; channel — which then failed to cast to a number, for every domain including
  ;; the valid ones. `clojure.core/get` cannot be written out either, since this
  ;; is .cljc and there it is `cljs.core/get`.
  (or ((zipmap conditional-write-domains (range)) domain)
      (throw (ex-info (str "Not a conditional-write domain: " (pr-str domain))
                      {:type   :konserve/unknown-conditional-write-domain
                       :domain domain
                       :known  (vec conditional-write-domains)}))))

(defn conditional-write?
  "Can this store make a write conditional on the revision the caller read, far
   enough for `required-domain`?

   Called WITHOUT a domain this asks only \"at all?\", which is the right question
   for a caller that already knows its deployment. Pass the domain you actually
   need — `:machine` for several processes on one host, `:global` for several
   hosts — and let the store answer, rather than assuming a `true` covers you."
  ([store]
   (some? (conditional-write-domain store)))
  ([store required-domain]
   ;; `required-domain` is RANKED, not defaulted: a name that is not a domain is a
   ;; mistake in the caller, and answering it is how a typo turns into a false
   ;; assurance. `have` may legitimately be nil (no capability), which is simply
   ;; below every domain.
   (let [required (rank-domain! required-domain)
         have     (conditional-write-domain store)]
     (boolean (and have (>= (rank-domain! have) required))))))

(defn revision
  "The store's current revision token for `key`, or `konserve.core/absent`
   when the key does not exist.

   OPAQUE. Read it, hold it, hand it back as `:expected-revision` — do not order
   it, derive from it, or persist it as if it meant anything. Backends are free to
   use whatever their storage gives them (a counter here, an ETag on S3)."
  ([store key] (revision store key {:sync? false}))
  ([store key opts]
   (when-not (conditional-write? store)
     (throw (ex-info "This store has no revisions to read: it cannot make a write conditional on one."
                     {:type :konserve/conditional-write-unsupported :store (type store)})))
   (-revision store key opts)))

(defn check-conditional-supported!
  "Throw unless `store` can honour an `:expected-revision` in `opts`.

   Public because `konserve.cache` calls the write protocols DIRECTLY rather than
   through this namespace, so it would otherwise bypass the check and silently
   ignore the option — the failure mode the capability exists to remove."
  [store opts]
  ;; A NIL TOKEN IS NOT A REQUEST FOR AN UNCONDITIONAL WRITE. Every gate below
  ;; this is a truthiness test, so a nil would sail past `check-revision!` and
  ;; write unconditionally — while the caller believes they fenced. That is
  ;; reachable through the front door: `revision` answers nil for a key whose
  ;; metadata predates `:revision` (an upgraded store, or a migrated key), so the
  ;; documented read-then-hand-it-back pattern would silently overwrite exactly
  ;; the keys someone tries it on first. `absent` is the way to say "no value
  ;; here"; nil is a mistake.
  (when (and (contains? opts :expected-revision)
             (nil? (:expected-revision opts)))
    (throw (ex-info (str ":expected-revision was nil, which is not a revision. Pass a token from "
                         "konserve.core/revision, or konserve.core/absent to require that "
                         "the key does not exist. Writing unconditionally here would silently withhold "
                         "the guarantee that was asked for.")
                    {:type :konserve/invalid-expected-revision})))
  (when (and (contains? opts :expected-revision)
             (not (conditional-write? store)))
    (throw (ex-info (str "This store cannot honour :expected-revision, so the conditional write was refused. "
                         "Writing it unconditionally would give back the very guarantee that was asked for.")
                    {:type  :konserve/conditional-write-unsupported
                     :store (type store)}))))

(defn refuse-conditional-unsupported!
  "Reject `:expected-revision` on an operation that does not implement it.

   Public for the same reason as [[check-conditional-supported!]]: `konserve.cache`
   reimplements these entry points rather than delegating, so a check kept private
   here is simply absent there — which is how `konserve.cache/dissoc` came to
   DELETE a key whose conditional write `konserve.core/dissoc` refuses.

   Ignoring it is the one outcome that must never happen: the caller asked for a
   guarantee, and a silent unconditional write is the exact failure the whole
   mechanism exists to remove. Refusing is recoverable; a lost update is not."
  [op opts]
  (when (contains? opts :expected-revision)
    (throw (ex-info (str op " cannot be made conditional; :expected-revision was refused rather than ignored.")
                    {:type :konserve/conditional-write-unsupported :op op})))
  ;; Same rule for `:with-revision?`. Dropping it is quieter but no safer: the
  ;; caller destructures the revision-bearing shape, binds the value where the
  ;; revision should be, and fences the NEXT write on garbage.
  (when (contains? opts :with-revision?)
    (throw (ex-info (str op " cannot report a revision; :with-revision? was refused rather than ignored.")
                    {:type :konserve/with-revision-unsupported :op op}))))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value).

  The optional `meta-up-fn` (5-arity, before the trailing `opts`) is
  `(fn [built-meta] -> meta)`, transforming the value's default metadata — the general
  metadata form (cf. `assoc`'s `meta` map). `opts` stays last.

  `opts` may carry **`:expected-revision`** — a token from [[revision]], or
  `konserve.core/absent` for \"the key must not exist\". The update then
  happens only if the stored revision is still that one, and otherwise throws
  `:konserve/revision-mismatch` having written nothing and WITHOUT running
  `up-fn`. Use it when the decision to update was made from an earlier read and
  must not silently drift; plain `update-in` remains the tool for \"recompute from
  whatever is current\", since `up-fn` already sees the current value.

  **`:with-revision? true`** additionally reports the revision the update
  PRODUCED, changing the result shape from `[old new]` to `[[old new] revision]`.
  Hand that back as the next `:expected-revision` to chain fenced updates without
  a re-read.

  THE REVISION IS PER KEY, NOT PER PATH. `[:k :a :b]` fences the whole `:k` blob,
  not the `[:a :b]` position inside it — the blob is the unit of storage and of the
  lock, so it cannot be otherwise."
  ([store key-vec up-fn]
   (update-in store key-vec up-fn nil {:sync? false}))
  ([store key-vec up-fn opts]
   (update-in store key-vec up-fn nil opts))
  ([store key-vec up-fn meta-up-fn opts]
   (log/trace :konserve/update-in {:key-vec key-vec})
   (check-conditional-supported! store opts)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (let [base (partial meta-update (first key-vec) :edn)
                      mfn  (if meta-up-fn (fn [old] (meta-up-fn (base old))) base)
                      result (<?- (-update-in store key-vec mfn up-fn opts))
                      ;; `:with-revision?` makes the result `[[old new] revision]`
                      ;; rather than `[old new]`, so destructuring it as the plain
                      ;; shape put the REVISION TOKEN in the hook's `:value` — and
                      ;; a hook consumer that replicates on `:value` (konserve-sync)
                      ;; would replicate the token as the key's data. The hook
                      ;; reports the value either way; the revision is the caller's
                      ;; business, not the replica's.
                      [old-val new-val] (if (:with-revision? opts) (first result) result)]
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
  from the built fields. `opts` stays last and purely runtime.

   FENCING (`opts`). `:expected-revision` makes the write CONDITIONAL: it lands
   only if the stored revision is still the one you pass, and otherwise raises
   `{:type :konserve/revision-mismatch}` having written nothing. Pass
   `konserve.core/absent` to mean: only if this key does not exist.
   `:with-revision? true` additionally reports the revision the write PRODUCED,
   which changes the result shape from `[old new]` to `[[old new] revision]`;
   hand that back as the next `:expected-revision` to chain fenced writes without
   a re-read. Both are REFUSED, not ignored, by a store that cannot
   compare-and-set — see [[conditional-write?]] and [[conditional-write-domain]]."
  ([store key-vec val]
   (assoc-in store key-vec val nil {:sync? false}))
  ([store key-vec val opts]
   (assoc-in store key-vec val nil opts))
  ([store key-vec val meta-up-fn opts]
   (log/trace :konserve/assoc-in {:key-vec key-vec})
   (check-conditional-supported! store opts)
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
   has. `opts` stays last and purely runtime.

   FENCING (`opts`). `:expected-revision` makes the write CONDITIONAL: it lands
   only if the stored revision is still the one you pass, and otherwise raises
   `{:type :konserve/revision-mismatch}` having written nothing. Pass
   `konserve.core/absent` to mean: only if this key does not exist.
   `:with-revision? true` additionally reports the revision the write PRODUCED,
   which changes the result shape from `[old new]` to `[[old new] revision]`;
   hand that back as the next `:expected-revision` to chain fenced writes without
   a re-read. Both are REFUSED, not ignored, by a store that cannot
   compare-and-set — see [[conditional-write?]] and [[conditional-write-domain]]."
  ([store key val]
   (assoc store key val nil {:sync? false}))
  ([store key val opts]
   (assoc store key val nil opts))
  ([store key val meta opts]
   (log/trace :konserve/assoc {:key key})
   (check-conditional-supported! store opts)
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
   ordering lives in `kvs` (see `multi-assoc`).

   PREFER `{:meta-all m}` IN `opts` FOR A WHOLE-BATCH ANNOTATION, which needs no
   keys at all and therefore cannot get them wrong.

   THE SHAPE TEST BELOW IS A GUESS AND CANNOT BE MADE SOUND. A key that is
   itself a 2-element vector is indistinguishable from a `[k v]` pair: `[[:a 1]]`
   is both a valid one-key seq and a valid one-pair seq, and no inspection of
   the data separates them. So `(uniform-meta (keys {[:a 1] v}) ..)` returns
   `{:a ..}` — the annotation lands on a key nobody wrote.

   Narrowing the test was tried and rejected: keying off `MapEntry` fixes the
   `(keys m)` case and breaks the hand-built pair-vector instead, which only
   moves the failure onto a different caller. What IS fixed is the SILENCE —
   `multi-assoc` refuses a `meta` map whose keys are disjoint from `kvs`, so a
   misread shape raises at the call site instead of dropping metadata."
  [kvs meta]
  (let [ks (cond
             (map? kvs)                      (clojure.core/keys kvs)
             ;; seq of [k v] pairs (an ordered multi-assoc batch)
             (and (sequential? (first kvs))
                  (= 2 (count (first kvs))))  (clojure.core/map first kvs)
             :else                            kvs)]
    (zipmap ks (clojure.core/repeat meta))))

(defn- refuse-conditional-multi!
  "Reject the fencing options on `multi-assoc`.

   The plural `:expected-revisions` was checked here too, and no such option
   exists anywhere in konserve — a guard against a key nobody defines guards
   nothing, while reading as though a batch form had been considered and handled."
  [opts]
  (when (or (contains? opts :expected-revision) (contains? opts :with-revision?))
    (throw (ex-info (str "multi-assoc cannot be made conditional. Verifying every key and then writing "
                         "every key is not one atomic step on a store whose locks are per blob: another "
                         "writer can change a key after it was checked and before it was written. Fencing "
                         "it here would promise an atomicity the backend does not have. Content-addressed "
                         "values — the usual reason to batch — cannot conflict anyway, since the same key "
                         "means the same bytes.")
                    {:type :konserve/conditional-multi-write-unsupported}))))

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
   (refuse-conditional-multi! opts)
   (when-not (multi-key-capable? store)
     (throw (#?(:clj ex-info :cljs js/Error.) "Store does not support multi-key operations"
                                              #?(:clj {:store-type (type store)
                                                       :reason "Store doesn't implement PMultiKeyEDNValueStore protocol or multi-key support is disabled"}))))
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [meta-all (:meta-all opts)
                      _ (when (and meta meta-all)
                          (throw (#?(:clj ex-info :cljs js/Error.)
                                  "Pass either a per-key `meta` or `:meta-all`, not both"
                                  {:type :konserve/ambiguous-meta})))
                      ;; UNIFORM ANNOTATION NEEDS NO KEYS, and that is the whole
                      ;; point of `:meta-all`. `uniform-meta` existed to turn one
                      ;; annotation into a per-key map, which meant DERIVING THE
                      ;; KEYS from `kvs` -- and a key that is itself a 2-element
                      ;; vector is indistinguishable from a `[k v]` pair, so
                      ;; `(uniform-meta (keys m) ..)` on `{[:a 1] v}` produced
                      ;; `{:a ..}` and the annotation was silently dropped.
                      ;; Naming the two intents removes the guess entirely.
                      mfn    (cond
                               meta-all
                               (fn [key type old] (clojure.core/merge meta-all (meta-update key type old)))
                               meta
                               ;; per-key meta map: `(get meta key)` (nil ⇒ just the built meta)
                               (fn [key type old] (clojure.core/merge (clojure.core/get meta key) (meta-update key type old)))
                               :else meta-update)
                      ;; A `meta` MAP THAT NAMES NO KEY OF THIS BATCH IS ALWAYS A
                      ;; MISTAKE, and it used to be a silent one: the write
                      ;; succeeded and the annotation vanished. `uniform-meta`
                      ;; produces exactly that when it misreads its input shape
                      ;; -- a 2-element vector key is indistinguishable from a
                      ;; [k v] pair, so `(uniform-meta (keys {[:a 1] v}) ..)`
                      ;; keys the map by `:a`. A hand-built map with a typo does
                      ;; the same. Checked here because this is the only place
                      ;; holding both halves.
                      ;;
                      ;; DISJOINT THROWS, PARTIAL DOES NOT: annotating a SUBSET
                      ;; is the documented mixed-batch case -- immutable nodes
                      ;; plus the mutable pointer that makes them reachable --
                      ;; so a single overlapping key is enough to be intentional.
                      _ (when (seq meta)
                          (let [ks (set (kv-keys kvs))]
                            (when-not (some ks (clojure.core/keys meta))
                              (throw (#?(:clj ex-info :cljs js/Error.)
                                      (str "multi-assoc: none of `meta`'s keys appear in `kvs`, "
                                           "so no value would receive it. If this came from "
                                           "`uniform-meta`, pass the kv-map itself, name the keys, "
                                           "or use {:meta-all m} in opts.")
                                      {:type :konserve/meta-keys-disjoint
                                       :meta-keys (vec (clojure.core/keys meta))
                                       :kvs-keys (vec ks)})))))
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
                  ;; HOOKS ALWAYS SEE A PER-KEY MAP, whichever way the caller
                  ;; expressed it. `:meta-all` is expanded here and never escapes
                  ;; this namespace, so a consumer like konserve-sync keeps its
                  ;; `(get m k)` and needs no change -- teaching every consumer a
                  ;; second spelling would recreate the dropped-annotation bug one
                  ;; layer out, in any consumer that was not updated.
                  ;;
                  ;; Expanded ONLY when a hook is registered, so the write path
                  ;; still avoids building an N-entry map to say one thing.
                  (invoke-write-hooks! store (cond-> {:api-op :multi-assoc :kvs kvs}
                                               meta     (clojure.core/assoc :meta meta)
                                               meta-all (clojure.core/assoc
                                                         :meta (zipmap (kv-keys kvs)
                                                                       (clojure.core/repeat meta-all)))))
                  result)))))

(defn dissoc
  "Removes an entry from the store. "
  ([store key]
   (dissoc store key {:sync? false}))
  ([store key opts]
   (log/trace :konserve/dissoc {:key key})
   (refuse-conditional-unsupported! "dissoc" opts)
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
   (refuse-conditional-unsupported! "append" opts)
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

(defn bget
  "Calls locked-cb with a platform specific binary representation inside the lock.
  You need to properly close/dispose the object when you are done!

  The callback receives a MAP, and which keys it carries depends on the backend:

    :blob          bytes, already materialised — nothing to drain
    :input-stream  a streaming handle, valid only for the callback's extent

  Concretely: the JVM filestore passes `:input-stream` (an InputStream);
  node-filestore passes `:blob` (a js/Buffer) when synchronous and
  `:input-stream` (an fs.ReadStream, plus `:size`) when asynchronous; indexeddb
  passes `:input-stream` (a WHATWG ReadableStream); the memory store passes both,
  since it holds the bytes outright.

  If you just want the bytes — which is most callers — do not write this by hand
  for each backend. `konserve.binary/to-bytes` is that callback:

    (k/bget store :my-key (konserve.binary/to-bytes opts) opts)

  Write your own when you need to stream rather than materialise, e.g.

  (fn [{is :input-stream}]
    (let [tmp-file (io/file \"/tmp/my-private-copy\")]
      (io/copy is tmp-file)))

  When called asynchronously (by default or w/ {:sync? false}), the locked-cb
  must synchronously return a channel.

  File stores accept `:streaming? true` to expose a bounded view over the stored
  payload instead of first materializing it. In that mode the callback must fully
  consume the stream before it (or its returned channel) completes; the view is
  valid only while Konserve owns the locked backing object."
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
  JVM, Blob in JavaScript) under key in the store."
  ([store key val]
   (bassoc store key val {:sync? false}))
  ([store key val opts]
   (log/trace :konserve/bassoc {:key key})
   (refuse-conditional-unsupported! "bassoc" opts)
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
