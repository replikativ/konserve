(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [clojure.core.async :refer [chan put! poll!]]
            [hasch.core :as hasch]
            [konserve.protocols :as protocols :refer [-exists? -get-meta -get-in -assoc-in
                                                      -update-in -dissoc -bget -bassoc
                                                      -keys -multi-get -multi-assoc -multi-dissoc
                                                      -assoc-serializers -get-write-hooks -lock-free?]]
            [konserve.utils :refer [meta-update multi-key-capable? invoke-write-hooks! #?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [konserve.impl.storage-layout :as storage-layout]
            [konserve.impl.defaults :as defaults]
            [konserve.store :as store]
            [superv.async :refer [go-try- <?-]]
            [taoensso.timbre :refer [trace #?(:cljs debug)]])
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
    :kvs <map of key->value> (for multi-assoc)}

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

(defn get-lock [{:keys [locks] :as store} key]
  (if (lock-free? store)
    ;; For lock-free stores, create a fresh unlocked channel
    ;; This is used by operations that still need locking (assoc-in, update-in)
    (let [c (chan)]
      (put! c :unlocked)
      c)
    ;; Normal case: get or create persistent lock
    (or (clojure.core/get @locks key)
        (let [c (chan)]
          (put! c :unlocked)
          (clojure.core/get (swap! locks (fn [old]
                                           (trace "creating lock for: " key)
                                           (if (old key) old
                                               (clojure.core/assoc old key c))))
                            key)))))

(defn wait [lock]
  #?(:clj (while (not (poll! lock))
            (Thread/sleep (long (rand-int 20))))
     :cljs (when-not (some-> lock poll!)
             (debug "WARNING: konserve lock is not active. Only use the synchronous variant with the memory store in JavaScript."))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defmacro locked [store key & code]
  `(let [l# (get-lock ~store ~key)]
     (try
       (wait l#)
       (trace "acquired spin lock for " ~key)
       ~@code
       (finally
         (trace "releasing spin lock for " ~key)
         (put! l# :unlocked)))))

(defmacro go-locked [store key & code]
  `(go-try-
    (let [l# (get-lock ~store ~key)]
      (try
        (<?- l#)
        (trace "acquired go-lock for: " ~key)
        ~@code
        (finally
          (trace "releasing go-lock for: " ~key)
          (put! l# :unlocked))))))

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
   (trace "exists? on key " key)
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
   (trace "get-in on key " key-vec)
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
   (trace "get-meta on key " key)
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
  value and the result of applying up-fn (the newly stored value)."
  ([store key-vec up-fn]
   (update-in store key-vec up-fn {:sync? false}))
  ([store key-vec up-fn opts]
   (trace "update-in on key " key-vec)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (let [[old-val new-val :as result] (<?- (-update-in store key-vec (partial meta-update (first key-vec) :edn) up-fn opts))]
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
   (update store key fn {:sync? false}))
  ([store key fn opts]
   (trace "update on key " key)
   (update-in store [key] fn opts)))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  ([store key-vec val]
   (assoc-in store key-vec val {:sync? false}))
  ([store key-vec val opts]
   (trace "assoc-in on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (let [result (<?- (-assoc-in store key-vec (partial meta-update (first key-vec) :edn) val opts))]
                  (invoke-write-hooks! store {:api-op :assoc-in
                                              :key (first key-vec)
                                              :key-vec key-vec
                                              :value val})
                  result)))))

(defn assoc
  "Associates the key to the value. This is a simple top-level overwrite
   and does not require locking for MVCC stores. For nested paths, use assoc-in."
  ([store key val]
   (assoc store key val {:sync? false}))
  ([store key val opts]
   (trace "assoc on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (maybe-go-locked
                store key
                (let [result (<?- (-assoc-in store [key] (partial meta-update key :edn) val opts))]
                  (invoke-write-hooks! store {:api-op :assoc
                                              :key key
                                              :value val})
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
   (trace "multi-get operation with " (count keys) " keys")
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

(defn multi-assoc
  "Atomically associates multiple key-value pairs with flat keys.
  Takes a map of keys to values and stores them in a single atomic transaction.
  All operations must succeed or all must fail (all-or-nothing semantics).

  Example:
  ```
  (multi-assoc store {:user1 {:name \"Alice\"}
                      :user2 {:name \"Bob\"}})
  ```

  Returns a map of keys to results (typically true for each key).

  Throws an exception if the store doesn't support multi-key operations."
  ([store kvs]
   (multi-assoc store kvs {:sync? false}))
  ([store kvs opts]
   (trace "multi-assoc operation with " (count kvs) " keys")
   (when-not (multi-key-capable? store)
     (throw (#?(:clj ex-info :cljs js/Error.) "Store does not support multi-key operations"
                                              #?(:clj {:store-type (type store)
                                                       :reason "Store doesn't implement PMultiKeyEDNValueStore protocol or multi-key support is disabled"}))))
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-try-
                (let [result (try
                               (<?- (-multi-assoc store kvs meta-update opts))
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
                  (invoke-write-hooks! store {:api-op :multi-assoc
                                              :kvs kvs})
                  result)))))

(defn dissoc
  "Removes an entry from the store. "
  ([store key]
   (dissoc store key {:sync? false}))
  ([store key opts]
   (trace "dissoc on key " key)
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
   (trace "multi-dissoc operation with " (count keys) " keys")
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
   (trace "append on key " key)
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
   (trace "log on key " key)
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
   (trace "reduce-log on key " key)
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
  "Calls locked-cb with a platform specific binary representation inside
  the lock, e.g. wrapped InputStream on the JVM and Blob in
  JavaScript. You need to properly close/dispose the object when you
  are done!

  You have to do all work in locked-cb, e.g.

  (fn [{is :input-stream}]
    (let [tmp-file (io/file \"/tmp/my-private-copy\")]
      (io/copy is tmp-file)))

  When called asynchronously (by default or w/ {:sync? false}), the locked-cb
  must synchronously return a channel."
  ([store key locked-cb]
   (bget store key locked-cb {:sync? false}))
  ([store key locked-cb opts]
   (trace "bget on key " key)
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
   (trace "bassoc on key " key)
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
   (trace "fetching keys")
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