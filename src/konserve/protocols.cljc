(ns konserve.protocols
  (:require [clojure.walk])
  #?(:cljs (:refer-clojure :exclude [-dissoc])))

(defprotocol PEDNKeyValueStore
  "Allows to access a store similar to hash-map in EDN."
  (-exists? [this key opts] "Checks whether value is in the store.")
  (-get-meta [this key opts] "Fetch only metadata for the key.")
  (-get-in [this key-vec not-found opts] "Returns the value stored described by key or nil if the path is not resolvable.")
  (-update-in [this key-vec meta-up-fn up-fn opts]
    "Updates a position described by key-vec by applying up-fn and storing the result atomically. Returns a vector [old new] of the previous value and the result of applying up-fn (the newly stored value).")
  (-assoc-in [this key-vec meta-up-fn val opts])
  (-dissoc [this key opts]))

(defprotocol PMultiKeySupport
  "Protocol for checking if a store supports atomic multi-key operations."
  (-supports-multi-key? [this]
    "Returns true if the store supports atomic multi-key operations."))

(defprotocol PMultiKeyEDNValueStore
  "Allows to atomically update multiple key-value pairs with all-or-nothing semantics.
   This is an optional protocol that backends can implement to provide transactional operations."
  (-multi-get [this keys opts]
    "Atomically retrieves multiple values by keys.
     Takes a collection of keys and returns a sparse map containing only found keys.
     Uses flat keys only (not key-vecs).
     Returns a map {key -> value} for all found keys. Missing keys are excluded from result.")
  (-multi-assoc [this kvs meta-up-fn opts]
    "Atomically associates multiple key-value pairs with flat keys.
     Takes a map of keys to values and stores them in a single atomic transaction.
     All operations must succeed or all must fail (all-or-nothing semantics).
     Returns a map of keys to results (typically true for each key).")
  (-multi-dissoc [this kvs opts]
    "Atomically dissociates multiple keys with flat keys.
     Takes a collection of keys to remove and deletes them in a single atomic transaction.
     All operations must succeed or all must fail (all-or-nothing semantics).
     Returns a map of keys to results (typically true for each key)."))

(defprotocol PBinaryKeyValueStore
  "Allows binary data byte array storage."
  (-bget [this key locked-cb opts] "Calls locked-cb with a platform specific binary representation inside the lock, e.g. wrapped InputStream on the JVM and Blob in JavaScript. You need to properly close/dispose the object when you are done!")
  (-bassoc [this key meta-up-fn val opts] "Copies given value (InputStream, Reader, File, byte[] or String on JVM, Blob in JavaScript) under key in the store."))

(defprotocol PAssocSerializers
  (-assoc-serializers [this serializers] "Assoc serializers onto this store."))

(defprotocol PKeyIterable
  "Allows lazy iteration of keys in this store."
  (-keys [this opts]
    "Return a channel that will continuously yield keys in this store."))

(defprotocol PStoreSerializer
  "Decouples serialization format from storage."
  (-serialize [this output-stream write-handlers val]
    "For the JVM we use streams, while for JavaScript we return the value for now.")
  (-deserialize [this read-handlers input-stream]))

(defprotocol PWriteHookStore
  "Protocol for stores that support write hooks.
   Write hooks are callbacks invoked after successful write operations.
   Stores just need to hold the hooks atom - invocation happens at the API layer (konserve.core)."
  (-get-write-hooks [this]
    "Returns the write-hooks atom containing a map of {hook-id hook-fn}, or nil if not supported.")
  (-set-write-hooks! [this hooks-atom]
    "Set the write-hooks atom. Returns the modified store."))

(defprotocol PLockFreeStore
  "Protocol for stores that handle concurrency internally (e.g., MVCC backends like LMDB).
   These stores don't need application-level locking for read/write operations."
  (-lock-free? [this]
    "Returns true if the store handles concurrency internally and doesn't need
     application-level locking. Default is false for all stores."))

(def store-config-key
  "Where a connected store carries the config it was connected with, when the
   backend has no field of its own for it. Namespaced, so attaching it to a
   record lands in the extension map without disturbing the declared fields.

   NOT `:config` — a `DefaultStore` already has one of those, holding the
   backend's behaviour options (`:in-place?`, `:lock-blob?`, `:sync-blob?`).
   Two different maps, and overwriting one with the other would break every
   backend that reads it."
  ::store-config)

(def credential-keys
  "Config keys stripped before a config is attached to a store.

   Store configs carry secrets — `:access-key` and `:secret` for S3,
   `:password` and `:jdbcUrl` for JDBC. Those are fine in a config a caller
   holds and passes once; they are not fine sitting on a long-lived object that
   any `pr-str`, log line or `ex-info` payload might carry off. Identity
   (`:backend`, `:id`, `:path`, `:bucket`, …) survives, which is what makes the
   attached config useful.

   A backend with its own secret-bearing keys should add them here rather than
   hope nobody prints a store."
  #{:access-key :secret-access-key :aws-secret-access-key
    :secret :secret-key :private-key
    :password :passphrase
    :token :session-token :credentials
    :api-key
    :jdbcUrl :jdbc-url :connection-uri})

(defn strip-credentials
  "`config` with every credential key removed, AT EVERY DEPTH.

   Depth is the point. Konserve configs NEST: a `:tiered` store's config holds a
   whole `:frontend-config` and `:backend-config`, each a complete store config
   with its own `:access-key` or `:password` (see `konserve.store`'s `:tiered`
   methods). A top-level `dissoc` walks straight past those and attaches them to
   the store, which is worse than not attaching a config at all — before, they
   were only in a map the caller held.

   The encryptor is handled by name rather than by key set: konserve's own AES
   key lives at `:encryptor {:type :aes :key ...}`, and `:key` is far too common
   a word to put in `credential-keys` — stripping it everywhere would gut
   ordinary configs."
  [config]
  (clojure.walk/postwalk
   (fn [x]
     (if (map? x)
       (cond-> (apply dissoc x credential-keys)
         (map? (:encryptor x)) (update :encryptor dissoc :key))
       x))
   config))

(defprotocol PStoreConfig
  "What config is this store connected with?

   `konserve.store/validate-store-config` REQUIRES a UUID `:id` on every store,
   and then every backend drops the config — nothing on a connected store
   carried it. A `DefaultStore`'s `:config` is a different map (behaviour
   options), and backends that bypass `DefaultStore` (LMDB) keep less still.

   That left components which must AGREE on a store's identity — the GC safe
   point above all, where datahike, geschichte and a scriptum index share one
   store and one sweep — passing the id alongside the store by hand.
   Disagreement there is invisible until a collection deletes something.

   The default implementation reads what `konserve.store` attaches on connect,
   so no backend has to do anything. A backend that would rather hold its config
   in a real field can implement this and be authoritative.

   Credential keys are stripped — see `credential-keys`. The result identifies a
   store; it is not guaranteed to be enough to reconnect one."
  (-store-config [this]
    "The (credential-stripped) config this store was connected with, or nil for
     a store built through a backend constructor directly, which never took one."))

;; Default implementations for Object

(extend-protocol PStoreConfig
  #?(:clj Object :cljs default)
  (-store-config [this] (get this store-config-key))
  nil
  (-store-config [_] nil))

(defn store-id
  "The UUID this store was connected with, or nil. Convenience over
   `-store-config`, since identity is what most callers actually want."
  [store]
  (:id (-store-config store)))

(extend-protocol PMultiKeySupport
  #?(:clj Object :cljs default)
  (-supports-multi-key? [_] false))

(extend-protocol PWriteHookStore
  #?(:clj Object :cljs default)
  (-get-write-hooks [_] nil)
  (-set-write-hooks! [this _] this))

(extend-protocol PLockFreeStore
  #?(:clj Object :cljs default)
  (-lock-free? [_] false))

