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

(defprotocol PConditionalWrite
  "Stores that can make a write conditional on the revision the caller read.

   The point of the capability being explicit is that a store which CANNOT do
   this must REFUSE `:expected-revision`, not ignore it. Silently degrading to an
   unconditional write is worse than having no fencing at all: the caller has
   asked for a guarantee, and would get a knob that reads as handled."
  (-conditional-write-domain [this]
    "How far this store's conditional writes actually reach, or nil for not at all.

     Named for what it RETURNS. It was `-conditional-write?` for a while, which
     reads as a predicate to every backend author who has to implement it — and a
     domain answered as if it were a boolean is the precise confusion this whole
     protocol exists to prevent. `konserve.core/conditional-write?` keeps the `?`,
     because that one really is a predicate: it compares a domain you need against
     the domain a store has.


       nil       cannot compare-and-write; `:expected-revision` is REFUSED
       :process  atomic against other threads in this runtime  (memory: one `swap!`)
       :machine  atomic against other processes on this host   (JVM filestore: an
                 OS advisory lock; node filestore: an O_EXCL lockfile, which a
                 crashed writer leaves behind where the kernel would have
                 released an advisory one. NOT across NFS or any network
                 filesystem, where advisory locks are unreliable)
       :global   atomic against every writer anywhere          (S3: If-Match)

     A DOMAIN rather than a boolean because the API is uniform and the guarantee is
     not. `true` would let a caller planning to run two processes against a memory
     store — or two machines against a filestore — believe they are fenced when they
     are only serialized against a narrower set of writers. That is the same failure
     the capability exists to prevent, one level finer: appearing fenced without
     being fenced.

     Ordered weakest-first, so a caller can state the domain it needs and compare.

     WHAT `:machine` EXCLUDES, precisely, because it is easy to assume too much:
     every writer to that KEY, not merely the ones that also fence. That has to
     hold — konserve replaces a value by renaming a new file over it, so a fenced
     write whose lock was taken on the old file would compare its revision
     against a detached one, pass, and overwrite the value that replaced it. So
     the lock lives on a sidecar that is never renamed, and every write to a key
     that HAS one takes it. A key gets one the first time a conditional write or
     a revision-bearing read touches it; keys that are never fenced pay only an
     existence probe.

     The residual, stated because a fence with an unstated hole is worse than no
     fence: the FIRST such write or read on a key can still race an unconditional
     write, since there is nothing to lock until the sidecar exists. It is
     self-healing — once created the key is covered for good — and a caller that
     re-reads a pointer before writing it (which is what fencing implies) creates
     it on that read, before its first conditional write.

     WHAT A DOMAIN CLAIMS IS A MECHANISM, not an intention. konserve's default
     backing enforces the comparison in `konserve.impl.defaults/io-operation`:
     read the stored revision and write the new value while holding a local lock.
     That is genuinely atomic against threads (`:process`) and, with an OS
     advisory lock on the blob, against processes on the host (`:machine`) — and
     it cannot reach further, because nothing in it is visible to another
     machine. A store that answers `:global` therefore does NOT get there through
     `io-operation`; it must implement the comparison in its own storage layer,
     where the guarantee actually lives — konserve-s3 does it with a conditional
     PUT (`If-Match` on the object's ETag), which S3 evaluates. Answering
     `:global` while relying on the default compare would be the exact failure
     this protocol exists to prevent, written one layer down.")
  (-revision [this key opts]
    "The token to hand back as `:expected-revision`, or the absent sentinel.

     Its OWN function rather than a field of `-get-meta`, because the two can
     disagree: a tiered store answers metadata from whichever tier its read-policy
     names, while the conditional write is evaluated by the tier that owns the
     durable value. Reading the revision from the frontend and comparing it
     against the backend's compares two independent counters, which is worse than
     not comparing at all. This function always resolves to the tier that decides."))

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

   WORTH DOING FOR IDENTITY, not just for storage: the attached `:id` is konserve's
   LOGICAL identity, deliberately the same across machines and backends holding
   one store. The GC safe point needs an id that is never FINER than the bytes a
   sweep deletes (see `konserve.gc-guard`), and a backend that knows its own
   physical location can return one derived from it — collapsing two connections
   to one path onto a single guard key, and separating replicas that would
   otherwise hold each other's collections back. Nothing else in konserve reads
   `store-id`, so overriding it costs nothing elsewhere.

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

