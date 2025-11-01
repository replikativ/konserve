(ns konserve.protocols
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

(defprotocol PKeyIterable
  "Allows lazy iteration of keys in this store."
  (-keys [this opts]
    "Return a channel that will continuously yield keys in this store."))

(defprotocol PStoreSerializer
  "Decouples serialization format from storage."
  (-serialize [this output-stream write-handlers val]
    "For the JVM we use streams, while for JavaScript we return the value for now.")
  (-deserialize [this read-handlers input-stream]))

;; Default implementations for Object

#?(:clj
   (extend-protocol PMultiKeySupport
     Object
     (-supports-multi-key? [_] false)))
