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
