(ns ^:shared konserve.protocols)

(defprotocol IEDNAsyncKeyValueStore
  "Allows to access a store similar to hash-map in EDN."
  (-get-in [this key-vec] "Returns the value stored described by key-vec or nil if the path is not resolvable.")
  (-assoc-in [this key-vec value] "Associates the key-vec to the value, any missing collections for the key-vec (nested maps and vectors) are newly created.")
  (-update-in [this key-vec up-fn] "Updates a position described by key-vec by applying up-fn and storing the result atomically. Returns a vector [old new] of the previous value and the result of applying up-fn (the newly stored value)." ))


(defprotocol IJSONAsyncKeyValueStore
  "SUBJECT TO CHANGE. Low-Level JSON implementation to give native performance. Might be merged into EDN later, when we find a comparable EDN solution. Assumes all values are JSON. Avoid using this protocol on the same store as an edn store."
  (-jget-in [this key-vec] "Returns the value stored described by key-vec or nil if the path is not resolvable.")
  (-jassoc-in [this key-vec value] "Associates the key-vec to the value, any missing collections for the key-vec (nested maps and vectors) are newly created.")
  (-jupdate-in [this key-vec up-fn] "Updates a position described by key-vec by applying up-fn and storing the result atomically. Returns a vector [old new] of the previous value and the result of applying up-fn (the newly stored value)." ))


(defprotocol IBinaryAsyncKeyValueStore
  "Allows binary data byte array storage."
  (-bget [this key] "Returns a platform specific binary representation, e.g. wrapped InputStream on the JVM and Blob in JavaScript. You need to properly close/dispose the object when you are done!")
  (-bassoc [this key val] "Copies given value (InputStream, Reader, File, byte[] or String on JVM, Blob in JavaScript) under key in the store."))
