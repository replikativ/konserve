(ns konserve.protocols
  (:require [clojure.string :as str])
  #?(:cljs (:refer-clojure :exclude [-dissoc]))
  #?(:cljs (:require-macros [konserve.protocols :refer [expand-store-protocols]])))

#?(:clj
   (defmacro expand-store-protocols [prefix]
     (let [prefix-sym (fn [n] (symbol (str "-" prefix (name n))))]
       `(do
          (defprotocol ~(symbol (str "P" (str/capitalize prefix) "EDNKeyValueStore"))
            "Allows to access a store similar to hash-map in EDN."
            (~(prefix-sym '-exists?) [~'this ~'key] "Checks whether value is in the store.")
            (~(prefix-sym '-get-meta) [~'this ~'key] "Fetch only metadata for the key.")
            (~(prefix-sym '-get-version) [~'this ~'key] "Fetch version for the key.")
            (~(prefix-sym '-get) [~'this ~'key] "Returns the value stored described by key or nil if the path is not resolvable.")
            (~(prefix-sym '-update-in) [~'this ~'key-vec ~'meta-up-fn ~'up-fn ~'up-fn-args]
              "Updates a position described by key-vec by applying up-fn and storing the result atomically. Returns a vector [old new] of the previous value and the result of applying up-fn (the newly stored value).")
            (~(prefix-sym '-assoc-in) [~'this ~'key-vec ~'meta-up-fn ~'val])
            (~(prefix-sym '-dissoc) [~'this ~'key]))

          (defprotocol ~(symbol (str "P" (str/capitalize prefix) "BinaryKeyValueStore"))
            "Allows binary data byte array storage."
            (~(prefix-sym '-bget) [~'this ~'key ~'locked-cb] "Calls locked-cb with a platform specific binary representation inside the lock, e.g. wrapped InputStream on the JVM and Blob in JavaScript. You need to properly close/dispose the object when you are done!")
            (~(prefix-sym '-bassoc) [~'this ~'key ~'meta-up-fn ~'val] "Copies given value (InputStream, Reader, File, byte[] or String on JVM, Blob in JavaScript) under key in the store."))

          (defprotocol ~(symbol (str "P" (str/capitalize prefix) "KeyIterable"))
            "Allows lazy iteration of keys in this store."
            (~(prefix-sym '-keys) [~'this]
              "Return a channel that will continuously yield keys in this store."))))))

(expand-store-protocols async)

(expand-store-protocols sync)

(defprotocol PStoreSerializer
  "Decouples serialization format from storage."
  (-serialize [this output-stream write-handlers val]
    "For the JVM we use streams, while for JavaScript we return the value for now.")
  (-deserialize [this read-handlers input-stream]))
