# Konserve API Walkthrough

## The big picture
+ persist key-value pairs with schemaless data, typically serialized edn
+ keys are created via [hasch](https://github.com/replikativ/hasch) such that the keys themselves can be arbitrary edn
+ the `konserve.core` api feels just like a clojure map except it has mutable persistence behind it
+ the core functions can be used synchronously or asynchronously
+ the details of the host system are abstracted away via protocols into host store implementations. sync vs async support is host dependent.
+ konserve provides 4 built in stores
  - konserve.filestore
  - konserve.node-filestore
  - konserve.indexeddb
  - konserve.memory
+ there are many others:
  - https://github.com/replikativ/konserve-s3
  - https://github.com/replikativ/konserve-jdbc
  - https://github.com/replikativ/konserve-redis
  - https://github.com/alekcz/konserve-fire
  - https://github.com/search?q=konserve&type=repositories&p=1

<hr>

## Connecting Stores

```clojure
(require '[konserve.filestore :refer [connect-fs-store]])

(def store (<!! (connect-fs-store "/tmp/store")))



(require '[konserve.node-filestore :refer [connect-fs-store]])
;; node-js supports sync but no <!!
(def store (connect-fs-store "/tmp/store" :opts {:sync? true}))



;; in the browser
(require '[konserve.indexeddb :refer [connect-idb-store]])

(go
  ;; indexeddb is async only!
  (let [store (<! (connect-idb-store "idb-store"))]
    ...))
```

Stores also accept var-args. Support for each entry varies per implementation

+ `:opts` => `{:sync? <boolean>}`
  - This is an env map passed around by most functions internally within konserve. The only entry you should typically need to concern yourself with is `:sync?` which is used to control whether functions return channels or values
  - an opts map is the last parameter accepted by `konserve.core` functions, but for creating stores, it must be identified by the keyword `:opts`
+ `:config` => map
  - this map includes options for manipulating blobs in store specific ways. Very rarely should you ever need to alter the defaults
+ `:buffer-size` => number
  - in clj this lets you control the chunk size used for writing blobs. the default is 1mb
+ `:default-serializer` => keyword
  - the default serializer is `:FressianSerializer`, but you can override with a keyword identifying a different serializer implementation
  - `(connect-store store-name :default-serializer :StringSerializer)` => writes string edn
  - jvm also supports `:CBORSerializer`
  - you can provide your own serializer by giving a map of `{:MySerializer PStoreSerializerImpl}` to `:serializers` (..see next bullet) and then referencing it via `:default-serializer :MySerializer`

+ `:serializers` => Map<keyword, PStoreSerializerImpl>
  - this is where you can provide your own serializer to reference via `:default-serializer`
  - `konserve.serializers/fressian-serializer` is a convenience function that accepts 2 maps: a map of read-handlers and a map of write-handlers and returns a fressian serializer supporting your custom types
    - in clj the handlers are reified `org.fressian.ReadHandlers` & `org.fressian.WriteHandlers`
      - see [https://github.com/clojure/data.fressian/wiki/Creating-custom-handlers](https://github.com/clojure/data.fressian/wiki/Creating-custom-handlers)
    - in cljs handlers are just functions
      - see [https://github.com/pkpkpk/fress](https://github.om/pkpkpk/fress)

+ `:encryptor` => `{:type :aes :key "s3cr3t"}`
  - currently only supports `:aes` in default stores

+ `:compressor` => `{:type :lz4}`
  - currently LZ4 compression is only supported on the jvm

### Incognito & Records
Konserve intercepts records and writes them as [incognito](https://github.com/replikativ/incognito) tagged literals such that the details of serialization formats are abstracted away and allowing easier interop between different formats. The `:read-handlers` and `:write-handlers` varg args are explicitly meant for working with incognito's tagged literals.
  - `:read-handlers` expects an atom wrapping `{'symbol.for.MyRecord map->MyRecord}` for recovering records from incognito tagged literals
  - `:write-handlers` expects an atom wrapping `{'symbol.for.MyRecord (fn [record] ..)}` for writing records as incognito tagged literals
  - the symbols used in these maps **are not safe for clojurescript** so you should avoid using them

<hr>


## Working with data
Once you have a store you can access it using `konserve.core` functions. By default functions are asynchronous and return channels yielding values or errors. You can override this by passing an opts map with `:sync? true`

```clojure
(require '[konserve.core :as k])

(k/exists? store :false) ;=> channel<false>

(<!! (k/assoc store :fruits {:parcha nil :mango nil :banana nil}))

(k/exists? store :fruits {:sync? true}) ;=> true
```

You can `get` `get-in` `update` `update-in` and `dissoc` just like a clojure map.

```clojure
(k/assoc-in store [:fruits :parcha] {:color "yellow" :taste "sour" :quantity 0})

(k/update-in store [:fruits :parcha :quantity] inc)

(k/get-in store [:fruits :parcha :quantity]) ;=> channel<1>

(k/dissoc store :fruits)
```

In the fruits example a simple keyword is the store key, but keys themselves can be arbitrary edn:

```clojure
(defn memoize-to-store-sync [f]
  (fn [& args]
    (if-let [result (<!! (k/get store args))]
      result
      (let [result (apply f args)]
        (<!! (k/assoc store args result))
        result))))

(def memoized-fn (memoize-to-store-sync expensive-fn))

(memoized-fn {:any/such #{"set"}}, [0x6F \f], 'haschable.argu/ments) ;=> channel<result>
```

## Working with binary data

```clojure
(k/bassoc store :blob blob)

(k/bget store :blob
              (fn locked-cb [{is :input-stream}]
                (go (input-stream->byte-buffer is)))) ;=> ch<bytebuffer>
```
With `bassoc` binary data is written as-is without passing through serialization/encryption/compression.

`bget` is probably the trickiest function in konserve. It accepts a callback function that is passed a map of `{:input-stream <host-input-stream>}`. While `locked-cb`  is running, konserve locks & holds onto underyling resources (ie file descriptors) until the function exits. You can choose to read from the input stream however you like, but rather than running side-effects within the callback, you should instead return your desired value else the lock will remain held.

+ when called async, the `locked-cb` should return a channel yielding the desired value that will be read from and yielded by `bget`'s channel
+ in both clojurescript stores, synchronous input streams are not possible.
+ On nodejs you can call `bget` synchronously but the locked-cb will be called with `{:blob js/Buffer}`
+ In the browser with indexedDB, the async only `bget` calls its locked-cb with `{:input-stream <readable-webstream> :offset <number>}` where offset indicates the amount of bytes to drop before reaching the desired blob start.
  - `konserve.indexeddb/read-web-stream` can serve as a locked-cb that will yield a `Uint8Array`.

## Metadata

Konserve does some bookkeeping for values by storing them with metadata

```clojure
(k/get-meta store :key {:sync? true})
;=>
;  {:key :key
;   :type <binary|edn>
;   :last-write <inst>}
```

## The append log
Konserve provides an append log for writing values quickly. These entries are a special case managed by konserve, where the sequence is stored as a linked list of blobs where each blob is a cons cell of the list. You can name the log with any key, but that key should only be written to or read from using the `append`, `log`, and `reduce-log` functions.

```clojure
(dotimes [n 6]
  (<!! (k/append store :log n)))

(k/get store :log) ;=> channel<(0 1 2 3 4 5)>

(k/reduce-log store :log
              (fn [acc n]
                (if (even? n)
                  (conj acc n)
                  acc))
              []) ;=> channel<[0 2 4]>
```

## konserve.gc

`konserve.gc/sweep!` lets you prune the store based on a whitelist set of keys to keep and a timestamp cutoff before which un whitelisted entries should be deleted

## konserve.cache

`konserve.cache/ensure-cache` wraps a store with a lru-cache to avoid hitting external memory for frequently accessed keys
