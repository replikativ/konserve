# konserve

A key-value store protocol implemented with [core.async](https://github.com/clojure/core.async) to allow Clojuresque collection operations on associative key-value stores, both from Clojure and ClojureScript. All data is serialized as [edn](https://github.com/edn-format/edn)  and can be accessed similar to `clojure.core` functions `get-in`,`assoc-in` and `update-in`. There is no internal JSON-representation of the underlying store, yet. This could bring speed-up in certain scenarios. For this a shallow representation of read-only edn in JSON form could be used for native JSON-stores. It has to implement the proper cljs protocols, to create copies of persistent data-structures when it is used. There is a JSON store in case the `edn` serialization is too slow for you at the moment (but this is not considered part of the API yet). 

You can read write custom records with edn, but you have to supply the proper reader-functions through the binding, on `cljs cljs.reader/*tag-table*` and `cljs.reader/*default-data-reader-fn*`. These are *best configured runtime wide*, if you don't need separate bindings, through: `cljs.reader/register-tag-parser!` and `cljs.reader/register-default-tag-parser`. On Clojure you use a map conform to `clojure.edn/read` for the binding `konserve.platform/*read-opts*`: `{:readers {'namespace.Symbol (fn [val] (println val))}, :default (fn [tag val] (println tag val))}`, which also can be bound to a runtime wide constant var in most cases.

The protocol is implemented at the moment for CouchDB with [clutch](https://github.com/clojure-clutch/clutch) in Clojure and for [IndexedDB](https://developer.mozilla.org/en-US/docs/IndexedDB) in ClojureScript. Each operation is run atomically and must be consistent, but further consistency is not supported (Riak, CouchDB and many scalable solutions don't have transactions for that reason). New storage backends, e.g. Riak, JDBC, WebSQL, Local-Storage, bare file system... are welcome. This was initially implemented as a storage protocol for [geschichte](https://github.com/ghubber/geschichte).

## Usage

Add to your leiningen dependencies:

~~~clojure
[net.polyc0l0r/konserve "0.1.3"]
~~~

From a browser (you need IndexedDB available in your js env) or analogous from a Clojure REPL with CouchDB run:

~~~clojure
(ns test-db
  (:require [konserve.platform :refer [new-indexeddb-store]]))

(go (def my-db (<! (new-indexeddb-store "konserve"))))
;; or (not stable API yet!):
(go (def my-db (<! (new-indexeddb-store "konserve" :json))))

(go (println "get:" (<! (-get-in my-db ["test" :a]))))

(go (doseq [i (range 10)]
       (<! (-assoc-in my-db [i] i))))

(go (doseq [i (range 10)]
      (println (<! (-get-in my-db [i])))))
;; prints 0 to 9 each on a line

(go (println (<! (-assoc-in my-db ["test"] {:a 1 :b 4.2}))))

(go (println (<! (-update-in my-db ["test" :a] inc))))
;; => "test" contains {:a 2 :b 4.2}
~~~
    
For simple purposes a memory store is implemented as well:

~~~clojure
(ns test-db
  (:require [konserve.store :refer [new-mem-store]]))

(go (def my-db (<! (new-mem-store)))) ;; or
(go (def my-db (<! (new-mem-store (atom {:init 42})))))
~~~

## License

Copyright © 2014 Christian Weilbach

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
