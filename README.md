# konserve

A key-value store protocol implemented with [core.async](https://github.com/clojure/core.async) to allow Clojuresque collection operations on associative key-value stores, both from Clojure and ClojureScript for different backends. Data is generally serialized with [edn](https://github.com/edn-format/edn) semantics or if supported as native binary blobs and can be accessed similar to `clojure.core` functions `get-in`,`assoc-in` and `update-in`. Each operation is run atomically and must be consistent, but further consistency is not supported (Riak, CouchDB and many scalable solutions don't have transactions for that reason). This is meant to be a building block for more sophisticated storage solutions.

You can read write custom records with edn, but you have to supply the proper reader-functions through a store-wide tag-table atom, in the format of `{'namespace.Symbol (fn [val] ...realize proper object...)}` (this might differ depending on the backend), which can be bound to a runtime wide atom in case you don't have different data schemas and code-versions to deal with. You can omit it, if you don't use tagged literals in your edn.

There is a JSON store protocol implemented for IndexedDB in case interoperability with a JavaScript application is wanted. Be careful not to collide values with edn, use different stores if possible.

The protocol is implemented at the moment for CouchDB with [clutch](https://github.com/clojure-clutch/clutch) and an experimental file-system store in Clojure and for [IndexedDB](https://developer.mozilla.org/en-US/docs/IndexedDB) in ClojureScript. The file-system store currently uses [fressian](https://github.com/clojure/data.fressian) and is quite efficient. For CouchDB and IndexedDB there is no internal JSON-representation of the underlying store like [transit](https://github.com/cognitect/transit-clj), yet, hence they are fairly slow.  New storage backends, e.g. Riak, JDBC, WebSQL, Local-Storage, bare file system... are welcome. This was initially implemented as a storage protocol for [geschichte](https://github.com/ghubber/geschichte).

## Usage

Add to your leiningen dependencies:

~~~clojure
[net.polyc0l0r/konserve "0.1.5"]
~~~

From a browser (you need IndexedDB available in your js env) or analogous from a Clojure REPL with CouchDB run:

~~~clojure
(ns test-db
  (:require [konserve.platform :refer [new-indexeddb-store]]))

(go (def my-db (<! (new-indexeddb-edn-store "konserve"))))
;; or (not stable API yet!):
(go (def my-db (<! (new-indexeddb-json-store "konserve-json"))))

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

## TODO
- allow to iterate keys
- move repl examples to tests

## License

Copyright © 2014 Christian Weilbach

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
