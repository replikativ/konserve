# konserve

A key-value store protocol implemented with [core.async](https://github.com/clojure/core.async) to allow Clojuresque collection operations on associative key-value stores, both from Clojure and ClojureScript. The protocol is implemented at the moment for CouchDB with [clutch](https://github.com/clojure-clutch/clutch) on Clojure and with [IndexedDB](https://developer.mozilla.org/en-US/docs/IndexedDB) for ClojureScript. Each operation is run atomically and must be consistent, but further consistency is not supported (Riak, CouchDB and many scalable solutions don't have transactions for that reason). New storage backends, e.g. Riak, JDBC, WebSQL, bare file system... are welcome. This was initially implemented as a storage protocol for [geschichte](https://github.com/ghubber/geschichte).

## Usage

From a browser (you need IndexedDB available in your js env) or analogous from a Clojure REPL with CouchDB run:

    (go (def my-db (<! (new-indexeddb-store "konserve"))))

    (go (println "get:" (<! (-get-in my-db ["test" :a]))))

    (go (doseq [i (range 10)]
          (<! (-assoc-in my-db [i] i))))

    (go (doseq [i (range 10)]
          (println (<! (-get-in my-db [i])))))
    ;; prints 0 to 9 each on a line

    (go (println (<! (-assoc-in my-db ["test"] {:a 1 :b 4.2}))))

    (go (println (<! (-update-in my-db ["test" :a] inc))))
    ;; => "test" contains {:a 2 :b 4.2}

## License

Copyright © 2014 Christian Weilbach

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
