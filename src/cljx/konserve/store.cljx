(ns konserve.store
  "Address globally aggregated immutable key-value store(s)."
  (:require #+clj
            [clojure.core.async :refer [go]]
            [clojure.java.io :as io]
            [konserve.protocols :refer [IEDNAsyncKeyValueStore
                                        IBinaryAsyncKeyValueStore]])
  #+cljs (:require-macros [cljs.core.async.macros :refer [go]]))

(defrecord MemAsyncKeyValueStore [state tag-table]
  IEDNAsyncKeyValueStore
  (-get-in [this key-vec] (go (get-in @state key-vec)))
  (-assoc-in [this key-vec value] (go (swap! state assoc-in key-vec value)
                                      nil))
  (-update-in [this key-vec up-fn] (go [(get-in @state key-vec) ;; HACK, can be inconsistent! (but only old can be too old); alternatively track old
                                        (get-in (swap! state update-in key-vec up-fn) key-vec)]))
  IBinaryAsyncKeyValueStore
  (-bget [this key]
    (go (get @state key)))
  (-bassoc [this key input]
    (go (swap! state assoc key {:input-stream input
                                :size :unknown})
        nil)))

(defn new-mem-store
  ([] (new-mem-store (atom {})))
  ([init-atom] (new-mem-store init-atom (atom {})))
  ([init-atom tag-table]
     (go (MemAsyncKeyValueStore. init-atom tag-table))))


(comment
  (require '[clojure.core.async :refer [<!!]]
           '[konserve.protocols :refer [-bget -bassoc]]
           '[clojure.java.io :as io])
  (def store (<!! (new-mem-store)))

  (<!! (-bassoc store "foo" (io/input-stream (byte-array 10 (byte 42)))))
  (<!! (-bget store "foo"))

  )
