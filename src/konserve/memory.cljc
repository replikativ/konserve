(ns konserve.memory
  "Address globally aggregated immutable key-value store(s)."
  (:require #?(:clj [clojure.core.async :refer [go]])
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        PAppendStore
                                        PBinaryAsyncKeyValueStore]])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go]])))

(defrecord MemAsyncKeyValueStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] (go (if (@state key) true false)))
  (-get-in [this key-vec] (go (get-in @state key-vec)))
  (-update-in [this key-vec up-fn] (go [(get-in @state key-vec)
                                        (get-in (swap! state update-in key-vec up-fn) key-vec)]))
  PAppendStore
  (-append [this key elem]
    (go (swap! state update key (fn [old] (conj (or old []) elem)))
        nil))
  (-log [this key]
    (go (get @state key)))
  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (go (locked-cb (get @state key))))
  (-bassoc [this key input]
    (go (swap! state assoc key {:input-stream input
                                :size :unknown})
        nil)))

(defn new-mem-store
  "Create in memory store. Binaries are not properly locked yet and
  the read and write-handlers are dummy ones for compatibility."
  ([] (new-mem-store (atom {})))
  ([init-atom]
   (go (map->MemAsyncKeyValueStore {:state init-atom
                                    :read-handlers (atom {})
                                    :write-handlers (atom {})
                                    :locks (atom {})}))))


(comment
  (require '[clojure.core.async :refer [<!!]]
           '[konserve.protocols :refer [-bget -bassoc -append -log]]
           '[clojure.java.io :as io])
  (def store (<!! (new-mem-store)))

  (<!! (-append store :foo :bar))
  (<!! (-log store :foo))

  (<!! (-bassoc store "foo" (io/input-stream (byte-array 10 (byte 42)))))
  (<!! (-bget store "foo" identity))

  )
