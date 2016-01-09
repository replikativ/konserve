(ns konserve.memory
  "Address globally aggregated immutable key-value store(s)."
  (:require #?(:clj [clojure.core.async :refer [go]])
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        PBinaryAsyncKeyValueStore]])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go]])))

(defrecord MemAsyncKeyValueStore [state read-handlers write-handlers]
  PEDNAsyncKeyValueStore
  (-exists? [this key] (go (if (@state key) true false)))
  (-get-in [this key-vec] (go (get-in @state key-vec)))
  (-update-in [this key-vec up-fn] (go [(get-in @state key-vec)
                                        ;; HACK, can be inconsistent!
                                        ;; (but only old can be too
                                        ;; old); alternatively track
                                        ;; old
                                        (get-in (swap! state update-in key-vec up-fn) key-vec)]))
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
                                    :write-handlers (atom {})}))))


(comment
  (require '[clojure.core.async :refer [<!!]]
           '[konserve.protocols :refer [-bget -bassoc]]
           '[clojure.java.io :as io])
  (def store (<!! (new-mem-store)))

  (<!! (-bassoc store "foo" (io/input-stream (byte-array 10 (byte 42)))))
  (<!! (-bget store "foo" identity))

  )
