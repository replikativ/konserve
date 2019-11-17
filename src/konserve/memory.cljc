(ns konserve.memory
  "Address globally aggregated immutable key-value store(s)."
  (:require #?(:clj [clojure.core.async :refer [go]])
            [konserve.key-compare :as kc]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        PBinaryAsyncKeyValueStore
                                        -update-in
                                        PKeyIterable
                                        -keys]]
            [clojure.core.async :as async])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go]])))

(defrecord MemoryStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] (go (if (@state key) true false)))
  (-get-in [this key-vec] (go (get-in @state key-vec)))
  (-update-in [this key-vec up-fn] (-update-in this key-vec up-fn []))
  (-update-in [this key-vec up-fn args] (go [(get-in @state key-vec)
                                             (get-in (apply swap! state update-in key-vec up-fn args) key-vec)]))
  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))
  (-dissoc [this key] (go (swap! state dissoc key) nil))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (locked-cb (get @state key)))
  (-bassoc [this key input]
    (go (swap! state assoc key {:input-stream input
                                :size :unknown})
        nil))

  PKeyIterable
  (-keys [this] (-keys this nil))

  (-keys [_ start-key]
    (let [ks (drop-while #(and (some? start-key) (neg? (compare % start-key))) (keys @state))]
      (async/to-chan ks))))

#?(:clj
   (defmethod print-method MemoryStore
     [^MemoryStore store writer]
     (.write writer (str "MemoryStore[\"" (.hasheq store) "\"]"))))


(defn new-mem-store
  "Create in memory store. Binaries are not properly locked yet and
  the read and write-handlers are dummy ones for compatibility."
  ([] (new-mem-store (atom {})))
  ([init-atom]
   (go
     (swap! init-atom #(into (apply sorted-map-by kc/key-compare (mapcat identity %))))
     (map->MemoryStore {:state init-atom
                        :read-handlers (atom {})
                        :write-handlers (atom {})
                        :locks (atom {})}))))


(comment
  (require '[clojure.core.async :refer [<!! go <!]]
           '[konserve.protocols :refer [-bget -bassoc]]
           '[clojure.java.io :as io])
  (def store (<!! (new-mem-store)))

  (go (def foo (<! (new-mem-store))))


  (<!! (-bassoc store "foo" (io/input-stream (byte-array 10 (byte 42)))))
  (<!! (-bget store "foo" identity)))


