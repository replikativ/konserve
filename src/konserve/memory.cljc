(ns konserve.memory
  "Address globally aggregated immutable key-value store(s)."
  (:require #?(:clj [clojure.core.async :refer [go <!!]])
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -get -get-meta -update-in -dissoc -assoc-in
                                        PBinaryAsyncKeyValueStore
                                        -update-in
                                        PBinaryAsyncKeyValueStore
                                        -bget -bassoc
                                        PKeyIterable
                                        -keys]]
            [clojure.core.async :as async])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go]])))


(defrecord MemoryStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] (go (if (@state key) true false)))
  (-get [this key] (go (second (get @state key))))
  (-get-meta [this key] (go (first (get @state key))))
  (-update-in [this key-vec meta-up-fn up-fn args]
    (go
      (let [[fkey & rkey] key-vec
            update-atom
            (fn [store]
              (swap! store
                     (fn [old]
                       (update old fkey
                               (fn [[meta data]]
                                 [(meta-up-fn meta)
                                  (if rkey
                                    (apply update-in data rkey up-fn args)
                                    (apply up-fn data args))])))))]
        [(second (get @state fkey))
         (second (get (update-atom state) fkey))])))
  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))
  (-dissoc [this key] (go (swap! state dissoc key) nil))
  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (locked-cb (second (get @state key))))
  (-bassoc [this key meta-up-fn input]
    (go
        (swap! state
               (fn [old]
                 (update old key
                         (fn [[meta data]]
                           [(meta-up-fn meta) {:input-stream input
                                               :size         :unknown}]))))
        nil))
  PKeyIterable
  (-keys [_]
    (go (set (map first (vals @state))))))

#?(:clj
   (defmethod print-method MemoryStore
     [^MemoryStore store writer]
     (.write ^java.io.StringWriter writer (str "MemoryStore[\"" (.hasheq store) "\"]"))))

(defn new-mem-store
  "Create in memory store. Binaries are not properly locked yet and
  the read and write-handlers are dummy ones for compatibility."
  ([] (new-mem-store (atom {})))
  ([init-atom]
   (go (map->MemoryStore {:state init-atom
                          :read-handlers (atom {})
                          :write-handlers (atom {})
                          :locks (atom {})}))))

