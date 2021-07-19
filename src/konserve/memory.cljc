(ns konserve.memory
  "Address globally aggregated immutable key-value store(s)."
  (:require [konserve.protocols :refer [PEDNAsyncKeyValueStore -update-in
                                        PBinaryAsyncKeyValueStore PKeyIterable]]
            #?(:clj [konserve.utils :refer [async+sync]])
            [clojure.core.async :as async :refer [go <!]])
  #?(:cljs (:require-macros [konserve.utils :refer [async+sync]])))

(defrecord MemoryStore [state read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [_ key sync?]
    (async+sync sync?
                {go do
                 <! do}
                (go  (if (@state key) true false))))
  (-get [_ key sync?]
    (async+sync sync?
                {go do
                 <! do}
                (go
                  (second (get @state key)))))
  (-get-meta [_ key sync?]
    (async+sync sync?
                {go do
                 <! do}
                (go (first (get @state key)))))
  (-update-in [_ key-vec meta-up-fn up-fn sync?]
    (async+sync sync?
                {go do}
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
                                                (update-in data rkey up-fn)
                                                (up-fn data))])))))
                        new-state (update-atom state)]
                    [(second (get new-state fkey))
                     (second (get new-state fkey))]))))
  (-assoc-in [this key-vec meta val sync?] (-update-in this key-vec meta (fn [_] val) sync?))
  (-dissoc [_ key sync?]
    (async+sync sync?
                {go do
                 <! do}
                (go (swap! state dissoc key) nil)))
  PBinaryAsyncKeyValueStore
  (-bget [_ key locked-cb sync?]
    (async+sync sync?
                {go do
                 <! do}
                (go
                  (<! (locked-cb (second (get @state key)))))))
  (-bassoc [_ key meta-up-fn input sync?]
    (async+sync sync?
                {go do
                 <! do}
                (go
                  (swap! state
                         (fn [old]
                           (update old key
                                   (fn [[meta data]]
                                     [(meta-up-fn meta) {:input-stream input
                                                         :size         :unknown}]))))
                  nil)))
  PKeyIterable
  (-keys [_ sync?]
    (async+sync sync?
                {go do
                 <! do}
                (go (set (map first (vals @state)))))))

#?(:clj
   (defmethod print-method MemoryStore
     [^MemoryStore store writer]
     (.write writer (str "MemoryStore[\"" (.hasheq store) "\"]"))))

(defn new-mem-store
  "Create in memory store. Binaries are not properly locked yet and
  the read and write-handlers are dummy ones for compatibility."
  ([] (new-mem-store (atom {})))
  ([init-atom] (new-mem-store init-atom false))
  ([init-atom sync?]
   (let [store
         (map->MemoryStore {:state init-atom
                            :read-handlers (atom {})
                            :write-handlers (atom {})
                            :locks (atom {})})]
     (if sync? store (go store)))))

