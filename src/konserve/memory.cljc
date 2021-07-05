(ns konserve.memory
  "Address globally aggregated immutable key-value store(s)."
  (:require [konserve.protocols :refer [PAsyncEDNKeyValueStore -async-update-in
                                        PAsyncBinaryKeyValueStore PAsyncKeyIterable

                                        PSyncEDNKeyValueStore -sync-update-in
                                        PSyncBinaryKeyValueStore PSyncKeyIterable]]
            [clojure.core.async :as async :refer [go]]))

(defrecord MemoryStore [state read-handlers write-handlers locks]
  ;; asynchronous
  PAsyncEDNKeyValueStore
  (-async-exists? [this key] (go (if (@state key) true false)))
  (-async-get [this key] (go (second (get @state key))))
  (-async-get-meta [this key] (go (first (get @state key))))
  (-async-update-in [this key-vec meta-up-fn up-fn args]
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
                                    (apply up-fn data args))])))))
            new-state (update-atom state)]
        [(second (get new-state fkey))
         (second (get new-state fkey))])))
  (-async-assoc-in [this key-vec meta val] (-async-update-in this key-vec meta (fn [_] val) []))
  (-async-dissoc [this key] (go (swap! state dissoc key) nil))
  PAsyncBinaryKeyValueStore
  (-async-bget [this key locked-cb]
    (go
      (<! (locked-cb (second (get @state key))))))
  (-async-bassoc [this key meta-up-fn input]
    (go
      (swap! state
             (fn [old]
               (update old key
                       (fn [[meta data]]
                         [(meta-up-fn meta) {:input-stream input
                                             :size         :unknown}]))))
      nil))
  PAsyncKeyIterable
  (-async-keys [_]
    (go (set (map first (vals @state)))))

  ;; synchronous
  PSyncEDNKeyValueStore
  (-sync-exists? [this key] (if (@state key) true false))
  (-sync-get [this key] (second (get @state key)))
  (-sync-get-meta [this key] (first (get @state key)))
  (-sync-update-in [this key-vec meta-up-fn up-fn args]
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
                                  (apply up-fn data args))])))))
          new-state (update-atom state)]
      [(second (get new-state fkey))
       (second (get new-state fkey))]))
  (-sync-assoc-in [this key-vec meta val] (-sync-update-in this key-vec meta (fn [_] val) []))
  (-sync-dissoc [this key] (do (swap! state dissoc key) nil))
  PSyncBinaryKeyValueStore
  (-sync-bget [this key locked-cb]
    (locked-cb (second (get @state key))))
  (-sync-bassoc [this key meta-up-fn input]
    (do
      (swap! state
             (fn [old]
               (update old key
                       (fn [[meta data]]
                         [(meta-up-fn meta) {:input-stream input
                                             :size         :unknown}]))))
      nil))
  PSyncKeyIterable
  (-sync-keys [_]
    (set (map first (vals @state)))))

#?(:clj
   (defmethod print-method MemoryStore
     [^MemoryStore store writer]
     (.write writer (str "MemoryStore[\"" (.hasheq store) "\"]"))))

(defn sync-new-mem-store
  "Create in memory store. Binaries are not properly locked yet and
  the read and write-handlers are dummy ones for compatibility."
  ([] (sync-new-mem-store (atom {})))
  ([init-atom]
   (map->MemoryStore {:state init-atom
                      :read-handlers (atom {})
                      :write-handlers (atom {})
                      :locks (atom {})})))

(defn async-new-mem-store
  "Create in memory store. Binaries are not properly locked yet and
  the read and write-handlers are dummy ones for compatibility."
  ([] (async-new-mem-store (atom {})))
  ([init-atom]
   (go (sync-new-mem-store init-atom))))
