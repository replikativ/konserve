(ns konserve.memory
  "Address globally aggregated immutable key-value store(s).
   Does not support serialization."
  (:require [clojure.core.async :as async :refer [go <!]]
            [konserve.protocols :refer [PEDNKeyValueStore -update-in
                                        PBinaryKeyValueStore PKeyIterable
                                        PMultiKeyEDNValueStore PMultiKeySupport
                                        PAssocSerializers PWriteHookStore]]
            [konserve.utils #?(:clj :refer :cljs :refer-macros) [async+sync]]))

;; =============================================================================
;; Memory Store Registry
;; =============================================================================

(def ^{:doc "Global registry of memory stores by ID.
             Allows multiple parts of an application to connect to the same memory store."}
  memory-store-registry
  (atom {}))

(defrecord MemoryStore [state read-handlers write-handlers locks write-hooks]
  PEDNKeyValueStore
  (-exists? [_ key opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  (go  (if (get @state key false) true false)))))
  (-get-in [_ key-vec not-found opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  (go (if-let [a (second (get @state (first key-vec)))]
                        (get-in a (rest key-vec) not-found)
                        not-found)))))
  (-get-meta [_ key opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do}
                  (go (first (get @state key))))))
  (-update-in [_ key-vec meta-up-fn up-fn opts]
    (let [{:keys [sync? overwrite?]} opts]
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
                          [_ old-val] (get @state fkey)
                          {[_ new-val] fkey} (update-atom state)]
                      (if overwrite?
                        [nil new-val]
                        [old-val new-val]))))))
  (-assoc-in [this key-vec meta val opts]
    (-update-in this key-vec meta (fn [_] val) (assoc opts :overwrite? true)))
  (-dissoc [_ key opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  (go
                    (let [v (get @state key ::not-found)]
                      (if (not= v ::not-found)
                        (do
                          (swap! state dissoc key)
                          true)
                        false))))))
  PBinaryKeyValueStore
  (-bget [_ key locked-cb opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  (go (<! (locked-cb {:input-stream (second (get @state key))}))))))
  (-bassoc [_ key meta-up-fn input opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  (go
                    (swap! state
                           (fn [old]
                             (update old key
                                     (fn [[meta _data]]
                                       [(meta-up-fn meta) input]))))
                    true))))
  PAssocSerializers ;; no serializers needed for memory
  (-assoc-serializers [this _serializers] this)
  PKeyIterable
  (-keys [_ opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  (go (set (map first (vals @state)))))))

  PMultiKeySupport
  (-supports-multi-key? [_] true)

  PMultiKeyEDNValueStore
  (-multi-assoc [_ kvs meta-up-fn opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do}
                  (go
                    ;; Use an atomic update on the state atom to ensure all key-val pairs are updated atomically
                    (swap! state
                           (fn [old-state]
                             (reduce (fn [acc [key val]]
                                       (update acc key
                                               (fn [[meta _data]]
                                                 [(meta-up-fn key :edn meta) val])))
                                     old-state
                                     kvs)))
                    ;; Return a map of keys to success status
                    (into {} (map (fn [[k _]] [k true]) kvs))))))

  (-multi-dissoc [_ keys opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do}
                  (go
                    ;; Atomically swap state and capture old value to avoid race conditions
                    (let [[old-state _new-state] (swap-vals! state
                                                             (fn [s]
                                                               (apply dissoc s keys)))]
                      ;; Check existence against the actual old state we swapped from
                      (into {} (map (fn [k]
                                      [k (contains? old-state k)])
                                    keys)))))))

  (-multi-get [_ keys opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do}
                  (go
                    ;; Single deref for atomicity, extract multiple keys
                    (let [current-state @state]
                      ;; Return sparse map - only found keys with their values
                      (reduce (fn [result key]
                                (if-let [entry (get current-state key)]
                                  (let [[_meta value] entry]
                                    (assoc result key value))
                                  result))
                              {}
                              keys))))))

  PWriteHookStore
  (-get-write-hooks [_] write-hooks)
  (-set-write-hooks! [this hooks-atom]
    (assoc this :write-hooks hooks-atom)))

#?(:clj
   (defmethod print-method MemoryStore
     [^MemoryStore store writer]
     (.write ^java.io.Writer writer (str "MemoryStore[\"" (.hasheq store) "\"]"))))

(defn new-mem-store
  "Create in memory store. Binaries are not properly locked yet and
  the read and write-handlers are dummy ones for compatibility.

  The store will be registered globally by :id and can be retrieved later
  via connect-mem-store.

  Options:
    :id     - String UUID for the store (required)
    :sync?  - Boolean for sync/async operation (default false)"
  ([] (new-mem-store (atom {}) {:sync? false}))
  ([init-atom] (new-mem-store init-atom {:sync? false}))
  ([init-atom opts]
   (let [id (:id opts)
         store
         (map->MemoryStore {:state init-atom
                            :read-handlers (atom {})
                            :write-handlers (atom {})
                            :locks (atom {})
                            :write-hooks (atom {})})
         result (if (:sync? opts) store (go store))]
     ;; Register the actual store (not the wrapped channel) if ID is provided
     (when id
       (swap! memory-store-registry assoc id store))
     result)))

(defn connect-mem-store
  "Connect to an existing memory store by ID. Returns nil if not found.

  Args:
    id   - String ID of the store to connect to
    opts - Options map with :sync? boolean

  Returns:
    Store instance if found, nil otherwise (or channel in async mode)"
  [id opts]
  (if-let [store (get @memory-store-registry id)]
    store
    (if (:sync? opts) nil (go nil))))

(defn delete-mem-store
  "Delete a memory store from the registry by ID.

  Args:
    id - String ID of the store to delete

  Returns:
    true if store was deleted, false if not found"
  [id]
  (if (contains? @memory-store-registry id)
    (do
      (swap! memory-store-registry dissoc id)
      true)
    false))
