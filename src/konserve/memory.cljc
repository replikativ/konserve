(ns konserve.memory
  "Address globally aggregated immutable key-value store(s)."
  (:require [konserve.protocols :refer [PEDNKeyValueStore -update-in
                                        PBinaryKeyValueStore PKeyIterable]]
            #?(:clj [konserve.utils :refer [async+sync]])
            [clojure.core.async :as async :refer [go <!]])
  #?(:cljs (:require-macros [konserve.utils :refer [async+sync]])))

(defrecord MemoryStore [state read-handlers write-handlers locks]
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
    (let [{:keys [sync?]} opts]
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
                       (second (get new-state fkey))])))))
  (-assoc-in [this key-vec meta val opts] (-update-in this key-vec meta (fn [_] val) opts))
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
                  (go
                    (<! (locked-cb (second (get @state key))))))))
  (-bassoc [_ key meta-up-fn input opts]
    (let [{:keys [sync?]} opts]
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
                    nil))))
  PKeyIterable
  (-keys [_ opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  (go (set (map first (vals @state))))))))

#?(:clj
   (defmethod print-method MemoryStore
     [^MemoryStore store writer]
     (.write ^java.io.StringWriter writer (str "MemoryStore[\"" (.hasheq store) "\"]"))))

(defn new-mem-store
  "Create in memory store. Binaries are not properly locked yet and
  the read and write-handlers are dummy ones for compatibility."
  ([] (new-mem-store (atom {})))
  ([init-atom] (new-mem-store init-atom {:sync? false}))
  ([init-atom opts]
   (let [store
         (map->MemoryStore {:state init-atom
                            :read-handlers (atom {})
                            :write-handlers (atom {})
                            :locks (atom {})})]
     (if (:sync? opts) store (go store)))))

