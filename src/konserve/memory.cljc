(ns konserve.memory
  "Address globally aggregated immutable key-value store(s).
   Does not support serialization."
  (:require [clojure.core.async :as async :refer [go <!]]
            [konserve.impl.defaults :as kd]
            [konserve.protocols :refer [PConditionalWrite -conditional-write-domain -revision
                                        PEDNKeyValueStore -update-in
                                        PBinaryKeyValueStore PKeyIterable
                                        PMultiKeyEDNValueStore PMultiKeySupport
                                        PAssocSerializers PWriteHookStore]]
            #?(:clj [konserve.nio-helpers :as nio])
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
    (let [{:keys [sync? with-revision?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  (go (let [entry (get @state (first key-vec))
                            v     (if-let [a (second entry)]
                                    (get-in a (rest key-vec) not-found)
                                    not-found)]
                        ;; `:with-revision?` is part of the contract, not an
                        ;; optimisation: a caller written against it destructures
                        ;; `[value revision]`, and returning a bare value here
                        ;; makes portable code throw on `nth` — or, worse, silently
                        ;; fence on nil when this store is a tiered frontend.
                        (if with-revision?
                          [v (if entry (:revision (first entry)) kd/absent)]
                          v))))))
  (-get-meta [_ key opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do}
                  (go (first (get @state key))))))
  (-update-in [_ key-vec meta-up-fn up-fn opts]
    (let [{:keys [sync? overwrite? expected-revision with-revision?]} opts]
      (async+sync sync?
                  {go do}
                  (go
                    ;; The rejection must arrive AS A VALUE on this channel. This
                    ;; store uses a plain `go`, not `go-try-`, so a throw from
                    ;; `check-revision!` would escape to the async thread's uncaught
                    ;; handler and close the channel EMPTY — an async caller could
                    ;; not tell a rejected fence from a successful write, and the
                    ;; error would surface only as noise in the log.
                    (try
                      (let [[fkey & rkey] key-vec
                            update-atom
                            (fn [store]
                            ;; The compare and the write are one `swap!`, so they
                            ;; are atomic against other threads in this JVM —
                            ;; which is the whole of a memory store's world. The
                            ;; check lives INSIDE the swap fn deliberately: doing
                            ;; it before would let another writer land between the
                            ;; comparison and the update, which is the very race
                            ;; this exists to prevent.
                              (swap! store
                                     (fn [old]
                                       (when expected-revision
                                         (kd/check-revision! fkey expected-revision
                                                             (first (get old fkey))))
                                       (update old fkey
                                               (fn [[meta data]]
                                                 [(meta-up-fn meta)
                                                  (if rkey
                                                    (update-in data rkey up-fn)
                                                    (up-fn data))])))))
                            [_ old-val] (get @state fkey)
                            {[new-meta new-val] fkey} (update-atom state)
                            res (if overwrite? [nil new-val] [old-val new-val])]
                        ;; `:with-revision?` reports the revision this write
                        ;; PRODUCED, so a caller can chain a fenced write without a
                        ;; re-read. It was destructured nowhere here, so this store
                        ;; — konserve's own reference implementation, which declares
                        ;; `:process` and passes the contract — accepted the option
                        ;; and returned the plain shape. A caller destructuring
                        ;; `[[old new] rev]` got the VALUE bound as the token and
                        ;; fenced the next write on it, and `k/update-in` threw
                        ;; `nth not supported on this type` outright. Accepted and
                        ;; silently dropped is the exact failure this whole
                        ;; mechanism exists to remove.
                        (if with-revision?
                          [res (:revision new-meta)]
                          res))
                      ;; The two arms report differently, and both must be honoured:
                      ;; a SYNC caller expects a throw, an ASYNC caller expects the
                      ;; error as a value on the channel (throwing there escapes to
                      ;; the async thread and closes the channel empty). `async+sync`
                      ;; collapses the `go` to a `do` for the sync arm, so one catch
                      ;; serves both only if it rethrows there.
                      (catch #?(:clj Exception :cljs js/Error) e
                        (if sync? (throw e) e)))))))
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

  PConditionalWrite
  ;; :process. The compare and the write are one `swap!`, which covers every
  ;; thread sharing this atom and says nothing about anyone else — a memory store
  ;; has no anyone else. Placed AFTER PEDNKeyValueStore's methods on purpose:
  ;; splitting a protocol's methods around another marker is silently tolerated on
  ;; the JVM and is a compile warning in ClojureScript, which is how this was found.
  (-conditional-write-domain [_] :process)
  (-revision [_ key opts]
    (async+sync (:sync? opts)
                {go do}
                (go (if-let [[meta _] (get @state key)]
                      (:revision meta)
                      kd/absent))))
  PBinaryKeyValueStore
  (-bget [_ key locked-cb opts]
    (let [{:keys [sync?]} opts]
      (async+sync sync?
                  {go do
                   <! do}
                  ;; `:blob` because that is what this store has — the bytes that
                  ;; were `bassoc`'d, with nothing to drain. `:input-stream` stays
                  ;; alongside it: it was always a misnomer here (it is not a
                  ;; stream and never was), but callbacks destructure it, and on
                  ;; the JVM they get away with it only because
                  ;; `clojure.java.io/copy` accepts a `byte[]`. Removing it would
                  ;; break them; naming the truth next to it does not.
                  (go (<! (locked-cb (let [v (second (get @state key))]
                                       {:input-stream v :blob v})))))))
  (-bassoc [_ key meta-up-fn input opts]
    ;; Normalized like every other backing: `bassoc` documents an InputStream,
    ;; a File, a String and a byte array, and storing the caller's object
    ;; verbatim handed all but the last straight back out of `bget` — `slurp`
    ;; then read a String as a FILENAME. The DefaultStore backings normalize in
    ;; `konserve.impl.defaults`; this store bypasses that layer, so it does its
    ;; own.
    (let [input #?(:clj (nio/blob->bytes input) :cljs input)
          {:keys [sync?]} opts]
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
    (if (:sync? opts) store (go store))
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
