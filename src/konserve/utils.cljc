(ns konserve.utils
  (:require [clojure.walk]
            [konserve.protocols :as protocols]
            #?(:clj [replikativ.logging :as log])))

(defn invert-map [m]
  (->> (map (fn [[k v]] [v k]) m)
       (into {})))

(def ^:dynamic *wall-clock-ms*
  "Raw wall-clock read in epoch milliseconds. Dynamic ONLY so tests can
   simulate clock retreat; never rebind in production."
  (fn []
    #?(:clj (System/currentTimeMillis)
       :cljs (.getTime (js/Date.)))))

(def ^:private last-stamp
  "High-water mark of `monotonic-now` stamps issued by this process, in epoch
   milliseconds. Seeded lazily from the wall clock on first use."
  (atom 0))

(defn monotonic-now-ms
  "Monotonic (non-decreasing) timestamp in epoch milliseconds:
   `max(wall-clock, previous-stamp)`.

   A monotone hold on the wall clock: stamps read as wall time under normal
   operation and NEVER go backwards — an NTP step-back, VM suspend/resume or
   manual clock set holds the stamp flat at the previous value until real
   time catches up. Deliberately NOT strictly increasing (no `+1` per stamp):
   under sustained write rates above 1000 stamps/second a strict clock would
   run ahead of physical time — and after a restart following a bulk import,
   the re-seeded wall clock would sit BELOW the persisted stamps, stalling
   collection until real time caught up. Garbage collection only needs a
   monotonic order: the sweep spares objects whose stamp EQUALS the cutoff,
   so same-millisecond ties are fail-safe (garbage retained one cycle, never
   a live object deleted). Restarts re-seed from the wall clock; a retreat
   across a restart likewise only retains garbage longer.

   All konserve write stamps and any collector comparing against them MUST
   read this one source: happens-before between a guard acquisition and the
   writes it covers is then literal in the stamps, instead of an assumption
   about the machine clock."
  []
  (swap! last-stamp (fn [prev] (max ^long prev (long (*wall-clock-ms*))))))

(defn now
  "Monotonic wall-clock Date — see `monotonic-now-ms`. Stamped into every
   key's `:last-write` metadata; `konserve.gc/sweep!` and external collectors
   (e.g. datahike's gc-guard safe-point) compare against these stamps and
   must obtain their cutoffs from THIS function, not a raw clock read."
  []
  #?(:clj (java.util.Date. ^long (monotonic-now-ms))
     :cljs (js/Date. (monotonic-now-ms))))

(defn meta-update
  "Metadata has following 'edn' format
  {:key 'The stored key'
   :type 'The type of the stored value binary or edn'
   :last-write Date timestamp in milliseconds.}
  Returns the meta value of the stored key-value tuple. Returns metadata if the key
  value not exist, if it does it will update the last-write to date now. "
  [key type old]
  (if (empty? old)
    {:key key :type type :last-write (now)}
    (clojure.core/assoc old :last-write (now))))

(defn kv-keys
  "The keys of a `multi-assoc` kvs argument, which may be a map OR an ORDERED
   sequence of [k v] pairs (see `konserve.core/multi-assoc`). For a pair-seq the
   key order is preserved; for a map it is unspecified, as always."
  [kvs]
  (if (map? kvs)
    (clojure.core/keys kvs)
    (clojure.core/map first kvs)))

(defn multi-key-capable?
  "Checks whether the store supports multi-key operations.

   This function is used by the high-level API to determine if a store supports multi-key operations."
  [store]
  (and (satisfies? protocols/PMultiKeyEDNValueStore store)
       (protocols/-supports-multi-key? store)))

(defn write-hooks-capable?
  "Checks whether the store supports write hooks.

   This function is used by the high-level API and compliance tests to determine
   if a store supports the PWriteHookStore protocol."
  [store]
  (and (satisfies? protocols/PWriteHookStore store)
       (some? (protocols/-get-write-hooks store))))

(defn invoke-write-hooks!
  "Invoke all registered write hooks with the operation details.
   Hooks are called synchronously after a successful write."
  [store hook-event]
  (when-let [hooks-atom (protocols/-get-write-hooks store)]
    (when-let [hooks @hooks-atom]
      (when (seq hooks)
        (doseq [[hook-id hook-fn] hooks]
          (try
            (hook-fn hook-event)
            (catch #?(:clj Exception :cljs js/Error) e
              ;; Log hook errors for debugging but don't break writes
              #?(:clj (log/warn :konserve/write-hook-error "Write hook error" {:hook-id hook-id
                                                                               :api-op (:api-op hook-event)
                                                                               :key (:key hook-event)
                                                                               :error e})
                 :cljs (js/console.warn "Write hook error:" hook-id (pr-str e))))))))))

#?(:clj
   (defmacro async+sync
     [sync? async->sync async-code]
     (let [async->sync (if (symbol? async->sync)
                         (or (resolve async->sync)
                             (when-let [_ns (or (get-in &env [:ns :use-macros async->sync])
                                                (get-in &env [:ns :uses async->sync]))]
                               (resolve (symbol (str _ns) (str async->sync)))))
                         async->sync)]
       (assert (some? async->sync))
       `(if ~sync?
          ~(clojure.walk/postwalk (fn [n]
                                    (if-not (meta n)
                                      (async->sync n n) ;; primitives have no metadata
                                      (with-meta (async->sync n n)
                                        (update (meta n) :tag (fn [t] (async->sync t t))))))
                                  async-code)
          ~async-code))))

(def ^:dynamic *default-sync-translation*
  '{go-try try
    <? do
    go-try- try
    <!- do
    <?- do
    go-locked locked
    maybe-go-locked maybe-locked})

#?(:clj
   (defmacro with-promise [sym & body]
     `(let [~sym (cljs.core.async/promise-chan)]
        ~@body
        ~sym)))
