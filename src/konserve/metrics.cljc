(ns konserve.metrics
  "Where the numbers come from.

   Every store operation passes through two narrow places — the API layer in
   `konserve.core` and the blob I/O in `konserve.impl.defaults` — and both
   report here, to a process-wide registry of named sinks. Nothing wraps a
   store and no store changes identity: the labels come from the config the
   store was connected with. Metrics are process-level by nature (a scrape is
   of a process); scoping is by label, not by routing.

   An event is a map:

     {:backend  :file            ; :backend of the connect/create config, or the store's type
      :store-id #uuid \"…\"       ; :id of that config, when it has one
      :op       :get-in           ; API op, blob op (:read-edn, :write-edn, :read-old, …), or :lock
      :level    :api | :io | :lock
      :nanos    12345             ; wall time of the operation
      :error    \"ExceptionInfo\" ; when it threw: the exception's class name
      :bytes    4096}             ; on a :bytes event: what a write serialized

   Three levels tell three stories. `:api` is what a caller waits for — lock
   wait, serialization and I/O together, one event per public function; a
   call rejected before it reaches the store (an unsupported option, a store
   without the capability) produces none, and a function that delegates
   (`get` to `get-in`, `update` to `update-in`) reports as what it delegates
   to. `:io` is the backend alone, below the lock, per blob operation — the
   S3 round trip, the fsync — from the first byte of a backup copy or header
   read to the last of the move; the read of the old value before a write is
   `:read-old`. `:lock` is the wait for the in-process key lock, absent on a
   lock-free store, which has no wait.

   Two rules for a sink, both because it runs on the operation's own thread —
   in the asynchronous arm a core.async dispatch thread, inside the completing
   go block: it must be THREAD-SAFE (events from concurrent operations
   interleave; each is self-contained, so aggregation needs no ordering) and
   NON-BLOCKING, O(1) — count into atomics, or hand the event to a buffered
   channel drained elsewhere. A sink that throws is logged and skipped; it
   never fails the operation it observed.

   Cost with no sink installed: one deref per measurement site — the API
   function, the lock, the blob operation — and nothing else: no extra go
   block, channel or allocation."
  (:require [clojure.core.async]
            [konserve.protocols :as p]
            [replikativ.logging :as log])
  #?(:cljs (:require-macros [konserve.metrics :refer [measured measured-go]]
                            [clojure.core.async :refer [go]])))

(defonce ^{:doc "id -> (fn [event]). See `add-sink!`."}
  sinks (atom {}))

(defn add-sink!
  "Register `f`, a `(fn [event])`, under `id`; every event goes to every
   registered sink. Returns `id`."
  [id f]
  (swap! sinks assoc id f)
  id)

(defn remove-sink!
  "Unregister the sink under `id`."
  [id]
  (swap! sinks dissoc id)
  nil)

(defn now-nanos []
  #?(:clj  (System/nanoTime)
     :cljs (* 1e6 (if (exists? js/performance) (.now js/performance) (.now js/Date)))))

(defn type-label
  "A keyword for `x`'s type, for a store that carries no config to label it
   by: `konserve.filestore.BackingFilestore` → `:backingfilestore`."
  [x]
  #?(:clj  (some-> x class .getSimpleName .toLowerCase keyword)
     :cljs (some-> x type pr-str keyword)))

(defn with-backend-label
  "`config` with `::backend` set from `backing`'s type unless a store spec
   already named the backend — called where a DefaultStore is built, so the
   `:config` map every blob operation carries labels itself the same way the
   store does."
  [config backing]
  (cond-> config
    (nil? (::backend config)) (assoc ::backend (type-label backing))))

(defn labels
  "The labels of `x` — a store, or the `:config` map a blob operation carries:
   `:backend` and `:store-id` from what `konserve.store/connect-store` stamped
   (on the store under `store-config-key`, into a `DefaultStore`'s `:config`
   under this namespace's keys). A store opened by a backend's own connect fn
   has no id and is labelled by its backing's type, at every level alike."
  [x]
  (let [cfg  (or (:config x) x)
        spec (get x p/store-config-key)]
    {:backend  (or (::backend cfg) (:backend spec)
                   (type-label (:backing x))
                   (when (record? x) (type-label x))
                   :unknown)
     :store-id (or (::store-id cfg) (:id spec))}))

(defn- emit!
  "One sink, one event, never an exception out."
  [id f event]
  (try (f event)
       (catch #?(:clj Throwable :cljs :default) e
         (log/warn :konserve/metrics-sink-failed {:sink id :error (str e)}))))

(defn report!
  "Send one event to every sink in `sinks`."
  [sinks x op level t0 error]
  (let [event (cond-> (assoc (labels x) :op op :level level :nanos (- (now-nanos) t0))
                error (assoc :error #?(:clj (.getSimpleName (class error))
                                       :cljs (str (type error)))))]
    (doseq [[id f] sinks]
      (emit! id f event))))

(defn start
  "The start of a hand-placed measurement, or nil with no sink installed."
  []
  (when-not (empty? @sinks) (now-nanos)))

(defn finish!
  "Report the operation `start` began, if it began one; `error` when it threw."
  ([x op level t0] (finish! x op level t0 nil))
  ([x op level t0 error]
   (when t0
     (let [s @sinks]
       (when-not (empty? s)
         (report! s x op level t0 error))))))

(defn bytes!
  "Report `n` bytes written for `op` on `x` — its own event, so a sink counts
   bytes per operation without the write's timing carrying them."
  [x op n]
  (let [s @sinks]
    (when-not (empty? s)
      (let [event (assoc (labels x) :op op :level :io :bytes n)]
        (doseq [[id f] s]
          (emit! id f event))))))

#?(:clj
   (defmacro measured
     "Run `body` synchronously and report one event for `op` at `level` on
      `x`: its wall time, and the exception's class if it threw (rethrown).
      The synchronous twin of `measured-go`; konserve's `async+sync` rewrites
      one into the other. With no sink: the body, and one deref."
     [x op level & body]
     (let [catch-class (if (:ns &env) :default 'Throwable)]
       `(let [s# @sinks]
          (if (empty? s#)
            (do ~@body)
            (let [t0# (now-nanos)]
              (try
                (let [r# (do ~@body)]
                  (report! s# ~x ~op ~level t0# nil)
                  r#)
                (catch ~catch-class e#
                  (report! s# ~x ~op ~level t0# e#)
                  (throw e#)))))))))

#?(:clj
   (defmacro measured-go
     "`body` evaluates to the operation's CHANNEL — what `go-try-` or
      `go-locked` returned. With no sink that channel is returned as it is:
      no go block, no wrapper. With sinks, a go block takes from it, reports
      one event — the exception's class when the channel delivered one, as
      `go-try-` does — and delivers the same result. Measuring on the channel
      keeps a `try` out of the operation's go block, which core.async's IOC
      compiler does not take around a body that parks inside a try/finally."
     [x op level & body]
     `(let [s#  @sinks
            t0# (when-not (empty? s#) (now-nanos))
            ch# (do ~@body)]
        (if t0#
          (clojure.core.async/go
            (let [r# (clojure.core.async/<! ch#)]
              (report! s# ~x ~op ~level t0#
                       (when (instance? ~(if (:ns &env) 'js/Error 'Throwable) r#) r#))
              r#))
          ch#))))
