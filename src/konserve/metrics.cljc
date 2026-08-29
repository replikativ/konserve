(ns konserve.metrics
  "Where the numbers come from.

   Every store operation passes through two narrow places — the API layer in
   `konserve.core` and the blob I/O in `konserve.impl.defaults` — and both
   report here, to ONE process-wide `sink`: a `(fn [event])`, or nil. Nothing
   wraps a store and no store changes identity; the labels come from the
   store's own config. With no sink installed the cost is one deref per
   operation.

   An event is a map:

     {:backend  :file            ; from the store's :config, or its type
      :store-id #uuid \"…\"       ; :id of the store's config, when it has one
      :op       :get-in           ; API op, blob op (:read-edn, :write-edn, …), or :lock
      :level    :api | :io | :lock
      :nanos    12345             ; wall time of the operation
      :error    \"ExceptionInfo\" ; when it threw, the exception's class name
      :bytes    4096}             ; on a :bytes event: what was written

   Three levels tell three stories. `:api` is what a caller waits for — lock
   wait, serialization and I/O together. `:io` is the backend alone, below the
   lock, per blob operation: the S3 round trip, the fsync. `:lock` is the wait
   for the in-process key lock, the number that explains contention.

   A sink aggregates as it likes — `replikativ.metrics` folds events into
   per-`[backend store-id op]` histograms for Prometheus; a test collects them
   in an atom."
  (:require [clojure.core.async]
            [konserve.protocols :as p]
            [superv.async])
  #?(:cljs (:require-macros [konserve.metrics :refer [measured measured-go]]
                            [clojure.core.async :refer [go]]
                            [superv.async :refer [go-try-]])))

(defonce ^{:doc "The one sink, `(fn [event])` or nil. See `set-sink!`."}
  sink (atom nil))

(defn set-sink!
  "Install `f` as the process-wide sink, or nil to stop recording."
  [f]
  (reset! sink f))

(defn now-nanos []
  #?(:clj (System/nanoTime)
     :cljs (* 1e6 (.now js/Date))))

(defn labels
  "The store's labels, `:backend` and `:store-id`, from the spec
   `konserve.store/connect-store` stamped on it — on the store record itself
   and on its `:config`, which is what a blob operation's `env` carries. A
   store opened by a backend's own connect fn has no spec; it is labelled by
   its backing's type, and its id is nil."
  [store]
  (let [spec (or (get store p/store-config-key) (:config store))]
    {:backend  (or (:backend spec)
                   #?(:clj (some-> (or (:backing store) store) class .getSimpleName .toLowerCase keyword)
                      :cljs :unknown))
     :store-id (:id spec)}))

(defn record!
  "Send one event to `f`."
  [f store op level t0 error]
  (f (cond-> (assoc (labels store) :op op :level level :nanos (- (now-nanos) t0))
       error (assoc :error #?(:clj (.getSimpleName (class error))
                              :cljs (str (type error)))))))

(defn start
  "The start time for a hand-placed measurement, or nil with no sink. For
   code where `measured-go` cannot go — inside a large go block, where one
   more closure over its locals is one too many for the JVM."
  []
  (when @sink (now-nanos)))

(defn finish!
  "Report the operation `start` began, if it began one."
  [store op level t0]
  (when t0
    (when-let [f @sink]
      (record! f store op level t0 nil))))

(defn bytes!
  "Report `n` bytes written for `op` on `store` — its own event, so a sink can
   count bytes per operation without the write's timing carrying them."
  [store op n]
  (when-let [f @sink]
    (f (assoc (labels store) :op op :level :io :bytes n))))

#?(:clj
   (defmacro measured
     "Run `body` synchronously, reporting one event for `op` at `level` on
      `store` — its wall time, and the exception's class if it threw (the
      exception is rethrown). The synchronous twin of `measured-go`; konserve's
      `async+sync` rewrites one into the other. The body appears once in the
      expansion: with no sink it runs with one deref of overhead."
     [store op level & body]
     (let [catch-class (if (:ns &env) :default 'Throwable)]
       `(let [f#  @sink
              t0# (when f# (now-nanos))]
          (try
            (let [r# (do ~@body)]
              (when f# (record! f# ~store ~op ~level t0# nil))
              r#)
            (catch ~catch-class e#
              (when f# (record! f# ~store ~op ~level t0# e#))
              (throw e#)))))))

#?(:clj
   (defmacro measured-go
     "Run `body` in a go block and return a channel that delivers its result —
      or its exception as a value, as `go-try-` does — reporting one event
      when it does. Measuring on the channel rather than around the body keeps
      a `try` out of the go block: core.async's IOC compiler does not take a
      try/catch wrapped around a try/finally that parks. The body appears once
      in the expansion, so a state machine is built for it once. Take with
      `<?-`, which rethrows a delivered exception."
     [store op level & body]
     `(let [f#  @sink
            t0# (when f# (now-nanos))
            ch# (superv.async/go-try- ~@body)]
        (if f#
          (clojure.core.async/go
            (let [r# (clojure.core.async/<! ch#)]
              (record! f# ~store ~op ~level t0#
                       (when (instance? ~(if (:ns &env) 'js/Error 'Throwable) r#) r#))
              r#))
          ch#))))
