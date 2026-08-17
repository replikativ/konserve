(ns konserve.gc-guard
  "The store's SAFE POINT: the instant before which every written object is
   either reachable from a pointer, or garbage.

   WHY THIS EXISTS. Crash-safety for a persistent index on konserve rests on one
   rule: write every value the new state references, and only THEN write the
   mutable pointer that makes it reachable. A torn write therefore leaves
   collectable orphans, never a dangling pointer.

   That rule is also a BLIND SPOT for the garbage collector. For the duration of
   such a sequence, freshly written objects sit in the store reachable from
   NOTHING — the pointer still names the previous state. A mark that runs inside
   the window classifies them as garbage, and `konserve.gc/sweep!` deletes them;
   the pointer then lands on deleted objects and the store is corrupt. The
   objects are not `new' by timestamp either — they were written BEFORE the
   collection started, so a cutoff of `now` does not spare them.

   THE BLIND SPOT BELONGS TO THE STORE, NOT TO THE WRITER, which is why this
   lives in konserve rather than in each library that writes through it. A single
   konserve store commonly carries several independent index structures — a
   datahike database, a geschichte repository, a scriptum fulltext index — each
   performing its own values-then-pointer sequences, all swept by one collector.
   A guard held privately inside one of them cannot protect the others: the
   safe point has to be shared, and it is shared by agreeing on `store-id`.

   USAGE — wrap the whole values-then-pointer sequence:

     (let [t (writing! store-id)]
       (try (write-values!) (write-pointer!)
            (finally (done! store-id t))))

   or `with-unreferenced-writes`, which does the same. The COLLECTOR then has an
   ordering obligation of its own — read `cutoff` BEFORE marking, and pass it to
   `konserve.gc/sweep!` — for the reason spelled out there. Writers taking the
   guard is necessary but not sufficient.

   A process that dies mid-sequence simply drops its entry: the objects it wrote
   are unreachable, i.e. garbage, and a later cycle collects them. Correct by
   construction.

   SCOPE: this is IN-PROCESS state, and it matches konserve's concurrency
   contract — a store has a single writer per runtime, or callers coordinate
   above konserve. A writer in another process is not protected: it takes its
   own guard, in its own heap, and a sweep here cannot see it. Multi-process
   GC therefore needs coordination this does not provide.

   That is narrower than saying cross-process writers are broken outright.
   konserve's `:lock-blob?` (default true) is a per-blob file lock, so
   individual writes do not corrupt one another. What is unguarded is the
   SEQUENCE — the window between values and pointer — which no per-blob lock
   spans. Readers are unconstrained."
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [konserve.utils :as ku])
  #?(:clj (:import [java.util Date]))
  ;; Self-require the macro namespace so `with-unreferenced-writes` is available
  ;; to ClojureScript consumers that `:refer` it.
  #?(:cljs (:require-macros [konserve.gc-guard])))

(defn- now
  "Monotonic stamp from konserve's write clock. MUST be the same source that
   stamps :last-write (`konserve.utils/now`): the sweep spares an object iff
   last-write >= safe-point, and that comparison is only meaningful when both
   sides come from one strictly-monotonic clock — a raw (Date.) here would let an
   NTP step-back stamp a live object BEFORE the safe-point that guards it."
  [] (ku/now))

(defn- ms [d] #?(:clj (.getTime ^Date d) :cljs (.getTime d)))

;; store-id -> {token start-instant}. Keyed by the store's id rather than by the
;; store object, because separate connections to one store are different objects
;; and a collection running on one must see a sequence in flight on another.
;;
;; THAT ID IS LOGICAL, NOT PHYSICAL — konserve's `:id` identifies a store across
;; machines and backends (`konserve.store/validate-store-config`), so a replica
;; elsewhere carries the same one. The guard's requirement is about the bytes a
;; sweep is about to delete, and only one direction of the mismatch is unsafe:
;;
;;   same bytes, same id       the intended case
;;   two stores, one id        each sweep held back by the other's writers:
;;                             conservative, nothing lost, though a busy replica
;;                             can hold a collection off
;;   same bytes, two ids       the sweep cannot see the other writer's in-flight
;;                             objects and DELETES LIVE DATA
;;
;; So the id may be coarser than the physical store, never finer — which a
;; logical id is. Taking it off the store (`konserve.protocols/store-id`) rather
;; than passing it alongside is what keeps two callers from disagreeing.
;;
;; THE LOGICAL ID IS THE DEFAULT, NOT THE REQUIREMENT. A backend that knows
;; where its bytes actually live can implement `PStoreConfig` and return an id
;; derived from that, which wins over the default and closes BOTH mismatches at
;; once — two connections to one path collapse to one guard key however their
;; configs were written, and two replicas stop holding each other's collections
;; back. Nothing else in konserve reads `store-id`, so a backend is free to do
;; this without surprising anything.
;;
;; Until backends do, the default is sound in practice for the reason the guard
;; is in-process to begin with: a logical store is normally connected once per
;; runtime, so within a process one id names one store. The unsafe case needs
;; the SAME bytes opened TWICE in ONE process under DIFFERENT ids, which takes
;; deliberate effort and is visible at the call site.
(defonce ^:private in-flight (atom {}))

;; Tokens are counter values, not fresh objects: a token is a MAP KEY, and a bare
;; `(js/Object.)` implements neither IHash nor IEquiv in cljs, so it cannot be one.
(defonce ^:private token-seq (atom 0))

(defn writing!
  "Open an unreferenced-write sequence on `store-id`. Returns a token to close it
   with. Call BEFORE the first value is written."
  [store-id]
  (let [token (swap! token-seq inc)]
    (swap! in-flight assoc-in [store-id token] (now))
    token))

(defn done!
  "Close the sequence — its pointer has landed, so everything it wrote is now
   reachable (or garbage, if the pointer superseded it)."
  [store-id token]
  (swap! in-flight (fn [m]
                     (let [m' (update m store-id dissoc token)]
                       (if (empty? (get m' store-id))
                         (dissoc m' store-id)
                         m'))))
  nil)

(defn in-flight?
  "Is an unreferenced-write sequence currently open on `store-id`?

   `safe-point` cannot answer this: it returns `now` both when nothing is in
   flight AND when a sequence opened within the same millisecond, so a test that
   compares timestamps cannot tell a held guard from a missing one. This can."
  [store-id]
  (boolean (seq (get @in-flight store-id))))

(defn safe-point
  "The instant before which every object written to `store-id` is either
   reachable from a pointer or garbage — i.e. the sweep cutoff.

   No sequence in flight => `now`: nothing is mid-write, so the mark's verdict on
   everything written so far is final. Sequences in flight => the START of the
   oldest one: everything it writes lands at or after that instant, so sparing
   from there spares exactly its objects and nothing else.

   Prefer `cutoff`, which handles the ordering requirement described there.

   NOT A PURE READ: the idle branch calls `konserve.utils/now`, which advances
   the process-global high-water mark. That is deliberate — the cutoff has to
   come from the same clock that stamps `:last-write`, or the comparison the
   sweep makes is meaningless — and it is harmless, since `now` is
   `max(wall, previous)` and so cannot push the stamp ahead of wall time. Worth
   knowing before polling this from a monitoring path."
  [store-id]
  (let [starts (vals (get @in-flight store-id))]
    (if (seq starts)
      (reduce (fn [a b] (if (< (ms a) (ms b)) a b)) starts)
      (now))))

(defn stale
  "Sequences on `store-id` open longer than `max-age-ms`, as `{token start}`.

  A LEAKED ENTRY IS SILENT AND TOTAL: an open sequence holds the safe point at
  its start instant, so one that never closes stops collection on this store
  for the lifetime of the process, and the store grows without bound with
  nothing logged. Nothing here expires entries automatically — a long-running
  sequence is legitimate and guessing at a timeout would break it — so this is
  the introspection a caller needs to notice, and `sweep-stale!` is the
  deliberate way to act on it.

  Every escape from a values-then-pointer sequence should already release its
  token: `with-unreferenced-writes` closes on the throw path and, for an async
  body, when its channel delivers. What this catches is the rest — a body whose
  channel never delivers, a hand-rolled `writing!` whose `done!` is missed, or
  a `done!` given the wrong store-id."
  [store-id max-age-ms]
  (let [cut (- (ms (now)) max-age-ms)]
    (into {} (filter (fn [[_ start]] (< (ms start) cut))) (get @in-flight store-id))))

(defn sweep-stale!
  "Force-close sequences on `store-id` older than `max-age-ms`. Returns what was
  closed, as `stale` reports it.

  A BLUNT INSTRUMENT, and the last resort: closing a sequence that is genuinely
  still writing reopens exactly the blind spot the guard exists to cover. Reach
  for it when `stale` shows an entry that cannot still be live — a crashed
  writer's, or one whose age exceeds anything the application can produce —
  and prefer fixing the missing `done!`."
  [store-id max-age-ms]
  (let [leaked (stale store-id max-age-ms)]
    (when (seq leaked)
      (swap! in-flight (fn [m]
                         (let [m' (apply update m store-id dissoc (keys leaked))]
                           (if (empty? (get m' store-id)) (dissoc m' store-id) m')))))
    leaked))

(defn cutoff
  "The sweep cutoff for `store-id`, given the collection's own `started-at`.

   ORDER MATTERS, which is the whole reason this exists rather than leaving
   callers to combine the two readings. `started-at` must be read BEFORE the
   guard, and the smaller of the two wins: a sequence that opens and closes
   between the two readings has landed its pointer, so the mark — which runs
   after — sees it. Reading the guard first would miss a sequence that opens in
   between, and sweeping from `started-at` would then delete its objects."
  [store-id started-at]
  (let [sp (safe-point store-id)]
    (if (< (ms sp) (ms started-at)) sp started-at)))

(defn close-when-delivered
  "Close the sequence when `ch` delivers, and hand back a channel carrying the
   same value. Implementation detail of `with-unreferenced-writes`.

   Public because the macro expands into it, and because a caller building its
   own async sequence needs the same thing."
  [store-id token ch]
  (let [out (async/chan 1)]
    (async/take! ch (fn [v]
                      (done! store-id token)
                      (when (some? v) (async/put! out v))
                      (async/close! out)))
    out))

#?(:clj
   (defmacro with-unreferenced-writes
     "Run `body` as one unreferenced-write sequence against `store-id`: no
      concurrent collection in this process will sweep what it writes, until it
      completes. Use it whenever you write values into the store that only a
      LATER write (a transaction, a branch head, an index manifest) makes
      reachable.

      WORKS FOR ASYNC BODIES TOO, which a plain `try`/`finally` does not — and
      that mattered, because konserve's default is `{:sync? false}`, so the most
      idiomatic body here returns a CHANNEL. `finally` then fires when the go
      block is CONSTRUCTED, releasing the guard before a single write has
      happened and making the whole form a no-op. So: if `body` yields a
      channel, this returns a channel that delivers the same value and closes
      the sequence when it does; otherwise it closes immediately, as before.

      One consequence worth knowing: an async body's guard is released when its
      channel DELIVERS. A body that returns a channel nobody ever puts to holds
      the sequence open forever, and an open sequence stops collection — see
      `stale` and `sweep-stale!`."
     [store-id & body]
     `(let [sid# ~store-id
            t#   (writing! sid#)]
        (try
          (let [res# (do ~@body)]
            (if (satisfies? async-protocols/ReadPort res#)
              (close-when-delivered sid# t# res#)
              (do (done! sid# t#) res#)))
          (catch ~(if (:ns &env) :default 'Throwable) e#
            (done! sid# t#)
            (throw e#))))))
