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

   or `with-unreferenced-writes`, which does the same. `konserve.gc/sweep!`
   consults the safe point itself when given `:store-id`, so a correct sweep
   needs only that the writers take the guard.

   A process that dies mid-sequence simply drops its entry: the objects it wrote
   are unreachable, i.e. garbage, and a later cycle collects them. Correct by
   construction.

   SCOPE: this is IN-PROCESS state, and it matches konserve's concurrency
   contract — a store has a single writer per runtime, or callers coordinate
   above konserve. Writers in another process are outside that contract already,
   for reasons more basic than GC (they lose each other's writes). Readers are
   unconstrained."
  (:require [konserve.utils :as ku])
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

;; store-id -> {token start-instant}. Keyed by an identifier of the PHYSICAL
;; store rather than by the store object: separate connections to one store are
;; different store instances, and a collection running on one must see a
;; sequence in flight on another.
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

   Prefer `cutoff`, which handles the ordering requirement described there."
  [store-id]
  (let [starts (vals (get @in-flight store-id))]
    (if (seq starts)
      (reduce (fn [a b] (if (< (ms a) (ms b)) a b)) starts)
      (now))))

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

#?(:clj
   (defmacro with-unreferenced-writes
     "Run `body` as one unreferenced-write sequence against `store-id`: no
      concurrent collection in this process will sweep what it writes, until it
      completes. Use it whenever you write values into the store that only a
      LATER write (a transaction, a branch head, an index manifest) makes
      reachable."
     [store-id & body]
     `(let [sid# ~store-id
            t#   (writing! sid#)]
        (try ~@body
             (finally (done! sid# t#))))))
