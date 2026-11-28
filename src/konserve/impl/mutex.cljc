(ns konserve.impl.mutex
  "A mutual-exclusion lock for synchronous and asynchronous code, on the JVM and
   in ClojureScript.

   konserve's locks used to be core.async channels holding one token: take the
   token to lock, put it back to unlock. A channel is not a mutex, and three
   things went wrong with it:

   * a waiter cannot withdraw. `<!!` leaves its take queued when the thread is
     interrupted; the next token put on the channel goes to that dead take;
   * a channel allows at most 1024 pending takes. The 1025th waiter's take
     throws instead of waiting;
   * callers took the token inside the `try` whose `finally` puts it back, so a
     take that failed (either case above, or ClojureScript, where a synchronous
     take is `poll!` and fails under contention) released a token it never
     held. Two holders could then run at once, for a while or for good.

   Here the lock state is an atom: whether the lock is held, and a queue of
   waiters. Only how a waiter waits depends on the caller:

   * asynchronously (`acquire`, `wait-chan`), on either platform: a promise
     channel it parks on, delivered when the lock is handed to it;
   * synchronously where a thread can block (JVM, Jolt; `acquire-sync!`): a
     promise it blocks on. An interrupted waiter withdraws from the queue, or,
     if the lock was handed to it in the meantime, releases it;
   * synchronously in ClojureScript, which cannot block: `try-acquire!` only.

   The only platform split is whether a thread can block, so a runtime that
   can takes the `:default` branch. Everything else is core: an atom,
   `swap-vals!`, a persistent queue of waiters, promises.

   The queue has no size limit, and acquisition happens before the caller's
   `try`, so a failed acquisition never releases anything.

   The lock is handed to waiters in arrival order. It is not reentrant."
  (:require [clojure.core.async :as async]
            #?@(:jolt [] :clj [[clojure.core.async.impl.dispatch]])))

(def ^:private empty-queue
  #?(:cljs cljs.core/PersistentQueue.EMPTY
     :default clojure.lang.PersistentQueue/EMPTY))

#?(:jolt nil
   :clj
   (def ^:private go-checking?
     "Read once, at load, as core.async reads it for `<!!`."
     (Boolean/getBoolean "clojure.core.async.go-checking")))

(defn check-would-block!
  "What `<!!` does under `clojure.core.async.go-checking` (JVM): throw if the
   caller is a core.async dispatch thread, since blocking one can stall the
   whole pool. Call it only where the caller is about to block. A no-op
   elsewhere and when go-checking was off at load."
  []
  #?(:jolt nil
     :clj (when go-checking?
            (clojure.core.async.impl.dispatch/check-blocking-in-dispatch))
     :cljs nil))

(defn mutex
  "A new, free lock."
  []
  (atom {:held? false :waiters empty-queue}))

(defn held?
  "Whether `m` is held right now. For diagnostics and tests."
  [m]
  (:held? @m))

(defn try-acquire!
  "Take `m` if it is free. True when taken; never waits."
  [m]
  (let [[old _] (swap-vals! m (fn [s] (if (:held? s) s (assoc s :held? true))))]
    (not (:held? old))))

(defn- waiter
  "A wait for the lock. `:state` moves once from :waiting to :granted (the lock
   is handed to it) or to :abandoned (it gave up), so a handoff and a give-up
   cannot both win, and giving up twice does nothing."
  [notify]
  {:state (atom :waiting) :notify notify})

(defn- grant!
  "Hand the lock to `w`. False if `w` already gave up."
  [w]
  (when (compare-and-set! (:state w) :waiting :granted)
    ((:notify w))
    true))

(defn- enqueue!
  "Take `m` for `w` if it is free, else queue `w`."
  [m w]
  (let [[old _] (swap-vals! m (fn [s]
                                (if (:held? s)
                                  (update s :waiters conj w)
                                  (assoc s :held? true))))]
    (when-not (:held? old)
      (grant! w))))

(defn release!
  "Release `m`: hand it to the longest-waiting waiter that has not given up, or
   free it. Releasing a lock that is not held throws; that is a caller's bug,
   and ignoring it would hide a second holder."
  [m]
  (loop []
    (let [[old _] (swap-vals! m (fn [s]
                                  (cond
                                    (not (:held? s)) s
                                    (seq (:waiters s)) (update s :waiters pop)
                                    :else (assoc s :held? false))))]
      (when-not (:held? old)
        (throw (ex-info "Released a konserve lock that is not held."
                        {:type :konserve/lock-not-held})))
      ;; still held: ownership passes to the waiter taken off the queue, unless
      ;; it gave up meanwhile; then on to the next one
      (when-let [w (peek (:waiters old))]
        (when-not (grant! w)
          (recur)))))
  nil)

(defn abandon!
  "Give up wait `w`. One still waiting leaves the queue; one the lock was
   already handed to releases it, to the next waiter or free; one already given
   up does nothing. Call it from a `finally` whenever the wait did not complete:
   an abandoned waiter left in the queue would receive the lock and hold it
   for nobody, and every later caller would wait forever."
  [m w]
  (cond
    (compare-and-set! (:state w) :waiting :abandoned)
    (swap! m update :waiters (fn [q] (into empty-queue (remove #(identical? % w)) q)))

    (compare-and-set! (:state w) :granted :abandoned)
    (release! m))
  nil)

(defn acquire
  "Start waiting for `m`. Returns the wait; `(wait-chan w)` yields `true` once
   the caller holds the lock, to park on in a go block. The caller must
   `release!` afterwards, and must `abandon!` the wait if it does not complete
   (an exception, or a go block or fiber interrupted or killed while parked)."
  [m]
  (let [ch (async/promise-chan)
        w (assoc (waiter #(async/put! ch true)) :ch ch)]
    (enqueue! m w)
    w))

(defn wait-chan
  "The channel of wait `w` (see `acquire`)."
  [w]
  (:ch w))

#?(:cljs nil
   :default
   (defn acquire-sync!
     "Take `m`, blocking the calling thread until it is free. Returns `true`.

      A wait that does not complete (an interrupted thread, an interrupted or
      killed Jolt fiber) abandons its place (`abandon!`) on the way out. The
      handoff itself needs no core.async dispatch thread: `release!` delivers
      the waiter's promise directly.

      Under `clojure.core.async.go-checking` (JVM) a call that would block
      inside a go block throws, as `<!!` does; an uncontended one does not."
     [m]
     (when-not (try-acquire! m)
       (check-would-block!)
       (let [p (promise)
             w (waiter #(deliver p true))
             done? (volatile! false)]
         (try
           (enqueue! m w)
           @p
           (vreset! done? true)
           (finally
             (when-not @done? (abandon! m w))))))
     true))
