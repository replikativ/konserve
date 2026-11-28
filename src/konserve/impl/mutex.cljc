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

   * asynchronously (`acquire`), on either platform: a promise channel it
     parks on, delivered when the lock is handed to it;
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

(defn- acquire-or-enqueue!
  "Take `m` if it is free, else queue `waiter`. True when taken."
  [m waiter]
  (let [[old _] (swap-vals! m (fn [s]
                                (if (:held? s)
                                  (update s :waiters conj waiter)
                                  (assoc s :held? true))))]
    (not (:held? old))))

(defn release!
  "Release `m`: hand it to the longest-waiting waiter, or free it. Releasing a
   lock that is not held throws; that is a caller's bug, and ignoring it would
   hide a second holder."
  [m]
  (let [[old _] (swap-vals! m (fn [s]
                                (cond
                                  (not (:held? s)) s
                                  (seq (:waiters s)) (update s :waiters pop)
                                  :else (assoc s :held? false))))]
    (when-not (:held? old)
      (throw (ex-info "Released a konserve lock that is not held."
                      {:type :konserve/lock-not-held})))
    ;; still held: ownership passes to the waiter taken off the queue
    (when-let [w (peek (:waiters old))]
      ((:grant w)))
    nil))

(defn acquire
  "A channel that yields `true` once `m` is held by the caller. Park on it in a
   go block. The caller must `release!` afterwards, and if the wait itself ends
   in an exception (a go block interrupted while parked, as Jolt can), must call
   `(abandon! m ch)` before rethrowing."
  [m]
  (let [ch (async/promise-chan)
        w {:token ch :grant #(async/put! ch true)}]
    (when (acquire-or-enqueue! m w)
      (async/put! ch true))
    ch))

(defn abandon!
  "Give up the wait identified by `token` (the channel `acquire` returned).
   A waiter still in the queue is removed. One the lock was already handed to
   holds it, so it is released, to the next waiter or free. Without this an
   abandoned waiter keeps its place, and the release that reaches it hands the
   lock to nobody: every later caller waits forever."
  [m token]
  (let [[old _] (swap-vals! m update :waiters
                            (fn [q] (into empty-queue (remove #(identical? (:token %) token)) q)))]
    (when-not (some #(identical? (:token %) token) (:waiters old))
      (release! m))
    nil))

#?(:cljs nil
   :default
   (defn acquire-sync!
     "Take `m`, blocking the calling thread until it is free. Returns `true`.

      A wait that ends in an exception (an interrupted thread, an interrupted
      Jolt fiber) abandons the lock (`abandon!`) and rethrows. The handoff
      itself needs no core.async dispatch thread: `release!` delivers the
      waiter's promise directly.

      Under `clojure.core.async.go-checking` (JVM) a call that would block
      inside a go block throws, as `<!!` does; an uncontended one does not."
     [m]
     (when-not (try-acquire! m)
       ;; The check `<!!` makes under `clojure.core.async.go-checking`:
       ;; blocking a dispatch thread here can stall the whole pool.
       #?(:jolt nil
          :clj (when (Boolean/getBoolean "clojure.core.async.go-checking")
                 (clojure.core.async.impl.dispatch/check-blocking-in-dispatch)))
       (let [p (promise)
             w {:token p :grant #(deliver p true)}]
         (when-not (acquire-or-enqueue! m w)
           (try
             @p
             (catch Throwable e
               (abandon! m p)
               (throw e))))))
     true))
