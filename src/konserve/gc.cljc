(ns konserve.gc
  (:require [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.gc-coordination :as coord]
            #?(:clj [konserve.utils :as utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :as utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

(defn- epoch-ms
  "Epoch millis of a timestamp.

  A function rather than an inline `.getTime` with a `^Date` hint: the call
  sites sit inside `go-try-`, and the go macro rewrites locals into its
  state-machine array, dropping their hints and leaving a reflective call
  behind. A parameter hint on an ordinary function survives."
  ^long [d]
  #?(:clj  (.getTime ^Date d)
     :cljs (.getTime d)))

(defn- delete-batch!
  "Delete one batch of keys, concurrently where the platform allows it.

   `:ignore-existence?` because GC never uses `dissoc`'s `existed?` return, and
   it lets miss-safe backings skip the per-key HEAD probe — a delete here is
   idempotent by construction.

   The concurrency is worth keeping rather than simplifying away: the stores
   that most need it are exactly the ones that take this path. A backing with
   multi-key support batches into one round trip above; everything else — the
   FILESTORE included, which is what scriptum and proximum run on — deletes key
   by key, and doing that serially against a remote store costs one round trip
   per key.

   Async mode merges the per-key channels. Sync mode has no channels to merge,
   so the JVM uses `pmap` for the same effect; ClojureScript runs serially,
   which costs it nothing it could have had — sync mode there is single-threaded
   by definition.

   `pmap`'s thread cost here is bounded at `cpus + 2`, measured at 11 on an
   8-core machine, drawn from the cached pool `future` uses. THAT BOUND DEPENDS
   ON `batch` NOT BEING CHUNKED: `pmap` realizes a chunk at a time, so a chunked
   seq jumps to 32 + lookahead (35, measured, for a `range`). `sweep!` passes
   `partition-all` output, which is not chunked — keep it that way, or bound the
   parallelism explicitly.

   A bounded virtual-thread executor would be the better instrument and a
   simpler one, but Loom is new enough that konserve should not require it of
   its users yet."
  [store batch sync?]
  (if sync?
    #?(:clj (dorun (pmap (fn [{:keys [key]}]
                           (k/dissoc store key {:sync? true :ignore-existence? true}))
                         batch))
       :cljs (doseq [{:keys [key]} batch]
               (k/dissoc store key {:sync? true :ignore-existence? true})))
    (let [pending (mapv (fn [{:keys [key]}]
                          (k/dissoc store key {:ignore-existence? true}))
                        batch)]
      (async/into [] (async/merge pending)))))

(defn sweep!
  "Delete every key that is neither in `whitelist` nor written at/after `cutoff`.

   PRECONDITION, and the whole reason `konserve.gc-guard` exists: on a store
   with guarded writers, `cutoff` must come from `konserve.gc-guard/cutoff`,
   and it must be read BEFORE the caller computes `whitelist`:

       (let [cutoff    (guard/cutoff store-id (utils/now))   ; 1. guard
             whitelist (mark-reachable! store)]              ; 2. mark
         (sweep! store whitelist cutoff))                    ; 3. sweep

   That order is not a style preference. A values-then-pointer sequence that is
   open at step 1 pins the cutoff at or before its own writes, so they survive
   however step 2 turns out. Mark first and the same sequence can land its
   pointer in between: step 2 walked roots that did not name its values yet, and
   by step 3 the guard is closed again, so nothing spares them — they are older
   than the cutoff and unreachable from the whitelist, and the sweep deletes
   objects a live pointer names.

   `sweep!` DELIBERATELY DOES NOT CONSULT THE GUARD ITSELF. It cannot: it
   receives `whitelist` already computed, so any reading it does is necessarily
   after the caller's mark — the broken order above, wearing the appearance of
   safety. Only the caller is in a position to interleave the two correctly.

   Passing a raw `(utils/now)` is correct only on a store no writer guards.

   `opts :coordination-token` carries the exclusive token from
   `konserve.gc-coordination/begin-collection!`. Once durable coordination has
   been activated for a store, a token is mandatory; an independent tokenless
   collector fails closed instead of bypassing publishers. A legacy store with
   no coordination record retains tokenless behavior. `sweep!` checks authority
   before enumeration and every destructive batch. The durable coordination key
   is always preserved."
  ([store whitelist cutoff]
   (sweep! store whitelist cutoff 1000 {}))
  ([store whitelist cutoff batch-size]
   (sweep! store whitelist cutoff batch-size {}))
  ([store whitelist cutoff batch-size opts]
   (async+sync
    (:sync? opts)
    *default-sync-translation*
    (go-try-
     (let [sync? (:sync? opts)
           coordination-token (:coordination-token opts)
           _ (<?- (coord/assert-sweep-authorized! store coordination-token
                                                  (select-keys opts [:sync?])))
           to-delete (->> (<?- (k/keys store (select-keys opts [:sync?])))
                          (filter (fn [{:keys [key last-write] :as meta}]
                                    (not
                                     (or (contains? coord/coordination-root-keys key)
                                         (contains? whitelist key)
                                         (<= (epoch-ms cutoff)
                                             (epoch-ms (if last-write
                                                         last-write
                                                         ;; old name
                                                         (:konserve.core/timestamp meta))))))))
                          (partition-all batch-size))]
       (<?-
        (reduce<?-
         (fn [deleted-files batch]
           (go-try-
            (<?- (coord/assert-sweep-authorized! store coordination-token
                                                 (select-keys opts [:sync?])))
            (if (utils/multi-key-capable? store)
              ;; one round trip for the whole batch where the backing allows it
              (let [keys-to-delete (mapv :key batch)]
                (<?- (k/multi-dissoc store keys-to-delete (select-keys opts [:sync?])))
                (into deleted-files keys-to-delete))
              (let [results (<?- (delete-batch! store batch sync?))]
                ;; In async mode `async/merge` transports a per-key failure as
                ;; an element inside the result vector. `<?-` only sees the
                ;; vector itself, so surface the nested failure explicitly
                ;; instead of claiming every key was deleted.
                (when-let [error (some #(when (instance?
                                               #?(:clj Throwable :cljs js/Error)
                                               %)
                                          %)
                                       results)]
                  (throw error))
                (into deleted-files (map :key batch))))))
         #{}
         to-delete)))))))
