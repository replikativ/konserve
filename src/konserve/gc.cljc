(ns konserve.gc
  (:require [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.utils :as utils]
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

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

   Passing a raw `(utils/now)` is correct only on a store no writer guards."
  ([store whitelist cutoff]
   (sweep! store whitelist cutoff 1000 {}))
  ([store whitelist cutoff batch-size]
   (sweep! store whitelist cutoff batch-size {}))
  ([store whitelist cutoff batch-size _opts]
   (go-try-
    (let [ts cutoff
          to-delete (->> (<?- (k/keys store))
                         (filter (fn [{:keys [key last-write] :as meta}]
                                   (not
                                    (or (contains? whitelist key)
                                        (<= (.getTime ^Date ts)
                                            (.getTime (if last-write
                                                        ^Date last-write
                                                        ;; old name
                                                        ^Date (:konserve.core/timestamp meta))))))))
                         (partition-all batch-size))]
      (<?-
       (reduce<?-
        (fn [deleted-files batch]
          (go-try-
           (if (utils/multi-key-capable? store)
             ;; Use multi-dissoc for batch deletion if supported
             (let [keys-to-delete (mapv :key batch)]
               (<?- (k/multi-dissoc store keys-to-delete))
               (into deleted-files keys-to-delete))
             ;; Fallback to single operations for stores without multi-key support.
             ;; GC does not use dissoc's existed? return, so :ignore-existence? lets
             ;; miss-safe backings skip the per-key HEAD probe (idempotent delete).
             (let [pending-deletes (mapv (fn [{:keys [key]}]
                                           (k/dissoc store key {:ignore-existence? true}))
                                         batch)]
               (<?- (async/into [] (async/merge pending-deletes)))
               (into deleted-files (map :key batch))))))
        #{}
        to-delete))))))