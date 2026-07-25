(ns konserve.gc
  (:require [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.gc-guard :as guard]
            [konserve.utils :as utils]
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

(defn sweep!
  "Delete every key that is neither in `whitelist` nor written at/after the
   cutoff. `ts` is the instant the collection started.

   PASS `:store-id` whenever writers on this store take `konserve.gc-guard`.
   The sweep then derives its own cutoff as `min(ts, safe-point)` instead of
   trusting `ts`, which is what keeps it from deleting objects that a
   values-then-pointer sequence has already written but not yet made reachable
   (see `konserve.gc-guard`). Getting that combination right is fiddly — the
   guard must be read AFTER `ts` — so it belongs here rather than in every
   caller. Without `:store-id` the behaviour is unchanged and `ts` is used
   verbatim, which is only safe if the caller computed it via
   `konserve.gc-guard/cutoff` itself."
  ([store whitelist ts]
   (sweep! store whitelist ts 1000 {}))
  ([store whitelist ts batch-size]
   (sweep! store whitelist ts batch-size {}))
  ([store whitelist ts batch-size {:keys [store-id]}]
   (go-try-
    (let [ts (if store-id (guard/cutoff store-id ts) ts)
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