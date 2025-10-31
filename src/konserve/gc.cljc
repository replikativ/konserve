(ns konserve.gc
  (:require [clojure.core.async :as async]
            [konserve.core :as k]
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

(defn sweep!
  ([store whitelist ts]
   (sweep! store whitelist ts 1000))
  ([store whitelist ts batch-size]
   (go-try-
    (let [to-delete (->> (<?- (k/keys store))
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
           (if false
             ;; insert batch dissoc for supporting stores here
             :TODO
             ;; fallback to single operations
             (let [pending-deletes (mapv (fn [{:keys [key]}]
                                           (k/dissoc store key))
                                         batch)]
               (<?- (async/into [] (async/merge pending-deletes)))
               (into deleted-files (map :key batch))))))
        #{}
        to-delete))))))