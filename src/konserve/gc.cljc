(ns konserve.gc
  (:require [konserve.core :as k]
            [superv.async :refer [go-try- <?- reduce<?-]]))

(defn sweep! [store whitelist ts]
  (go-try-
   (<?- (reduce<?-
         (fn [deleted-files {:keys [key last-write] :as meta}]
           (go-try-
            (if (or (contains? whitelist key) (<= (.getTime ts) (.getTime (or last-write
                                                                             ;; old name
                                                                              (:konserve.core/timestamp meta)))))
              deleted-files
              (do
                (<?- (k/dissoc store key))
                (conj deleted-files key)))))
         #{}
         (<?- (k/keys store))))))
