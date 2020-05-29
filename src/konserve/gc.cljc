(ns konserve.gc
  (:require [konserve.core :refer [dissoc keys]]
            [konserve.utils :refer [go-try <? reduce<]]))


(defn sweep! [store whitelist ts]
  (go-try
      (<? (reduce<
           (fn [deleted-files {:keys [key konserve.core/timestamp] :as meta}]
             (go-try
                 (if (or (contains? whitelist key) (<= (.getTime ts) (.getTime timestamp)))
                   deleted-files
                   (do
                     (<? (dissoc store key))
                     (conj deleted-files key)))))
           #{}
           (<? (keys store))))))
