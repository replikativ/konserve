(ns konserve.gc
  (:require [konserve.core :as k]
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:cljs (:require-macros [superv.async :refer [go-try- <?-]])))

(defn sweep! [store whitelist ts]
  (go-try-
      (<?- (reduce<?-
           (fn [deleted-files {:keys [key konserve.core/timestamp] :as meta}]
             (go-try-
                 (if (or (contains? whitelist key) (<= (.getTime ts) (.getTime timestamp)))
                   deleted-files
                   (do
                     (<?- (k/dissoc store key))
                     (conj deleted-files key)))))
           #{}
           (<?- (k/keys store))))))
