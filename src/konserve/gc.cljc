(ns konserve.gc
  (:require [konserve.core :as k]
            [superv.async :refer [go-try- <?- reduce<?-]])
  (:import [java.util Date])
  #?(:cljs (:require-macros [superv.async :refer [go-try- <?-]])))

(defn sweep! [store whitelist ts]
  (go-try-
      (<?- (reduce<?-
           (fn [deleted-files {:keys [key konserve.core/timestamp] :as meta}]
             (go-try-
                 (if (or (contains? whitelist key) (<= (.getTime ^Date ts) (.getTime ^Date timestamp)))
                   deleted-files
                   (do
                     (<?- (k/dissoc store key))
                     (conj deleted-files key)))))
           #{}
           (<?- (k/keys store))))))
