(ns konserve.gc
  (:require [clojure.core.async :refer [go]]
            [konserve.core :as k]
            [superv.async :refer [<?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

(defn sweep! [store whitelist ts]
  (go
    (<?- (reduce<?-
          (fn [deleted-files {:keys [key last-write] :as meta}]
            (go
              (if (or (contains? whitelist key)
                      (<= (.getTime ^Date ts)
                          (.getTime (if last-write
                                      ^Date last-write
                                    ;; old name
                                      ^Date (:konserve.core/timestamp meta)))))
                deleted-files
                (do
                  (<?- (k/dissoc store key))
                  (conj deleted-files key)))))
          #{}
          (<?- (k/keys store))))))
