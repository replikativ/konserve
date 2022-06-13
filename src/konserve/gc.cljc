(ns konserve.gc
  (:require [clojure.core.async]
            [konserve.core :as k]
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

(defn sweep! [store whitelist ts]
  (go-try-
   (<?- (reduce<?-
         (fn [deleted-files {:keys [key last-write] :as meta}]
           (go-try-
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
