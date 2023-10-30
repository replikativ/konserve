(ns konserve.tests.gc
  "gc/sweep! is async only"
  (:require [clojure.core.async :refer [go] :as async]
            [clojure.test :refer [deftest is testing]]
            #?(:cljs [fress.util :refer [byte-array]])
            [konserve.core :as k]
            [konserve.gc :as gc]
            [superv.async :refer [<?-]]))

;; TODO incorporate timestamps into tests
;; --> this can be tested by abstract out timestamps where there are written
;; and then using redef in tests. CLJS impls missing entirely

;it seems clock of threads are not sync.
;(prn (.getTime ts) (map (fn [e] [(:key e) (-> e :konserve.core/timestamp .getTime)]) (<?! (keys store))))))))

(defn test-gc-async [store]
  (go
   (and
    (<?- (k/assoc store :foo :bar))
    (<?- (k/assoc-in store [:foo] :bar2))
    (<?- (k/assoc-in store [:foo2] :bar2))
    (<?- (k/assoc-in store [:foo3] :bar2))
    (let [ts        #?(:cljs (js/Date.) :clj (java.util.Date.))
          whitelist #{:baz}]
      (and
       (<?- (k/update-in store [:foo] name))
       (<?- (k/assoc-in store  [:baz] {:bar 42}))
       (<?- (k/update-in store [:baz :bar] inc))
       (<?- (k/update-in store [:baz :bar] #(+ % 2 3)))
       (<?- (k/bassoc store :binbar (byte-array (into-array (range 10)))))
       (is (= #{:foo2 :foo3} (<?- (gc/sweep! store whitelist ts)))))))))
