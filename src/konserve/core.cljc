(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [konserve.protocols :refer [-exists? -get-meta -get -assoc-in
                                        -update-in -dissoc -bget -bassoc
                                        -keys]]
            [hasch.core :refer [uuid]]
            [taoensso.timbre :as timbre :refer [trace debug]]
            [konserve.utils :refer [meta-update #?(:clj async+sync)]]
            [clojure.core.async :refer [go chan poll! put! <!]])
  #?(:cljs (:require-macros [konserve.utils :refer [async+sync]]
                            [konserve.core :refer [go-locked locked]])))


;; ACID

;; atomic
;; consistent
;; isolated
;; durable


(defn get-lock [{:keys [locks] :as store} key]
  (or (clojure.core/get @locks key)
      (let [c (chan)]
        (put! c :unlocked)
        (clojure.core/get (swap! locks (fn [old]
                                         (trace "creating lock for: " key)
                                         (if (old key) old
                                             (clojure.core/assoc old key c))))
                          key))))

(defn wait [lock]
  #?(:clj (while (not (poll! lock))
            (Thread/sleep (rand-int 20)))
     :cljs (debug "WARNING: konserve lock is not active. Only use the synchronous variant with the memory store in JavaScript.")))

(defmacro locked [store key & code]
  `(let [l# (get-lock ~store ~key)]
     (try
       (wait l#)
       (trace "acquired spin lock for " ~key)
       ~@code
       (finally
         (trace "releasing spin lock for " ~key)
         (put! l# :unlocked)))))

(defmacro go-locked [store key & code]
  `(go
     (let [l# (get-lock ~store ~key)]
       (try
         (<! l#)
         (trace "acquired go-lock for: " ~key)
         ~@code
         (finally
           (trace "releasing go-lock for: " ~key)
           (put! l# :unlocked))))))

(defn exists?
  "Checks whether value is in the store."
  ([store key]
   (exists? store key {:sync? false}))
  ([store key opts]
   (trace "exists? on key " key)
   (async+sync (:sync? opts)
               {go-locked locked}
               (go-locked
                store key
                (-exists? store key opts)))))
