(ns konserve.cache
  "Provides core functions, but with additional caching. Still subject to internal
  changes. Use this namespace only if you anticipate to be the only process
  accessing the store, otherwise you should implement your own caching strategy."
  (:refer-clojure :exclude [assoc-in assoc exists? dissoc get get-in update-in update])
  (:require [konserve.protocols :refer [-exists? -get -assoc-in
                                        -update-in -dissoc]]
            #?(:clj [clojure.core.cache :as cache]
               :cljs [cljs.cache :as cache])
            [konserve.core :refer [meta-update]]
            [konserve.core :refer [go-locked]]
            [clojure.core.async :refer [chan poll! put! <! go]]))

(defn ensure-cache
  "Adds a cache to the store. If none is provided it takes a LRU cache with 32
  elements per default."
  ([store]
   (ensure-cache store (atom (cache/lru-cache-factory {} :threshold 32))))
  ([store cache]
   (clojure.core/assoc store :cache cache)))

(defn exists?
  "Checks whether value is in the store."
  [store key]
  (go-locked
   store key
   (or (cache/has? @(:cache store) key)
       (<! (-exists? store key)))))

(defn get-in
  "Returns the value stored described by key-vec or nil if the path is
  not resolvable."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
   (go-locked
    store (first key-vec)
    (let [cache         (:cache store)
          [fkey & rkey] key-vec]
      (if-let [v (cache/lookup @cache fkey)]
        (do
          #_(prn "hitting cache")
          (swap! cache cache/hit fkey)
          (clojure.core/get-in v rkey))
        (let [v (<! (-get store fkey))]
          #_(prn "getting fkey" fkey)
          (swap! cache cache/miss fkey v)
          (clojure.core/get-in v rkey not-found)))))))

(defn get
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get store key nil))
  ([store key not-found]
   (get-in store [key] not-found)))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key-vec up-fn & args]
  (go-locked
   store (first key-vec)
   (let [cache (:cache store)
         [old new] (<! (-update-in store key-vec (partial meta-update (first key-vec) :edn) up-fn args))]
     (swap! cache cache/evict (first key-vec))
     [old new])))

(defn update
  "Updates a position described by key by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key meta-up-fn fn & args]
  (apply update-in store [key] meta-up-fn fn args))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  [store key-vec val]
  (go-locked
   store (first key-vec)
   (let [cache (:cache store)]
     (swap! cache cache/evict (first key-vec))
     (<! (-assoc-in store key-vec (partial meta-update (first key-vec) :edn) val)))))

(defn assoc
  "Associates the key-vec to the value, any missing collections for
 the key-vec (nested maps and vectors) are newly created."
  [store key val]
  (assoc-in store [key] val))

(defn dissoc
  "Removes an entry from the store. "
  [store key]
  (go-locked
   store key
   (let [cache (:cache store)]
     (swap! cache cache/evict key)
     (<! (-dissoc store key)))))




