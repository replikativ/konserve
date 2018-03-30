(ns konserve.cache
  "Provides core functions, but with additional caching. Still subject to internal
  changes."
  (:refer-clojure :exclude [get-in update-in assoc-in exists? dissoc])
  (:require [konserve.protocols :refer [-exists? -get-in -assoc-in
                                        -update-in -dissoc -bget -bassoc]]
            #?(:clj [clojure.core.cache :as cache]
              :cljs [cljs.cache :as cache])
            [konserve.core :refer [get-lock]]
            #?(:clj [konserve.core :refer [go-locked]])
            #?(:clj [clojure.core.async :refer [chan poll! put! <! go]]
              :cljs [cljs.core.async :refer [chan poll! put! <!]]))
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go]]
                           [konserve.core :refer [go-locked]])))


(defn ensure-cache
  "Adds a cache to the store. If none is provided it takes a LRU cache with 32
  elements per default."
  ([store]
   (ensure-cache store (atom (cache/lru-cache-factory {} :threshold 32))))
  ([store cache]
   (assoc store :cache cache)))


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
  [store key-vec]
  (go-locked
   store (first key-vec)
   (let [cache (:cache store)
         [fkey & rkey] key-vec]
     (if-let [v (cache/lookup @cache fkey)]
       (do
         (swap! cache cache/hit fkey)
         (clojure.core/get-in v rkey))
       (let [v (<! (-get-in store [fkey]))]
         (swap! cache cache/miss fkey v)
         (clojure.core/get-in v rkey))))))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key-vec fn]
  (go-locked
   store (first key-vec)
   (let [cache (:cache store)
         [old new] (<! (-update-in store key-vec fn))]
     (swap! cache cache/evict (first key-vec))
     [old new])))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  [store key-vec val]
  (go-locked
   store (first key-vec)
   (let [cache (:cache store)]
     (swap! cache cache/evict (first key-vec))
     (<! (-assoc-in store key-vec val)))))

(defn dissoc
  "Removes an entry from the store. "
  [store key]
  (go-locked
   store key
   (let [cache (:cache store)]
     (swap! cache cache/evict key)
     (<! (-dissoc store key)))))



