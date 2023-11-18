(ns konserve.cache
  "Provides core functions, but with additional caching. Still subject to internal
  changes. Use this namespace only if you anticipate to be the only process
  accessing the store, otherwise you should implement your own caching strategy."
  (:refer-clojure :exclude [assoc-in assoc exists? dissoc get get-in
                            keys update-in update])
  (:require [konserve.protocols :refer [-get-in -assoc-in
                                        -update-in -dissoc]]
            #?(:clj [clojure.core.cache :as cache]
               :cljs [cljs.cache :as cache])
            [konserve.core #?@(:clj (:refer [go-locked locked])) :as core]
            [konserve.utils :refer [meta-update #?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [taoensso.timbre :refer [trace]]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async])
  #?(:cljs (:require-macros [konserve.core :refer [go-locked locked]])))

(defn ensure-cache
  "Adds a cache to the store. If none is provided it takes a LRU cache with 32
  elements per default."
  ([store]
   (ensure-cache store (atom (cache/lru-cache-factory {} :threshold 32))))
  ([store cache]
   (clojure.core/assoc store :cache cache)))

(defn- read-through [store key opts]
  (async+sync
   (:sync? opts)
   *default-sync-translation*
   (go-try-
    (let [cache         (:cache store)]
      (if (cache/has? @cache key)
        (let [v (cache/lookup @cache key)]
          (swap! cache cache/hit key)
          v)
        (let [v (<?- (-get-in store [key] ::missing opts))]
          (swap! cache cache/miss key v)
          v))))))

(defn exists?
  "Checks whether value is in the store."
  ([store key]
   (exists? store key {:sync? false}))
  ([store key opts]
   (trace "exists? on key " key opts)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (let [v (<?- (read-through store key opts))]
                  (not= v ::missing))))))

(defn get-in
  "Returns the value stored described by key-vec or nil if the path is
  not resolvable."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
   (get-in store key-vec not-found {:sync? false}))
  ([store key-vec not-found opts]
   (trace "get-in on key " key opts)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (let [[fkey & rkey] key-vec
                      v (<?- (read-through store fkey opts))]
                  (if (not= v ::missing)
                    (clojure.core/get-in v rkey not-found)
                    not-found))))))

(defn get
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get store key nil))
  ([store key not-found]
   (get store key not-found {:sync? false}))
  ([store key not-found opts]
   (trace "get on key " key opts)
   (get-in store [key] not-found opts)))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  ([store key-vec up-fn]
   (update-in store key-vec up-fn {:sync? false}))
  ([store key-vec up-fn opts]
   (trace "update-in on key " key opts)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (let [cache (:cache store)
                      key (first key-vec)
                      [old-val new-val] (<?- (-update-in store key-vec (partial meta-update (first key-vec) :edn) up-fn opts))
                      had-key? (cache/has? @cache key)]
                  (swap! cache cache/evict key)
                  (when had-key?
                    (swap! cache cache/miss key new-val))
                  [old-val new-val])))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn update
  "Updates a position described by key by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  ([store key fn]
   (update store key fn {:sync? false}))
  ([store key fn opts]
   (trace "update on key " key opts)
   (update-in store [key] fn opts)))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  ([store key-vec val]
   (assoc-in store key-vec val {:sync? false}))
  ([store key-vec val opts]
   (trace "assoc-in on key " key opts)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store (first key-vec)
                (let [cache (:cache store)
                      [old-val new-val] (<?- (-assoc-in store key-vec (partial meta-update (first key-vec) :edn) val opts))
                      had-key? (cache/has? @cache key)]
                  (swap! cache cache/evict (first key-vec))
                  (when had-key?
                    (swap! cache cache/miss (first key-vec) new-val))
                  [old-val new-val])))))

(defn assoc
  "Associates the key-vec to the value, any missing collections for
 the key-vec (nested maps and vectors) are newly created."
  ([store key val]
   (assoc store key val {:sync? false}))
  ([store key val opts]
   (trace "assoc on key " key opts)
   (assoc-in store [key] val opts)))

(defn dissoc
  "Removes an entry from the store. "
  ([store key]
   (dissoc store key {:sync? false}))
  ([store key opts]
   (trace "dissoc on key " key)
   (async+sync (:sync? opts)
               *default-sync-translation*
               (go-locked
                store key
                (let [cache (:cache store)]
                  (swap! cache cache/evict key)
                  (<?- (-dissoc store key opts)))))))

;; alias core functions without caching for convenience
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def append core/append)
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def log core/log)
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def reduce-log core/reduce-log)

(def bassoc core/bassoc)
(def bget core/bget)
(def keys core/keys)
