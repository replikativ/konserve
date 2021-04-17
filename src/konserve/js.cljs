(ns konserve.js
  #_(:refer-clojure :exclude [get-in update-in assoc-in exists?])
  #_(:require [konserve.memory :as mem]
              [konserve.indexeddb :as idb]
              [konserve.core :as k]
              [cljs.core.async :refer [take!]]))

#_(comment
    (defn ^:export new-mem-store [cb]
      (take! (mem/new-mem-store) cb))

    (defn ^:export new-indexeddb-store [name cb]
      (take! (idb/new-indexeddb-store name) cb))

    (defn ^:export exists [store k cb]
      (take! (k/exists? store (js->clj k)) cb))

    (defn ^:export get-in [store k cb]
      (take! (k/get-in store (js->clj k)) cb))

    (defn ^:export assoc-in [store k v cb]
      (take! (k/assoc-in store (js->clj k) v) cb))

    (defn ^:export update-in [store k trans-fn cb]
      (take! (k/update-in store (js->clj k) trans-fn)
             (fn [res] (-> (into-array res) cb)))))
