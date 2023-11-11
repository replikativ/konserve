(ns konserve.memory-test
  (:require [clojure.core.async :as a :refer [go <! take! #?(:clj <!!)]]
            [clojure.test :refer [deftest #?(:cljs async)]]
            [konserve.compliance-test :refer [#?(:clj compliance-test)
                                              async-compliance-test]]
            [konserve.memory :refer [new-mem-store map->MemoryStore]]
            [konserve.tests.cache :as ct]
            [konserve.tests.encryptor :as et]
            [konserve.tests.gc :as gct]
            [konserve.tests.serializers :as st]))

(defn connect-mem-store
  [init-atom & {:as params opts :opts}]
  (let [store-config (merge {:state init-atom
                             :read-handlers (atom {})
                             :write-handlers (atom {})
                             :locks (atom {})}
                            (dissoc params :config))
        store (map->MemoryStore store-config)]
    (if (:sync? opts) store (go store))))

#?(:clj
   (deftest memory-store-compliance-test
     (compliance-test (<!! (new-mem-store)))))

#!============
#! Cache tests

(deftest cache-PEDNKeyValueStore-test
  (let [store (connect-mem-store (atom nil) :opts {:sync? true})]
    #?(:clj (<!! (ct/test-cached-PEDNKeyValueStore-async store))
       :cljs (async done (take! (ct/test-cached-PEDNKeyValueStore-async store) done)))))

(deftest cache-PKeyIterable-test
  (let [store (connect-mem-store (atom nil) :opts {:sync? true})]
    #?(:clj  (<!! (ct/test-cached-PKeyIterable-async store))
       :cljs (async done (take! (ct/test-cached-PKeyIterable-async store) done)))))

(deftest cache-PBin-test
  (let [store (connect-mem-store (atom nil) :opts {:sync? true})
        f (fn [{:keys [input-stream] :as arg}]
            (a/to-chan! [input-stream]))]
    #?(:clj  (<!! (ct/test-cached-PBin-async store f))
       :cljs (async done (take! (ct/test-cached-PBin-async store f) done)))))

#!============
#! GC tests

(deftest async-gc-test
  (let [store (connect-mem-store (atom nil) :opts {:sync? true})]
    #?(:clj  (<!! (gct/test-gc-async store))
       :cljs (async done (take! (gct/test-gc-async store) done)))))

