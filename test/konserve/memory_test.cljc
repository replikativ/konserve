(ns konserve.memory-test
  (:require [clojure.core.async :as a :refer [go <! take! #?(:clj <!!)]]
            [clojure.test :refer [deftest #?(:cljs async)]]
            [konserve.compliance-test :refer [#?(:clj compliance-test)
                                              async-compliance-test conditional-write-compliance-test
                                              async-conditional-write-compliance-test]]
            [konserve.memory :refer [new-mem-store map->MemoryStore]]
            [konserve.tests.cache :as ct]
            [konserve.tests.gc :as gct]
            [konserve.tests.tiered :as tiered-tests]))

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
#! Tiered Store tests (Memory + Memory)

(defn create-tiered-mem-stores []
  #?(:clj
     {:frontend (<!! (new-mem-store))
      :backend (<!! (new-mem-store))}
     :cljs
     (go
       {:frontend (<! (new-mem-store (atom {})))
        :backend (<! (new-mem-store (atom {})))})))

#?(:clj
   (deftest tiered-fenced-write-through-test
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (tiered-tests/test-tiered-fenced-write-through-sync frontend backend))))

(deftest memory-async-conditional-write-test
  ;; The async arm of the same contract, on both platforms. It exists for
  ;; async-only backends that the sync suite cannot reach (konserve-s3's cljs
  ;; backing), and running it here keeps it honest: a suite whose only caller
  ;; lives in another repository drifts, and this one already shipped once with a
  ;; compliance test nothing called.
  #?(:clj
     (<!! (async-conditional-write-compliance-test (<!! (new-mem-store))))
     :cljs
     (async done
            (go
              (<! (async-conditional-write-compliance-test (<! (new-mem-store))))
              (done)))))

(deftest tiered-store-memory-compliance-test
  #?(:clj
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (<!! (tiered-tests/test-tiered-compliance-async frontend backend)))
     :cljs
     (async done
            (go
              (let [{:keys [frontend backend]} (<! (create-tiered-mem-stores))]
                (<! (tiered-tests/test-tiered-compliance-async frontend backend))
                (done))))))

#?(:clj
   (deftest tiered-store-memory-compliance-sync-test
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (tiered-tests/test-tiered-compliance-sync frontend backend))))

(deftest tiered-store-memory-write-policies-test
  #?(:clj
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (<!! (tiered-tests/test-write-policies-async frontend backend)))
     :cljs
     (async done
            (go
              (let [{:keys [frontend backend]} (<! (create-tiered-mem-stores))]
                (<! (tiered-tests/test-write-policies-async frontend backend))
                (done))))))

(deftest tiered-store-memory-frontend-only-test
  #?(:clj
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (<!! (tiered-tests/test-frontend-only-async frontend backend)))
     :cljs
     (async done
            (go
              (let [{:keys [frontend backend]} (<! (create-tiered-mem-stores))]
                (<! (tiered-tests/test-frontend-only-async frontend backend))
                (done))))))

(deftest tiered-store-memory-read-policies-test
  #?(:clj
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (<!! (tiered-tests/test-read-policies-async frontend backend)))
     :cljs
     (async done
            (go
              (let [{:keys [frontend backend]} (<! (create-tiered-mem-stores))]
                (<! (tiered-tests/test-read-policies-async frontend backend))
                (done))))))

(deftest tiered-store-memory-key-operations-test
  #?(:clj
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (<!! (tiered-tests/test-key-operations-async frontend backend)))
     :cljs
     (async done
            (go
              (let [{:keys [frontend backend]} (<! (create-tiered-mem-stores))]
                (<! (tiered-tests/test-key-operations-async frontend backend))
                (done))))))

(deftest tiered-store-memory-binary-operations-test
  #?(:clj
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (<!! (tiered-tests/test-binary-operations-async frontend backend)))
     :cljs
     (async done
            (go
              (let [{:keys [frontend backend]} (<! (create-tiered-mem-stores))]
                (<! (tiered-tests/test-binary-operations-async frontend backend))
                (done))))))

(deftest tiered-store-memory-sync-on-connect-test
  #?(:clj
     (let [{:keys [frontend backend]} (create-tiered-mem-stores)]
       (<!! (tiered-tests/test-sync-on-connect-async frontend backend)))
     :cljs
     (async done
            (go
              (let [{:keys [frontend backend]} (<! (create-tiered-mem-stores))]
                (<! (tiered-tests/test-sync-on-connect-async frontend backend))
                (done))))))

(deftest tiered-store-memory-error-handling-test
  (tiered-tests/test-error-handling nil nil))

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

(deftest memory-conditional-write-test
  ;; Runs on BOTH platforms: the memory store declares a `:process` domain under
  ;; ClojureScript too, and an unenforced claim there would be the same silent
  ;; failure the capability exists to prevent. The test itself runs only its sync
  ;; arm under cljs — see `conditional-write-compliance-test`.
  (conditional-write-compliance-test (new-mem-store (atom {}) {:sync? true})))
