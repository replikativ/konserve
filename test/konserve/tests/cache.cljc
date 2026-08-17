(ns konserve.tests.cache
  (:require [clojure.core.async :refer [go <! promise-chan put! take!]]
            [clojure.test :refer [deftest is]]
            [fress.api :as fress]
            [konserve.cache :as kc]
            [konserve.core :as k]
            #?(:cljs [fress.util :refer [byte-array]])))

(defn test-cached-PEDNKeyValueStore-async [store]
  (go
    (let [store (kc/ensure-cache store)
          opts {:sync? false}]
      (and
       (is (nil? (<! (kc/get store :foo nil opts))))
       (is (false? (<! (kc/exists? store :foo opts))))
       (is [nil :bar] (<! (kc/assoc store :foo :bar opts)))
       (is (true? (<! (kc/exists? store :foo))))
       (is (= :bar (<! (kc/get store :foo nil opts))))
       (is (= [nil :bar2] (<! (kc/assoc-in store [:foo] :bar2 opts))))
       (is (= :bar2 (<! (kc/get store :foo nil opts))))
       (is (= :default (<! (kc/get-in store [:fuu] :default opts))))
       (is (= :bar2 (<! (kc/get store :foo nil opts))))
       (is (= :default (<! (kc/get-in store [:fuu] :default opts))))
       (is (= [:bar2 "bar2"] (<! (kc/update-in store [:foo] name opts))))
       (is (= "bar2" (<! (kc/get store :foo nil opts))))
       (is (=  [nil {:bar 42}] (<! (kc/assoc-in store [:baz] {:bar 42} opts))))
       (is (= 42 (<! (kc/get-in store [:baz :bar] nil opts))))
       (is (= [{:bar 42} {:bar 43}] (<! (kc/update-in store [:baz :bar] inc opts))))
       (is (= 43 (<! (kc/get-in store [:baz :bar] nil opts))))
       (is (= [{:bar 43} {:bar 48}] (<! (kc/update-in store [:baz :bar] (fn [x] (+ x 2 3)) opts))))
       (is (= 48 (<! (kc/get-in store [:baz :bar] nil opts))))
       (is (true? (<! (kc/dissoc store :foo opts))))
       (is (nil? (<! (kc/get-in store [:foo] nil opts))))))))

(defn test-cached-revision-sync
  "`:with-revision?` through the CACHE. Sync only: the shapes are the thing under
   test and the async arm delivers a rejection as a value, which would obscure
   them.

   The cache stores whatever the backing hands back, so a revision-bearing write
   used to poison it: the `[[old new] revision]` shape destructured as the plain
   `[old new]` one, and the REVISION was cached as the key's value. The read that
   catches it is the plain one AFTER the write."
  [store]
  (let [store (kc/ensure-cache store)
        opts  {:sync? true}]
    (when (k/conditional-write? store)
      (kc/assoc store :rev-k {:v 1} opts)
      ;; warm the cache, so the assertion below is about the cache and not the store
      (is (= {:v 1} (kc/get store :rev-k nil opts)))
      (let [[[old new] revision] (kc/assoc store :rev-k {:v 2} (assoc opts :with-revision? true))]
        ;; `old` is nil for a full overwrite whether or not a revision was asked
        ;; for — konserve never reads the old value on that path. What matters
        ;; here is that `new` is the VALUE and the revision is beside it, not
        ;; that the pair collapsed into the `old` slot.
        (is (nil? old) "a full overwrite reports no old value, as it always has")
        (is (= {:v 2} new) "the new value, not the revision")
        (is (some? revision) "and a revision alongside it")
        (is (= {:v 2} (kc/get store :rev-k nil opts))
            "a later cached read must return the VALUE, not the revision token"))
      (let [[[old new] revision] (kc/update-in store [:rev-k] #(assoc % :v 3)
                                               (assoc opts :with-revision? true))]
        (is (= {:v 2} old) "update-in reads the old value, so it must survive the shape")
        (is (= {:v 3} new))
        (is (some? revision))
        (is (= {:v 3} (kc/get store :rev-k nil opts))))
      ;; A cached read cannot answer this: a hit carries no revision.
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (kc/get store :rev-k nil (assoc opts :with-revision? true)))))))

(defn test-cached-PKeyIterable-async [store]
  (go
    (let [store (kc/ensure-cache store)
          opts {:sync? false}]
      (and
       (is (= #{} (<! (kc/keys store opts))))
       (is (= [nil 42] (<! (kc/assoc-in store [:value-blob] 42 opts))))
       (is (true? (<! (kc/bassoc store :bin-blob (byte-array [255 255 255]) opts))))
       (let [store-keys (<! (kc/keys store opts))]
         (and
          (is (every? inst? (map :last-write store-keys)))
          (is (= #{{:key :bin-blob :type :binary} {:key :value-blob :type :edn}}
                 (set (map #(dissoc % :last-write :revision) store-keys))))))))))

(defn test-cached-PBin-async [store locked-cb]
  (let [store (kc/ensure-cache store)
        data [:this/is
              'some/fressian
              "data 😀😀😀"
              #?(:cljs (js/Date.) :clj (java.util.Date.))
              #{true false nil}]
        bytes #?(:cljs (fress/write data)
                 :clj (.array (fress/write data)))
        bytes-ch (promise-chan)]
    (go
      (and
       (is (true? (<! (kc/bassoc store :key bytes {:sync? false}))))
       (is (= data (fress/read (<! (kc/bget store :key locked-cb {:sync? false})))))))))
