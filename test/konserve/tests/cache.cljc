(ns konserve.tests.cache
  (:require [clojure.core.async :refer [go <! promise-chan put! take!]]
            [clojure.test :refer [deftest is]]
            [fress.api :as fress]
            [konserve.cache :as kc]
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
                (set (map #(dissoc % :last-write) store-keys))))))))))

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
