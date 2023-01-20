(ns konserve.cache-test
  (:require [cljs.core.async :refer [<! go promise-chan put!]]
            [cljs.test :refer [deftest is async]]
            [fress.api :as fress]
            [konserve.cache :as k]
            [konserve.node-filestore :as fstore]))

(def store-path "/tmp/cache-store")

(deftest cache-PEDNKeyValueStore-test
  (fstore/delete-store store-path)
  (async done
         (go
           (let [opts {:sync? false}
                 test-store (<! (fstore/connect-fs-store store-path :opts opts))
                 store (k/ensure-cache test-store)]
             (is (nil? (<! (k/get store :foo nil opts))))
             (is (false? (<! (k/exists? store :foo opts))))
             (is [nil :bar] (<! (k/assoc store :foo :bar opts)))
             (is (true? (<! (k/exists? store :foo))))
             (is (= :bar (<! (k/get store :foo nil opts))))
             (is (= [nil :bar2] (<! (k/assoc-in store [:foo] :bar2 opts))))
             (is (= :bar2 (<! (k/get store :foo nil opts))))
             (is (= :default (<! (k/get-in store [:fuu] :default opts))))
             (is (= :bar2 (<! (k/get store :foo nil opts))))
             (is (= :default (<! (k/get-in store [:fuu] :default opts))))
             (is (= [:bar2 "bar2"] (<! (k/update-in store [:foo] name opts))))
             (is (= "bar2" (<! (k/get store :foo nil opts))))
             (is (=  [nil {:bar 42}] (<! (k/assoc-in store [:baz] {:bar 42} opts))))
             (is (= 42 (<! (k/get-in store [:baz :bar] nil opts))))
             (is (= [{:bar 42} {:bar 43}] (<! (k/update-in store [:baz :bar] inc opts))))
             (is (= 43 (<! (k/get-in store [:baz :bar] nil opts))))
             (is (= [{:bar 43} {:bar 48}] (<! (k/update-in store [:baz :bar] (fn [x] (+ x 2 3)) opts))))
             (is (= 48 (<! (k/get-in store [:baz :bar] nil opts))))
             (is (true? (<! (k/dissoc store :foo opts))))
             (is (nil? (<! (k/get-in store [:foo] nil opts))))
             (done)))))

(deftest cache-PBin-test
  (fstore/delete-store store-path)
  (async done
         (let [opts {:sync? false}
               data [:this/is 'some/fressian "data 😀😀😀" (js/Date.) #{true false nil}]
               bytes (fress/write data)
               test-data (promise-chan)
               locked-cb (fn [{readable :input-stream}]
                           (put! test-data (fress/read (.read readable)))
                           (.destroy readable))]
           (go
             (let [store (k/ensure-cache (<! (fstore/connect-fs-store store-path :opts opts)))]
               (is (true? (<! (k/bassoc store :key bytes opts))))
               (<! (k/bget store :key locked-cb opts))
               (is (= data (<! test-data)))
               (done))))))

(deftest cache-PKeyIterable-test
  (fstore/delete-store store-path)
  (async done
         (go
           (let [opts {:sync? false}
                 store (k/ensure-cache (<! (fstore/connect-fs-store store-path :opts opts)))]
             (is (= #{} (<! (k/keys store opts))))
             (is (= [nil 42] (<! (k/assoc-in store [:value-blob] 42 opts))))
             (is (true? (<! (k/bassoc store :bin-blob #js[255 255 255] opts))))
             (let [store-keys (<! (k/keys store opts))]
               (is (every? inst? (map :last-write store-keys)))
               (is (= #{{:key :bin-blob :type :binary} {:key :value-blob :type :edn}}
                      (set (map #(dissoc % :last-write) store-keys)))))
             (done)))))
