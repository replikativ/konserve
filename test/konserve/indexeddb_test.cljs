(ns konserve.indexeddb-test
  (:require [cljs.core.async :refer [go take! <! >! put! take! close!]]
            [cljs.test :refer-macros [deftest is testing async]]
            [fress.api :as fress]
            [konserve.core :as k]
            [konserve.indexeddb :as idb]
            [konserve.protocols :as p])
  (:import [goog.userAgent]))

(when-not ^boolean goog.userAgent.GECKO
  (deftest lifecycle-test
    "test creation & deletion of databases"
    (async done
     (go
      (let [db-name "lifecycle-db"]
        (when (is (false? (<! (idb/db-exists? db-name))))
          (let [[err ok] (<! (idb/connect-to-idb db-name))]
            (is (nil? err))
            (is (true? (<! (idb/db-exists? db-name))))
            (.close ok)
            (is (nil? (first (<! (idb/delete-idb db-name)))))
            (is (false? (<! (idb/db-exists? db-name))))))
        (done))))))

(deftest blob-io-test
  "test simulating fs with js/Blobs"
  (let [db-name "blob-io-test"
        key "test-blob"
        data [:this/is 'some/fressian "data 😀😀😀" (js/Date.) #{true false nil}]
        bytes (fress/write data)]
    (async done
     (go
      (<! (idb/delete-idb db-name))
      (let [[err db] (<! (idb/connect-to-idb db-name))]
        (when (is (nil? err))
          (let [bb (idb/open-backing-blob db key)]
            (is (nil? (:buf bb)))
            (is [nil] (<! (idb/get-buf bb)))
            (is (nil? (:buf bb)))
            (set! (.-buf bb) bytes)
            (is (nil? (first (<! (.force bb)))))
            (let [[err ok] (<! (idb/get-buf bb))]
              (is (nil? err))
              (is (some? (:buf bb)))
              (is (identical? (:buf bb) ok))
              (is (= data (fress/read ok)))))
          (.close db)))
      (testing "we've closed conn, but kv should still exist"
        (let [[err db] (<! (idb/connect-to-idb db-name))]
          (when (is (nil? err))
            (let [bb (idb/open-backing-blob db key)
                  [err ok] (<! (idb/get-buf bb))]
              (is (nil? err))
              (is (= data (fress/read ok)))
              (testing "a second identical blob peacefully co-exists"
                (let [key' "test-blob'"
                      bb' (idb/open-backing-blob db key')]
                  (set! (.-buf bb') bytes)
                  (is (nil? (first (<! (.force bb')))))
                  (let [[err ok] (<! (idb/get-buf bb))
                        [err' ok'] (<! (idb/get-buf bb'))]
                    (is (nil? err))
                    (is (nil? err'))
                    (is (= data (fress/read ok) (fress/read ok')))))))
            (.close db))))
      (done)))))

(deftest PEDNKeyValueStore-async-test
  (async done
   (go
    (let [db-name "pednkv-test"
          _(<! (idb/delete-idb db-name))
          opts {:sync? false}
          {backing :backing :as store} (<! (idb/connect-idb-store db-name :opts opts))]
      (is (false? (<! (p/-exists? store :bar opts))))
      (is (= :not-found (<! (p/-get-in store [:bar] :not-found opts))))
      (is (= [nil 42] (<! (p/-assoc-in store [:bar] identity 42 opts))))
      (is (true? (<! (p/-exists? store :bar opts))))
      (is (= 42 (<! (p/-get-in store [:bar] :not-found opts))))
      (is (= [42 43] (<! (p/-update-in store [:bar] identity inc opts))))
      (is (= nil (<! (p/-get-meta store :bar opts))))
      (is (= [43 44] (<! (p/-update-in store [:bar] #(assoc % :foo :baz) inc opts))))
      (is (= {:foo :baz} (<! (p/-get-meta store :bar opts))))
      (is (= [44 45] (<! (p/-update-in store [:bar] (fn [_] nil) inc opts))))
      (is (= nil (<! (p/-get-meta store :bar opts))))
      (is (= 45 (<! (p/-get-in store [:bar] :not-found opts))))
      (is (= [nil {::foo 99}] (<! (p/-assoc-in store [:bar] identity {::foo 99} opts)))) ;; assoc-in overwrites
      (is (= [{::foo 99} {::foo 100}] (<! (p/-update-in store [:bar ::foo] identity inc opts))))
      (is (= [{::foo 100} {}] (<! (p/-update-in store [:bar] identity #(dissoc % ::foo) opts))))
      (is (true? (<! (p/-dissoc store :bar opts))))
      (is (false? (<! (p/-exists? store :bar opts))))
      (.close backing)
      (done)))))

(deftest PKeyIterable-async-test
  (async done
   (go
    (let [db-name "pkeyiterable-test"
          _(<! (idb/delete-idb db-name))
          opts {:sync? false}
          {backing :backing :as store} (<! (idb/connect-idb-store db-name :opts opts))]
      (is (= #{} (<! (k/keys store opts))))
      (is (= [nil 42] (<! (k/assoc-in store [:value-blob] 42 opts))))
      (is (true? (<! (k/bassoc store :bin-blob #js[255 255 255] opts))))
      (is (= #{{:key :bin-blob :type :binary} {:key :value-blob :type :edn}}
             (set (map #(dissoc % :last-write) (<! (k/keys store opts))))))
      (is (every? inst? (map :last-write (<! (k/keys store opts)))))
      (.close backing)
      (done)))))

(deftest append-store-async-test
  (async done
    (go
     (let [db-name "append-test"
           _(<! (idb/delete-idb db-name))
           opts {:sync? false}
           {backing :backing :as store} (<! (idb/connect-idb-store db-name :opts opts))]
       (is (uuid? (second (<! (k/append store :foolog {:bar 42} opts)))))
       (is (uuid? (second (<! (k/append store :foolog {:bar 43} opts)))))
       (is (= '({:bar 42} {:bar 43}) (<! (k/log store :foolog opts))))
       (is (=  [{:bar 42} {:bar 43}] (<! (k/reduce-log store :foolog conj [] opts))))
       (let [{:keys [key type last-write] :as metadata} (<! (k/get-meta store :foolog :not-found opts))]
         (is (= key :foolog))
         (is (= type :append-log))
         (is (inst? last-write)))
       (.close backing)
       (done)))))
