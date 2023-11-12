(ns konserve.indexeddb-test
  (:require [clojure.core.async :refer [go <! put! take! promise-chan]]
            [cljs.test :refer-macros [deftest is testing async]]
            [konserve.cache :as kc]
            [konserve.core :as k]
            [konserve.indexeddb :as idb]
            [konserve.protocols :as p]
            [konserve.tests.cache :as ct]
            [konserve.tests.encryptor :as et]
            [konserve.tests.gc :as gct]
            [konserve.tests.serializers :as st])
  (:import [goog userAgent]))

(deftest ^:browser lifecycle-test
  (if ^boolean userAgent.GECKO
    (is (true? true))
    (async done
           (go
             (let [db-name "lifecycle-db"]
               (<! (idb/delete-idb db-name))
               (testing "test creation & deletion of databases"
                 (when (is (false? (<! (idb/db-exists? db-name))))
                   (let [res (<! (idb/connect-to-idb db-name))]
                     (when (is (instance? js/IDBDatabase res))
                       (is (true? (<! (idb/db-exists? db-name))))
                       (.close res)
                       (is (nil? (first (<! (idb/delete-idb db-name)))))
                       (is (false? (<! (idb/db-exists? db-name))))))))
               (<! (idb/delete-idb db-name))
               (done))))))

(deftest ^:browser PEDNKeyValueStore-async-test
  (async done
         (go
           (let [db-name "pednkv-test"
                 _ (<! (idb/delete-idb db-name))
                 opts {:sync? false}
                 store (<! (idb/connect-idb-store db-name :opts opts))]
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
             (<! (.close (:backing store)))
             (<! (idb/delete-idb db-name))
             (done)))))

(deftest ^:browser PKeyIterable-async-test
  (async done
         (go
           (let [db-name "pkeyiterable-test"
                 _ (<! (idb/delete-idb db-name))
                 opts {:sync? false}
                 store (<! (idb/connect-idb-store db-name :opts opts))]
             (is (= #{} (<! (k/keys store opts))))
             (is (= [nil 42] (<! (k/assoc-in store [:value-blob] 42 opts))))
             (is (true? (<! (k/bassoc store :bin-blob #js[255 255 255] opts))))
             (is (= #{{:key :bin-blob :type :binary} {:key :value-blob :type :edn}}
                    (set (map #(dissoc % :last-write) (<! (k/keys store opts))))))
             (is (every? inst? (map :last-write (<! (k/keys store opts)))))
             (<! (.close (:backing store)))
             (<! (idb/delete-idb db-name))
             (done)))))

(deftest ^:browser append-store-async-test
  (async done
         (go
           (let [db-name "append-test"
                 _ (<! (idb/delete-idb db-name))
                 opts {:sync? false}
                 store (<! (idb/connect-idb-store db-name :opts opts))]
             (is (uuid? (second (<! (k/append store :foolog {:bar 42} opts)))))
             (is (uuid? (second (<! (k/append store :foolog {:bar 43} opts)))))
             (is (= '({:bar 42} {:bar 43}) (<! (k/log store :foolog opts))))
             (is (=  [{:bar 42} {:bar 43}] (<! (k/reduce-log store :foolog conj [] opts))))
             (let [{:keys [key type last-write]} (<! (k/get-meta store :foolog :not-found opts))]
               (is (= key :foolog))
               (is (= type :append-log))
               (is (inst? last-write)))
             (<! (.close (:backing store)))
             (<! (idb/delete-idb db-name))
             (done)))))

#!============
#! Cache tests

(deftest cache-PEDNKeyValueStore-test
  (async done
         (go
           (<! (idb/delete-idb "cache-store"))
           (let [store (<! (idb/connect-idb-store "cache-store"))]
             (<! (ct/test-cached-PEDNKeyValueStore-async store))
             (<! (.close (:backing store)))
             (<! (idb/delete-idb "cache-store"))
             (done)))))

(deftest cache-PKeyIterable-test
  (async done
         (go
           (<! (idb/delete-idb "cache-store"))
           (let [store (<! (idb/connect-idb-store "cache-store"))]
             (<! (ct/test-cached-PKeyIterable-async store))
             (<! (.close (:backing store)))
             (<! (idb/delete-idb "cache-store"))
             (done)))))

(deftest cache-PBin-test
  (async done
         (go
           (<! (idb/delete-idb "cache-store"))
           (let [store (<! (idb/connect-idb-store "cache-store"))]
             (<! (ct/test-cached-PBin-async store idb/read-web-stream))
             (<! (.close (:backing store)))
             (<! (idb/delete-idb "cache-store"))
             (done)))))

#!============
#! GC tests

(deftest async-gc-test
  (async done
         (go
           (<! (idb/delete-idb "gc-store"))
           (let [store (<! (idb/connect-idb-store "gc-store"))]
             (<! (gct/test-gc-async store))
             (<! (.close (:backing store)))
             (<! (idb/delete-idb "gc-store"))
             (done)))))

#!==================
#! Serializers tests

(deftest fressian-serializer-test
  (async done
         (go
           (<! (st/test-fressian-serializers-async "serializers-test"
                                                   idb/connect-idb-store
                                                   idb/delete-idb
                                                   idb/read-web-stream))
           (done))))

#!==================
#! Encryptor tests

(deftest encryptor-async-test
  (async done
         (go
           (<! (et/async-encryptor-test "encryptor-test"
                                        idb/connect-idb-store
                                        idb/delete-idb))
           (done))))