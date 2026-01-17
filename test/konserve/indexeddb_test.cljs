(ns konserve.indexeddb-test
  (:require [clojure.core.async :refer [go <! put! take! promise-chan]]
            [cljs.test :refer-macros [deftest is testing async]]
            [konserve.cache :as kc]
            [konserve.core :as k]
            [konserve.utils :refer [multi-key-capable?]]
            [konserve.indexeddb :as idb]
            [konserve.protocols :as p]
            [konserve.tests.cache :as ct]
            [konserve.tests.encryptor :as et]
            [konserve.tests.gc :as gct]
            [konserve.tests.serializers :as st]
            [konserve.tests.tiered :as tiered-tests]
            [konserve.memory :as memory])
  (:import [goog userAgent]))

(defn create-tiered-stores [db-name]
  (go
    (<! (idb/delete-idb db-name))
    (let [f-atom (atom {})
          frontend (<! (memory/new-mem-store f-atom))
          backend (<! (idb/connect-idb-store db-name))]
      {:frontend frontend
       :backend backend
       :db-name db-name})))

(defn cleanup-tiered-stores [{:keys [backend db-name]}]
  (go
    (when (:backing backend)
      (.close (:backing backend)))
    (<! (idb/delete-idb db-name))))

(deftest ^:browser tiered-store-compliance-test
  (async done
         (go
           (let [stores (<! (create-tiered-stores "tiered-comp-test"))]
             (<! (tiered-tests/test-tiered-compliance-async (:frontend stores) (:backend stores)))
             (<! (cleanup-tiered-stores stores))
             (done)))))

(deftest ^:browser tiered-store-write-policies-test
  (async done
         (go
           (let [stores (<! (create-tiered-stores "tiered-write-test"))]
             (<! (tiered-tests/test-write-policies-async (:frontend stores) (:backend stores)))
             (<! (cleanup-tiered-stores stores))
             (done)))))

(deftest ^:browser tiered-store-read-policies-test
  (async done
         (go
           (let [stores (<! (create-tiered-stores "tiered-read-test"))]
             (<! (tiered-tests/test-read-policies-async (:frontend stores) (:backend stores)))
             (<! (cleanup-tiered-stores stores))
             (done)))))

(deftest ^:browser tiered-store-key-operations-test
  (async done
         (go
           (let [stores (<! (create-tiered-stores "tiered-keys-test"))]
             (<! (tiered-tests/test-key-operations-async (:frontend stores) (:backend stores)))
             (<! (cleanup-tiered-stores stores))
             (done)))))

(deftest ^:browser tiered-store-binary-operations-test
  (async done
         (go
           (let [stores (<! (create-tiered-stores "tiered-binary-test"))]
             (<! (tiered-tests/test-binary-operations-async (:frontend stores) (:backend stores)))
             (<! (cleanup-tiered-stores stores))
             (done)))))

(deftest ^:browser tiered-store-sync-on-connect-test
  (async done
         (go
           (let [stores (<! (create-tiered-stores "tiered-sync-test"))]
             (<! (tiered-tests/test-sync-on-connect-async (:frontend stores) (:backend stores)))
             (<! (cleanup-tiered-stores stores))
             (done)))))

(deftest ^:browser tiered-store-error-handling-test
  (tiered-tests/test-error-handling nil nil))

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

(deftest ^:browser multi-key-operations-test
  (async done
         (go
           (let [db-name "multi-key-test"
                 _ (<! (idb/delete-idb db-name))
                 opts {:sync? false}
                 store (<! (idb/connect-idb-store db-name :opts opts))]

             ;; Test multi-key capabilities using konserve.core API
             (is (true? (multi-key-capable? store)))

             ;; Test multi-assoc with multiple keys
             (let [result (<! (k/multi-assoc store {:key1 "value1"
                                                    :key2 "value2"
                                                    :key3 42}
                                             opts))]
               ;; Verify all operations succeeded
               (is (= {:key1 true :key2 true :key3 true} result))

               ;; Verify each value was stored correctly
               (is (= "value1" (<! (k/get store :key1 nil opts))))
               (is (= "value2" (<! (k/get store :key2 nil opts))))
               (is (= 42 (<! (k/get store :key3 nil opts)))))

             ;; Test multi-get - retrieve multiple keys at once
             (let [result (<! (k/multi-get store [:key1 :key2 :key3] opts))]
               ;; Verify all values were retrieved correctly (sparse map with all found)
               (is (= {:key1 "value1" :key2 "value2" :key3 42} result)))

             ;; Test multi-get with some missing keys (sparse map behavior)
             (let [result (<! (k/multi-get store [:key1 :missing-key :key3] opts))]
               ;; Verify only existing keys are in result
               (is (= {:key1 "value1" :key3 42} result))
               ;; Verify missing key is not in result
               (is (not (contains? result :missing-key))))

             ;; Test multi-get with all missing keys
             (let [result (<! (k/multi-get store [:missing1 :missing2] opts))]
               ;; Should return empty map
               (is (= {} result)))

             ;; Clean up
             (<! (.close (:backing store)))
             (<! (idb/delete-idb db-name))
             (done)))))

;; Test using k/create-store multimethod dispatch path (issue #134)
(deftest ^:browser multi-key-via-create-store-test
  (async done
         (go
           (let [db-name "multi-key-create-store-test"
                 _ (<! (idb/delete-idb db-name))
                 opts {:sync? false}
                 config {:backend :indexeddb
                         :id #uuid "550e8400-e29b-41d4-a716-446655440134"
                         :name db-name}
                 ;; Use k/create-store which goes through multimethod dispatch
                 store (<! (k/create-store config opts))]

             ;; This was failing in issue #134 - multi-key-capable? returned false
             (is (true? (multi-key-capable? store)))

             ;; Test multi-assoc works via multimethod-created store
             (let [result (<! (k/multi-assoc store {:key1 "value1" :key2 "value2"} opts))]
               (is (= {:key1 true :key2 true} result)))

             ;; Clean up
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