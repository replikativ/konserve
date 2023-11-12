(ns konserve.node-filestore-test
  (:require [clojure.core.async :refer [go promise-chan <! put! chan timeout to-chan!]]
            [cljs-node-io.fs :as fs]
            [cljs-node-io.core :as io]
            [cljs.test :refer-macros [deftest is testing async use-fixtures]]
            [fress.api :as fress]
            [konserve.core :as k]
            [konserve.impl.defaults :as d]
            [konserve.node-filestore :as filestore :refer [connect-fs-store]]
            [konserve.protocols :as p]
            [konserve.tests.cache :as ct]
            [konserve.tests.encryptor :as et]
            [konserve.tests.gc :as gct]
            [konserve.tests.serializers :as st]))

(def store-path "/tmp/konserve-fs-nodejs-test")

(use-fixtures :each {:before #(fs/rm-rf store-path)})

(deftest PEDNKeyValueStore-sync-test
  (let [opts {:sync? true}
        store (connect-fs-store store-path :opts opts)]
    (and
     (is (false? (p/-exists? store :bar opts)))
     (is (= :not-found (p/-get-in store [:bar] :not-found opts)))
     (is (= [nil 42] (p/-assoc-in store [:bar] identity 42 opts)))
     (is (true? (p/-exists? store :bar opts)))
     (is (= 42 (p/-get-in store [:bar] :not-found opts)))
     (is (= [42 43] (p/-update-in store [:bar] identity inc opts)))
     (is (= nil (p/-get-meta store :bar opts)))
     (is (= [43 44] (p/-update-in store [:bar] #(assoc % :foo :baz) inc opts)))
     (is (= {:foo :baz} (p/-get-meta store :bar opts)))
     (is (= [44 45] (p/-update-in store [:bar] (fn [_] nil) inc opts)))
     (is (= nil (p/-get-meta store :bar opts)))
     (is (= 45 (p/-get-in store [:bar] :not-found opts)))
     (is (= [nil {::foo 99}] (p/-assoc-in store [:bar] identity {::foo 99} opts))) ;; assoc-in overwrites
     (is (= [{::foo 99} {::foo 100}] (p/-update-in store [:bar ::foo] identity inc opts)))
     (is (= [{::foo 100} {}] (p/-update-in store [:bar] identity #(dissoc % ::foo) opts)))
     (is (true? (p/-dissoc store :bar opts)))
     (is (false? (p/-exists? store :bar opts))))))

(deftest existing-store-sync-test
  (let [opts {:sync? true}
        store (connect-fs-store store-path :opts opts)]
    (is (= [nil {:a 42 :b "a string"}] (p/-assoc-in store [:my-db] identity {:a 42 :b "a string"} opts)))
    (let [store' (connect-fs-store store-path :opts opts)]
      (is (= [{:a 42 :b "a string"} {:a 42 :b "a string" :c 'sym}]
             (p/-update-in store' [:my-db] identity (fn [m] (assoc m :c 'sym)) opts))))))

(deftest existing-store-async-test
  (async done
         (go
           (let [opts {:sync? false}
                 store (<! (connect-fs-store store-path :opts opts))]
             (is (= [nil {:a 42 :b "a string"}] (<! (p/-assoc-in store [:my-db] identity {:a 42 :b "a string"} opts))))
             (let [store' (<! (connect-fs-store store-path :opts opts))]
               (is (= [{:a 42 :b "a string"} {:a 42 :b "a string" :c 'sym}]
                      (<! (p/-update-in store' [:my-db] identity (fn [m] (assoc m :c 'sym)) opts))))
               (done))))))

(deftest PEDNKeyValueStore-async-test
  (async done
         (go
           (let [opts {:sync? false}
                 store (<! (connect-fs-store store-path :opts opts))]
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
             (done)))))

(deftest PBinaryKeyValueStore-async-test
  (async done
         (let [opts {:sync? false}
               data [:this/is 'some/fressian "data 😀😀😀" (js/Date.) #{true false nil}]
               bytes (fress/write data)
               locked-cb (fn [{readable :input-stream}]
                           (to-chan! [(fress/read (.read readable))]))]
           (go
             (let [store (<! (connect-fs-store store-path :opts opts))]
               (and
                (is (true? (<! (p/-bassoc store :key identity bytes opts))))
                (is (= data (<! (p/-bget store :key locked-cb opts)))))
               (done))))))

(deftest PKeyIterable-sync-test
  (let [opts {:sync? true}
        store (connect-fs-store store-path :opts opts)]
    (is (= #{} (k/keys store opts)))
    (is (= [nil 42] (k/assoc-in store [:value-blob] 42 opts)))
    (is (true? (k/bassoc store :bin-blob #js[255 255 255] opts)))
    (is (= #{{:key :bin-blob :type :binary} {:key :value-blob :type :edn}}
           (set (map #(dissoc % :last-write) (k/keys store opts)))))))

(deftest PKeyIterable-async-test
  (async done
         (go
           (let [opts {:sync? false}
                 store (<! (connect-fs-store store-path :opts opts))]
             (is (= #{} (<! (k/keys store opts))))
             (is (= [nil 42] (<! (k/assoc-in store [:value-blob] 42 opts))))
             (is (true? (<! (k/bassoc store :bin-blob #js[255 255 255] opts))))
             (is (= #{{:key :bin-blob :type :binary} {:key :value-blob :type :edn}}
                    (set (map #(dissoc % :last-write) (<! (k/keys store opts))))))
             (is (every? inst? (map :last-write (<! (k/keys store opts)))))
             (done)))))

(deftest append-store-sync-test
  (let [opts {:sync? true}
        store (connect-fs-store store-path :opts opts)]
    (k/append store :foolog {:bar 42} opts)
    (k/append store :foolog {:bar 43} opts)
    (is (= '({:bar 42} {:bar 43}) (k/log store :foolog opts)))
    (is (= [{:bar 42} {:bar 43}] (k/reduce-log store :foolog conj [] opts)))
    (let [{:keys [key type last-write]} (k/get-meta store :foolog :not-found opts)]
      (is (= key :foolog))
      (is (= type :append-log))
      (is (inst? last-write)))))

(deftest append-store-async-test
  (async done
         (go
           (let [opts {:sync? false}
                 store (<! (connect-fs-store store-path :opts opts))]
             (is (uuid? (second (<! (k/append store :foolog {:bar 42} opts)))))
             (is (uuid? (second (<! (k/append store :foolog {:bar 43} opts)))))
             (is (= '({:bar 42} {:bar 43}) (<! (k/log store :foolog opts))))
             (is (=  [{:bar 42} {:bar 43}] (<! (k/reduce-log store :foolog conj [] opts))))
             (let [{:keys [key type last-write]} (<! (k/get-meta store :foolog :not-found opts))]
               (is (= key :foolog))
               (is (= type :append-log))
               (is (inst? last-write)))
             (done)))))

(def path (js/require "path"))

;; lock used for reading/update blobs, list-keys
(deftest lock-acquisition-test
  (async done
         (go
           (let [opts {:sync? false}
                 store (<! (connect-fs-store store-path :opts opts))
                 key :key
                 key-file-path (path.join store-path (d/key->store-key key))]
             (testing "no lock, writes ok"
               (is (= [nil 42] (<! (k/assoc-in store [key] 42 opts))))
               (is (= 42 (<! (k/get-in store [key] :not-found opts)))))
             (let [fc (<! (filestore/open-async-file-channel key-file-path))
                   lock (<! (.lock fc))]
               (testing "held lock blocks reads + writes"
                 (is (instance? js/Error (<! (k/update-in store [key] inc opts)))))
               (testing "lock is released within the retry period, op succeeds"
                 (let [op-chan (k/update-in store [key] inc opts)]
                   (<! (timeout 50))
                   (<! (.release lock))
                   (is (= [42 43] (<! op-chan)))))
               (is (nil? (<! (.close fc)))))
             (done)))))

#!============
#! Cache tests

(deftest cache-PEDNKeyValueStore-test
  (filestore/delete-store "/tmp/cache-store")
  (async done
         (go
           (let [store (<! (connect-fs-store "/tmp/cache-store"))]
             (<! (ct/test-cached-PEDNKeyValueStore-async store))
             (done)))))

(deftest cache-PKeyIterable-test
  (filestore/delete-store "/tmp/cache-store")
  (async done
         (go
           (let [store (<! (connect-fs-store "/tmp/cache-store"))]
             (<! (ct/test-cached-PKeyIterable-async store))
             (done)))))

(deftest cache-PBin-test
  (filestore/delete-store "/tmp/cache-store")
  (async done
         (go
           (let [store (<! (connect-fs-store "/tmp/cache-store"))
                 f (fn [{:keys [input-stream]}]
                     (to-chan! [(.read input-stream)]))]
             (<! (ct/test-cached-PBin-async store f))
             (done)))))

#!============
#! GC tests

(deftest async-gc-test
  (filestore/delete-store "/tmp/gc-store")
  (async done
         (go
           (let [store (<! (connect-fs-store "/tmp/gc-store"))]
             (<! (gct/test-gc-async store))
             (done)))))

#!==================
#! Serializers tests

(deftest fressian-serializer-test
  (async done
         (go
           (<! (st/test-fressian-serializers-async "/tmp/serializers-test"
                                                   connect-fs-store
                                                   (fn [store-name]
                                                     (go (filestore/delete-store store-name)))
                                                   (fn [{:keys [input-stream]}]
                                                     (to-chan! [(.read input-stream)]))))
           (done))))

#!==================
#! Encryptor tests

(deftest encryptor-async-test
  (async done
         (go
           (<! (et/async-encryptor-test "/tmp/encryptor-test"
                                        connect-fs-store
                                        (fn [store-name]
                                          (go (filestore/delete-store store-name)))))
           (done))))