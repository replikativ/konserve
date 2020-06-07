(ns konserve.filestore-migration-test
 (:refer-clojure :exclude [get get-in keys update update-in assoc assoc-in dissoc exists? bget bassoc])
 (:require [clojure.test :refer :all]
           [konserve.old-filestore :as old-store]
           [clojure.core.async :refer [<!! >!! chan go put!]]
           [konserve.core :refer :all]
           [konserve.filestore :refer [new-fs-store delete-store]]
           [clojure.java.io :as io]
           [clojure.core.async :as async])
 (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

(deftest old-filestore-v1
  (testing "edn migration"
    (let [_              (delete-store "/tmp/konserve-fs-migration-test-v1")
          store          (<!! (old-store/new-fs-store-v1 "/tmp/konserve-fs-migration-test-v1"))
          _              (dotimes [x 10]
                           (<!! (old-store/-assoc-in store [x] x)))
          list-old-store (<!! (old-store/list-keys store))
          new-store      (<!! (new-fs-store "/tmp/konserve-fs-migration-test-v1" :detect-old-file-schema? true))
          list-keys-new  (map #(<!! (get new-store %)) (range 0 10))
          ]
      (are [x y] (= x y)
        list-old-store #{[4] [7] [6] [9] [3] [8] [0] [5] [2] [1]}
        list-keys-new  (range 0 10))))

#_(deftest old-filestore-v2
  (testing "edn migration"
    (let [store          (<!! (old-store/new-fs-store "/tmp/konserve-fs-migration-test-v2"))
          _              (dotimes [x 10]
                           (<!! (old-store/-assoc-in store [x] x)))
          list-old-store (<!! (old-store/list-keys store))
          new-store      (<!! (new-fs-store "/tmp/konserve-fs-migration-test-v2" :detect-old-file-schema? true))]
      (are [x y] (= x y)
        list-old-store #{[4] [7] [6] [9] [3] [8] [0] [5] [2] [1]})))
  (testing "read old binary files"
    (let [store                          (<!! (old-store/new-fs-store "/tmp/konserve-fs-migration-test-v2"))
          _                              (dotimes [x 10]
                                           (<!! (old-store/-bassoc store x (byte-array (range 10)))))
          list-old-store                 (<!! (old-store/list-keys-v2 store))
          new-store                      (<!! (new-fs-store "/tmp/konserve-fs-migration-test-v2" :detect-old-file-schema? true))
          _                              (dotimes [x 10]
                                           (<!! (bget new-store x (fn [{:keys [input-stream]}]
                                                                    (go
                                                                      (is (= (map byte (slurp input-stream))
                                                                             (range 10))))))))
          list-new-store-after-migration (<!! (keys store))
          _                              (delete-store "/tmp/konserve-fs-migration-test-v2")]
      (are [x y] (= x y)
        list-old-store                 #{}
        list-new-store-after-migration #{}))))



