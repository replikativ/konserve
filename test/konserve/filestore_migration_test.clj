(ns konserve.filestore-migration-test
  #_(:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? bget bassoc keys])
  #_(:require [clojure.test :refer :all]
            [konserve.old-filestore :as old-store]
            [clojure.core.async :refer [<!! >!! chan go]]
            [konserve.core :refer :all]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store delete-store list-keys]]
            [clojure.java.io :as io]
            [clojure.core.async :as async])
  #_(:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

#_(comment
  (deftest old-filestore
    (testing "edn migration"
      (let [store                          (<!! (old-store/new-fs-store "/tmp/konserve-fs-migration-test"))
            _                              (dotimes [x 10]
                                             (<!! (assoc-in store [x] x)))
            list-old-store                 (<!! (old-store/list-keys store))
            new-store                      (<!! (new-fs-store "/tmp/konserve-fs-migration-test"))
            list-new-store                 (<!! (list-keys new-store))
            list-old-store-after-migration (<!! (old-store/list-keys store))]
        (are [x y] (= x y)
          list-old-store                 #{[4] [7] [6] [9] [3] [8] [0] [5] [2] [1]} 
          list-new-store                 #{{:key 1, :format :edn} {:key 3, :format :edn}
                                           {:key 5, :format :edn} {:key 4, :format :edn}
                                           {:key 7, :format :edn} {:key 0, :format :edn}
                                           {:key 8, :format :edn} {:key 6, :format :edn}
                                           {:key 9, :format :edn} {:key 2, :format :edn}}
          list-old-store-after-migration #{})))
    (testing "read old binary files"
      (let [store                          (<!! (old-store/new-fs-store "/tmp/konserve-fs-migration-test"))
            _                              (dotimes [x 10]
                                             (<!! (bassoc store x (byte-array (range 10)))))
            list-old-store                 (<!! (old-store/list-keys store))
            new-store                      (<!! (new-fs-store "/tmp/konserve-fs-migration-test"))
            list-new-store                 (<!! (list-keys new-store))
            _                              (dotimes [x 10]
                                             (<!! (bget new-store x (fn [{:keys [input-stream]}]
                                                                      (go
                                                                        (is (= (map byte (slurp input-stream))
                                                                               (range 10))))))))
            list-old-store-after-migration (<!! (old-store/list-keys store))
            list-new-store-after-migration (<!! (list-keys new-store))
            _                              (delete-store "/tmp/konserve-fs-migration-test")]
        (are [x y] (= x y)
          list-old-store                 #{}
          list-new-store                 #{{:key 1, :format :edn} {:key 3, :format :edn}
                                           {:key 5, :format :edn} {:key 4, :format :edn}
                                           {:key 7, :format :edn} {:key 0, :format :edn}
                                           {:key 8, :format :edn} {:key 6, :format :edn}
                                           {:key 9, :format :edn} {:key 2, :format :edn}}
          list-old-store-after-migration #{}
          list-new-store-after-migration #{{:key 1, :format :binary} {:key 3, :format :binary}
                                           {:key 9, :format :binary} {:key 2, :format :binary}
                                           {:key 7, :format :binary} {:key 0, :format :binary}
                                           {:key 5, :format :binary} {:key 8, :format :binary}
                                           {:key 4, :format :binary} {:key 6, :format :binary}})))))
