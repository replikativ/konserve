(ns konserve.filestore-migration-test
  (:refer-clojure :exclude [get get-in keys update update-in assoc assoc-in exists? bget bassoc])
  (:require [clojure.test :refer [deftest testing are]]
            [konserve.old-filestore :as old-store]
            [clojure.core.async :refer [go <!!]]
            [konserve.core :refer [get bget keys]]
            [konserve.filestore :refer [connect-fs-store delete-store]]))

(deftest old-filestore-v1
  (testing "edn migration single call"
    (let [_              (delete-store "/tmp/konserve-fs-migration-test-v1-1")
          store          (<!! (old-store/new-fs-store-v1 "/tmp/konserve-fs-migration-test-v1-1"))
          _              (dotimes [x 10]
                           (<!! (old-store/-assoc-in store [x] x)))
          list-old-store (<!! (old-store/list-keys store))
          new-store      (<!! (connect-fs-store "/tmp/konserve-fs-migration-test-v1-1" :detect-old-file-schema? true))
          list-keys-new  (doall (map #(<!! (get new-store %)) (range 0 10)))]
      (are [x y] (= x y)
        #{[4] [7] [6] [9] [3] [8] [0] [5] [2] [1]} list-old-store
        (range 0 10) list-keys-new)))
  (testing "edn migration list-keys"
    (let [_              (delete-store "/tmp/konserve-fs-migration-test-v1-2")
          store          (<!! (old-store/new-fs-store-v1 "/tmp/konserve-fs-migration-test-v1-2"))
          _              (dotimes [x 10]
                           (<!! (old-store/-assoc-in store [x] x)))
          list-old-store (<!! (old-store/list-keys store))
          new-store      (<!! (connect-fs-store "/tmp/konserve-fs-migration-test-v1-2" :detect-old-file-schema? true))
          list-keys-new  (<!! (keys new-store))]
      (are [x y] (= x y)
        #{[4] [7] [6] [9] [3] [8] [0] [5] [2] [1]} list-old-store
        #{{:key 0, :type :edn}
          {:key 2, :type :edn}
          {:key 7, :type :edn}
          {:key 9, :type :edn}
          {:key 6, :type :edn}
          {:key 1, :type :edn}
          {:key 3, :type :edn}
          {:key 4, :type :edn}
          {:key 5, :type :edn}
          {:key 8, :type :edn}}
        (into #{} (map #(dissoc % :last-write) list-keys-new)))))
  (testing "binary migration single calls"
    (let [_         (delete-store "/tmp/konserve-fs-migration-test-v1-3")
          store     (<!! (old-store/new-fs-store-v1 "/tmp/konserve-fs-migration-test-v1-3"))
          _         (dotimes [x 10]
                      (<!! (old-store/-bassoc store x (byte-array (range 10)))))
          new-store (<!! (connect-fs-store "/tmp/konserve-fs-migration-test-v1-3" :detect-old-file-schema? true))
          _         (dotimes [x 10]
                      (<!! (bget new-store x
                                 (fn [{:keys [input-stream]}]
                                   (go
                                     true)))))

          list-keys (<!! (keys new-store))]
      (are [x y] (= x y)
        #{{:key 0, :type :binary}
          {:key 2, :type :binary}
          {:key 7, :type :binary}
          {:key 9, :type :binary}
          {:key 6, :type :binary}
          {:key 1, :type :binary}
          {:key 3, :type :binary}
          {:key 4, :type :binary}
          {:key 5, :type :binary}
          {:key 8, :type :binary}}
        (into #{} (map #(dissoc % :last-write) list-keys)))))
  (testing "binary migration list-keys"
    (let [_         (delete-store "/tmp/konserve-fs-migration-test-v1-4")
          store     (<!! (old-store/new-fs-store-v1 "/tmp/konserve-fs-migration-test-v1-4"))
          _         (dotimes [x 10]
                      (<!! (old-store/-bassoc store x (byte-array (range 10)))))
          new-store (<!! (connect-fs-store "/tmp/konserve-fs-migration-test-v1-4" :detect-old-file-schema? true))
          list-keys (<!! (keys new-store))]
      (are [x y] (= x y)
        #{{:type :stale-binary,
           :msg
           "Old binary file detected. Use bget instead of keys for migration."}}
        (into #{} (map #(dissoc % :store-key) list-keys))))))

(deftest old-filestore-v2
  (testing "edn migration single call. migration via get."
    (let [_              (delete-store "/tmp/konserve-fs-migration-test-v2-1")
          store          (<!! (old-store/new-fs-store "/tmp/konserve-fs-migration-test-v2-1"))
          _              (dotimes [x 10]
                           (<!! (old-store/-assoc-in store [x] x)))
          list-old-store (<!! (old-store/list-keys-v2 store))
          new-store      (<!! (connect-fs-store "/tmp/konserve-fs-migration-test-v2-1" :detect-old-file-schema? true))
          list-keys-new  (doall (map #(<!! (get new-store %)) (range 0 10)))]
      (are [x y] (= x y)
        #{{:key 1, :format :edn}
          {:key 3, :format :edn}
          {:key 5, :format :edn}
          {:key 4, :format :edn}
          {:key 7, :format :edn}
          {:key 0, :format :edn}
          {:key 8, :format :edn}
          {:key 6, :format :edn}
          {:key 9, :format :edn}
          {:key 2, :format :edn}}
        list-old-store
        (range 0 10) list-keys-new)))
  ;; hangs for me
  (testing "edn migration list-keys"
    (let [_              (delete-store "/tmp/konserve-fs-migration-test-v2-2")
          store          (<!! (old-store/new-fs-store "/tmp/konserve-fs-migration-test-v2-2"))
          _              (dotimes [x 10]
                           (<!! (old-store/-assoc-in store [x] x)))
          list-old-store (<!! (old-store/list-keys-v2 store))
          new-store      (<!! (connect-fs-store "/tmp/konserve-fs-migration-test-v2-2" :detect-old-file-schema? true))
          list-keys-new  (<!! (keys new-store))]
      (are [x y] (= x y)
        #{{:key 1, :format :edn}
          {:key 3, :format :edn}
          {:key 5, :format :edn}
          {:key 4, :format :edn}
          {:key 7, :format :edn}
          {:key 0, :format :edn}
          {:key 8, :format :edn}
          {:key 6, :format :edn}
          {:key 9, :format :edn}
          {:key 2, :format :edn}} list-old-store
        #{{:key 0, :type :edn}
          {:key 2, :type :edn}
          {:key 7, :type :edn}
          {:key 9, :type :edn}
          {:key 6, :type :edn}
          {:key 1, :type :edn}
          {:key 3, :type :edn}
          {:key 4, :type :edn}
          {:key 5, :type :edn}
          {:key 8, :type :edn}}
        (into #{} (map #(dissoc % :last-write) list-keys-new)))))
  (testing "binary migration single calls"
    (let [_         (delete-store "/tmp/konserve-fs-migration-test-v2-3")
          store     (<!! (old-store/new-fs-store "/tmp/konserve-fs-migration-test-v2-3"))
          _         (dotimes [x 10]
                      (<!! (old-store/-bassoc store x (byte-array (range 10)))))
          new-store (<!! (connect-fs-store "/tmp/konserve-fs-migration-test-v2-3" :detect-old-file-schema? true))
          _         (dotimes [x 10]
                      (<!! (bget new-store x
                                 (fn [{:keys [_]}]
                                   (go
                                     true)))))

          list-keys (<!! (keys new-store))]
      (are [x y] (= x y)
        #{{:key 0, :type :binary}
          {:key 2, :type :binary}
          {:key 7, :type :binary}
          {:key 9, :type :binary}
          {:key 6, :type :binary}
          {:key 1, :type :binary}
          {:key 3, :type :binary}
          {:key 4, :type :binary}
          {:key 5, :type :binary}
          {:key 8, :type :binary}}
        (into #{} (map #(dissoc % :last-write) list-keys)))))
  (testing "binary migration list-keys"
    (let [_         (delete-store "/tmp/konserve-fs-migration-test-v2-4")
          store     (<!! (old-store/new-fs-store "/tmp/konserve-fs-migration-test-v2-4"))
          _         (dotimes [x 10]
                      (<!! (old-store/-bassoc store x (byte-array (range 10)))))
          new-store (<!! (connect-fs-store "/tmp/konserve-fs-migration-test-v2-4" :detect-old-file-schema? true))
          list-keys (<!! (keys new-store))]
      (are [x y] (= x y)
        #{{:key 0, :type :binary}
          {:key 2, :type :binary}
          {:key 7, :type :binary}
          {:key 9, :type :binary}
          {:key 6, :type :binary}
          {:key 1, :type :binary}
          {:key 3, :type :binary}
          {:key 4, :type :binary}
          {:key 5, :type :binary}
          {:key 8, :type :binary}}
        (into #{} (map #(dissoc % :last-write) list-keys))))))
