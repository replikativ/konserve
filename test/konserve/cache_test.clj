(ns konserve.cache-test
  (:refer-clojure :exclude [get-in update-in assoc-in dissoc exists?])
  (:require [konserve.cache :refer :all]
            [konserve.filestore :as fstore]
            [clojure.core.async :refer [<!!]]
            [clojure.test :refer :all]))


(deftest cache-test
  (testing "Testing the cache namespace"
    (let [test-store (<!! (fstore/new-fs-store "/tmp/cache-store"))
          cached-store (ensure-cache test-store)]
      (<!! (get-in cached-store [:foo]))
      (is (=  @(:cache cached-store) {:foo nil}))
      (<!! (update-in cached-store [:foo :bar] (fnil inc 0)))
      (is (=  @(:cache cached-store) {}))
      (<!! (get-in cached-store [:foo]))
      (is (=  @(:cache cached-store) {:foo {:bar 1}}))
      (<!! (assoc-in cached-store [:foo :bar] 0))
      (is (= @(:cache cached-store) {}))
      (<!! (get-in cached-store [:foo]))
      (is (=  @(:cache cached-store) {:foo {:bar 0}}))
      (<!! (dissoc cached-store :foo))
      (is (= @(:cache cached-store) {}))
      (fstore/delete-store "/tmp/cache-store"))))


