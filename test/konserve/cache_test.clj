(ns konserve.cache-test
  (:refer-clojure :exclude [get-in update-in assoc-in dissoc exists?])
  (:require [konserve.cache :as k]
            [konserve.filestore :as fstore]
            [konserve.compliance-test :refer [compliance-test]]
            [clojure.core.async :refer [<!!]]
            [clojure.test :refer :all]))

(deftest cache-test
  (testing "Testing the cache namespace"
    (let [test-store (<!! (fstore/new-fs-store "/tmp/cache-store"))
          cached-store (k/ensure-cache test-store)]
      (compliance-test cached-store)
      (fstore/delete-store "/tmp/cache-store"))))


