(ns konserve.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.memory :refer [new-mem-store]]))

(deftest memory-store-compliance-test
  (compliance-test (<!! (new-mem-store))))

