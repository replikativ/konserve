(ns konserve.konserve-test
  (:require [cljs.test :refer [run-tests]]
            [konserve.filestore-test]
            [konserve.serializers-test]))

(run-tests 'konserve.filestore-test
           'konserve.serializers-test)

