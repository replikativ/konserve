(ns konserve.clojurific-runner
  (:require [cljs.test :as t :refer-macros [run-tests]]
            [konserve.boring-serializer-test]
            [konserve.memory-test]
            [konserve.node-filestore-test]))

;; An async test that never calls done can leave Node's event loop empty.
(set! (.-exitCode js/process) 1)
(defmethod t/report [::t/default :end-run-tests] [summary]
  (when (t/successful? summary)
    (set! (.-exitCode js/process) 0)))

(run-tests 'konserve.boring-serializer-test
           'konserve.memory-test
           'konserve.node-filestore-test)
