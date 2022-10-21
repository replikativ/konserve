(ns konserve.node-runner
  (:require [cljs.nodejs]
            [cljs.test]
            [konserve.cache-test]
            [konserve.gc-test]
            [konserve.node-filestore-test]
            [konserve.serializers-test]))

(defn run-tests
  ([] (run-tests nil))
  ([opts]
   (cljs.test/run-tests (merge (cljs.test/empty-env) opts)
                        'konserve.cache-test
                        'konserve.gc-test
                        'konserve.node-filestore-test
                        'konserve.serializers-test)))

; clj -M:test -m cljs.main  -t node  -o out/runner.js -c konserve.node-runner
(defn -main [& args]
  (cljs.nodejs/enable-util-print!)
  (defmethod cljs.test/report [:cljs.test/default :end-run-tests] [_] (js/process.exit 0))
  (run-tests))

(set! *main-cli-fn* -main)
