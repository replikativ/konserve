(ns konserve.electron-test
  (:require [clojure.core.async :refer [go <! timeout promise-chan put! alts!]]
            [cljs.test :refer-macros [deftest is testing async]]
            [cljs-node-io.fs :as node-fs]
            [konserve.core :as k]
            [konserve.tiered :as tiered]
            [konserve.memory :as memory]
            [konserve.indexeddb :as idb]
            [konserve.node-filestore :as fs]
            [konserve.tests.tiered :as tiered-tests]
            [konserve.compliance-test :refer [async-compliance-test]]
            [taoensso.timbre :as timbre]))

(assert (and (= "nodejs" *target*) (some? (.-indexedDB js/window))))

(timbre/set-min-level! :debug)

(deftest tiered-mem-idb-fs-integration-test
  (let [hook-promise (promise-chan)
        hook (fn [e] (put! hook-promise e))]
    (async done
     (go
      (let [fs-path "/tmp/tiered-deep-test"
            idb-name "tiered-deep-idb"
            _ (node-fs/rm-rf fs-path)
            _ (<! (idb/delete-idb idb-name))
            fs-store (<! (fs/connect-fs-store fs-path))
            idb-store (<! (idb/connect-idb-store idb-name))
            mem-store  (<! (memory/new-mem-store (atom {})))]
        (<! (tiered-tests/test-tiered-deep-async mem-store idb-store fs-store))
        (<! (.close (:backing idb-store)))
        (<! (idb/delete-idb idb-name))
        (node-fs/rm-rf fs-path)
        (done))))))

