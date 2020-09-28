(ns konserve.serializers-test
  #_(:require [cljs.core.async :as async :refer [<! >!]]
              [konserve.core :refer [get-in assoc-in dissoc]]
              [konserve.serializers :refer [fressian-serializer]]
              [cljs.test :refer-macros [run-tests deftest is testing async use-fixtures]]
              [konserve.filestore :refer [new-fs-store delete-store list-keys]]
              [cljs.nodejs :as node])
  #_(:require-macros [cljs.core.async.macros :refer [go]]))

#_(comment
    (enable-console-print!)

    (def custom-read-handler
      {"js-date" (fn [reader tag field-count]
                   (js/Date. (fress.api/read-object reader)))})

    (def custom-write-handler
      {js/Date (fn [writer o] (let [date-time (.getTime o)]
                                (fress.api/write-tag writer "js-date" 1)
                                (fress.api/write-object writer date-time)))})

    (use-fixtures :once
      {:before
       (fn []
         (async done
                (go (delete-store "/tmp/konserve-fs-serializer-node-test")
                    (def store (<! (new-fs-store "/tmp/konserve-fs-serializers-test" :serializer (fressian-serializer custom-read-handler custom-write-handler))))
                    (done))))})

    (deftest serializers-test
      (testing "Test the custom fressian serializers functionality."
        (async done
               (go
                 (is (= (<! (get-in store [:foo]))
                        nil))
                 (<! (assoc-in store [:foo] (js/Date.)))
                 (is (= (type (<! (get-in store [:foo])))
                        js/Date))
                 (is (= (<! (list-keys store {}))
                        #{{:key :foo, :format :edn}}))
                 (<! (dissoc store :foo))
                 (is (= (<! (get-in store [:foo]))
                        nil))
                 (is (= (<! (list-keys store {}))
                        #{}))
                 (delete-store (:folder store))))))

    (run-tests))
