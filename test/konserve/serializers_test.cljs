(ns konserve.serializers-test
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require   [cljs.core.async :as async :refer [<! >!]]
              [cljs.test :refer-macros [deftest is testing async use-fixtures]]
              [fress.api :as fress]
              [konserve.core :as k]
              [konserve.serializers :refer [fressian-serializer]]
              [konserve.node-filestore :as filestore]))

(deftype MyType [field0 field1]
  IEquiv
  (-equiv [this o]
    (and (instance? MyType o)
         (= field0 (.-field0 o))
         (= field1 (.-field1 o)))))

(def custom-read-handler
  {"my-type" (fn [reader tag field-count]
               (let [field0 (fress.api/read-object reader)
                     field1 (fress.api/read-object reader)]
                 (MyType. field0 field1)))})

(def custom-write-handler
  {MyType (fn [writer o]
            (fress.api/write-tag writer "my-type" 2)
            (fress.api/write-object writer (.-field0 o))
            (fress.api/write-object writer (.-field1 o)))})

(deftest serializers-test
  (async done
   (go
    (let [store-path "/tmp/konserve-fs-serializer-node-test"
          _(filestore/delete-store store-path)
          ser (fressian-serializer custom-read-handler custom-write-handler)
          store (<! (filestore/connect-fs-store store-path :serializers {:FressianSerializer ser}))
          mt (MyType. :a/b "c")]
      (is (= [nil mt] (<! (k/assoc-in store [:my-type] mt))))
      (is (= mt (<! (k/get-in store [:my-type]))))
      (done)))))
