(ns konserve.serializers-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [konserve.core :refer :all]
            [konserve.memory :refer [new-mem-store]]
            [konserve.serializers :refer [fressian-serializer]]
            [konserve.filestore :refer [new-fs-store delete-store]])
  (:import [org.fressian.handlers WriteHandler ReadHandler]))

(def custom-tag "java.util.Date")

(def custom-read-handler
  {custom-tag (reify ReadHandler
                (read [_ reader tag component-count]
                  (java.util.Date. (.readObject reader))))})

(def custom-write-handler
  {java.util.Date {custom-tag (reify WriteHandler
                                (write [_ writer instant]
                                  (.writeTag    writer custom-tag 1)
                                  (.writeObject writer (.getTime  instant))))}})

(deftest serializers-test
  (testing "Test the custom fressian serializers functionality."
    (let [folder "/tmp/konserve-fs-serializers-test"
          _      (delete-store folder)
          store  (<!! (new-fs-store folder :serializers {:FressianSerializer (fressian-serializer custom-read-handler custom-write-handler)}))]
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (assoc-in store [:foo] (java.util.Date.)))
      (is (= (type (<!! (get-in store [:foo])))
             java.util.Date))
      (<!! (dissoc store :foo))
      (is (= (<!! (get-in store [:foo]))
             nil))
      (delete-store folder))))
