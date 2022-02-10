(ns konserve.serializers-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.serializers :refer [fressian-serializer]]
            [konserve.filestore :refer [connect-fs-store delete-store]])
  (:import [org.fressian.handlers WriteHandler ReadHandler]
           (java.util Date)))

(def custom-tag "java.util.Date")

(def custom-read-handler
  {custom-tag (reify ReadHandler
                (read [_ reader tag component-count]
                  (Date. ^long (.readObject reader))))})

(def custom-write-handler
  {Date {custom-tag (reify WriteHandler
                      (write [_ writer instant]
                        (.writeTag    writer custom-tag 1)
                        (.writeObject writer (.getTime ^Date instant))))}})

(deftest serializers-test
  (testing "Test the custom fressian serializers functionality."
    (let [folder "/tmp/konserve-fs-serializers-test"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder :serializers {:FressianSerializer (fressian-serializer custom-read-handler custom-write-handler)}))]
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/assoc-in store [:foo] (Date.)))
      (is (= (type (<!! (k/get-in store [:foo])))
             Date))
      (<!! (k/dissoc store :foo))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (delete-store folder))))
