(ns konserve.serializers-test
  (:refer-clojure :exclude [get-in update-in assoc-in dissoc exists?])
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [konserve.core :refer :all]
            [konserve.memory :refer [new-mem-store]]
            [konserve.serializers :refer [fressian-serializer]]
            [konserve.filestore :refer [new-fs-store delete-store list-keys]]
            [clojure.java.io :as io]
            [clj-time.core :as t]
            [clj-time.format :as f])
  (:import [org.fressian.handlers WriteHandler ReadHandler]))

(def custom-tag "joda")

(def custom-read-handler
  {custom-tag (reify ReadHandler
              (read [_ reader tag component-count]
                (f/parse (f/formatters :date-time) (.readObject reader))))})

(def custom-write-handler
  {org.joda.time.DateTime {custom-tag (reify WriteHandler
                                      (write [_ writer instant]
                                        (.writeTag    writer custom-tag 1)
                                        (.writeObject writer (f/unparse (f/formatters :date-time) instant))))}})



(deftest serializers-test
  (testing "Test the custom fressian serializers functionality."
    (let [folder "/tmp/konserve-fs-serializers-test"
          _      (delete-store folder)
          store  (<!! (new-fs-store folder :serializer (fressian-serializer custom-read-handler custom-write-handler)))]
      (is (= (<!! (get-in store [:foo]))
             nil))
      (<!! (assoc-in store [:foo] (t/now)))
      (is (= (type (<!! (get-in store [:foo])))
             org.joda.time.DateTime))
      (is (= (<!! (list-keys store))
             #{{:key :foo, :format :edn}}))
      (<!! (dissoc store :foo))
      (is (= (<!! (get-in store [:foo]))
             nil))
      (is (= (<!! (list-keys store))
             #{}))
      (delete-store folder))))
