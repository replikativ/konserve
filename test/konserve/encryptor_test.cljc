(ns konserve.encryptor-test
  (:require [clojure.test :refer [deftest]]
            [#?(:clj konserve.filestore :cljs konserve.node-filestore) :refer [connect-fs-store delete-store]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.encryptor :refer [aes-encryptor]]))

(deftest encryptor-test
  (let [folder "/tmp/konserve-fs-encryptor-test"
        _      (delete-store folder)
        store  (connect-fs-store folder
                                 :encryptor aes-encryptor
                                 :config {:encryptor {:type :aes
                                                      :key "s3cr3t"}}
                                 :opts {:sync? true})]
    (compliance-test store)
    (delete-store folder)))
