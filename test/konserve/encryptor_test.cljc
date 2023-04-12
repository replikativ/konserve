(ns konserve.encryptor-test
  (:require [clojure.test :refer [deftest]]
            #?(:cljs [clojure.core.async :refer [go <!]])
            [#?(:clj konserve.filestore :cljs konserve.node-filestore) :refer [connect-fs-store delete-store]]
            [konserve.compliance-test :refer [#?(:clj compliance-test
                                                 :cljs async-compliance-test)]]))

(deftest encryptor-test
  (let [folder "/tmp/konserve-fs-encryptor-test"
        _      (delete-store folder)
        store  (connect-fs-store folder
                                 :config {:encryptor {:type :aes
                                                      :key "s3cr3t"}}
                                 :opts {:sync? true})]
    #?(:clj
       (do
         (compliance-test store)
         (delete-store folder))
       :cljs
       (cljs.test/async done
                        (go
                          (<! (async-compliance-test store))
                          (delete-store folder)
                          (done))))))
