(ns konserve.tests.encryptor
  (:require [clojure.core.async :refer [go <!]]
            [clojure.test :refer [deftest is]]
            [konserve.compliance-test :refer
             [#?(:clj compliance-test)
              async-compliance-test]]
            [superv.async :refer [<?-]]))

(defn async-encryptor-test
  [store-name create-store delete-store-async]
  (go
    (<! (delete-store-async store-name))
    (let [config {:encryptor {:type :aes :key "s3cr3t"}}
          store  (<?- (create-store store-name :config config))]
      (<! (async-compliance-test store))
      #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store)))))
      (<! (delete-store-async store-name)))))

#?(:clj
   (defn sync-encryptor-test
     [store-name create-store delete-store]
     (delete-store store-name)
     (let [store  (create-store store-name
                                :config {:encryptor {:type :aes
                                                     :key "s3cr3t"}}
                                :opts {:sync? true})]
       (compliance-test store)
       (delete-store store-name))))