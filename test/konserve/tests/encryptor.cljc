(ns konserve.tests.encryptor
  (:require [clojure.core.async :refer [go <!]]
            [clojure.test :refer [deftest is testing]]
            [konserve.compliance-test :refer
             [#?(:clj compliance-test)
              async-compliance-test]]
            [konserve.core :as k]
            [konserve.encryptor :as e]
            [konserve.protocols :refer [-decrypt]]
            [org.replikativ.geheimnis.codec :as codec]
            [superv.async :refer [<?-]]
            #?(:clj [clojure.java.io :as io])))

(def gcm-key
  "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")

(def other-gcm-key
  "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100")

#?(:clj
   (defn- contains-subarray? [^bytes haystack ^bytes needle]
     (let [h (alength haystack)
           n (alength needle)]
       (boolean
        (some (fn [i]
                (loop [j 0]
                  (cond
                    (= j n) true
                    (= (aget haystack (+ i j)) (aget needle j)) (recur (inc j))
                    :else false)))
              (range 0 (inc (- h n))))))))

(defn async-aes-gcm-test
  [store-name create-store delete-store-async]
  (go
    (<! (delete-store-async store-name))
    (let [store (<?- (create-store
                      store-name
                      :config {:encoding
                               {:encryptor {:type :aes-gcm :key gcm-key}}}))
          payload (codec/str->bytes "raw bytes konserve never looks at")]
      (<?- (k/assoc store :secret {:very "confidential"}))
      (is (= {:very "confidential"} (<?- (k/get store :secret))))
      (let [sealed (<?- (k/seal store :blob payload))]
        (is (not= (codec/bytes->hex payload) (codec/bytes->hex sealed)))
        (is (= (codec/bytes->hex payload)
               (codec/bytes->hex (<?- (k/unseal store :blob sealed)))))
        (is (instance? #?(:clj Throwable :cljs js/Error)
                       (<! (k/unseal store :other-key sealed)))))
      #?(:cljs (when (.-close (:backing store))
                 (<! (.close (:backing store))))))
    (let [wrong-store (<?- (create-store
                            store-name
                            :config {:encoding
                                     {:encryptor
                                      {:type :aes-gcm :key other-gcm-key}}}))]
      (is (instance? #?(:clj Throwable :cljs js/Error)
                     (<! (k/get wrong-store :secret)))
          "a wrong key must fail authentication")
      #?(:cljs (when (.-close (:backing wrong-store))
                 (<! (.close (:backing wrong-store))))))
    (<! (delete-store-async store-name))))

#?(:clj
   (defn sync-aes-gcm-test
     [store-name create-store delete-store]
     (delete-store store-name)
     (let [store (create-store
                  store-name
                  :config {:encoding
                           {:encryptor {:type :aes-gcm :key gcm-key}}}
                  :opts {:sync? true})]
       (compliance-test store)

       (testing "binary is never silently written in cleartext"
         (is (thrown? clojure.lang.ExceptionInfo
                      (k/bassoc store :bin (byte-array (range 10))
                                {:sync? true}))))

       (testing "raw binary and explicit sealing"
         (let [payload (.getBytes "ANOTHER-SECRET-MARKER")
               sealed (k/seal store :sealed-bin payload {:sync? true})]
           (is (not (contains-subarray? sealed payload)))
           (k/bassoc store :sealed-bin sealed {:sync? true :raw? true})
           (k/bget store
                   :sealed-bin
                   (fn [{:keys [input-stream]}]
                     (let [output (java.io.ByteArrayOutputStream.)]
                       (io/copy input-stream output)
                       (is (= (seq payload)
                              (seq (k/unseal store
                                             :sealed-bin
                                             (.toByteArray output)
                                             {:sync? true}))))))
                   {:sync? true :raw? true})
           (is (thrown? javax.crypto.AEADBadTagException
                        (k/unseal store :other-key sealed {:sync? true})))))

       (testing "plaintext does not occur in the stored files"
         (k/assoc store :leak-check "SUPER-SECRET-MARKER" {:sync? true})
         (let [marker (.getBytes "SUPER-SECRET-MARKER")
               blobs (filter #(.isFile ^java.io.File %)
                             (file-seq (io/file store-name)))]
           (is (seq blobs))
           (is (not-any? (fn [file]
                           (let [output (java.io.ByteArrayOutputStream.)]
                             (io/copy file output)
                             (contains-subarray? (.toByteArray output) marker)))
                         blobs))))
       (delete-store store-name))))

(defn async-encryptor-test
  [store-name create-store delete-store-async]
  (go
    (<! (delete-store-async store-name))
    (let [config {:encoding {:encryptor {:type :aes :key "s3cr3t"}}}
          store  (<?- (create-store store-name :config config))]
      (<! (async-compliance-test store))
      #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store)))))
      (<! (delete-store-async store-name)))))

#?(:clj
   (defn sync-encryptor-test
     [store-name create-store delete-store]
     (delete-store store-name)
     (let [store  (create-store store-name
                                :config {:encoding
                                         {:encryptor {:type :aes
                                                      :key "s3cr3t"}}}
                                :opts {:sync? true})]
       (compliance-test store)
       (delete-store store-name))))

#?(:clj
   (deftest legacy-aes-format-test
     (let [blob (codec/hex->bytes
                 (str "6917da4cff96500eaebdb6a363ee5d88f20f4c61d372d32316fdea028c35dfcd"
                      "e953638a33934c90bba14dbc55a9fcbed6500e8352d8cd26de00b8144830e3d0"
                      "e43abc2ea89e6818a862c96b80ce9d66ac58637fc956c570ce371b100c8684bc"))
           enc (e/aes-encryptor {:key "s3cr3t"})]
       (is (= "konserve legacy aes payload"
              (String. ^bytes (-decrypt enc blob nil {:sync? true})))))))
