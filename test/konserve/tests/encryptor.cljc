(ns konserve.tests.encryptor
  (:require [clojure.core.async :refer [go <!]]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.encryptor :as e]
            [konserve.protocols :refer [-decrypt]]
            [konserve.compliance-test :refer
             [#?(:clj compliance-test)
              async-compliance-test]]
            [org.replikativ.geheimnis.codec :as codec]
            [superv.async :refer [<?-]]
            #?(:clj [clojure.java.io :as io])))

;; A 256-bit key, hex. Fixed here so a test failure is reproducible; real stores
;; want (konserve.encryptor/generate-key).
(def gcm-key "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
(def other-gcm-key "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100")

#?(:clj
   (defn- contains-subarray? [^bytes haystack ^bytes needle]
     (let [h (alength haystack) n (alength needle)]
       (boolean
        (some (fn [i]
                (loop [j 0]
                  (cond (= j n)                             true
                        (= (aget haystack (+ i j)) (aget needle j)) (recur (inc j))
                        :else                               false)))
              (range 0 (inc (- h n))))))))

;; -----------------------------------------------------------------------------
;; :aes-gcm
;; -----------------------------------------------------------------------------

(defn async-aes-gcm-test
  [store-name create-store delete-store-async]
  (go
    (<! (delete-store-async store-name))
    (let [config {:encryptor {:type :aes-gcm :key gcm-key}}
          store  (<?- (create-store store-name :config config))]
      (<! (async-compliance-test store))
      #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store)))))
      (<! (delete-store-async store-name)))))

(defn async-aes-gcm-wrong-key-test
  "A blob written under one key must not be readable under another. The AEAD tag
   makes that a hard error rather than a garbage deserialization."
  [store-name create-store delete-store-async]
  (go
    (<! (delete-store-async store-name))
    (let [store (<?- (create-store store-name
                                   :config {:encryptor {:type :aes-gcm :key gcm-key}}))]
      (<! (k/assoc store :secret {:very "confidential"}))
      #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store))))))
    (let [store (<?- (create-store store-name
                                   :config {:encryptor {:type :aes-gcm :key other-gcm-key}}))
          res   (<! (k/get store :secret))]
      (is (instance? #?(:clj Throwable :cljs js/Error) res)
          "reading with the wrong key is rejected, not silently wrong")
      #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store))))))
    (<! (delete-store-async store-name))))

(defn async-seal-unseal-test
  "seal/unseal over the store's key, on whichever cipher the platform runs
   (Web Crypto on JS, javax.crypto on the JVM)."
  [store-name create-store delete-store-async]
  (go
    (<! (delete-store-async store-name))
    (let [store   (<?- (create-store store-name
                                     :config {:encryptor {:type :aes-gcm :key gcm-key}}))
          payload (codec/str->bytes "raw bytes konserve never looks at")
          sealed  (<?- (k/seal store :blob payload))]
      (is (not= (codec/bytes->hex payload) (codec/bytes->hex sealed))
          "the plaintext is not in the output")
      (is (= (codec/bytes->hex payload)
             (codec/bytes->hex (<?- (k/unseal store :blob sealed))))
          "round-trips under the same key")
      (is (instance? #?(:clj Throwable :cljs js/Error)
                     (<! (k/unseal store :a-different-key sealed)))
          "does not unseal under another key")
      #?(:cljs (when (.-close (:backing store)) (<! (.close (:backing store)))))
      (<! (delete-store-async store-name)))))

#?(:clj
   (defn sync-aes-gcm-test
     [store-name create-store delete-store]
     (delete-store store-name)
     (let [store (create-store store-name
                               :config {:encryptor {:type :aes-gcm :key gcm-key}}
                               :opts {:sync? true})]
       ;; binary is covered too: compliance-test passes {:raw? true} on an encrypted
       ;; store, which is the only legal way to write binary there
       (compliance-test store)

       (testing "binary values are refused rather than silently written in the clear"
         (is (thrown? clojure.lang.ExceptionInfo
                      (k/bassoc store :bin (byte-array (range 10)) {:sync? true}))))

       (testing "{:raw? true} is the explicit opt-out: bytes pass through untouched"
         (let [payload (byte-array (range 10))]
           (k/bassoc store :bin payload {:sync? true :raw? true})
           (k/bget store :bin
                   (fn [{:keys [input-stream]}]
                     (let [bs (java.io.ByteArrayOutputStream.)]
                       (io/copy input-stream bs)
                       (is (= (seq payload) (seq (.toByteArray bs))))))
                   {:sync? true :raw? true})))

       (testing "seal/unseal encrypt raw bytes under the store's key"
         (let [payload (.getBytes "ANOTHER-SECRET-MARKER")
               sealed  (k/seal store :sealed-bin payload {:sync? true})]
           (is (not (contains-subarray? sealed (.getBytes "ANOTHER-SECRET-MARKER"))))
           (k/bassoc store :sealed-bin sealed {:sync? true :raw? true})
           (k/bget store :sealed-bin
                   (fn [{:keys [input-stream]}]
                     (let [bs (java.io.ByteArrayOutputStream.)]
                       (io/copy input-stream bs)
                       (is (= (seq payload)
                              (seq (k/unseal store :sealed-bin (.toByteArray bs)
                                             {:sync? true}))))))
                   {:sync? true :raw? true})

           (testing "and bind the ciphertext to the key it was sealed under"
             (is (thrown? javax.crypto.AEADBadTagException
                          (k/unseal store :some-other-key sealed {:sync? true}))))))

       (testing "the value never reaches the disk in the clear"
         (k/assoc store :leak-check "SUPER-SECRET-MARKER" {:sync? true})
         (let [marker (.getBytes "SUPER-SECRET-MARKER")
               blobs  (->> (file-seq (io/file store-name))
                           (filter #(.isFile ^java.io.File %)))]
           (is (seq blobs) "the store wrote something")
           (is (not-any? (fn [f]
                           (let [bs (java.io.ByteArrayOutputStream.)]
                             (io/copy f bs)
                             (contains-subarray? (.toByteArray bs) marker)))
                         blobs))))
       (delete-store store-name))))

;; -----------------------------------------------------------------------------
;; :aes — deprecated, but stores written with it must keep working
;; -----------------------------------------------------------------------------

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

#?(:clj
   ;; Golden vector produced by konserve <= 0.7's AESEncryptor on the JVM:
   ;; [salt 64B][AES-256-CBC ciphertext]. It pins the legacy on-disk layout, so a
   ;; store encrypted by an older konserve keeps decrypting after this refactor.
   (deftest legacy-aes-format-test
     (let [blob (codec/hex->bytes
                 (str "6917da4cff96500eaebdb6a363ee5d88f20f4c61d372d32316fdea028c35dfcd"
                      "e953638a33934c90bba14dbc55a9fcbed6500e8352d8cd26de00b8144830e3d0"
                      "e43abc2ea89e6818a862c96b80ce9d66ac58637fc956c570ce371b100c8684bc"))
           enc  (e/aes-encryptor {:key "s3cr3t"})]
       (is (= "konserve legacy aes payload"
              (String. ^bytes (-decrypt enc blob nil {:sync? true})))))))
