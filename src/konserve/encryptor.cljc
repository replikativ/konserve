(ns konserve.encryptor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]]
            [geheimnis.aes :refer [encrypt decrypt]]
            [hasch.core :refer [edn-hash]])
  #?(:clj (:import [java.io ByteArrayInputStream ByteArrayOutputStream])))

(defrecord NullEncryptor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

(defn null-encryptor [_store-key _config]
  (fn [serializer]
    (NullEncryptor. serializer)))

;; Following advise at
;; https://crypto.stackexchange.com/questions/84439/is-it-dangerous-to-encrypt-lots-of-small-files-with-the-same-key

(defn store-key->iv [store-key]
  (subvec (vec (edn-hash store-key)) 0 16))

(defrecord AESEncryptor [serializer store-key key]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    #?(:cljs (-deserialize serializer read-handlers (decrypt key bytes))
       :clj (let [decrypted (decrypt [store-key key] (.readAllBytes ^ByteArrayInputStream bytes)
                             :iv (store-key->iv store-key))]
      (-deserialize serializer read-handlers #?(:clj (ByteArrayInputStream. decrypted)
                                                     :cljs decrypted)))))
  (-serialize [_ bytes write-handlers val]
    #?(:cljs (encrypt key (-serialize serializer bytes write-handlers val))
       :clj (let [bos (ByteArrayOutputStream.)
                  _ (-serialize serializer bos write-handlers val)
                  ba (.toByteArray bos)
                  encrypted ^bytes (encrypt [store-key key] ba :iv (store-key->iv store-key))]
              (.write ^ByteArrayOutputStream bytes encrypted)))))

(defn aes-encryptor [store-key config]
  (let [{:keys [key]} config]
    (if (nil? key)
      (throw (ex-info "AES key not provided."
                      {:type   :aes-encryptor-key-missing
                       :config config}))
      (fn [serializer]
        (AESEncryptor. serializer store-key key)))))

(def byte->encryptor
  {0 null-encryptor
   1 aes-encryptor})

(def encryptor->byte
  (invert-map byte->encryptor))

(defn get-encryptor [type]
  (case type
    :aes aes-encryptor
    null-encryptor))
