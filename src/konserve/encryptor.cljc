(ns konserve.encryptor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]]
            [geheimnis.aes :refer [encrypt decrypt]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defrecord NullEncryptor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

(defn null-encryptor [_config]
  (fn [serializer]
    (NullEncryptor. serializer)))

(defrecord AESEncryptor [serializer key]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (let [decrypted (decrypt key (.readAllBytes ^ByteArrayInputStream bytes))]
      (-deserialize serializer read-handlers (ByteArrayInputStream. decrypted))))
  (-serialize [_ bytes write-handlers val]
    #?(:cljs (encrypt key (-serialize serializer bytes write-handlers val))
       :clj (let [bos (ByteArrayOutputStream.)
                  _ (-serialize serializer bos write-handlers val)
                  ba (.toByteArray bos)
                  encrypted ^bytes (encrypt key ba)]
              (.write ^ByteArrayOutputStream bytes encrypted)))))

(defn aes-encryptor [config]
  (let [{:keys [key]} config]
    (if (nil? key)
      (throw (ex-info "AES key not provided."
                      {:type   :aes-encryptor-key-missing
                       :config config}))
      (fn [serializer]
        (AESEncryptor. serializer key)))))

(def byte->encryptor
  {0 null-encryptor
   1 aes-encryptor})

(def encryptor->byte
  (invert-map byte->encryptor))

(defn get-encryptor [type]
  (case type
    :aes aes-encryptor
    null-encryptor))
