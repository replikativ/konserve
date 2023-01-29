(ns konserve.encryptor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]]
            [geheimnis.aes :refer [encrypt decrypt]]
            [hasch.core :refer [edn-hash uuid]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

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

(defn get-iv [salt store-key key]
  (subvec (vec (edn-hash ["initial-value" salt store-key key])) 0 16))

(defn get-key [salt store-key key]
  ["key" salt store-key key])

;; TODO cljs support incomplete, prepending of salt is missing
(defrecord AESEncryptor [serializer store-key key]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (let [salt-array (byte-array 64)
          _ (.read ^ByteArrayInputStream bytes salt-array)
          salt (map int salt-array)
          decrypted (decrypt (get-key salt store-key key)
                             #?(:clj (.readAllBytes ^ByteArrayInputStream bytes) :cljs bytes)
                             :iv (get-iv salt store-key key))]
      (-deserialize serializer read-handlers (ByteArrayInputStream. decrypted))))
  (-serialize [_ bytes write-handlers val]
    #?(:cljs (encrypt key (-serialize serializer bytes write-handlers val))
       :clj (let [salt (map #(int (- % 128)) (edn-hash (uuid)))
                  bos (ByteArrayOutputStream. (* 16 1024))
                  _ (.write ^ByteArrayOutputStream bytes (byte-array salt))
                  _ (-serialize serializer bos write-handlers val)
                  ba (.toByteArray bos)
                  encrypted ^bytes (encrypt (get-key salt store-key key)
                                            ba :iv (get-iv salt store-key key))]
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
