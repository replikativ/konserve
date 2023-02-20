(ns konserve.encryptor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]]
            [geheimnis.aes :refer [encrypt decrypt]]
            [hasch.core :refer [edn-hash uuid]])
  #?(:clj (:import [java.io ByteArrayInputStream ByteArrayOutputStream])))

(defrecord NullEncryptor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

(defn null-encryptor [_config]
  (fn [serializer]
    (NullEncryptor. serializer)))

;; Following advise at
;; https://crypto.stackexchange.com/questions/84439/is-it-dangerous-to-encrypt-lots-of-small-files-with-the-same-key

(defn get-initial-vector [salt key]
  (subvec (vec (edn-hash ["initial-value" salt key])) 0 16))

(defn get-key [salt key]
  ["key" salt key])

;; TODO cljs support incomplete, prepending of salt is missing
(defrecord AESEncryptor [serializer key]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    #?(:cljs (-deserialize serializer read-handlers (decrypt key bytes))
       :clj (let [salt-array (byte-array 64)
                  _ (.read ^ByteArrayInputStream bytes salt-array)
                  salt (map int salt-array)
                  decrypted (decrypt (get-key salt key)
                                     (.readAllBytes ^ByteArrayInputStream bytes)
                                     :iv (get-initial-vector salt key))]
              (-deserialize serializer read-handlers (ByteArrayInputStream. decrypted)))))
  (-serialize [_ bytes write-handlers val]
    #?(:cljs (encrypt key (-serialize serializer bytes write-handlers val))
       :clj (let [unsigned-byte-offset 128
                  salt (map #(int (- % unsigned-byte-offset)) (edn-hash (uuid)))
                  buffer-size (* 16 1024)
                  bos (ByteArrayOutputStream. buffer-size)
                  _ (.write ^ByteArrayOutputStream bytes (byte-array salt))
                  _ (-serialize serializer bos write-handlers val)
                  ba (.toByteArray bos)
                  encrypted ^bytes (encrypt (get-key salt key)
                                            ba :iv (get-initial-vector salt key))]
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
