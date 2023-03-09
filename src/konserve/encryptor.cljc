(ns konserve.encryptor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]]
            [geheimnis.aes :refer [encrypt decrypt]]
            [hasch.core :refer [edn-hash uuid]])
  #?(:clj (:import [java.io ByteArrayInputStream ByteArrayOutputStream])))

(def ^:const salt-size 64)

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

(defrecord AESEncryptor [serializer key]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (let [salt #?(:clj (let [salt-array (byte-array salt-size)]
                         (.read ^ByteArrayInputStream bytes salt-array)
                         (map int salt-array))
                  :cljs
                  (map (fn [b] (if (> b 128) (- b 256) b)) (.slice bytes 0 salt-size)))
          data #?(:clj (.readAllBytes ^ByteArrayInputStream bytes)
                  :cljs (.slice bytes salt-size))
          decrypted (decrypt (get-key salt key) data :iv (get-initial-vector salt key))]
      (-deserialize serializer read-handlers #?(:clj (ByteArrayInputStream. decrypted)
                                                :cljs decrypted))))
  (-serialize [_ bytes write-handlers val]
    (let [unsigned-byte-offset 128
          salt (map #(int (- (#?(:cljs inc :clj identity) %) unsigned-byte-offset)) (edn-hash (uuid)))]
      #?(:cljs
          (let [data (-serialize serializer bytes write-handlers val)
                iv (get-initial-vector salt key)
                bytes (encrypt (get-key salt key) (.from js/Array data) :iv iv)
                output (js/Uint8Array. (+ salt-size (count bytes)))]
            (.set output (js/Uint8Array.from (into-array salt)) 0)
            (.set output (js/Uint8Array.from bytes) salt-size)
            output)
         :clj
         (let [buffer-size (* 16 1024)
               bos (ByteArrayOutputStream. buffer-size)
               _ (.write ^ByteArrayOutputStream bytes (byte-array salt))
               _ (-serialize serializer bos write-handlers val)
               ba (.toByteArray bos)
               iv (get-initial-vector salt key)
               encrypted ^bytes (encrypt (get-key salt key) ba :iv iv)]
          (.write ^ByteArrayOutputStream bytes encrypted))))))

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
