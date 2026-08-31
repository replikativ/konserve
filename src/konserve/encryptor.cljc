(ns konserve.encryptor
  "Whole-value encryption for konserve blobs."
  (:require [clojure.core.async]
            [geheimnis.aes :refer [decrypt encrypt]]
            [hasch.core :refer [edn-hash uuid]]
            [konserve.protocols :refer [PEncryptor]]
            [konserve.utils :refer [invert-map]]
            [org.replikativ.geheimnis.aead :as aead]
            [org.replikativ.geheimnis.codec :as codec]
            [org.replikativ.geheimnis.core :as csprng]
            [org.replikativ.geheimnis.hash :as hash]
            [superv.async :refer [go-try- <?-]]))

(defn associated-data
  "AAD binding a ciphertext to its layout version, store key, and slot."
  [version store-key part]
  (codec/str->bytes (str "konserve|" (int version) "|" store-key "|" (name part))))

(defn- byte-slice [bs from to]
  #?(:clj (java.util.Arrays/copyOfRange ^bytes bs (int from) (int to))
     :cljs (.slice bs from to)))

(defn- byte-array? [x]
  #?(:clj (bytes? x)
     :cljs (instance? js/Uint8Array x)))

(defn- passthrough [bytes env]
  (if (:sync? env) bytes (go-try- bytes)))

(defrecord NullEncryptor []
  PEncryptor
  (-encrypt [_ plaintext _aad env] (passthrough plaintext env))
  (-decrypt [_ ciphertext _aad env] (passthrough ciphertext env)))

(defn null-encryptor [_config]
  (->NullEncryptor))

(defn encrypting?
  "True when encryptor is not the null implementation."
  [encryptor]
  (not (instance? NullEncryptor encryptor)))

(def ^:const gcm-key-size 32)
(def ^:const gcm-salt-size 32)
(def ^:const gcm-nonce-size 12)
(def ^:const gcm-tag-size 16)

(def ^:private gcm-info (codec/str->bytes "konserve-aes-gcm-v1"))
(def ^:private gcm-prefix-size (+ gcm-salt-size gcm-nonce-size))

#?(:cljs
   (defn- no-sync-aead! []
     (throw (ex-info
             "The :aes-gcm encryptor requires Konserve's asynchronous API on ClojureScript."
             {:type ::sync-aead-unsupported}))))

(defn- gcm-encrypt [master-key plaintext aad env]
  (let [salt (csprng/random-bytes gcm-salt-size)
        nonce (csprng/random-bytes gcm-nonce-size)
        blob-key (hash/hkdf master-key salt gcm-info gcm-key-size)]
    (if (:sync? env)
      #?(:clj (codec/concat-bytes
               [salt nonce (aead/aead-encrypt-sync blob-key nonce aad plaintext)])
         :cljs (no-sync-aead!))
      (go-try-
       (codec/concat-bytes
        [salt nonce (<?- (aead/aead-encrypt blob-key nonce aad plaintext))])))))

(defn- gcm-decrypt [master-key ciphertext aad env]
  (when (< (codec/blen ciphertext) (+ gcm-prefix-size gcm-tag-size))
    (throw (ex-info "Blob is too short to be an :aes-gcm ciphertext."
                    {:type ::malformed-ciphertext
                     :size (codec/blen ciphertext)})))
  (let [salt (byte-slice ciphertext 0 gcm-salt-size)
        nonce (byte-slice ciphertext gcm-salt-size gcm-prefix-size)
        ciphertext (byte-slice ciphertext gcm-prefix-size (codec/blen ciphertext))
        blob-key (hash/hkdf master-key salt gcm-info gcm-key-size)]
    (if (:sync? env)
      #?(:clj (aead/aead-decrypt-sync blob-key nonce aad ciphertext)
         :cljs (no-sync-aead!))
      (go-try- (<?- (aead/aead-decrypt blob-key nonce aad ciphertext))))))

(defrecord AESGCMEncryptor [key]
  PEncryptor
  (-encrypt [_ plaintext aad env] (gcm-encrypt key plaintext aad env))
  (-decrypt [_ ciphertext aad env] (gcm-decrypt key ciphertext aad env)))

(defn generate-key
  "Generate a fresh 256-bit AES-GCM master key as hexadecimal."
  []
  (codec/bytes->hex (csprng/random-bytes gcm-key-size)))

(defn- coerce-gcm-key [key]
  (cond
    (and (string? key) (re-matches #"(?i)[0-9a-f]{64}" key))
    (codec/hex->bytes key)

    (and (byte-array? key) (= gcm-key-size (codec/blen key)))
    key

    :else
    (throw (ex-info
            (str "The :aes-gcm encryptor needs a 256-bit key: 32 raw bytes or "
                 "a 64-character hexadecimal string.")
            {:type ::invalid-key
             :provided-type (type key)}))))

(defn aes-gcm-encryptor [{:keys [key] :as config}]
  (when (nil? key)
    (throw (ex-info "AES-GCM key not provided."
                    {:type ::key-missing
                     :config (dissoc config :key)})))
  (->AESGCMEncryptor (coerce-gcm-key key)))

;; Legacy unauthenticated AES-CBC. Its byte layout remains unchanged so existing
;; stores stay readable. Do not select it for new stores.
(def ^:const salt-size 64)

(defn get-initial-vector [salt key]
  (subvec (vec (edn-hash ["initial-value" salt key])) 0 16))

(defn get-key [salt key]
  ["key" salt key])

(defn- legacy-salt []
  (let [unsigned-byte-offset 128]
    (map #(int (- (#?(:cljs inc :clj identity) %) unsigned-byte-offset))
         (edn-hash (uuid)))))

(defn- legacy-encrypt [master-key plaintext env]
  (let [salt (legacy-salt)
        iv (get-initial-vector salt master-key)
        encrypted #?(:clj (encrypt (get-key salt master-key) plaintext :iv iv)
                     :cljs (encrypt (get-key salt master-key)
                                    (.from js/Array plaintext)
                                    :iv iv))
        output #?(:clj (codec/concat-bytes [(byte-array salt) encrypted])
                  :cljs (let [output (js/Uint8Array.
                                      (+ salt-size (count encrypted)))]
                          (.set output (js/Uint8Array.from (into-array salt)) 0)
                          (.set output (js/Uint8Array.from encrypted) salt-size)
                          output))]
    (passthrough output env)))

(defn- legacy-decrypt [master-key ciphertext env]
  (let [salt #?(:clj (map int (byte-slice ciphertext 0 salt-size))
                :cljs (map (fn [b] (if (> b 128) (- b 256) b))
                           (.slice ciphertext 0 salt-size)))
        data (byte-slice ciphertext salt-size (codec/blen ciphertext))
        plaintext (decrypt (get-key salt master-key)
                           data
                           :iv (get-initial-vector salt master-key))]
    (passthrough plaintext env)))

(defrecord AESEncryptor [key]
  PEncryptor
  (-encrypt [_ plaintext _aad env] (legacy-encrypt key plaintext env))
  (-decrypt [_ ciphertext _aad env] (legacy-decrypt key ciphertext env)))

(defn aes-encryptor [{:keys [key] :as config}]
  (if (nil? key)
    (throw (ex-info "AES key not provided."
                    {:type :aes-encryptor-key-missing
                     :config config}))
    (->AESEncryptor key)))

(def byte->encryptor
  {0 null-encryptor
   1 aes-encryptor
   2 aes-gcm-encryptor})

(def encryptor->byte
  (invert-map byte->encryptor))

(defn get-encryptor [type]
  (case type
    :aes aes-encryptor
    :aes-gcm aes-gcm-encryptor
    null-encryptor))
