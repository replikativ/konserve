(ns konserve.encryptor
  "Value encryption for konserve blobs.

   An encryptor transforms a whole serialized byte array into a whole stored byte
   array and back (see `konserve.protocols/PEncryptor`). It is deliberately NOT a
   `PStoreSerializer` decorator: an AEAD cipher has to verify its authentication
   tag over the complete ciphertext before it may hand any plaintext to the
   deserializer, so there is nothing to stream.

   Two ciphers ship:

   `:aes-gcm` (header byte 2) — AES-256-GCM via geheimnis v2. Authenticated: a
   tampered, truncated or relocated blob is rejected rather than deserialized.
   This is the one to use.

   `:aes` (header byte 1) — DEPRECATED. Unauthenticated AES-256-CBC via geheimnis
   v1, kept so existing stores stay readable. It cannot detect tampering, and its
   per-blob salt comes from `Math.random` on ClojureScript rather than a CSPRNG.
   Do not choose it for new stores."
  (:require [clojure.core.async]        ;; go-try- expands to core.async/go — needs the ns loaded
            [konserve.protocols :refer [PEncryptor]]
            [konserve.utils :refer [invert-map]]
            [geheimnis.aes :refer [encrypt decrypt]]
            [org.replikativ.geheimnis.aead :as aead]
            [org.replikativ.geheimnis.codec :as codec]
            [org.replikativ.geheimnis.core :as csprng]
            [org.replikativ.geheimnis.hash :as hash]
            [superv.async :refer [go-try- <?-]]
            [hasch.core :refer [edn-hash uuid]]))

;; -----------------------------------------------------------------------------
;; associated data
;;
;; The AEAD tag covers the AAD, so binding the blob's identity into it means a
;; ciphertext cannot be moved to another key or into another slot of the same
;; blob and still verify. Both are things an attacker with write access to the
;; storage medium can otherwise do without touching a single ciphertext byte.
;; -----------------------------------------------------------------------------

(defn associated-data
  "AAD binding a ciphertext to (layout version, store-key, slot). `part` is
   :meta or :value."
  [version store-key part]
  (codec/str->bytes (str "konserve|" (int version) "|" store-key "|" (name part))))

;; -----------------------------------------------------------------------------
;; byte-array helpers (JVM byte[] / CLJS Uint8Array)
;; -----------------------------------------------------------------------------

(defn- byte-slice [bs from to]
  #?(:clj  (java.util.Arrays/copyOfRange ^bytes bs (int from) (int to))
     :cljs (.slice bs from to)))

(defn- byte-array? [x]
  #?(:clj (bytes? x) :cljs (instance? js/Uint8Array x)))

;; -----------------------------------------------------------------------------
;; null
;; -----------------------------------------------------------------------------

;; NOTE for every cipher below: the bodies live in plain functions rather than in
;; the record methods, because ClojureScript cannot macroexpand a core.async `go`
;; inside a deftype/defrecord method body.

(defn- passthrough [bytes env]
  (if (:sync? env) bytes (go-try- bytes)))

(defrecord NullEncryptor []
  PEncryptor
  (-encrypt [_ plaintext _aad env] (passthrough plaintext env))
  (-decrypt [_ ciphertext _aad env] (passthrough ciphertext env)))

(defn null-encryptor [_config]
  (NullEncryptor.))

(defn encrypting?
  "Does this encryptor actually encrypt? (False for the null encryptor.)"
  [encryptor]
  (not (instance? NullEncryptor encryptor)))

;; -----------------------------------------------------------------------------
;; :aes-gcm — AES-256-GCM
;;
;; Layout per blob part:  [salt 32B][nonce 12B][ciphertext ‖ tag 16B]
;;
;; The per-blob key is HKDF(master, salt) with a fresh CSPRNG salt on every write,
;; so no key ever encrypts more than one message and nonce reuse — GCM's one
;; catastrophic failure mode — cannot arise, whatever the store's write volume.
;; The bytes are identical on JVM and ClojureScript (geheimnis carries interop
;; known-answer tests), so a store written by one platform reads on the other.
;; -----------------------------------------------------------------------------

(def ^:const gcm-key-size 32)
(def ^:const gcm-salt-size 32)
(def ^:const gcm-nonce-size 12)
(def ^:const gcm-tag-size 16)

(def ^:private gcm-info (codec/str->bytes "konserve-aes-gcm-v1"))
(def ^:private gcm-prefix-size (+ gcm-salt-size gcm-nonce-size))

#?(:cljs
   (defn- no-sync-aead! []
     (throw (ex-info (str "The :aes-gcm encryptor cannot run in {:sync? true} mode on "
                          "ClojureScript: Web Crypto exposes no synchronous cipher. "
                          "Use the store asynchronously.")
                     {:type ::sync-aead-unsupported}))))

(defn- gcm-encrypt [master-key plaintext aad env]
  (let [salt     (csprng/random-bytes gcm-salt-size)
        nonce    (csprng/random-bytes gcm-nonce-size)
        blob-key (hash/hkdf master-key salt gcm-info gcm-key-size)]
    (if (:sync? env)
      #?(:clj  (codec/concat-bytes
                [salt nonce (aead/aead-encrypt-sync blob-key nonce aad plaintext)])
         :cljs (no-sync-aead!))
      (go-try- (codec/concat-bytes
                [salt nonce (<?- (aead/aead-encrypt blob-key nonce aad plaintext))])))))

(defn- gcm-decrypt [master-key ciphertext aad env]
  (when (< (codec/blen ciphertext) (+ gcm-prefix-size gcm-tag-size))
    (throw (ex-info "Blob is too short to be an :aes-gcm ciphertext."
                    {:type ::malformed-ciphertext
                     :size (codec/blen ciphertext)})))
  (let [salt     (byte-slice ciphertext 0 gcm-salt-size)
        nonce    (byte-slice ciphertext gcm-salt-size gcm-prefix-size)
        ct       (byte-slice ciphertext gcm-prefix-size (codec/blen ciphertext))
        blob-key (hash/hkdf master-key salt gcm-info gcm-key-size)]
    (if (:sync? env)
      #?(:clj  (aead/aead-decrypt-sync blob-key nonce aad ct)
         :cljs (no-sync-aead!))
      (go-try- (<?- (aead/aead-decrypt blob-key nonce aad ct))))))

(defrecord AESGCMEncryptor [key]
  PEncryptor
  (-encrypt [_ plaintext aad env] (gcm-encrypt key plaintext aad env))
  (-decrypt [_ ciphertext aad env] (gcm-decrypt key ciphertext aad env)))

(defn generate-key
  "A fresh 256-bit master key for the :aes-gcm encryptor, as a 64-character hex
   string. Store it outside the store you are encrypting."
  []
  (codec/bytes->hex (csprng/random-bytes gcm-key-size)))

(defn- coerce-gcm-key [key]
  (cond
    (and (string? key) (re-matches #"(?i)[0-9a-f]{64}" key))
    (codec/hex->bytes key)

    (and (byte-array? key) (= gcm-key-size (codec/blen key)))
    key

    :else
    (throw (ex-info (str "The :aes-gcm encryptor needs a 256-bit key: either 32 raw bytes "
                         "or a 64-character hex string. It does not stretch passphrases — "
                         "a guessable one would be brute-forced offline against the stored "
                         "blobs. Generate one with (konserve.encryptor/generate-key).")
                    {:type ::invalid-key
                     :provided-type (type key)}))))

(defn aes-gcm-encryptor [config]
  (let [{:keys [key]} config]
    (when (nil? key)
      (throw (ex-info "AES-GCM key not provided."
                      {:type   ::key-missing
                       :config (dissoc config :key)})))
    (AESGCMEncryptor. (coerce-gcm-key key))))

;; -----------------------------------------------------------------------------
;; :aes — DEPRECATED, unauthenticated AES-256-CBC (geheimnis v1)
;;
;; Kept read/write compatible byte-for-byte with konserve <= 0.7 so existing
;; stores keep working. Layout: [salt 64B][CBC ciphertext], where both the key and
;; the IV are derived from the salt, per
;; https://crypto.stackexchange.com/questions/84439/is-it-dangerous-to-encrypt-lots-of-small-files-with-the-same-key
;;
;; Note the salt is encoded differently on JVM and ClojureScript (the `inc` below,
;; and the signed/unsigned reading in -decrypt). That asymmetry means a blob
;; written by one platform fails to decrypt on the other whenever a salt byte
;; lands on 128 — roughly one blob in five. It is preserved rather than fixed
;; because fixing it would break the stores it currently works for. :aes-gcm has
;; no such problem.
;; -----------------------------------------------------------------------------

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
  (let [salt      (legacy-salt)
        iv        (get-initial-vector salt master-key)
        encrypted #?(:clj  (encrypt (get-key salt master-key) plaintext :iv iv)
                     :cljs (encrypt (get-key salt master-key) (.from js/Array plaintext) :iv iv))
        out       #?(:clj  (codec/concat-bytes [(byte-array salt) encrypted])
                     :cljs (let [out (js/Uint8Array. (+ salt-size (count encrypted)))]
                             (.set out (js/Uint8Array.from (into-array salt)) 0)
                             (.set out (js/Uint8Array.from encrypted) salt-size)
                             out))]
    (passthrough out env)))

(defn- legacy-decrypt [master-key ciphertext env]
  (let [salt      #?(:clj  (map int (byte-slice ciphertext 0 salt-size))
                     :cljs (map (fn [b] (if (> b 128) (- b 256) b))
                                (.slice ciphertext 0 salt-size)))
        data      #?(:clj  (byte-slice ciphertext salt-size (codec/blen ciphertext))
                     :cljs (.slice ciphertext salt-size))
        decrypted (decrypt (get-key salt master-key) data
                           :iv (get-initial-vector salt master-key))]
    (passthrough decrypted env)))

(defrecord AESEncryptor [key]
  PEncryptor
  (-encrypt [_ plaintext _aad env] (legacy-encrypt key plaintext env))
  (-decrypt [_ ciphertext _aad env] (legacy-decrypt key ciphertext env)))

(defn aes-encryptor [config]
  (let [{:keys [key]} config]
    (if (nil? key)
      (throw (ex-info "AES key not provided."
                      {:type   :aes-encryptor-key-missing
                       :config config}))
      (AESEncryptor. key))))

;; -----------------------------------------------------------------------------

(def byte->encryptor
  {0 null-encryptor
   1 aes-encryptor
   2 aes-gcm-encryptor})

(def encryptor->byte
  (invert-map byte->encryptor))

(defn get-encryptor [type]
  (case type
    :aes     aes-encryptor
    :aes-gcm aes-gcm-encryptor
    null-encryptor))
