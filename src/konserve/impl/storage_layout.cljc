(ns konserve.impl.storage-layout
  "One of these protocols must be implemented by each store to provide low level
  access depending on the low-level storage layout chosen. Stores can support
  multiple layouts."
  (:require [konserve.serializers :refer [serializer-class->byte byte->key]]
            [konserve.compressor :refer [compressor->byte byte->compressor]]
            [konserve.encryptor :refer [encryptor->byte byte->encryptor]])
  #?(:clj (:import [java.nio ByteBuffer])))

(def ^:const header-size 20)

(defn create-header
  "Return Byte Array with following content
     1th Byte = Storage layout used
     2th Byte = Serializer Type
     3th Byte = Compressor Type
     4th Byte = Encryptor Type
   5-8th Byte = Meta-Size
  9th-20th Byte are spare"
  [version serializer compressor encryptor meta]
  #?(:clj
     (let [serializer-id        (get serializer-class->byte (type serializer))
           compressor-id        (get compressor->byte compressor)
           encryptor-id         (get encryptor->byte encryptor)

           env-array        (byte-array [version serializer-id compressor-id encryptor-id])
           return-buffer    (ByteBuffer/allocate header-size)
           _                (.put return-buffer env-array)
           _                (.putInt return-buffer 4 meta)
           return-array     (.array return-buffer)]
       return-array)
     :cljs
     (let [serializer-id        (get serializer-class->byte (type serializer)) ;;TODO
           compressor-id        (get compressor->byte compressor)
           encryptor-id         (get encryptor->byte encryptor)
           env-array        #js [version serializer-id compressor-id encryptor-id]
           return-buffer    (js/Uint8Array. header-size)] ;;possibly sparse?
       (dotimes [i (alength env-array)]
         (aset return-buffer i (aget env-array i)))
       (aset return-buffer 4 meta)
       return-buffer)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn header-not-zero-padded? [^bytes bs] ;; TODO: can this be removed?
  ;; does not have zero padding from byte 9 to 20
  (or (not= 0 (aget bs  8))
      (not= 0 (aget bs  9))
      (not= 0 (aget bs 10))
      (not= 0 (aget bs 11))
      (not= 0 (aget bs 12))
      (not= 0 (aget bs 13))
      (not= 0 (aget bs 14))
      (not= 0 (aget bs 15))
      (not= 0 (aget bs 16))
      (not= 0 (aget bs 17))
      (not= 0 (aget bs 18))
      (not= 0 (aget bs 19))))

(defn parse-header
  "Inverse function to create-header. serializers are a map of serializer-id to
  instance that are potentially initialized with custom handlers by the store
  user. We assume compressors and encryptors to use system-wide standard configurations."
  [header-bytes serializers]
  #?(:clj
     (let [bb (ByteBuffer/allocate header-size)
           _ (.put bb ^bytes header-bytes)
           version (.get bb 0)
           _ (when-not (= version 1)
               (throw (ex-info "Konserve version not supported."
                               {:type :konserve-version-in-header-no-supported
                                :header-version version
                                :supported-versions #{1}})))
           serializer-id (.get bb 1)
           compressor-id (.get bb 2)
           encryptor-id (.get bb 3)
           meta-size (.getInt bb 4)
           ;; was used temporarily at some point during 0.6.0-alpha (JVM only)
           small-header-size 8
           actual-header-size (if (and (= version 1)
                                       (= 20 (count header-bytes))
                                       ;; use rest of header to detect actual 8
                                       ;; byte size requires version bump to set
                                       ;; these bytes to non-zero now
                                       (header-not-zero-padded? header-bytes))
                                small-header-size header-size)
           serializer (serializers (byte->key serializer-id))
           compressor (byte->compressor compressor-id)
           encryptor (byte->encryptor encryptor-id)]
       (when-not serializer
         (throw (ex-info "Serializer not found."
                         {:type        :serializer-not-found
                          :serializers serializers
                          :serializer-id serializer-id})))
       (when-not compressor
         (throw (ex-info "Compressor not found."
                         {:type          :compressor-not-found
                          :compressor-id compressor-id})))
       (when-not encryptor
         (throw (ex-info "Encryptor not found."
                         {:type          :encryptor-not-found
                          :encryptor-id encryptor-id})))
       [version
        serializer
        compressor
        encryptor
        meta-size
        actual-header-size])
     :cljs
     (let [version (aget header-bytes 0)
           _ (when-not (= version 1)
               (throw (ex-info "Konserve version not supported."
                               {:type :konserve-version-in-header-no-supported
                                :header-version version
                                :supported-versions #{1}})))
           serializer-id (aget header-bytes 1)
           compressor-id (aget header-bytes 2)
           encryptor-id (aget header-bytes 3)
           meta-size (aget header-bytes 4)
           serializer (serializers (byte->key serializer-id))
           compressor (byte->compressor compressor-id)
           encryptor (byte->encryptor encryptor-id)]
       (when-not serializer
         (throw (ex-info "Serializer not found."
                         {:type          :serializer-not-found
                          :serializers   serializers
                          :serializer-id serializer-id})))
       (when-not compressor
         (throw (ex-info "Compressor not found."
                         {:type          :compressor-not-found
                          :compressor-id compressor-id})))
       (when-not encryptor
         (throw (ex-info "Encryptor not found."
                         {:type         :encryptor-not-found
                          :encryptor-id encryptor-id})))
       [version
        serializer
        compressor
        encryptor
        meta-size
        header-size])))

(def ^:const default-version 1)

(defprotocol PBackingStore
  "Backing store protocol for default implementation of the high-level konserve protocol."
  (-create-blob [this store-key env] "Create a blob object to write a metadata and value into.")
  (-delete-blob [this store-key env] "Delete a blob object under path.")
  (-blob-exists? [this store-key env] "Check whether blob exists under path")
  (-migratable [this key store-key env] "Check if blob exists elsewhere and return a migration key or nil.")
  (-migrate [this migration-key key-vec serializer read-handlers write-handlers env] "Use the key returned from -migratable to import blob for key-vec.")
  (-copy [this from to env] "Copy a blob from one key to another.")
  (-atomic-move [this from to env] "Atomically move (rename) a blob.")
  (-create-store [this env] "Create the underlying store.")
  #_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
  (-delete-store [this env] "Delete the underlying store.")
  #_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
  (-store-exists? [this env] "Check if underlying store already exists.")
  (-sync-store [this env] "Synchronize the store. This is only needed if your store does not guarantee durability without this synchronisation command, e.g. fsync in the file system.")
  (-keys [this env] "List all the keys representing blobs in the store.")
  (-handle-foreign-key [this migration-key serializer read-handlers write-handlers env] "Handle keys not recognized by the current konserve version."))

(defprotocol PMultiWriteBackingStore
  "Protocol for backing stores that support atomic multi-key writes."
  (-multi-write-blobs [this store-key-values env]
    "Write multiple blobs atomically in a single operation.
     store-key-values is a sequence of [store-key serialized-data] pairs.
     serialized-data is a map containing :header, :meta-arr, and :value-arr.
     Returns a map of store-keys to success values (typically true).
     Backends must implement this to support multi-key operations.")
  (-multi-delete-blobs [this store-keys env]
    "Delete multiple blobs atomically in a single operation.
     store-keys is a sequence of store-key strings to delete.
     Returns a map of store-keys to boolean indicating if the blob existed before deletion.
     Backends must implement this to support multi-key operations."))

(defprotocol PMultiReadBackingStore
  "Protocol for backing stores that support atomic multi-key reads."
  (-multi-read-blobs [this store-keys env]
    "Read multiple blobs atomically in a single operation.
     store-keys is a sequence of store-key strings to read.
     Returns a sparse map of {store-key -> blob} for found keys only.
     Missing keys are excluded from the result map.
     Backends must implement this to support multi-key read operations."))

(defprotocol PBackingBlob
  "Blob object that is backing a stored value and its metadata."
  (-sync [this env] "Synchronize this object and ensure it is stored. This is not necessary in many stores.")
  (-close [this env] "Close the blob and ensure that pending data is send to the store.")
  (-get-lock [this env] "Acquire a store specific lock on the blob. This is needed for access to the store from concurrent processes.")

  (-read-header [this env] "Read header array.")
  (-read-meta [this meta-size env] "Read metadata array.")
  (-read-value [this meta-size env] "Read serialized edn value array.")
  (-read-binary [this meta-size locked-cb env] "Read binary object and pass to locked-cb.")

  (-write-header [this header-arr env] "Write header array.")
  (-write-meta [this meta-arr env] "Write metadata array.")
  (-write-value [this value-arr meta-size env] "Write value array.")
  (-write-binary [this meta-size blob env] "Write binary blob."))

(defprotocol PBackingLock
  (-release [this env] "Release this lock."))
