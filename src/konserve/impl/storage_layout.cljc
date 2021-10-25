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
       (.clear return-buffer)
       return-array)
     :cljs (throw (ex-info "Not supported yet." {}))))

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
           meta-size (.getInt bb 4)]
       [version
        (serializers (byte->key serializer-id))
        (byte->compressor compressor-id)
        (byte->encryptor encryptor-id)
        meta-size])
     :cljs
     (throw (ex-info "Not supported yet." {}))))

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
  (-delete-store [this env] "Delete the underlying store.")
  (-store-exists? [this env] "Check if underlying store already exists.")
  (-sync-store [this env] "Synchronize the store. This is only needed if your store does not guarantee durability without this synchronisation command, e.g. fsync in the file system.")
  (-keys [this env] "List all the keys representing blobs in the store.")
  (-handle-foreign-key [this migration-key serializer read-handlers write-handlers env] "Handle keys not recognized by the current konserve version."))

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
