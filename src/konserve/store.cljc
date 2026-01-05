(ns konserve.store
  "Unified multimethod-based store dispatch layer.

   This namespace provides a polymorphic interface for connecting to different
   konserve backend stores using a `:backend` key in the configuration map.

   Built-in backends:
   - :memory - In-memory store (all platforms)
   - :file - File-based store (JVM only)

   External backends (register via require):
   - :file - File-based store for Node.js (konserve.node-filestore)
   - :indexeddb - Browser IndexedDB (konserve.indexeddb - browser only)
   - :s3 - AWS S3 backend (konserve-s3)
   - :dynamodb - AWS DynamoDB backend (konserve-dynamodb)
   - :redis - Redis backend (konserve-redis)
   - :lmdb - LMDB backend (konserve-lmdb)
   - :rocksdb - RocksDB backend (konserve-rocksdb)

   Example usage:

     (require '[konserve.store :as store])

     ;; Memory store
     (store/connect-store {:backend :memory :opts {:sync? true}})

     ;; File store (JVM)
     (store/connect-store {:backend :file :path \"/tmp/konserve\" :opts {:sync? true}})

     ;; After requiring konserve-s3:
     (store/connect-store {:backend :s3 :bucket \"my-bucket\" :region \"us-east-1\"})"
  (:require [konserve.memory]
            #?(:clj [konserve.filestore])))

;; =============================================================================
;; Multimethod Definitions
;; =============================================================================

(defmulti connect-store
  "Connect to a konserve store based on :backend key in config.

   Each backend expects different config keys. See documentation for specific backend
   configuration requirements.

   Args:
     config - A map with :backend key and backend-specific configuration

   Returns:
     A store instance (or channel if async mode, determined by :opts {:sync? false})"
  :backend)

(defmulti empty-store
  "Create a new empty store (equivalent to connect-store for most backends).

   Args:
     config - A map with :backend key and backend-specific configuration

   Returns:
     A new store instance"
  :backend)

(defmulti delete-store
  "Delete/clean up an existing store (removes underlying storage).

   Args:
     config - A map with :backend key and backend-specific configuration

   Returns:
     nil or cleanup status"
  :backend)

(defmulti release-store
  "Release connections and resources held by a store.

   Args:
     config - A map with :backend key
     store - The store instance to release

   Returns:
     nil or completion indicator"
  (fn [config _store]
    (:backend config)))

;; =============================================================================
;; Built-in Backend Implementations
;; =============================================================================

;; ===== :memory Backend =====

(defmethod connect-store :memory
  [{:keys [opts] :as config}]
  (konserve.memory/new-mem-store (atom {}) (or opts {:sync? false})))

(defmethod empty-store :memory
  [config]
  (connect-store config))

(defmethod delete-store :memory
  [_config]
  nil)

(defmethod release-store :memory
  [_config _store]
  nil)

;; ===== :file Backend (JVM only) =====
;; ClojureScript/Node.js :file backend is external - require konserve.node-filestore

#?(:clj
   (defmethod connect-store :file
     [{:keys [path config filesystem] :as all-config}]
     (konserve.filestore/connect-fs-store path
                                          :config config
                                          :filesystem filesystem
                                          :opts (:opts all-config))))

#?(:clj
   (defmethod empty-store :file
     [config]
     (connect-store config)))

#?(:clj
   (defmethod delete-store :file
     [{:keys [path filesystem]}]
     (konserve.filestore/delete-store filesystem path)))

#?(:clj
   (defmethod release-store :file
     [_config _store]
     nil))

;; =============================================================================
;; Default Error Handling
;; =============================================================================

(defmethod connect-store :default
  [{:keys [backend] :as config}]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only)"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod empty-store :default
  [{:keys [backend] :as config}]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only)"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod delete-store :default
  [{:keys [backend] :as config}]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only)"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod release-store :default
  [_config _store]
  nil)
