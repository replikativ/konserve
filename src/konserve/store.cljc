(ns konserve.store
  "Unified multimethod-based store dispatch layer.

   This namespace provides a polymorphic interface for connecting to different
   konserve backend stores using a `:backend` key in the configuration map.

   Built-in backends:
   - :memory - In-memory store (all platforms)
   - :file - File-based store (JVM only)
   - :tiered - Tiered store with frontend cache and backend persistence (all platforms)

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
            [konserve.tiered :as tiered]
            #?(:clj [konserve.filestore])
            [clojure.core.async :refer [go] :include-macros true]
            [superv.async :refer [go-try- <?-] :include-macros true]))

;; =============================================================================
;; Validation
;; =============================================================================

(defn validate-store-config
  "Validates that store config has a valid UUID :id.

   All stores require an :id field containing a UUID type for proper
   identification and matching across different backends and machines.

   Args:
     config - Store configuration map

   Returns:
     config if valid

   Throws:
     ex-info if :id is missing or not a UUID type"
  [config]
  (let [id (:id config)]
    (cond
      (nil? id)
      (throw (ex-info
              (str "Store :id is required. Please add :id with a UUID to your store config.\n"
                   "Generate a UUID with: #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))\n"
                   "Example: {:backend :memory :id #uuid \"550e8400-e29b-41d4-a716-446655440000\" ...}")
              {:config config :error :missing-id}))

      (not (uuid? id))
      (throw (ex-info
              (str "Store :id must be a UUID type. Got: " (type id) "\n"
                   "Use #uuid \"...\" literal or generate with: #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))")
              {:config config :id id :error :invalid-id-type}))

      :else config)))

;; =============================================================================
;; Private Multimethods (fixed arity, both config and opts)
;; =============================================================================

(defmulti -connect-store
  "Private multimethod for connecting to stores with fixed arity.

   Dispatch is based on :backend in config."
  (fn [config _opts]
    (validate-store-config config)
    (:backend config)))

(defmulti -create-store
  "Private multimethod for creating stores with fixed arity.

   Dispatch is based on :backend in config."
  (fn [config _opts]
    (validate-store-config config)
    (:backend config)))

(defmulti -store-exists?
  "Private multimethod for checking store existence with fixed arity.

   Dispatch is based on :backend in config."
  (fn [config _opts]
    (validate-store-config config)
    (:backend config)))

(defmulti -delete-store
  "Private multimethod for deleting stores with fixed arity.

   Dispatch is based on :backend in config."
  (fn [config _opts]
    (validate-store-config config)
    (:backend config)))

(defmulti -release-store
  "Private multimethod for releasing stores with fixed arity.

   Dispatch is based on :backend in config."
  (fn [config _store _opts]
    (validate-store-config config)
    (:backend config)))

;; =============================================================================
;; Public Functions with Variable Arity (defaults to async)
;; =============================================================================

(defn connect-store
  "Connect to a konserve store based on :backend key in config.

   Each backend expects different config keys. See documentation for specific backend
   configuration requirements.

   Args:
     config - A map with :backend key and backend-specific configuration
     opts - (optional) Runtime options map. Defaults to {:sync? false}
            Common options:
            - :sync? - true for synchronous, false for async (default: false)

   Returns:
     A store instance (or channel if async mode)"
  ([config]
   (connect-store config {:sync? false}))
  ([config opts]
   (-connect-store config (or opts {:sync? false}))))

(defn create-store
  "Create a new store.

   Note: Most backends auto-create on connect-store, so this is often equivalent.
   Use this when you explicitly want to create a new store.

   Args:
     config - A map with :backend key and backend-specific configuration
     opts - (optional) Runtime options map. Defaults to {:sync? false}

   Returns:
     A new store instance"
  ([config]
   (create-store config {:sync? false}))
  ([config opts]
   (-create-store config (or opts {:sync? false}))))

(defn store-exists?
  "Check if a store exists at the given configuration.

   Args:
     config - A map with :backend key and backend-specific configuration
     opts - (optional) Runtime options map. Defaults to {:sync? false}

   Returns:
     true if store exists, false otherwise (or channel in async mode)"
  ([config]
   (store-exists? config {:sync? false}))
  ([config opts]
   (-store-exists? config (or opts {:sync? false}))))

(defn delete-store
  "Delete/clean up an existing store (removes underlying storage).

   Args:
     config - A map with :backend key and backend-specific configuration
     opts - (optional) Runtime options map. Defaults to {:sync? false}

   Returns:
     nil or cleanup status"
  ([config]
   (delete-store config {:sync? false}))
  ([config opts]
   (-delete-store config (or opts {:sync? false}))))

(defn release-store
  "Release connections and resources held by a store.

   Args:
     config - A map with :backend key
     store - The store instance to release
     opts - (optional) Runtime options map. Defaults to {:sync? false}

   Returns:
     nil or completion indicator"
  ([config store]
   (release-store config store {:sync? false}))
  ([config store opts]
   (-release-store config store (or opts {:sync? false}))))

;; =============================================================================
;; Built-in Backend Implementations
;; =============================================================================

;; ===== :memory Backend =====

(defmethod -connect-store :memory
  [{:keys [id] :as config} opts]
  (if id
    ;; Strict mode with :id - must exist in registry
    (let [store (konserve.memory/connect-mem-store id opts)]
      (if (:sync? opts)
        (or store
            (throw (ex-info (str "Memory store with ID '" id "' does not exist. Use create-store first.")
                            {:id id :config config})))
        (go-try- (or (<?- store)
                     (throw (ex-info (str "Memory store with ID '" id "' does not exist. Use create-store first.")
                                     {:id id :config config}))))))
    ;; No :id - ephemeral mode (backwards compatible)
    (konserve.memory/new-mem-store (atom {}) opts)))

(defmethod -create-store :memory
  [{:keys [id] :as config} opts]
  (let [existing (get @konserve.memory/memory-store-registry id)]
    (when existing
      (throw (ex-info (str "Memory store with ID '" id "' already exists.")
                      {:id id :config config})))
    (konserve.memory/new-mem-store (atom {}) (assoc opts :id id))))

(defmethod -store-exists? :memory
  [{:keys [id] :as config} opts]
  (if id
    ;; Check registry if :id provided
    (let [exists (contains? @konserve.memory/memory-store-registry id)]
      (if (:sync? opts) exists (go-try- exists)))
    ;; No :id - ephemeral stores don't "exist" in persistent sense
    (if (:sync? opts) false (go-try- false))))

(defmethod -delete-store :memory
  [{:keys [id] :as config} _opts]
  ;; Only delete from registry if :id provided
  (when id
    (konserve.memory/delete-mem-store id))
  nil)

(defmethod -release-store :memory
  [_config _store _opts]
  nil)

;; ===== :file Backend (JVM only) =====
;; ClojureScript/Node.js :file backend is external - require konserve.node-filestore

#?(:clj
   (defmethod -connect-store :file
     [{:keys [path config filesystem] :as all-config} opts]
     (let [exists (konserve.filestore/store-exists? filesystem path)]
       (when-not exists
         (throw (ex-info (str "File store does not exist at path: " path)
                         {:path path :config all-config})))
       (konserve.filestore/connect-fs-store path
                                            :config config
                                            :filesystem filesystem
                                            :opts opts))))

#?(:clj
   (defmethod -create-store :file
     [{:keys [path config filesystem] :as all-config} opts]
     (let [exists (konserve.filestore/store-exists? filesystem path)]
       (when exists
         (throw (ex-info (str "File store already exists at path: " path)
                         {:path path :config all-config})))
       (konserve.filestore/connect-fs-store path
                                            :config config
                                            :filesystem filesystem
                                            :opts opts))))

#?(:clj
   (defmethod -store-exists? :file
     [{:keys [path filesystem] :as config} opts]
     (let [exists (konserve.filestore/store-exists? filesystem path)]
       (if (:sync? opts)
         exists
         (go-try- exists)))))

#?(:clj
   (defmethod -delete-store :file
     [{:keys [path filesystem] :as config} _opts]
     (konserve.filestore/delete-store filesystem path)))

#?(:clj
   (defmethod -release-store :file
     [_config _store _opts]
     nil))

;; =============================================================================
;; Default Error Handling
;; =============================================================================

(defmethod -connect-store :default
  [{:keys [backend] :as config} _opts]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only), :tiered"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod -create-store :default
  [{:keys [backend] :as config} _opts]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only), :tiered"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod -store-exists? :default
  [{:keys [backend] :as config} _opts]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only), :tiered"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

;; ===== :tiered Backend (built-in) =====
;; Tiered store combines a fast frontend cache with a durable backend

(defmethod -create-store :tiered
  [{:keys [frontend backend write-policy read-policy] :as config} opts]
  (if (:sync? opts)
    ;; Synchronous mode
    (let [frontend-store (create-store frontend opts)
          backend-store (create-store backend opts)]
      (tiered/connect-tiered-store frontend-store backend-store
                                   :write-policy (or write-policy :write-through)
                                   :read-policy (or read-policy :frontend-first)
                                   :opts opts))
    ;; Asynchronous mode
    (go-try-
     (let [frontend-store (<?- (create-store frontend opts))
           backend-store (<?- (create-store backend opts))]
       (<?- (tiered/connect-tiered-store frontend-store backend-store
                                         :write-policy (or write-policy :write-through)
                                         :read-policy (or read-policy :frontend-first)
                                         :opts opts))))))

(defmethod -connect-store :tiered
  [{:keys [frontend backend write-policy read-policy] :as config} opts]
  (if (:sync? opts)
    ;; Synchronous mode
    (let [frontend-store (connect-store frontend opts)
          backend-store (connect-store backend opts)]
      (tiered/connect-tiered-store frontend-store backend-store
                                   :write-policy (or write-policy :write-through)
                                   :read-policy (or read-policy :frontend-first)
                                   :opts opts))
    ;; Asynchronous mode
    (go-try-
     (let [frontend-store (<?- (connect-store frontend opts))
           backend-store (<?- (connect-store backend opts))]
       (<?- (tiered/connect-tiered-store frontend-store backend-store
                                         :write-policy (or write-policy :write-through)
                                         :read-policy (or read-policy :frontend-first)
                                         :opts opts))))))

(defmethod -store-exists? :tiered
  [{:keys [backend] :as config} opts]
  ;; Tiered store exists if backend exists (frontend is just a cache)
  (store-exists? backend opts))

(defmethod -delete-store :tiered
  [{:keys [backend frontend] :as config} _opts]
  ;; Delete backend (authoritative source)
  ;; Optionally delete frontend if it's persistent
  (delete-store backend)
  ;; Only delete frontend if it has persistence (e.g., file-based)
  (when (and frontend (#{:file :indexeddb :lmdb :rocksdb} (:backend frontend)))
    (delete-store frontend))
  nil)

(defmethod -release-store :tiered
  [{:keys [frontend backend]} store _opts]
  ;; Release both stores
  (when frontend
    (release-store frontend (:frontend-store store)))
  (when backend
    (release-store backend (:backend-store store)))
  nil)

;; ===== Default handlers for unsupported backends =====

(defmethod -delete-store :default
  [{:keys [backend] :as config} _opts]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only), :tiered"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod -release-store :default
  [_config _store _opts]
  nil)
