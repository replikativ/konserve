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
            [zufall.core]
            #?(:clj  [clojure.core.async :refer [go <!]]
               :cljs [cljs.core.async :refer [go <!]])))

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

(defmulti create-store
  "Create a new store.

   Note: Most backends auto-create on connect-store, so this is often equivalent.
   Use this when you explicitly want to create a new store.

   Args:
     config - A map with :backend key and backend-specific configuration

   Returns:
     A new store instance"
  :backend)

(defmulti store-exists?
  "Check if a store exists at the given configuration.

   Args:
     config - A map with :backend key and backend-specific configuration

   Returns:
     true if store exists, false otherwise (or channel in async mode)"
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
  [{:keys [id opts] :as config}]
  (let [opts (or opts {:sync? false})]
    (if id
      ;; Strict mode with :id - must exist in registry
      (let [store (konserve.memory/connect-mem-store id opts)]
        (if (:sync? opts)
          (or store
              (throw (ex-info (str "Memory store with ID '" id "' does not exist. Use create-store first.")
                              {:id id :config config})))
          (go (or (<! store)
                  (throw (ex-info (str "Memory store with ID '" id "' does not exist. Use create-store first.")
                                  {:id id :config config}))))))
      ;; No :id - ephemeral mode (backwards compatible)
      (konserve.memory/new-mem-store (atom {}) opts))))

(defmethod create-store :memory
  [{:keys [id opts] :as config}]
  (let [id (or id (zufall.core/rand-german-mammal))
        opts (or opts {:sync? false})
        existing (get @konserve.memory/memory-store-registry id)]
    (when existing
      (throw (ex-info (str "Memory store with ID '" id "' already exists.")
                      {:id id :config config})))
    (konserve.memory/new-mem-store (atom {}) (assoc opts :id id))))

(defmethod store-exists? :memory
  [{:keys [id opts] :as config}]
  (let [opts (or opts {:sync? false})]
    (if id
      ;; Check registry if :id provided
      (let [exists (contains? @konserve.memory/memory-store-registry id)]
        (if (:sync? opts) exists (go exists)))
      ;; No :id - ephemeral stores don't "exist" in persistent sense
      (if (:sync? opts) false (go false)))))

(defmethod delete-store :memory
  [{:keys [id] :as config}]
  ;; Only delete from registry if :id provided
  (when id
    (konserve.memory/delete-mem-store id))
  nil)

(defmethod release-store :memory
  [_config _store]
  nil)

;; ===== :file Backend (JVM only) =====
;; ClojureScript/Node.js :file backend is external - require konserve.node-filestore

#?(:clj
   (defmethod connect-store :file
     [{:keys [path config filesystem opts] :as all-config}]
     (let [opts (or opts {:sync? false})]
       (let [exists (konserve.filestore/store-exists? filesystem path)]
         (when-not exists
           (throw (ex-info (str "File store does not exist at path: " path)
                           {:path path :config all-config})))
         (konserve.filestore/connect-fs-store path
                                              :config config
                                              :filesystem filesystem
                                              :opts opts)))))

#?(:clj
   (defmethod create-store :file
     [{:keys [path config filesystem opts] :as all-config}]
     (let [opts (or opts {:sync? false})
           exists (konserve.filestore/store-exists? filesystem path)]
       (when exists
         (throw (ex-info (str "File store already exists at path: " path)
                         {:path path :config all-config})))
       (konserve.filestore/connect-fs-store path
                                            :config config
                                            :filesystem filesystem
                                            :opts opts))))

#?(:clj
   (defmethod store-exists? :file
     [{:keys [path filesystem opts]}]
     (let [exists (konserve.filestore/store-exists? filesystem path)]
       (if (:sync? opts)
         exists
         (go exists)))))

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
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only), :tiered"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod create-store :default
  [{:keys [backend] :as config}]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only), :tiered"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod store-exists? :default
  [{:keys [backend] :as config}]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only), :tiered"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

;; ===== :tiered Backend (built-in) =====
;; Tiered store combines a fast frontend cache with a durable backend

(defmethod create-store :tiered
  [{:keys [frontend backend write-policy read-policy opts] :as config}]
  (let [opts (or opts {:sync? false})]
    (if (:sync? opts)
      ;; Synchronous mode
      (let [frontend-store (create-store (assoc frontend :opts opts))
            backend-store (create-store (assoc backend :opts opts))]
        (tiered/connect-tiered-store frontend-store backend-store
                                     :write-policy (or write-policy :write-through)
                                     :read-policy (or read-policy :frontend-first)
                                     :opts opts))
      ;; Asynchronous mode
      (go
        (let [frontend-store (<! (create-store (assoc frontend :opts opts)))
              backend-store (<! (create-store (assoc backend :opts opts)))]
          (<! (tiered/connect-tiered-store frontend-store backend-store
                                           :write-policy (or write-policy :write-through)
                                           :read-policy (or read-policy :frontend-first)
                                           :opts opts)))))))

(defmethod connect-store :tiered
  [{:keys [frontend backend write-policy read-policy opts] :as config}]
  (let [opts (or opts {:sync? false})]
    (if (:sync? opts)
      ;; Synchronous mode
      (let [frontend-store (connect-store (assoc frontend :opts opts))
            backend-store (connect-store (assoc backend :opts opts))]
        (tiered/connect-tiered-store frontend-store backend-store
                                     :write-policy (or write-policy :write-through)
                                     :read-policy (or read-policy :frontend-first)
                                     :opts opts))
      ;; Asynchronous mode
      (go
        (let [frontend-store (<! (connect-store (assoc frontend :opts opts)))
              backend-store (<! (connect-store (assoc backend :opts opts)))]
          (<! (tiered/connect-tiered-store frontend-store backend-store
                                           :write-policy (or write-policy :write-through)
                                           :read-policy (or read-policy :frontend-first)
                                           :opts opts)))))))

(defmethod store-exists? :tiered
  [{:keys [backend opts] :as config}]
  ;; Tiered store exists if backend exists (frontend is just a cache)
  (store-exists? (assoc backend :opts (or opts {:sync? false}))))

(defmethod delete-store :tiered
  [{:keys [backend frontend] :as config}]
  ;; Delete backend (authoritative source)
  ;; Optionally delete frontend if it's persistent
  (delete-store backend)
  ;; Only delete frontend if it has persistence (e.g., file-based)
  (when (and frontend (#{:file :indexeddb :lmdb :rocksdb} (:backend frontend)))
    (delete-store frontend))
  nil)

(defmethod release-store :tiered
  [{:keys [frontend backend]} store]
  ;; Release both stores
  (when frontend
    (release-store frontend (:frontend-store store)))
  (when backend
    (release-store backend (:backend-store store)))
  nil)

;; ===== Default handlers for unsupported backends =====

(defmethod delete-store :default
  [{:keys [backend] :as config}]
  (throw (ex-info
          (str "Unsupported store backend: " backend
               "\n\nBuilt-in backends: :memory (all platforms), :file (JVM only), :tiered"
               "\nExternal backends: :file (Node.js - konserve.node-filestore), :indexeddb (browser), :s3, :dynamodb, :redis, :lmdb, :rocksdb"
               "\nMake sure the corresponding backend module is required before use.")
          {:backend backend :config config})))

(defmethod release-store :default
  [_config _store]
  nil)
