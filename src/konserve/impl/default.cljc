(ns konserve.impl.default
  "Default implementation of the high level protocol given a binary backing implementation as defined in the storage-layout namespace."
  (:require
   [konserve.serializers :refer [key->serializer]]
   [konserve.compressor :refer [null-compressor]]
   [konserve.encryptor :refer [null-encryptor]]
   [hasch.core :refer [uuid]]
   [clojure.string :refer [ends-with?]]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore
                               PBinaryAsyncKeyValueStore
                               -serialize -deserialize
                               PKeyIterable]]
   [konserve.impl.storage-layout :refer [-atomic-move -create-store
                                         -copy -create-blob -delete -exists
                                         -keys -path -sync-store
                                         -close -get-lock -sync
                                         -read-header -read-meta -read-value -read-binary
                                         -write-header -write-meta -write-value -write-binary
                                         PBackingLock -release
                                         default-layout-id
                                         parse-header create-header]]
   [konserve.utils :refer [async+sync *default-sync-translation*]]
   [superv.async :refer [go-try- <?-]]
   [taoensso.timbre :as timbre :refer [trace]])
  #?(:clj
     (:import
      [java.io ByteArrayOutputStream ByteArrayInputStream])))

(extend-protocol PBackingLock
  nil
  (-release [this env]
    (if (:sync? env) nil (go-try- nil))))

(defn update-blob
  "Write file into file system. It write first the meta-size, that is stored in (1Byte),
  the meta-data and the actual data."
  [backing path serializer write-handlers
   {:keys [base key-vec compressor encryptor store-key up-fn up-fn-meta
           config operation input sync? storage-layout] :as env} [old-meta old-value]]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [[key & rkey] key-vec
          to-array (fn [value]
                     (let [bos (ByteArrayOutputStream.)]
                       (try (-serialize (encryptor (compressor serializer))
                                        bos write-handlers value)
                            (.toByteArray bos)
                            (finally (.close bos)))))
          meta                 (up-fn-meta old-meta)
          value                (when (= operation :write-edn)
                                 (if-not (empty? rkey)
                                   (update-in old-value rkey up-fn)
                                   (up-fn old-value)))

          path-new             (<?- (-path backing (if (:in-place? config)
                                                     store-key
                                                     (str store-key ".new"))
                                           env))
          _ (when (:in-place? config) ;; let's back things up before writing then
              (trace "backing up to blob: " (str store-key ".backup") " for key " key)
                 ;; TODO make sync
              (<?- (-copy backing path-new (str store-key ".backup") env)))
          meta-arr             (to-array meta)
          meta-size            (count meta-arr)
          header               (create-header storage-layout
                                              serializer compressor encryptor meta-size)
          ac-new               (<?- (-create-blob backing path-new env))]
      (try
        (<?- (-write-header ac-new header env))
        (<?- (-write-meta ac-new meta-arr env))
        (if (= operation :write-binary)
          (<?- (-write-binary ac-new meta-size input env))
          (let [value-arr            (to-array value)]
            (<?- (-write-value ac-new value-arr meta-size env))))

        (when (:fsync? config)
          (trace "syncing for " key)
          (<?- (-sync ac-new env))
          (<?- (-sync-store backing env)))
        (<?- (-close ac-new env))

        (when-not (:in-place? config)
          (trace "moving blob: " key)
          (<?- (-atomic-move backing path-new path env)))
        (if (= operation :write-edn) [old-value value] true)
        (finally
          (<?- (-close ac-new env))))))))

(defn read-header [ac serializers env]
  (let [{:keys [sync?]} env]
    (async+sync sync? *default-sync-translation*
                (go-try-
                 (let [arr (<?- (-read-header ac env))]
                   (parse-header arr serializers))))))

(defn- read-blob
  "Read meta, edn and binary."
  [ac read-handlers serializers {:keys [sync? operation store-key locked-cb] :as env}]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [[_ serializer compressor encryptor meta-size] (<?- (read-header ac serializers env))
          fn-read (partial -deserialize
                           (compressor (encryptor serializer))
                           read-handlers)]
      (case operation
        :read-meta (let [bais-read (ByteArrayInputStream.
                                    (<?- (-read-meta ac meta-size env)))
                         value     (fn-read bais-read)
                         _         (.close bais-read)]
                     value)
        :read-edn (let [bais-read (ByteArrayInputStream.
                                   (<?- (-read-value ac meta-size env)))
                        value     (fn-read bais-read)
                        _         (.close bais-read)]
                    value)
        :write-binary          (let [bais-read (ByteArrayInputStream.
                                                (<?- (-read-meta ac meta-size env)))
                                     meta      (fn-read bais-read)
                                     _         (.close bais-read)]
                                 [meta nil])
        :write-edn             (let [bais-meta  (ByteArrayInputStream.
                                                 (<?- (-read-meta ac meta-size env)))
                                     meta       (fn-read bais-meta)
                                     _          (.close bais-meta)
                                     bais-value (ByteArrayInputStream.
                                                 (<?- (-read-value ac meta-size env)))
                                     value     (fn-read bais-value)
                                     _          (.close bais-value)]
                                 [meta value])
        :read-binary           (<?- (-read-binary ac meta-size locked-cb env)))))))

(defn- delete-blob
  "Remove/Delete key-value pair of backing store by given key. If success it will return true."
  [backing env]
  (async+sync
   (:sync? env) *default-sync-translation*
   (go-try-
    (let [{:keys [key-vec base]} env
          key          (first key-vec)
          store-key    (str base "/" (uuid key) ".ksv")
          path         (<?- (-path backing store-key env))
          blob-exists? (<?- (-exists backing path env))]
      (if blob-exists?
        (try
          (<?- (-delete backing path env))
          true
          (catch Exception e
            (throw (ex-info "Could not delete key."
                            {:key key
                             :base base
                             :exeption e}))))
        false)))))

(defn- io-operation
  "Read/Write blob. For better understanding use the flow-chart of Konserve."
  [{:keys [backing migrate-in-io-operation]} serializers read-handlers write-handlers
   {:keys [base key-vec detect-old-blobs operation default-serializer
           sync? overwrite? config] :as env}]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [key           (first  key-vec)
          uuid-key      (uuid key)
          store-key     (str base "/" uuid-key ".ksv")
          env           (assoc env :store-key store-key)
          path          (<?- (-path backing store-key env))
          store-key-exists?  (<?- (-exists backing path env))
          old-store-key (when detect-old-blobs
                          (let [old-meta (str base "/meta/" uuid-key)
                                old (str base "/"  uuid-key)
                                old-binary (str base "/B_"  uuid-key)]
                            (or (@detect-old-blobs old-meta)
                                (@detect-old-blobs old)
                                (@detect-old-blobs old-binary))))
          serializer    (get serializers default-serializer)]
      (if (and old-store-key (not store-key-exists?))
        (<?- (migrate-in-io-operation old-store-key store-key env serializer read-handlers write-handlers))
        (if (or store-key-exists? (= :write-edn operation) (= :write-binary operation))
          (let [ac (<?- (-create-blob backing path env))
                lock   (when (:lock-blob? config)
                         (trace "Acquiring blob lock for: " (first key-vec) (str ac))
                         (<?- (-get-lock ac env)))]
            (try
              (let [old (if (and store-key-exists? (not overwrite?))
                          (<?- (read-blob ac read-handlers serializers env))
                          [nil nil])]
                (if (or (= :write-edn operation) (= :write-binary operation))
                  (<?- (update-blob backing path serializer write-handlers env old))
                  old))
              (finally
                (when (:lock-blob? config)
                  (trace "Releasing lock for " (first key-vec) (str ac))
                  (<?- (-release lock env)))
                (<?- (-close ac env)))))
          nil))))))

(defn- list-keys
  "Return all keys in the store."
  [{:keys [backing migrate-in-list-keys]}
   serializers read-handlers write-handlers {:keys [sync? config base] :as env}]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [path (<?- (-path backing base env))
          serializer (get serializers (:default-serializer env))
          blob-paths (<?- (-keys backing path env))]
      (loop [list-keys  #{}
             [path & blob-paths] blob-paths]
        (if path
          (cond
            (ends-with? (str path) ".new")
            (recur list-keys blob-paths)

            (ends-with? (str path) ".ksv")
            (let [ac          (<?- (-create-blob backing path env))
                  path-name   (str path)
                  env         (update-in env [:msg :keys] (fn [_] path-name))
                  lock   (when (and (:in-place? config) (:lock-blob? config))
                           (trace "Acquiring blob lock for: " path-name (str ac))
                           (<?- (-get-lock ac env)))]
              (recur
               (try
                 (conj list-keys (<?- (read-blob ac read-handlers serializers env)))
                    ;; it can be that the blob has been deleted, ignore reading errors
                 (catch Exception _
                   list-keys)
                 (finally
                   (<?- (-release lock env))
                   (<?- (-close ac env))))
               blob-paths))

            :else ;; need migration
            (let [[list-keys blob-paths]
                  (<?- (migrate-in-list-keys backing path base env serializer read-handlers write-handlers
                                             list-keys blob-paths))]
              (recur list-keys blob-paths)))
          list-keys))))))

(defrecord DefaultStore [storage-layout-id base backing serializers default-serializer compressor encryptor
                         read-handlers write-handlers buffer-size detected-old-blobs locks config
                         migrate-in-io-operation migrate-in-list-keys]
  PEDNAsyncKeyValueStore
  (-exists? [_ key env]
    (async+sync
     (:sync? env) *default-sync-translation*
     (go-try-
      (let [path (str base "/" (uuid key) ".ksv")]
        (<?- (-exists backing
                      (<?- (-path backing path env))
                      env))))))
  (-get [this key opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :base base
                     :operation :read-edn
                     :compressor compressor
                     :encryptor encryptor
                     :format    :data
                     :storage-layout storage-layout-id
                     :sync? sync?
                     :buffer-size buffer-size
                     :config config
                     :default-serializer default-serializer
                     :detect-old-blobs detected-old-blobs
                     :msg       {:type :read-edn-error
                                 :key  key}})))
  (-get-meta [this key opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :base base
                     :operation :read-meta
                     :compressor compressor
                     :encryptor encryptor
                     :detect-old-blobs detected-old-blobs
                     :default-serializer default-serializer
                     :storage-layout storage-layout-id
                     :sync? sync?
                     :buffer-size buffer-size
                     :config config
                     :msg       {:type :read-meta-error
                                 :key  key}})))

  (-assoc-in [this key-vec meta-up val opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec key-vec
                     :base base
                     :operation  :write-edn
                     :compressor compressor
                     :encryptor encryptor
                     :detect-old-blobs detected-old-blobs
                     :storage-layout storage-layout-id
                     :default-serializer default-serializer
                     :up-fn      (fn [_] val)
                     :up-fn-meta meta-up
                     :config     config
                     :sync? sync?
                     :buffer-size buffer-size
                     :overwrite? true
                     :msg        {:type :write-edn-error
                                  :key  (first key-vec)}})))

  (-update-in [this key-vec meta-up up-fn opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec key-vec
                     :base base
                     :operation  :write-edn
                     :compressor compressor
                     :encryptor encryptor
                     :detect-old-blobs detected-old-blobs
                     :storage-layout storage-layout-id
                     :default-serializer default-serializer
                     :up-fn      up-fn
                     :up-fn-meta meta-up
                     :config     config
                     :sync? sync?
                     :buffer-size buffer-size
                     :msg        {:type :write-edn-error
                                  :key  (first key-vec)}})))
  (-dissoc [_ key opts]
    (delete-blob backing
                 {:key-vec  [key]
                  :base base
                  :operation  :write-edn
                  :compressor compressor
                  :encryptor encryptor
                  :detect-old-blobs detected-old-blobs
                  :storage-layout storage-layout-id
                  :default-serializer default-serializer
                  :config     config
                  :sync?      (:sync? opts)
                  :buffer-size buffer-size
                  :msg        {:type :deletion-error
                               :key  key}}))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :base base
                     :operation :read-binary
                     :detect-old-blobs detected-old-blobs
                     :default-serializer default-serializer
                     :compressor compressor
                     :encryptor encryptor
                     :config    config
                     :storage-layout storage-layout-id
                     :sync? sync?
                     :buffer-size buffer-size
                     :locked-cb locked-cb
                     :msg       {:type :read-binary-error
                                 :key  key}})))
  (-bassoc [this key meta-up input opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :base base
                     :operation  :write-binary
                     :detect-old-blobs detected-old-blobs
                     :default-serializer default-serializer
                     :compressor compressor
                     :encryptor  encryptor
                     :input      input
                     :storage-layout storage-layout-id
                     :up-fn-meta meta-up
                     :config     config
                     :sync?      sync?
                     :buffer-size buffer-size
                     :msg        {:type :write-binary-error
                                  :key  key}})))

  PKeyIterable
  (-keys [this opts]
    (let [{:keys [sync?]} opts]
      (list-keys this serializers read-handlers write-handlers
                 {:operation :read-meta
                  :base base
                  :default-serializer default-serializer
                  :detect-old-blobs detected-old-blobs
                  :storage-layout storage-layout-id
                  :compressor compressor
                  :encryptor encryptor
                  :config config
                  :sync? sync?
                  :buffer-size buffer-size
                  :msg {:type :read-all-keys-error}}))))

(defn new-default-store
  "Create general store in given path.
  Optional serializer, read-handerls, write-handlers, buffer-size and config (for fsync) can be changed.
  Defaults are
  {:base         path
   :serializer     fressian-serializer
   :read-handlers  empty
   :write-handlers empty
   :buffer-size    1 MB
   :config         config} "
  [base
   backing
   old-files
   migrate-in-io-operation
   migrate-in-list-keys
   {:keys [default-serializer serializers compressor encryptor
           read-handlers write-handlers
           buffer-size config opts]
    :or   {default-serializer :FressianSerializer
           compressor         null-compressor
           ;; lz4-compressor
           encryptor          null-encryptor
           read-handlers      (atom {})
           write-handlers     (atom {})
           buffer-size        (* 1024 1024)
           opts               {:sync? false}
           config             {:fsync? true
                               :in-place? false
                               :lock-blob? true}}}]
  ;; check config
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (if (and (:in-place? config) (not (:lock-blob? config)))
      (throw (ex-info "You need to activate file-locking for in-place mode."
                      {:type :store-configuration-error
                       :config config}))
      (let [_                  (<?- (-create-store backing opts))
            store              (map->DefaultStore {:detected-old-blobs old-files
                                                   :base               base
                                                   :backing            backing
                                                   :default-serializer default-serializer
                                                   :serializers        (merge key->serializer serializers)
                                                   :storage-layout-id  default-layout-id
                                                   :compressor         compressor
                                                   :encryptor          encryptor
                                                   :read-handlers      read-handlers
                                                   :write-handlers     write-handlers
                                                   :buffer-size        buffer-size
                                                   :locks              (atom {})
                                                   :config             (merge {:fsync? true
                                                                               :in-place? false
                                                                               :lock-blob? true}
                                                                              config)
                                                   :migrate-in-io-operation migrate-in-io-operation
                                                   :migrate-in-list-keys migrate-in-list-keys})]
        store)))))
