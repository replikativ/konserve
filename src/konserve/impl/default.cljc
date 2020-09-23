(ns konserve.impl.default
  "Default implementation of the high level protocol given a binary backing implementation as defined in the storage-layout namespace."
  (:require
   [konserve.serializers :refer [key->serializer]]
   [konserve.compressor :refer [null-compressor]]
   [konserve.encryptor :refer [null-encryptor]]
   [hasch.core :refer [uuid]]
   [clojure.string :refer [ends-with?]]
   [konserve.protocols :refer [PEDNKeyValueStore -exists?
                               PBinaryKeyValueStore
                               -serialize -deserialize
                               PKeyIterable]]
   [konserve.impl.storage-layout :refer [-atomic-move -create-store
                                         -copy -create-blob -delete-blob -blob-exists?
                                         -keys -sync-store
                                         -migratable -migrate -handle-foreign-key
                                         -close -get-lock -sync
                                         -read-header -read-meta -read-value -read-binary
                                         -write-header -write-meta -write-value -write-binary
                                         PBackingLock -release
                                         default-version
                                         parse-header create-header]]
   [konserve.utils :refer [async+sync *default-sync-translation*]]
   [superv.async :refer [go-try- <?-]]
   [clojure.core.async :refer [<! timeout]]
   [taoensso.timbre :refer [trace]])
  #?(:clj
     (:import
      [java.io ByteArrayOutputStream ByteArrayInputStream])))

(extend-protocol PBackingLock
  nil
  (-release [_this env]
    (if (:sync? env) nil (go-try- nil))))

(defn key->store-key [key]
  (str (uuid key) ".ksv"))

(defn store-key->uuid-key [^String store-key]
  (cond
    (.endsWith store-key ".ksv") (subs store-key 0 (- (.length store-key) 4))
    (.endsWith store-key ".ksv.new") (subs store-key 0 (- (.length store-key) 8))
    (.endsWith store-key ".ksv.backup") (subs store-key 0 (- (.length store-key) 11))
    :else (throw (ex-info (str "Invalid konserve store key: " store-key)
                          {:key store-key}))))

(defn update-blob
  "This function writes first the meta-size, then the meta-data and then the
  actual updated data into the underlying backing store."
  [backing store-key serializer write-handlers
   {:keys [key-vec compressor encryptor up-fn up-fn-meta
           config operation input sync? version] :as env} [old-meta old-value]]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [[key & rkey] key-vec
          store-key (or store-key (key->store-key key))
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
          new-store-key (if (:in-place? config)
                          store-key
                          (str store-key ".new"))
          backup-store-key (str store-key ".backup")
          _ (when (:in-place? config) ;; let's back things up before writing then
              (trace "backing up to blob: " backup-store-key " for key " key)
              (<?- (-copy backing store-key backup-store-key env)))
          meta-arr             (to-array meta)
          meta-size            (count meta-arr)
          header               (create-header version
                                              serializer compressor encryptor meta-size)
          new-blob             (<?- (-create-blob backing new-store-key env))]
      (try
        (<?- (-write-header new-blob header env))
        (<?- (-write-meta new-blob meta-arr env))
        (if (= operation :write-binary)
          (<?- (-write-binary new-blob meta-size input env))
          (let [value-arr            (to-array value)]
            (<?- (-write-value new-blob value-arr meta-size env))))

        (when (:sync-blob? config)
          (trace "syncing for " key)
          (<?- (-sync new-blob env))
          (<?- (-sync-store backing env)))
        (<?- (-close new-blob env))

        (when-not (:in-place? config)
          (trace "moving blob: " key)
          (<?- (-atomic-move backing new-store-key store-key env)))
        (if (= operation :write-edn) [old-value value] true)
        (finally
          (<?- (-close new-blob env))))))))

(defn read-header [ac serializers env]
  (let [{:keys [sync?]} env]
    (async+sync sync? *default-sync-translation*
                (go-try-
                 (let [arr (<?- (-read-header ac env))]
                   (parse-header arr serializers))))))

(defn read-blob
  "Read meta, edn or binary from blob."
  [blob read-handlers serializers {:keys [sync? operation locked-cb] :as env}]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [[_ serializer compressor encryptor meta-size] (<?- (read-header blob serializers env))
          fn-read (partial -deserialize
                           (compressor (encryptor serializer))
                           read-handlers)]
      (case operation
        :read-meta (let [bais-read (ByteArrayInputStream.
                                    (<?- (-read-meta blob meta-size env)))
                         value     (fn-read bais-read)
                         _         (.close bais-read)]
                     value)
        :read-edn (let [bais-read (ByteArrayInputStream.
                                   (<?- (-read-value blob meta-size env)))
                        value     (fn-read bais-read)
                        _         (.close bais-read)]
                    value)
        :write-binary          (let [bais-read (ByteArrayInputStream.
                                                (<?- (-read-meta blob meta-size env)))
                                     meta      (fn-read bais-read)
                                     _         (.close bais-read)]
                                 [meta nil])
        :write-edn             (let [bais-meta  (ByteArrayInputStream.
                                                 (<?- (-read-meta blob meta-size env)))
                                     meta       (fn-read bais-meta)
                                     _          (.close bais-meta)
                                     bais-value (ByteArrayInputStream.
                                                 (<?- (-read-value blob meta-size env)))
                                     value     (fn-read bais-value)
                                     _          (.close bais-value)]
                                 [meta value])
        :read-binary           (<?- (-read-binary blob meta-size locked-cb env)))))))

(defn delete-blob
  "Remove/Delete key-value pair of backing store by given key. If success it will return true."
  [backing env]
  (async+sync
   (:sync? env) *default-sync-translation*
   (go-try-
    (let [{:keys [key-vec base]} env
          key          (first key-vec)
          store-key    (key->store-key key)
          blob-exists? (<?- (-blob-exists? backing store-key env))]
      (if blob-exists?
        (try
          (<?- (-delete-blob backing store-key env))
          true
          (catch Exception e
            (throw (ex-info "Could not delete key."
                            {:key key
                             :base base
                             :exception e}))))
        false)))))

(def ^:const max-lock-attempts 100)

(defn get-lock [this store-key env]
  (async+sync
   (:sync? env)
   *default-sync-translation*
   (go-try-
    (loop [i 0]
      (let [[l e] (try
                    [(<?- (-get-lock this env)) nil]
                    (catch Exception e
                      (trace "Failed to acquire lock: " e)
                      [nil e]))]
        (if-not (nil? l)
          l
          (do
            (if (:sync? env)
              (Thread/sleep (rand-int 20))
              (<! (timeout (rand-int 20))))
            (if (> i max-lock-attempts)
              (throw (ex-info (str "Failed to acquire lock after " i " iterations.")
                              {:type :file-lock-acquisition-error
                               :error e
                               :store-key store-key}))
              (recur (inc i))))))))))

(defn io-operation
  "Read/Write blob. For better understanding use the flow-chart of konserve."
  [{:keys [backing]} serializers read-handlers write-handlers
   {:keys [key-vec operation default-serializer sync? overwrite? config] :as env}]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [key           (first  key-vec)
          store-key     (key->store-key key)
          env           (assoc env :store-key store-key)
          serializer    (get serializers default-serializer)
          store-key-exists? (<?- (-blob-exists? backing store-key env))
          migration-key (<?- (-migratable backing key store-key env))]
      (if (and (not store-key-exists?) migration-key)
        (<?- (-migrate backing migration-key key-vec serializer read-handlers write-handlers env))
        (if (or store-key-exists? (= :write-edn operation) (= :write-binary operation))
          (let [blob (<?- (-create-blob backing store-key env))
                lock   (when (:lock-blob? config)
                         (trace "Acquiring blob lock for: " key (str blob))
                         (<?- (get-lock blob (first key-vec) env)))]
            (try
              (let [old (if (and store-key-exists? (not overwrite?))
                          (<?- (read-blob blob read-handlers serializers env))
                          [nil nil])]
                (if (or (= :write-edn operation) (= :write-binary operation))
                  (<?- (update-blob backing store-key serializer write-handlers env old))
                  old))
              (finally
                (when (:lock-blob? config)
                  (trace "Releasing lock for " (first key-vec) (str blob))
                  (<?- (-release lock env)))
                (<?- (-close blob env)))))
          nil))))))

(defn list-keys
  "Return all keys in the store."
  [{:keys [backing]}
   serializers read-handlers write-handlers {:keys [sync? config] :as env}]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [serializer (get serializers (:default-serializer env))
          store-keys (<?- (-keys backing env))]
      (loop [keys  #{}
             [store-key & store-keys] store-keys]
        (if store-key
          (cond
            (or (ends-with? store-key ".new")
                (ends-with? store-key ".backup"))
            (recur keys store-keys)

            (ends-with? store-key ".ksv")
            (let [blob        (<?- (-create-blob backing store-key env))
                  env         (update-in env [:msg :keys] (fn [_] store-key))
                  lock   (when (and (:in-place? config) (:lock-blob? config))
                           (trace "Acquiring blob lock for: " store-key (str blob))
                           (<?- (-get-lock blob env)))
                  keys-new (try (conj keys (<?- (read-blob blob read-handlers serializers env)))
                                    ;; it can be that the blob has been deleted, ignore reading errors
                                (catch Exception _
                                  keys)
                                (finally
                                  (<?- (-release lock env))
                                  (<?- (-close blob env))))]
              (recur keys-new store-keys))

            :else ;; needs migration
            (let [additional-keys (<! (-handle-foreign-key backing store-key serializer read-handlers write-handlers env))]
              (recur (into keys additional-keys) store-keys)))
          keys))))))

(defrecord DefaultStore [version backing serializers default-serializer compressor encryptor
                         read-handlers write-handlers buffer-size locks config]
  PEDNKeyValueStore
  (-exists? [_ key env]
    (async+sync
     (:sync? env) *default-sync-translation*
     (go-try-
      (let [store-key (key->store-key key)]
        (or (<?- (-blob-exists? backing store-key env))
            (<?- (-migratable backing key store-key env))
            false)))))
  (-get-in [this key-vec not-found opts]
    (let [{:keys [sync?]} opts]
      (async+sync
       sync?
       *default-sync-translation*
       (go-try-
        (if (<?- (-exists? this (first key-vec) opts))
          (let [a (<?-
                   (io-operation this serializers read-handlers write-handlers
                                 {:key-vec key-vec
                                  :operation :read-edn
                                  :compressor compressor
                                  :encryptor encryptor
                                  :format    :data
                                  :version version
                                  :sync? sync?
                                  :buffer-size buffer-size
                                  :config config
                                  :default-serializer default-serializer
                                  :msg       {:type :read-edn-error
                                              :key  key}}))]
            (clojure.core/get-in a (rest key-vec)))
          not-found)))))
  (-get-meta [this key opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :operation :read-meta
                     :compressor compressor
                     :encryptor encryptor
                     :default-serializer default-serializer
                     :version version
                     :sync? sync?
                     :buffer-size buffer-size
                     :config config
                     :msg       {:type :read-meta-error
                                 :key  key}})))

  (-assoc-in [this key-vec meta-up val opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec key-vec
                     :operation  :write-edn
                     :compressor compressor
                     :encryptor encryptor
                     :version version
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
                     :operation  :write-edn
                     :compressor compressor
                     :encryptor encryptor
                     :version version
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
                  :operation  :write-edn
                  :compressor compressor
                  :encryptor encryptor
                  :version version
                  :default-serializer default-serializer
                  :config     config
                  :sync?      (:sync? opts)
                  :buffer-size buffer-size
                  :msg        {:type :deletion-error
                               :key  key}}))

  PBinaryKeyValueStore
  (-bget [this key locked-cb opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :operation :read-binary
                     :default-serializer default-serializer
                     :compressor compressor
                     :encryptor encryptor
                     :config    config
                     :version version
                     :sync? sync?
                     :buffer-size buffer-size
                     :locked-cb locked-cb
                     :msg       {:type :read-binary-error
                                 :key  key}})))
  (-bassoc [this key meta-up input opts]
    (let [{:keys [sync?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :operation  :write-binary
                     :default-serializer default-serializer
                     :compressor compressor
                     :encryptor  encryptor
                     :input      input
                     :version version
                     :up-fn-meta meta-up
                     :config     config
                     :sync?      sync?
                     :buffer-size buffer-size
                     :msg        {:type :write-binary-error
                                  :key  key}})))

  PKeyIterable
  (-keys [this opts]
    (let [{:keys [sync?]} opts]
      (list-keys this
                 serializers read-handlers write-handlers
                 {:operation :read-meta
                  :default-serializer default-serializer
                  :version version
                  :compressor compressor
                  :encryptor encryptor
                  :config config
                  :sync? sync?
                  :buffer-size buffer-size
                  :msg {:type :read-all-keys-error}}))))

(defn connect-default-store
  "Create general store in given base of backing store."
  [backing
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
           opts               {:sync? false}}}]
  ;; check config
  (let [complete-config (merge {:sync-blob? true
                                :in-place? false
                                :lock-blob? true}
                               config)]
    (async+sync
     (:sync? opts) *default-sync-translation*
     (go-try-
      (if (and (:in-place? complete-config) (not (:lock-blob? complete-config)))
        (throw (ex-info "You need to activate file-locking for in-place mode."
                        {:type :store-configuration-error
                         :config complete-config}))
        (let [_                  (<?- (-create-store backing opts))
              store              (map->DefaultStore {:backing             backing
                                                     :default-serializer  default-serializer
                                                     :serializers         (merge key->serializer serializers)
                                                     :version             default-version
                                                     :compressor          compressor
                                                     :encryptor           encryptor
                                                     :read-handlers       read-handlers
                                                     :write-handlers      write-handlers
                                                     :buffer-size         buffer-size
                                                     :locks               (atom {})
                                                     :config              complete-config})]
          store))))))
