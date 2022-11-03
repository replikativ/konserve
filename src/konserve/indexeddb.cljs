(ns konserve.indexeddb
  (:require [cljs.core.async :refer [go take! put! close!]]
            [konserve.compressor]
            [konserve.encryptor]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as storage-layout :refer [PBackingStore]]
            [konserve.serializers]
            [konserve.utils :refer [with-promise]]))

(defn- connect-to-idb [db-name]
  (let [req (js/window.indexedDB.open db-name 1)]
    (with-promise out
      (set! (.-onblocked req) (fn [event] (put! out [event])))
      (set! (.-onerror req) (fn [event] (put! out [event])))
      (set! (.-onsuccess req)
        (fn [event] (put! out [nil (-> event .-target .-result)])))
      (set! (.-onupgradeneeded req)
        (fn [ev]
          (if (== 1 (.-oldVersion ev))
            (throw (js/Error. "upgrade not supported at this time"))
            (let [db (-> ev .-target .-result)
                  store (.createObjectStore db "")]
              (set! (-> ev .-target .-transaction) #(put! out [nil db])))))))))

(defn- IDBRequest->ch [req]
  (with-promise out
    (set! (.-onerror req) (fn [event] (put! out [event])))
    (set! (.-onsuccess req) (fn [event] (put! out [nil event])))))

(defn delete-idb [db-name]
  (IDBRequest->ch (js/window.indexedDB.deleteDatabase db-name)))

(defn list-dbs []
  (with-promise out
    (let [p (js/window.indexedDB.databases)]
      (.then p #(put! out [nil %]))
      (.catch p #(put! out [%])))))

(defn db-exists? [db-name]
  (with-promise out
    (take! (list-dbs)
      (fn [[err ok :as res]]
        (if err
          (put! out false)
          (put! out (reduce (fn [acc o]
                          (if (= db-name (goog.object.get o "name"))
                            (reduced true)
                            false))
                        false
                        ok)))))))

(defn- blob->arraybuf [blob]
  (with-promise out
    (if (nil? blob)
      (put! out [nil])
      (let [p (.arrayBuffer blob)]
        (.then p #(put! out [nil %]))
        (.catch p #(put! out [%]))))))

(defn read-blob [db key]
  (with-promise out
    (take! (IDBRequest->ch (.get (.objectStore (.transaction db #js[""]) "") key))
      (fn [[err ok :as res]]
        (if err
          (put! out res)
          (let [blob (-> ok .-target .-result)]
            (put! out [nil blob])))))))

(defn write-blob [db key blob]
  (let [ostore (.objectStore (.transaction db #js[""] "readwrite") "")]
    (IDBRequest->ch (.put ostore blob key))))

;;==============================================================================

(defn flush-blob
  [^BackingBlob {:keys [db key buf header metadata value] :as bb}]
  (let [bin (if (some? buf)
              #js[buf]
              #js[header metadata value])
        blob (js/Blob. bin #js{:type "application/octet-stream"})]
    (with-promise out
      (take! (write-blob db key blob)
        (fn [[err]]
          (if err
            (put! out (ex-info "error writing blob to objectStore"
                               {:cause err
                                :caller (symbol ::flush-blob)}))
            (close! out)))))))

(defn- get-buf
  "this ensures that blobs are cached on BackingBlobs as ArrayBuffers and read
   from the db only once."
  [^BackingBlob {:keys [db key buf] :as bb}]
  (with-promise out
    (if (some? buf)
      (put! out [nil buf])
      (take! (read-blob db key)
        (fn [[err blob :as res]]
          (if err
            (put! out res)
            (take! (blob->arraybuf blob)
              (fn [[err ok :as res]]
                (if err
                  (put! out res)
                  (do
                    (set! (.-buf bb) ok)
                    (put! out res)))))))))))

(defn- read-header
  [^BackingBlob {:keys [db key buf] :as this}]
  (with-promise out
    (take! (get-buf this)
      (fn [[err ok :as res]]
        (if err
          (put! out (ex-info "error reading blob from objectStore"
                             {:cause err
                              :caller (symbol ::read-header)}))
          (let [view (js/Uint8Array. ok)
                header (.slice view 0 storage-layout/header-size)]
            (put! out header)))))))

(defn- read-binary
  [^BackingBlob {:keys [db key buf] :as this} meta-size locked-cb]
  (with-promise out
    (take! (read-blob db key)
      (fn [[err blob :as res]]
        (if err
          (put! out (ex-info "error reading blob from objectStore"
                             {:cause err
                              :caller (symbol ::read-binary)}))
          (do
            (locked-cb {:input-stream (.stream blob)
                        :size (.-size blob)
                        :offset (+ meta-size storage-layout/header-size)})
            (close! out)))))))

(defrecord ^{:doc "buf is cached data that has been read from the db,
                   & {header metadata value} are bin data to be written.
                   If a write begins, buf is discarded."}
  BackingBlob [db key buf header metadata value]
  storage-layout/PBackingBlob
  (-get-lock [this _env]
    (let [lock (reify
                 storage-layout/PBackingLock
                 (-release [this env] (go)))]
      (go lock))) ;no-op but the alternative is to overwrite defaults/update-blob
  (-sync [this _env] (.force this true))
  (-close [this _env] (go (.close this)))
  (-read-header [this _env] (read-header this)) ;=> ch<buf|err>
  (-read-meta [this meta-size _env]
    (let [view (js/Uint8Array. buf)
          bytes (.slice view
                        storage-layout/header-size
                        (+ storage-layout/header-size meta-size))]
      (go bytes)))
  (-read-value [this meta-size _env]
    (let [view (js/Uint8Array. buf)
          bytes (.slice view (+ storage-layout/header-size meta-size))]
      (go bytes)))
  (-read-binary [this meta-size locked-cb _env]
    (read-binary this meta-size locked-cb))
  (-write-header [this header _env]
    (go (set! (.-buf this) nil)
        (set! (.-header this) header)))
  (-write-meta [this meta-arr _env] (go (set! (.-metadata this) meta-arr)))
  (-write-value [this value-arr meta-size _env]
    (go (set! (.-value this) value-arr)))
  (-write-binary [this meta-size blob env]
    (go (set! (.-value this) blob)))
  Object
  (force [this metadata?] (flush-blob this))
  (close [this]
    (do
      (set! (.-db this) nil)
      (set! (.-key this) nil)
      (set! (.-buf this) nil)
      (set! (.-header this) nil)
      (set! (.-metadata this) nil)
      (set! (.-value this) nil))))

(defn open-backing-blob [db key] (BackingBlob. db key nil nil nil nil))

(defrecord IndexedDBackingStore [db-name db]
  Object
  ;; needed to unref conn before can cycle database construction
  (close [_] (go (when (some? db)(.close db))))
  storage-layout/PBackingStore
  (-create-blob [this store-key env]
    (assert (not (:sync? env)))
    (go (open-backing-blob db store-key)))
  (-delete-blob [this key env]
    (assert (not (:sync? env)))
    (with-promise out
      (let [tx (.transaction db #js[""] "readwrite")
            ostore (.objectStore tx "")]
        (take! (IDBRequest->ch (.delete ostore key))
          (fn [[err]]
            (if err
              (put! out (ex-info "error deleting blob"
                                 {:cause err
                                  :caller (symbol ::-delete-blob)}))
              (close! out)))))))
  (-migratable [this key store-key env] (go false))
  (-blob-exists? [this store-key env]
    (assert (not (:sync? env)))
    (with-promise out
      (let [tx (.transaction db #js[""])
            ostore (.objectStore tx "")]
        (take! (IDBRequest->ch (.getKey ostore store-key))
          (fn [[err ok]]
            (if err
              (put! out (ex-info "error getting key from objectStore"
                                 {:cause err
                                  :caller (symbol ::-blob-exists?)}))
              (let [res (-> ok .-target .-result boolean)]
                (put! out res))))))))
  (-keys [this env]
    (assert (not (:sync? env)))
    (with-promise out
      (let [tx (.transaction db #js[""])
            ostore (.objectStore tx "")]
        (take! (IDBRequest->ch (.getAllKeys ostore))
          (fn [[err ok]]
            (if err
              (put! out (ex-info "error listing keys in objectStore"
                                 {:cause err
                                  :caller (symbol ::-keys)}))
              (let [ks (-> ok .-target .-result)]
                (put! out ks))))))))
  (-copy [this from to env]
    (assert (not (:sync? env)))
    (with-promise out
      (take! (read-blob db from)
        (fn [[err blob]]
          (if err
            (put! out (ex-info "error reading blob from objectStore"
                               {:cause err
                                :caller (symbol ::-copy)}))
            (take! (write-blob db to blob)
             (fn [[err]]
               (if err
                 (put! out (ex-info "error writing blob to objectStore"
                                    {:cause err
                                     :caller (symbol ::-copy)}))
                 (close! out)))))))))
  (-create-store [this env]
    (assert (not (:sync? env)))
    (with-promise out
      (take! (connect-to-idb db-name)
        (fn [[err db]]
          (if err
            (put! out (ex-info "error connecting to database"
                               {:cause err
                                :caller (symbol ::-create-store)}))
            (do
              (set! (.-db this) db)
              (put! out this)))))))
  (-delete-store [this env]
    (assert (not (:sync? env)))
    (with-promise out
      (.close db)
      (take! (delete-idb db-name)
        (fn [[err]]
          (if err
            (put! out (ex-info "error deleting database"
                               {:cause err
                                :caller (symbol ::-delete-store)}))
            (close! out))))))
  (-store-exists? [this env]
    (assert (not (:sync? env)))
    (db-exists? db-name))
  (-sync-store [this env] (when-not (:sync? env) (go))))

(defn connect-idb-store
  "Connect to a IndexedDB backed KV store with the given db name.
   Optional serializer, read-handlers, write-handlers, buffer-size and config.

   This implementation stores all values as js/Blobs in an IndexedDB
   object store instance. The object store itself is nameless, and there
   is no use of versioning, indexing, or cursors.

   This data is stored as if indexedDB was a filesystem. Since we do not have
   file-descriptors to write with, strategy is to build a blob from components
   (js/Blobs have a nice api explicitly for this) and flush the blob to
   indexeddb storage on -sync-store calls (which doesnt concern consumers)

   + all store ops are asynchronous only

   + the database object must be 'unref'ed by calling db.close() before database
   instances can be deleted. You can do this easily by calling store.close()
   object method, which is unique to this impl. If you fail to maintain access to
   db and core.async gets into a weird state due to an unhandled error, you
   will be unable to delete the database until the vm is restarted

   + As of November 2022 firefox does not support IDBFactory.databases() so
   expect list-dbs, db-exists?, & PBackingStore/-store-exists? to all throw. You
   must work around this by keeping track of each db name you intend to delete
   https://developer.mozilla.org/en-US/docs/Web/API/IDBFactory/databases#browser_compatibility

   + PBackingBlob/-read-binary returns a webstream that is *not* queued to the
   value offset in the same way that the filestore implementations are. Consumers
   must discard the amount of bytes found in the :offset key of the locked-cb
   arg map. See: https://developer.mozilla.org/en-US/docs/Web/API/Blob/stream"
  [db-name & {:keys [config] :as params}]
  (let [store-config (merge {:default-serializer :FressianSerializer
                             :compressor         konserve.compressor/null-compressor
                             :encryptor          konserve.encryptor/null-encryptor
                             :read-handlers      (atom {})
                             :write-handlers     (atom {})
                             :config             (merge {:sync-blob? true
                                                         :in-place? true
                                                         :lock-blob? true}
                                                        config)}
                            (dissoc params :config))
        backing            (IndexedDBackingStore. db-name nil)]
    (defaults/connect-default-store backing store-config)))
