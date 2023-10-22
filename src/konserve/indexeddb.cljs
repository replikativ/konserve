(ns konserve.indexeddb
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [cljs.core.async :refer [take! put! close!]]
            [konserve.compressor]
            [konserve.encryptor]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as storage-layout]
            [konserve.serializers]
            [konserve.utils :refer-macros [with-promise]]))

(defn connect-to-idb [db-name]
  (let [req (js/window.indexedDB.open db-name 1)]
    (with-promise out
      (set! (.-onblocked req)
            #(put! out (ex-info "connecting to indexed-db blocked"
                                {:cause %
                                 :caller 'konserve.indexeddb/connect-to-idb})))
      (set! (.-onerror req)
            #(put! out (ex-info "error connecting to indexed-db"
                                {:cause %
                                 :caller 'konserve.indexeddb/connect-to-idb})))
      (set! (.-onsuccess req)
            #(put! out (-> % .-target .-result)))
      (set! (.-onupgradeneeded req)
            (fn [ev]
              (if (== 1 (.-oldVersion ev))
                (throw (js/Error. "upgrade not supported at this time"))
                (-> ev .-target .-result (.createObjectStore ""))))))))

(defn delete-idb [db-name]
  (let [req (js/window.indexedDB.deleteDatabase db-name)]
    (with-promise out
      (set! (.-onsuccess req) #(close! out))
      (set! (.-onerror req)
            #(put! out (ex-info (str "error deleting indexed-db with name '" db-name "'")
                                {:cause %
                                 :caller 'konserve.indexeddb/delete-idb}))))))

(defn list-dbs []
  (with-promise out
    (let [p (js/window.indexedDB.databases)]
      (.then p #(put! out %))
      (.catch p #(put! out (ex-info "error listing databases"
                                    {:cause %
                                     :caller 'konserve.indexeddb/list-dbs}))))))

(defn db-exists? [db-name]
  (with-promise out
    (take! (list-dbs)
           (fn [res]
             (if (instance? js/Error res)
               (put! out false)
               (put! out (reduce (fn [acc o]
                                   (if (= db-name (goog.object.get o "name"))
                                     (reduced true)
                                     false))
                                 false
                                 res)))))))

(defn read-blob [db key]
  (let [req (.get (.objectStore (.transaction db #js[""]) "") key)]
    (with-promise out
      (set! (.-onsuccess req)
            (fn [ev]
              (if-some [v (-> ev .-target .-result)]
                (put! out v)
                (close! out))))
      (set! (.-onerror req)
            #(put! out (ex-info (str "error reading blob at key '" key "'")
                                {:cause %
                                 :caller 'konserve.indexeddb/read-blob}))))))

(defn write-blob [db key blob]
  (let [req (.put (.objectStore (.transaction db #js[""] "readwrite") "") blob key)]
    (with-promise out
      (set! (.-onsuccess req) #(close! out))
      (set! (.-onerror req)
            #(put! out (ex-info (str "error writing blob at key '" key "'")
                                {:cause %
                                 :caller 'konserve.indexeddb/write-blob}))))))

;;==============================================================================

(defn flush-blob
  [^BackingBlob {:keys [db key buf header metadata value]}]
  (let [bin (if (some? buf)
              #js[buf]
              #js[header metadata value])
        blob (js/Blob. bin #js{:type "application/octet-stream"})]
    (with-promise out
      (take! (write-blob db key blob)
             (fn [err]
               (if err
                 (put! out (ex-info (str "error writing blob to objectStore at key '" key "'")
                                    {:cause err
                                     :caller 'konserve.indexeddb/flush-blob}))
                 (close! out)))))))

(defn- get-buf
  "this ensures that blobs are cached on BackingBlobs as ArrayBuffers and read
   from the db only once."
  [^BackingBlob {:keys [db key buf] :as bb}]
  (with-promise out
    (if (some? buf)
      (put! out buf)
      (take! (read-blob db key)
             (fn [res]
               (if (instance? js/Error res)
                 (put! out res)
                 (let [p (.arrayBuffer res)]
                   (.catch p #(put! out %))
                   (.then p #(do
                               (set! (.-buf bb) %)
                               (put! out %))))))))))

(defn- read-header
  [^BackingBlob {:keys [_db _key _buf] :as this}]
  (with-promise out
    (take! (get-buf this)
           (fn [res]
             (if (instance? js/Error res)
               (put! out (ex-info "error reading blob from objectStore"
                                  {:cause res
                                   :caller 'konserve.indexeddb/read-header}))
               (let [view (js/Uint8Array. res)
                     header (.slice view 0 storage-layout/header-size)]
                 (put! out header)))))))

(defn- read-binary
  [^BackingBlob {:keys [db key _buf]} meta-size locked-cb]
  (with-promise out
    (take! (read-blob db key)
           (fn [res]
             (if (instance? js/Error res)
               (put! out (ex-info "error reading blob from objectStore"
                                  {:cause res
                                   :caller 'konserve.indexeddb/read-binary}))
               (do
                 (locked-cb {:input-stream (.stream res)
                             :size (.-size res)
                             :offset (+ meta-size storage-layout/header-size)})
                 (close! out)))))))

(defrecord ^{:doc "buf is cached data that has been read from the db,
                   & {header metadata value} are bin data to be written.
                   If a write begins, buf is discarded."}
 BackingBlob [db key buf header metadata value]
  storage-layout/PBackingBlob
  (-get-lock [_this _env]
    (let [lock (reify
                 storage-layout/PBackingLock
                 (-release [_this _env] (go)))]
      (go lock))) ;no-op but the alternative is to overwrite defaults/update-blob
  (-sync [this _env] (.force this true))
  (-close [this _env] (go (.close this)))
  (-read-header [this _env] (read-header this)) ;=> ch<buf|err>
  (-read-meta [_this meta-size _env]
    (let [view (js/Uint8Array. buf)
          bytes (.slice view
                        storage-layout/header-size
                        (+ storage-layout/header-size meta-size))]
      (go bytes)))
  (-read-value [_this meta-size _env]
    (let [view (js/Uint8Array. buf)
          bytes (.slice view (+ storage-layout/header-size meta-size))]
      (go bytes)))
  (-read-binary [this meta-size locked-cb _env]
    (read-binary this meta-size locked-cb))
  (-write-header [this header _env]
    (go (set! (.-buf this) nil)
        (set! (.-header this) header)))
  (-write-meta [this meta-arr _env] (go (set! (.-metadata this) meta-arr)))
  (-write-value [this value-arr _meta-size _env]
    (go (set! (.-value this) value-arr)))
  (-write-binary [this _meta-size blob _env]
    (go (set! (.-value this) blob)))
  Object
  (force [this _metadata?] (flush-blob this))
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
  (close [_] (go (when (some? db) (.close db))))
  storage-layout/PBackingStore
  (-create-blob [_this store-key env]
    (assert (not (:sync? env)))
    (go (open-backing-blob db store-key)))
  (-delete-blob [_this key env]
    (assert (not (:sync? env)))
    (let [req (.delete (.objectStore (.transaction db #js[""] "readwrite") "") key)]
      (with-promise out
        (set! (.-onsuccess req) #(close! out))
        (set! (.-onerror req)
              #(put! out (ex-info (str "error deleting blob at key '" key "'")
                                  {:cause %
                                   :caller 'konserve.indexeddb/-delete-blob}))))))
  (-migratable [_this _key _store-key _env] (go false))
  (-blob-exists? [_this key env]
    (assert (not (:sync? env)))
    (let [req (.getKey (.objectStore (.transaction db #js[""]) "") key)]
      (with-promise out
        (set! (.-onsuccess req) #(put! out (-> % .-target .-result boolean)))
        (set! (.-onerror req)
              #(put! out (ex-info (str "error getting key in objectStore '" key "'")
                                  {:cause %
                                   :caller 'konserve.indexeddb/-blob-exists?}))))))
  (-keys [_this env]
    (assert (not (:sync? env)))
    (let [req (.getAllKeys (.objectStore (.transaction db #js[""]) ""))]
      (with-promise out
        (set! (.-onsuccess req) #(put! out (-> % .-target .-result)))
        (set! (.-onerror req)
              #(put! out (ex-info "error listing keys in objectStore"
                                  {:cause %
                                   :caller 'konserve.indexeddb/-keys}))))))
  (-copy [_this from to env]
    (assert (not (:sync? env)))
    (with-promise out
      (take! (read-blob db from)
             (fn [res]
               (if (instance? js/Error res)
                 (put! out (ex-info "error reading blob from objectStore"
                                    {:cause res
                                     :caller 'konserve.indexeddb/-copy}))
                 (take! (write-blob db to res)
                        (fn [?err]
                          (if ?err
                            (put! out (ex-info "error writing blob to objectStore"
                                               {:cause ?err
                                                :caller 'konserve.indexeddb/-copy}))
                            (close! out)))))))))
  (-create-store [this env]
    (assert (not (:sync? env)))
    (with-promise out
      (take! (connect-to-idb db-name)
             (fn [res]
               (if (instance? js/Error res)
                 (put! out (ex-info "error connecting to database"
                                    {:cause res
                                     :caller 'konserve.indexeddb/-create-store}))
                 (do
                   (set! (.-db this) res)
                   (put! out this)))))))
  (-delete-store [_this env]
    (assert (not (:sync? env)))
    (with-promise out
      (.close db)
      (take! (delete-idb db-name)
             (fn [?err]
               (if ?err
                 (put! out (ex-info "error deleting store"
                                    {:cause ?err
                                     :caller 'konserve.indexeddb/-delete-store}))
                 (close! out))))))
  (-store-exists? [_this env]
    (assert (not (:sync? env)))
    (db-exists? db-name))
  (-sync-store [_this env] (when-not (:sync? env) (go))))

(defn connect-idb-store
  "Connect to a IndexedDB backed KV store with the given db name.
   Optional serializer, read-handlers, write-handlers.

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
  [db-name & {:as params}]
  (let [store-config (merge {:default-serializer :FressianSerializer
                             :compressor         konserve.compressor/null-compressor
                             :encryptor          konserve.encryptor/null-encryptor
                             :read-handlers      (atom {})
                             :write-handlers     (atom {})
                             :config             {:sync-blob? true
                                                  :in-place? true
                                                  :lock-blob? true}}
                            (dissoc params :config))
        backing            (IndexedDBackingStore. db-name nil)]
    (defaults/connect-default-store backing store-config)))
