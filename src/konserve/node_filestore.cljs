(ns konserve.node-filestore
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [cljs.core.async :refer [take! <! put! take! close!]]
            [cljs.nodejs :as node]
            [cljs-node-io.core :as io]
            [cljs-node-io.fs :as iofs]
            [clojure.string :as string]
            [konserve.compressor]
            [konserve.encryptor]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as storage-layout]
            [konserve.serializers]
            [konserve.utils :refer-macros [with-promise]]
            [taoensso.timbre :refer [info]]))

(def stream (node/require "stream"))
(def path (node/require "path"))
(def fs (node/require "fs"))

;;==============================================================================
;; synchronous FileChannel

(defn- _lock
  "Tries to open a .lock file exclusively.
   If fails --> throws
   If succeeds --> returns a reified IPBackingLock impl which can delete the .lock file"
  [^FileChannel this]
  (let [lock (iofs/lock-file (.-path this))
        release #(when (some? (.-locked this))
                   (.release lock)
                   (set! (.-locked this) nil))
        lock (reify
               Object
               (release [_] (release))
               storage-layout/PBackingLock
               (-release [_ env]
                 (if (:sync? env)
                   (release)
                   (go (release)))))]
    (set! (.-locked this) lock)
    lock))

(deftype FileChannel [path fd open? locked]
  storage-layout/PBackingBlob
  (-sync [this _env] (.force this true))
  (-close [this _env] (.close this))
  (-get-lock [this _env] (.lock this))
  (-read-header [_this _env]
    (let [buf (js/Buffer.alloc storage-layout/header-size)
          bytes-read (iofs/read fd buf {:position 0})]
      (assert (== bytes-read storage-layout/header-size))
      buf))
  (-read-meta [_this meta-size _env]
    (let [buf (js/Buffer.alloc meta-size)
          bytes-read (iofs/read fd buf {:position storage-layout/header-size})]
      (assert (== bytes-read meta-size))
      buf))
  (-read-value [_this meta-size _env]
    (let [blob-size ^number (.-size (fs.fstatSync fd))
          value-size (- blob-size meta-size storage-layout/header-size)
          buf (js/Buffer.alloc value-size)
          pos (+ meta-size storage-layout/header-size)
          bytes-read (iofs/read fd buf {:position pos :length (alength buf)})]
      (assert (== bytes-read value-size))
      buf))
  (-read-binary [this meta-size locked-cb _env]
    (let [blob-size ^number (.-size (fs.fstatSync fd))
          pos (+ meta-size storage-layout/header-size)
          rstream (fs.createReadStream nil #js{:fd (.-fd this) :start pos})]
      (.on rstream "readable"
           #(locked-cb {:input-stream rstream :size blob-size}))))
  (-write-header [_this header _env]
    (let [buffer (js/Buffer.from header)
          bytes-written (iofs/write fd buffer {:position 0})]
      (assert (== bytes-written (alength header)))))
  (-write-meta [_this meta-arr _env]
    (let [buffer (js/Buffer.from meta-arr)
          pos storage-layout/header-size
          bytes-written (iofs/write fd buffer {:position pos})]
      (assert (== bytes-written (alength buffer)))))
  (-write-value [_this value-arr meta-size _env]
    (let [buffer (js/Buffer.from value-arr)
          pos (+ storage-layout/header-size meta-size)
          bytes-written (iofs/write fd buffer {:position pos})]
      (assert (== bytes-written (alength buffer)))))
  (-write-binary [_this meta-size blob _env]
    (let [buffer  (js/Buffer.from blob)
          pos     (+ storage-layout/header-size meta-size)
          bytes-written (iofs/write fd buffer {:position pos})]
      (assert (== bytes-written (alength buffer)))))
  Object
  (force [_this _] (fs.fsyncSync fd))
  (close [this]
    (when (.-open? this)
      (fs.closeSync (.-fd this))
      (when locked
        (.release locked))
      (set! (.-open? this) false)))
  (lock [this] (_lock this)))

(defn ^FileChannel open-file-channel
  ([path] (open-file-channel path {}))
  ([path {flags :flags}]
   (let [fd (if flags
              (fs.openSync path flags)
              (try
                (fs.openSync path "wx+")
                (catch js/Error _
                  (fs.openSync path "a+"))))]
     (FileChannel. path fd true nil))))

;;==============================================================================
;; AsyncFileChannel

(defn- _force-async
  [^AsyncFileChannel this _]
  (with-promise out
    (fs.fsync (.-fd this)
              (fn [?err]
                (if (some? ?err)
                  (put! out ?err)
                  (close! out))))))

(defn- _close-async
  "close fd, cleanup lockfile if any"
  [^AsyncFileChannel this]
  (with-promise out
    (if-not (.-open? this)
      (close! out)
      (fs.close (.-fd this)
                (fn [?err]
                  (if (some? ?err)
                    (put! out ?err)
                    (do
                      (set! (.-open? this) false)
                      (if (.-locked this)
                        (take! (.release (.-locked this))
                               (fn [?err]
                                 (if ?err
                                   (put! out ?err)
                                   (close! out))))
                        (close! out)))))))))

(defn- _lock-async
  [^AsyncFileChannel fc]
  (with-promise out
    (take! (iofs/alock-file (.-path fc))
           (fn [[?err lock]]
             (if (some? ?err)
               (put! out ?err)
               (let [release #(go (or (first (<! (.release lock)))
                                      (set! (.-locked fc) nil)))
                     lock  (reify
                             storage-layout/PBackingLock
                             (-release [this env] (release))
                             Object
                             (release [this] (release)))]
                 (set! (.-locked fc) lock)
                 (put! out lock)))))))

(defn- afread
  "Read data from the fd into a chan. yields err|buf"
  [fd buf pos]
  (with-promise out
    (fs.read fd buf 0 (alength buf) pos
             (fn [?err bytes-read]
               (if (some? ?err)
                 (put! out ?err)
                 (do
                   (assert (== bytes-read (alength buf)))
                   (put! out buf)))))))

(defn- afwrite
  "Write buffer to the fd. yields ?err"
  [fd buf pos]
  (let [buf (js/Buffer.from buf)
        offset 0
        len (alength buf)]
    (with-promise out
      (fs.write fd buf offset len pos
                (fn [?err ?byte-count _]
                  (if ?err
                    (put! out ?err)
                    (close! out)))))))

(defn- afsize
  "The size of the file in bytes. yields ;=> [?err ?size]"
  [fd]
  (with-promise out
    (fs.fstat fd
              (fn [?err ?stat]
                (if ?err
                  (put! out [?err])
                  (put! out [nil (.-size ?stat)]))))))

(defn- afread-binary ;=> ch<readable|err>
  [fd meta-size locked-cb _env]
  (go
    (let [[?err total-size] (<! (afsize fd))]
      (or ?err
          (try
            (let [opts #js{:fd fd
                           :encoding ""
                           :start (+ meta-size storage-layout/header-size)}
                  rstream (fs.createReadStream nil opts)]
              (.on rstream "readable"
                   #(locked-cb {:input-stream rstream :size total-size})))
            (catch js/Error e e))))))

(defn- afwrite-binary ;  => ch<?err>
  [fd meta-size blob _env]
  (with-promise out
    (try
      (let [pos     (+ storage-layout/header-size meta-size)
            rstream (io/readable blob)
            wstream (fs.createWriteStream nil #js{:fd fd
                                                  :start pos
                                                  :encoding ""
                                                  :autoClose false})]
        (stream.pipeline rstream wstream
                         (fn [?err]
                           (if (some? ?err)
                             (put! out ?err)
                             (close! out)))))
      (catch js/Error e
        (put! out e)))))

(deftype AsyncFileChannel [path fd open? locked]
  storage-layout/PBackingBlob
  (-sync [this _env] (.force this true))
  (-close [this _env] (.close this))
  (-get-lock [this _env] (.lock this))
  (-read-header [_this _env] ;=> ch<buf|err>
    (let [buf (js/Buffer.alloc storage-layout/header-size)]
      (afread fd buf 0)))
  (-read-meta [_this meta-size _env] ;=> ch<buf|err>
    (let [buf (js/Buffer.alloc meta-size)]
      (afread fd buf storage-layout/header-size)))
  (-read-value [_this meta-size _env] ;=> ch<buf|err>
    (go
      (let [[?err blob-size] (<! (afsize fd))]
        (or ?err
            (let [size (- blob-size meta-size storage-layout/header-size)
                  pos (+ meta-size storage-layout/header-size)
                  buf (js/Buffer.alloc size)]
              (<! (afread fd buf pos)))))))
  (-read-binary [_this meta-size locked-cb _env]
    (afread-binary fd meta-size locked-cb _env))
  (-write-header [_this header _env]
    (afwrite fd header 0))
  (-write-meta [_this meta-arr _env]
    (let [buffer (js/Buffer.from meta-arr)]
      (afwrite fd buffer storage-layout/header-size)))
  (-write-value [_this value-arr meta-size _env] ;=> ch<?err>
    (let [buffer (js/Buffer.from value-arr)
          pos (+ storage-layout/header-size meta-size)]
      (afwrite fd buffer pos)))
  (-write-binary [_this meta-size blob env]
    (afwrite-binary fd meta-size blob env)) ;=> ch<?err>
  Object
  (force [this metadata?] (_force-async this metadata?))
  (close [this] (_close-async this))
  (lock [this] (_lock-async this)))

(defn open-async-file-channel ;=> ch<err|fc>
  ([path] (open-async-file-channel path {}))
  ([path {flags :flags}]
   (go
     (if flags
       (let [[?err fd] (<! (iofs/aopen path flags))]
         (or ?err (AsyncFileChannel. path fd true nil)))
       (let [[?err fd] (<! (iofs/aopen path "wx+"))]
        ;; if file exists expect this to fail, so we retry with append else
        ;; we clobber contents. On node this is necessary because an existence
        ;; check can create race condition between io ticks, better to fail
         (if (nil? ?err)
           (AsyncFileChannel. path fd true nil)
           (let [[?err fd] (<! (iofs/aopen path "a+"))]
             (or ?err (AsyncFileChannel. path fd true nil)))))))))

;;==============================================================================
;;  synchronous backing impl

(defn list-files ;=> Vector<String>
  "Lists all files on the first level of a directory."
  ([directory]
   (list-files directory (fn [_] false)))
  ([directory ephemeral?]
   (into []
         (remove ephemeral?)
         (iofs/readdir directory))))

(defn count-konserve-keys
  "Counts konserve files in the directory."
  [dir]
  (reduce (fn [c p] (if (string/ends-with? p ".ksv") (inc c) c))
          0
          (list-files dir)))

(defn- copy
  "Copy a blob from one key to another."
  [base from to _env]
  (fs.copyFileSync (path.join base from) (path.join base to)))

(defn- atomic-move
  "Atomically move (rename) a blob."
  [^string base from to _env]
  ;; https://stackoverflow.com/questions/66780210/is-fs-renamesync-an-atomic-operation-that-is-resistant-to-corruption
  (let [current-path (path.join base from)
        next-path (path.join base to)]
    (iofs/rename current-path next-path)))

(defn check-and-create-backing-store
  "Helper Function to Check if Base is not writable"
  [^string base]
  (let [f         (io/file base)
        test-file (io/file (path.join base (str (random-uuid))))]
    (when-not (.exists f)
      (.mkdir f))
    (when-not (.createNewFile test-file)
      (throw (ex-info "Cannot write to base." {:type   :not-writable
                                               :base base})))
    (.delete test-file)))

(defn store-exists?
  "Check if underlying store already exists."
  [^string base]
  (let [f (io/file base)]
    (if (.exists f)
      (do (info "Store directory at " (str base) " exists with " (count-konserve-keys base) " konserve keys.")
          true)
      (do (info "Store directory at " (str base) " does not exist.")
          false))))

(defn- sync-base
  "Helper Function to synchronize the base of the filestore"
  [^string base]
  ;; https://www.reddit.com/r/node/comments/4r8k11/how_to_call_fsync_on_a_directory/
  (let [fc (open-file-channel base {:flags "r"})]
    (.force fc true)
    (.close fc)))

(defn delete-store
  "Permanently deletes the base of the store with all files."
  [^string base]
  (let [f             (io/file base)
        parent-base (.getParent f)]
    (doseq [c (.list (io/file base))]
      (.delete (io/file (path.join base c))))
    (.delete f)
    (try
      (sync-base parent-base)
      (catch js/Error e
        e))))

;;==============================================================================
;;  async backing impl

(defn list-files-async
  ([directory]
   (list-files-async directory (fn [_] false)))
  ([directory ephemeral?]
   (with-promise out
     (take! (iofs/areaddir directory)
            (fn [[?err ?files :as res]]
              (put! out (or ?err (into [] (remove ephemeral?) ?files))))))))

(defn- copy-async
  "Copy a blob from one key to another."
  [^string base from to _env]
  (with-promise out
    (fs.copyFile (path.join base from) (path.join base to)
                 (fn [?err]
                   (if (some? ?err)
                     (put! out ?err)
                     (close! out))))))

(defn- atomic-move-async
  "Atomically move (rename) a blob."
  [^string base from to _env]
  ;; https://stackoverflow.com/questions/66780210/is-fs-renamesync-an-atomic-operation-that-is-resistant-to-corruption
  (with-promise out
    (take! (iofs/arename (path.join base from) (path.join base to))
           (fn [[?err]]
             (if (some? ?err)
               (put! out ?err)
               (close! out))))))

(defn check-and-create-backing-store-async
  "Helper Function to Check if Base is not writable"
  [^string base]
  (let [test-path (path.join base (str (random-uuid)))]
    (go
      (or (and (not (<! (iofs/aexists? base)))
               (first (<! (iofs/amkdir base))))
          (first (<! (iofs/awrite-file test-path "" {:flags "w"})))
          (first (<! (iofs/aunlink test-path)))))))

(defn- sync-base-async
  "Helper Function to synchronize the base of the filestore"
  [^string base]
  ;; https://www.reddit.com/r/node/comments/4r8k11/how_to_call_fsync_on_a_directory/
  (go
    (let [fc (<! (open-async-file-channel base {:flags "r"}))]
      (or (<! (.force fc true))
          (<! (.close fc))))))

(defn- store-exists?-async
  "Checks if path exists."
  [base]
  (with-promise out
    (take! (iofs/aexists? base)
           (fn [yes?]
             (if yes?
               (info "Store directory at " (str base) " exists with " (count-konserve-keys base) " konserve keys.")
               (info "Store directory at " (str base) " does not exist."))
             (put! out yes?)))))

(defn delete-store-async
  "Permanently deletes the base of the store with all files."
  [^string base]
  (with-promise out
    (take! (iofs/arm-r base)
           (fn [?err]
             (if (some? ?err)
               (put! out ?err)
               (take! (sync-base-async base) #(put! out %))))))) ;; TODO pipe bad

;;==============================================================================

(defn- _delete-blob
  "Delete a blob object under path."
  [^NodejsBackingFilestore this store-key env]
  (let [store-path (path.join (.-base this) store-key)]
    (if (:sync? env)
      (iofs/unlink store-path)
      (with-promise out
        (take! (iofs/aunlink store-path)
               (fn [[?err]]
                 (if (some? ?err)
                   (put! out ?err)
                   (close! out))))))))

(defn- _migratable
  "Check if blob exists elsewhere and return a migration key or nil."
  [^NodejsBackingFilestore this _key store-key _]
  (when-let [detected-old-blobs (.-detected-old-blobs this)]
    (let [uuid-key (defaults/store-key->uuid-key store-key)]
      (or (@detected-old-blobs (path.join (.-base this) "meta" uuid-key))
          (@detected-old-blobs (path.join (.-base this) uuid-key))
          (@detected-old-blobs (path.join (.-base this) (str "B_" uuid-key)))))))

(defrecord NodejsBackingFilestore
           [base detected-old-blobs ephemeral?]
  storage-layout/PBackingStore
  (-create-blob [_this store-key env]
    (let [store-path (path.join base store-key)]
      (if (:sync? env)
        (open-file-channel store-path)
        (open-async-file-channel store-path))))
  (-delete-blob [this store-key env] (_delete-blob this store-key env))
  (-blob-exists? [this store-key env]
    (let [store-path (path.join (.-base this) store-key)]
      (if (:sync? env)
        (iofs/exists? store-path)
        (iofs/aexists? store-path))))
  (-migratable [this key store-key env]
    (cond-> (_migratable this key store-key env) (not (:sync? env)) go))
  (-migrate [_this _migration-key _key-vec _serializer _read-handlers _write-handlers _env] (throw (js/Error "TODO")))
  (-handle-foreign-key [_this _migration-key _serializer _read-handlers _write-handlers _env] (throw (js/Error "TODO")))
  (-keys [this env]
    (if (:sync? env)
      (list-files base (.-ephemeral? this))
      (list-files-async base (.-ephemeral? this))))
  (-copy [this from to env]
    (if (:sync? env)
      (copy this from to env)
      (copy-async this from to env)))
  (-atomic-move [_this from to env]
    (if (:sync? env)
      (atomic-move base from to env)
      (atomic-move-async base from to env)))
  (-create-store [_this env]
    (if (:sync? env)
      (check-and-create-backing-store base)
      (check-and-create-backing-store-async base)))
  (-delete-store [_this env]
    (if (:sync? env)
      (delete-store (:base env))
      (delete-store-async (:base env))))
  (-store-exists? [_this env]
    (if (:sync? env)
      (store-exists? base)
      (store-exists?-async base)))
  (-sync-store [_this env]
    (if (:sync? env)
      (sync-base base)
      (sync-base-async base))))

(defn detect-old-file-schema [& _args] (throw (js/Error "TODO detect-old-file-schema")))
;; get-file-channel
;; migration

(defn connect-fs-store
  "Create Filestore in given path.
  Optional serializer, read-handlers, write-handlers, buffer-size and config (for fsync) can be changed.
  Defaults are
  {:base           path
   :serializer     fressian-serializer
   :read-handlers  empty
   :write-handlers empty
   :buffer-size    1 MB
   :config         config} "
  [path & {:keys [detect-old-file-schema? ephemeral? config]
           :or {detect-old-file-schema? false
                ephemeral? (fn [pathstr]
                             (some #(re-matches % pathstr)
                                   [#"\.nfs.*"]))}
           :as params}]
  ;; check config
  (let [store-config (merge {:default-serializer :FressianSerializer
                             :compressor         konserve.compressor/null-compressor
                             :encryptor          konserve.encryptor/null-encryptor
                             :read-handlers      (atom {})
                             :write-handlers     (atom {})
                             :buffer-size        (* 1024 1024)
                             :opts               {:sync? false}
                             :config             (merge {:sync-blob? true
                                                         :in-place? false
                                                         :lock-blob? true}
                                                        config)}
                            (dissoc params :config))
        detect-old-blob (when detect-old-file-schema?
                          (atom (detect-old-file-schema path)))
        _                  (when detect-old-file-schema?
                             (when-not (empty? @detect-old-blob)
                               (info (count @detect-old-blob) "files in old storage schema detected. Migration for each key will happen transparently the first time a key is accessed. Invoke konserve.core/keys to do so at once. Once all keys are migrated you can deactivate this initial check by setting detect-old-file-schema to false.")))
        backing            (NodejsBackingFilestore. path detect-old-blob ephemeral?)]
    (defaults/connect-default-store backing store-config)))
