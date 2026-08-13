(ns ^:no-doc konserve.simulation.memory
  "INTERNAL. In-memory `PBackingStore` for simulation testing.

   Not public API: this namespace ships in the jar so that backends and
   downstream projects (datahike, stratum) can test against it, but it carries
   no compatibility guarantee and may change or move in any release. Depend on
   it with that understood.

   A pure in-memory backing store implementing konserve's low-level
   `PBackingStore` protocol. Useful for:
   - Fast CI tests without filesystem I/O
   - Testing DefaultStore logic in isolation
   - Wrapping with `konserve.simulation.backing` for fault injection

   Architecture:
     [k/assoc] → [DefaultStore] → [SimulatedBackingStore] → [MemoryBackingStore]
                                           ↓
                                   No filesystem I/O!"
  (:require [konserve.impl.storage-layout :as sl]
            [superv.async :refer [go-try-]]
            [clojure.core.async :refer [go]])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

;; =============================================================================
;; Lock Implementation (must be before MemoryBlob)
;; =============================================================================

;; Simple lock that tracks if released
(defrecord MemoryLock [lock-atom lock-id]
  sl/PBackingLock
  (-release [_this env]
    (compare-and-set! lock-atom lock-id nil)
    (if (:sync? env) nil (go-try- nil))))

(defn- acquire-lock [lock-atom sync?]
  (let [lock-id (Object.)]
    (if sync?
      (loop []
        (if (compare-and-set! lock-atom nil lock-id)
          (->MemoryLock lock-atom lock-id)
          (do
            (Thread/sleep (rand-int 10))
            (recur))))
      (go-try-
       (loop []
         (if (compare-and-set! lock-atom nil lock-id)
           (->MemoryLock lock-atom lock-id)
           (do
             (Thread/sleep (rand-int 10))
             (recur))))))))

;; =============================================================================
;; In-Memory Blob
;; =============================================================================

(defrecord MemoryBlob [store-key data-atom lock-atom]
  sl/PBackingBlob

  (-sync [_this env]
    ;; No-op for memory - data is already "synced"
    (if (:sync? env) nil (go-try- nil)))

  (-close [_this env]
    ;; No-op for memory
    (if (:sync? env) nil (go-try- nil)))

  (-get-lock [_this env]
    ;; Return a MemoryLock that implements PBackingLock
    (acquire-lock lock-atom (:sync? env)))

  (-read-header [_this env]
    (let [data @data-atom
          header (when data (byte-array (take 20 data)))]
      (if (:sync? env)
        header
        (go header))))

  (-read-meta [_this meta-size env]
    (let [data @data-atom
          header-size 20
          meta (when data
                 (byte-array (take meta-size (drop header-size data))))]
      (if (:sync? env)
        meta
        (go meta))))

  (-read-value [_this meta-size env]
    (let [data @data-atom
          header-size 20
          value (when data
                  (byte-array (drop (+ header-size meta-size) data)))]
      (if (:sync? env)
        value
        (go value))))

  (-read-binary [_this meta-size locked-cb env]
    (let [data @data-atom
          header-size 20
          binary-data (when data (byte-array (drop (+ header-size meta-size) data)))
          input-stream (when binary-data (ByteArrayInputStream. binary-data))]
      (if (:sync? env)
        (locked-cb {:input-stream input-stream
                    :size (if data (count data) 0)})
        (go-try-
         (locked-cb {:input-stream input-stream
                     :size (if data (count data) 0)})))))

  (-write-header [_this header-arr env]
    ;; Initialize or update header portion
    (let [current @data-atom
          header-size 20
          new-data (if current
                     ;; Update header in existing data
                     (let [rest-data (drop header-size current)]
                       (vec (concat (seq header-arr) rest-data)))
                     ;; New blob - just header for now
                     (vec (seq header-arr)))]
      (reset! data-atom new-data)
      (if (:sync? env) nil (go nil))))

  (-write-meta [_this meta-arr env]
    ;; Append meta to header
    (let [current @data-atom
          header-size 20
          header (take header-size current)
          ;; Keep any existing value data if present
          old-meta-size (- (count current) header-size)
          old-value (when (> (count current) header-size)
                      (drop (+ header-size old-meta-size) current))
          new-data (vec (concat header (seq meta-arr) old-value))]
      (reset! data-atom new-data)
      (if (:sync? env) nil (go nil))))

  (-write-value [_this value-arr meta-size env]
    ;; Replace value portion
    (let [current @data-atom
          header-size 20
          header (take header-size current)
          meta (take meta-size (drop header-size current))
          new-data (vec (concat header meta (seq value-arr)))]
      (reset! data-atom new-data)
      (if (:sync? env) nil (go nil))))

  (-write-binary [_this meta-size blob env]
    ;; Write binary blob
    (let [{:keys [buffer-size]} env
          current @data-atom
          header-size 20
          header (take header-size current)
          meta (take meta-size (drop header-size current))
          ;; Read blob into byte array
          bos (ByteArrayOutputStream.)
          _ (if (instance? java.io.InputStream blob)
              (let [buf (byte-array (or buffer-size 8192))]
                (loop []
                  (let [n (.read ^java.io.InputStream blob buf)]
                    (when (pos? n)
                      (.write bos buf 0 n)
                      (recur)))))
              ;; Assume it's already bytes
              (.write bos ^bytes blob))
          binary-bytes (.toByteArray bos)
          new-data (vec (concat header meta (seq binary-bytes)))]
      (reset! data-atom new-data)
      (if (:sync? env) nil (go nil)))))

;; =============================================================================
;; In-Memory Backing Store
;; =============================================================================

(defrecord MemoryBackingStore [blobs-atom store-created?-atom]
  sl/PBackingStore

  (-create-blob [_this store-key env]
    ;; Get or create blob for this key
    (let [existing (get @blobs-atom store-key)
          blob (or existing
                   (let [new-blob (->MemoryBlob store-key (atom nil) (atom nil))]
                     (swap! blobs-atom assoc store-key new-blob)
                     new-blob))]
      (if (:sync? env)
        blob
        (go blob))))

  (-delete-blob [_this store-key env]
    (swap! blobs-atom dissoc store-key)
    (if (:sync? env) nil (go-try- nil)))

  (-blob-exists? [_this store-key env]
    (let [exists? (and (contains? @blobs-atom store-key)
                       (some? @(:data-atom (get @blobs-atom store-key))))]
      (if (:sync? env)
        exists?
        (go exists?))))

  (-migratable [_this _key _store-key env]
    ;; No migration for memory store
    (if (:sync? env) nil (go nil)))

  (-migrate [_this _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    ;; No migration for memory store
    (if (:sync? env) nil (go nil)))

  (-copy [_this from to env]
    ;; Copy blob data
    (when-let [from-blob (get @blobs-atom from)]
      (let [data @(:data-atom from-blob)
            to-blob (->MemoryBlob to (atom (when data (vec data))) (atom nil))]
        (swap! blobs-atom assoc to to-blob)))
    (if (:sync? env) nil (go-try- nil)))

  (-atomic-move [_this from to env]
    ;; Atomic rename - just swap the keys
    (let [from-blob (get @blobs-atom from)]
      (when from-blob
        ;; Create new blob with same data but new key
        (let [new-blob (->MemoryBlob to (:data-atom from-blob) (atom nil))]
          (swap! blobs-atom (fn [m]
                              (-> m
                                  (dissoc from)
                                  (assoc to new-blob)))))))
    (if (:sync? env) nil (go-try- nil)))

  (-create-store [_this env]
    (reset! store-created?-atom true)
    (if (:sync? env) nil (go-try- nil)))

  (-delete-store [_this env]
    (reset! blobs-atom {})
    (reset! store-created?-atom false)
    (if (:sync? env) nil (go-try- nil)))

  (-store-exists? [_this env]
    (let [exists? @store-created?-atom]
      (if (:sync? env)
        exists?
        (go exists?))))

  (-sync-store [_this env]
    ;; No-op for memory
    (if (:sync? env) nil (go-try- nil)))

  (-keys [_this env]
    ;; Return all store keys that have data
    (let [keys-with-data (->> @blobs-atom
                              (filter (fn [[_k v]] (some? @(:data-atom v))))
                              (map first)
                              vec)]
      (if (:sync? env)
        keys-with-data
        (go keys-with-data))))

  (-handle-foreign-key [_this _migration-key _serializer _read-handlers _write-handlers env]
    ;; No foreign keys in memory store
    (if (:sync? env) [] (go []))))

;; =============================================================================
;; Factory Functions
;; =============================================================================

(defn create-memory-backing
  "Create a new in-memory backing store.

   Returns a MemoryBackingStore that implements PBackingStore.
   Can be wrapped with SimulatedBackingStore for fault injection,
   then connected to DefaultStore.

   Example:
     (def backing (create-memory-backing))
     (def store (<!! (connect-default-store backing {:opts {:sync? false}})))
     (k/assoc store :key {:value 1})"
  []
  (->MemoryBackingStore (atom {}) (atom false)))

