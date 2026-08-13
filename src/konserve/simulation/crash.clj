(ns ^:no-doc konserve.simulation.crash
  "INTERNAL. Crash simulation for `PBackingStore` with sync-point tracking.

   Not public API: this namespace ships in the jar so that backends and
   downstream projects (datahike, stratum) can test against it, but it carries
   no compatibility guarantee and may change or move in any release. Depend on
   it with that understood.

   Crash points are selected explicitly rather than at random — there is no
   RNG here, so a scenario is deterministic by construction.

   Based on SQLite/RocksDB/CrashMonkey research (100% of crash bugs occur
   after fsync-like calls), this provides proper crash simulation by:

   1. Tracking pending writes (not yet synced)
   2. Maintaining synced state snapshots
   3. On sync, moving pending to synced
   4. On crash, discarding pending and restoring synced state

   Crash Points (from DefaultStore write path):
   1. After -write-header, before -write-meta → partial header
   2. After -write-meta, before -write-value → header+meta, no value
   3. After -write-value, before -sync       → all written, not synced
   4. After -sync, before -atomic-move       → .new file complete, not moved
   5. After -atomic-move, before -sync-store → moved, not store-synced
   6. After -sync-store, before -delete-blob → complete, backup not deleted

   Invariants to verify after crash:
   - No partial data visible (all-or-nothing)
   - No data loss for synced operations
   - Orphan files (.new, .backup) don't break operations
   - Store reopens successfully after crash"
  (:require [konserve.impl.storage-layout :as sl]
            [superv.async :refer [go-try-]]
            [clojure.core.async :refer [go]])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

;; =============================================================================
;; Crash Point Definitions
;; =============================================================================

(def crash-points
  "All possible crash points in the write path."
  #{:after-write-header    ; 1. After -write-header, before -write-meta
    :after-write-meta      ; 2. After -write-meta, before -write-value
    :after-write-value     ; 3. After -write-value, before -sync
    :after-sync            ; 4. After -sync, before -atomic-move
    :after-atomic-move     ; 5. After -atomic-move, before -sync-store
    :after-sync-store})    ; 6. After -sync-store, before -delete-blob

(defn crash-exception
  "Create a crash exception for a specific point."
  [crash-point store-key]
  (ex-info (str "Simulated crash at " (name crash-point))
           {:type :simulated-crash
            :crash-point crash-point
            :store-key store-key}))

;; =============================================================================
;; Crash State Management
;; =============================================================================

(defrecord CrashState
           [;; Synced data - survives crashes
            synced-blobs       ; {store-key -> blob-data-vec}

   ;; Pending data - lost on crash
            pending-blobs      ; {store-key -> blob-data-vec}

   ;; Pending .new files (after sync, before atomic-move)
            pending-new-files  ; {store-key -> blob-data-vec}

   ;; Pending moves (after atomic-move, before sync-store)
   ;; [{:from store-key :to store-key :data new-blob-data :old-data old-blob-data}]
            pending-moves

   ;; Pending backups to delete (after sync-store, before delete-blob)
            pending-backup-deletes ; #{store-key}

   ;; Store metadata
            store-exists?

   ;; Resource limits (nil = unlimited)
            max-total-bytes    ; Maximum total bytes across all blobs
            max-keys])         ; Maximum number of keys

(defn create-crash-state
  "Create initial crash state."
  ([] (create-crash-state nil nil))
  ([max-total-bytes max-keys]
   (->CrashState {} {} {} [] #{} false max-total-bytes max-keys)))

;; =============================================================================
;; Resource Limit Helpers
;; =============================================================================

(defn total-bytes
  "Calculate total bytes used in state."
  [state]
  (reduce + 0 (map count (vals (:synced-blobs state)))))

(defn total-keys
  "Count total keys in state."
  [state]
  (count (:synced-blobs state)))

(defn check-resource-limits!
  "Check if adding data would exceed resource limits. Throws if exceeded."
  [state data-size new-key?]
  (when-let [max-bytes (:max-total-bytes state)]
    (when (> (+ (total-bytes state) data-size) max-bytes)
      (throw (ex-info "Storage limit exceeded"
                      {:type :resource-exhausted
                       :resource :storage
                       :current (total-bytes state)
                       :requested data-size
                       :limit max-bytes}))))
  (when (and new-key? (:max-keys state))
    (when (>= (total-keys state) (:max-keys state))
      (throw (ex-info "Key limit exceeded"
                      {:type :resource-exhausted
                       :resource :keys
                       :current (total-keys state)
                       :limit (:max-keys state)})))))

;; =============================================================================
;; Lock Implementation
;; =============================================================================

(defrecord CrashAwareLock [lock-atom lock-id]
  sl/PBackingLock
  (-release [_this env]
    (compare-and-set! lock-atom lock-id nil)
    (if (:sync? env) nil (go-try- nil))))

(defn- acquire-lock [lock-atom sync?]
  (let [lock-id (Object.)]
    (if sync?
      (loop []
        (if (compare-and-set! lock-atom nil lock-id)
          (->CrashAwareLock lock-atom lock-id)
          (do
            (Thread/sleep (rand-int 10))
            (recur))))
      (go-try-
       (loop []
         (if (compare-and-set! lock-atom nil lock-id)
           (->CrashAwareLock lock-atom lock-id)
           (do
             (Thread/sleep (rand-int 10))
             (recur))))))))

;; =============================================================================
;; CrashAwareBlob - Tracks pending writes per blob
;; =============================================================================

(defrecord CrashAwareBlob [store-key
                           state-atom         ; Atom containing CrashState
                           lock-atom          ; Per-blob lock
                           crash-point-atom   ; Current crash injection point (or nil)
                           history-atom]      ; Operation history for debugging
  sl/PBackingBlob

  (-sync [_this env]
    ;; Sync commits pending writes to pending-new-files (ready for atomic-move)
    (swap! state-atom
           (fn [state]
             (if-let [pending-data (get (:pending-blobs state) store-key)]
               ;; Move from pending to pending-new-files
               (-> state
                   (update :pending-blobs dissoc store-key)
                   (update :pending-new-files assoc store-key pending-data))
               state)))

    ;; Check for crash after sync
    (when (= @crash-point-atom :after-sync)
      (throw (crash-exception :after-sync store-key)))

    (if (:sync? env) nil (go-try- nil)))

  (-close [_this env]
    (if (:sync? env) nil (go-try- nil)))

  (-get-lock [_this env]
    (acquire-lock lock-atom (:sync? env)))

  (-read-header [_this env]
    ;; Read from most current data: pending > pending-new > synced
    (let [state @state-atom
          data (or (get (:pending-blobs state) store-key)
                   (get (:pending-new-files state) store-key)
                   (get (:synced-blobs state) store-key))
          header (when (and data (>= (count data) 20))
                   (byte-array (take 20 data)))]
      (if (:sync? env)
        header
        (go header))))

  (-read-meta [_this meta-size env]
    (let [state @state-atom
          data (or (get (:pending-blobs state) store-key)
                   (get (:pending-new-files state) store-key)
                   (get (:synced-blobs state) store-key))
          header-size 20
          meta (when (and data (>= (count data) (+ header-size meta-size)))
                 (byte-array (take meta-size (drop header-size data))))]
      (if (:sync? env)
        meta
        (go meta))))

  (-read-value [_this meta-size env]
    (let [state @state-atom
          data (or (get (:pending-blobs state) store-key)
                   (get (:pending-new-files state) store-key)
                   (get (:synced-blobs state) store-key))
          header-size 20
          value (when data
                  (byte-array (drop (+ header-size meta-size) data)))]
      (if (:sync? env)
        value
        (go value))))

  (-read-binary [_this meta-size locked-cb env]
    (let [state @state-atom
          data (or (get (:pending-blobs state) store-key)
                   (get (:pending-new-files state) store-key)
                   (get (:synced-blobs state) store-key))
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
    ;; Initialize pending blob with header
    (swap! state-atom
           (fn [state]
             (let [current (get (:pending-blobs state) store-key)
                   header-size 20
                   new-data (if current
                              ;; Update header in existing pending data
                              (vec (concat (seq header-arr) (drop header-size current)))
                              ;; New blob - just header
                              (vec (seq header-arr)))]
               (update state :pending-blobs assoc store-key new-data))))

    ;; Check for crash after write-header
    (when (= @crash-point-atom :after-write-header)
      (throw (crash-exception :after-write-header store-key)))

    (if (:sync? env) nil (go nil)))

  (-write-meta [_this meta-arr env]
    ;; Append meta to pending blob
    (swap! state-atom
           (fn [state]
             (let [current (get (:pending-blobs state) store-key [])
                   header-size 20
                   header (take header-size current)
                   ;; Replace meta wholesale: the write path is
                   ;; header → meta → value, so no value is present yet.
                   new-data (vec (concat header (seq meta-arr)))]
               (update state :pending-blobs assoc store-key new-data))))

    ;; Check for crash after write-meta
    (when (= @crash-point-atom :after-write-meta)
      (throw (crash-exception :after-write-meta store-key)))

    (if (:sync? env) nil (go nil)))

  (-write-value [_this value-arr meta-size env]
    ;; Append value to pending blob
    (swap! state-atom
           (fn [state]
             (let [current (get (:pending-blobs state) store-key [])
                   header-size 20
                   header (take header-size current)
                   meta (take meta-size (drop header-size current))
                   new-data (vec (concat header meta (seq value-arr)))]
               (update state :pending-blobs assoc store-key new-data))))

    ;; Check for crash after write-value
    (when (= @crash-point-atom :after-write-value)
      (throw (crash-exception :after-write-value store-key)))

    (if (:sync? env) nil (go nil)))

  (-write-binary [_this meta-size blob env]
    (let [state @state-atom
          current (get (:pending-blobs state) store-key [])
          header-size 20
          header (take header-size current)
          meta (take meta-size (drop header-size current))
          ;; Read blob into byte array
          bos (ByteArrayOutputStream.)
          _ (if (instance? java.io.InputStream blob)
              (let [buf (byte-array (or (:buffer-size env) 8192))]
                (loop []
                  (let [n (.read ^java.io.InputStream blob buf)]
                    (when (pos? n)
                      (.write bos buf 0 n)
                      (recur)))))
              (.write bos ^bytes blob))
          binary-bytes (.toByteArray bos)
          new-data (vec (concat header meta (seq binary-bytes)))]

      (swap! state-atom update :pending-blobs assoc store-key new-data)

      ;; Check for crash after write-value (binary uses same point)
      (when (= @crash-point-atom :after-write-value)
        (throw (crash-exception :after-write-value store-key)))

      (if (:sync? env) nil (go nil)))))

;; =============================================================================
;; CrashAwareBackingStore
;; =============================================================================

(defrecord CrashAwareBackingStore [state-atom          ; CrashState
                                   crash-point-atom   ; Current crash injection point
                                   blob-locks-atom    ; {store-key -> lock-atom}
                                   history-atom]      ; Operation history
  sl/PBackingStore

  (-create-blob [_this store-key env]
    ;; Get or create lock for this blob
    (let [lock-atom (get (swap! blob-locks-atom
                                (fn [locks]
                                  (if (contains? locks store-key)
                                    locks
                                    (assoc locks store-key (atom nil)))))
                         store-key)
          blob (->CrashAwareBlob store-key state-atom lock-atom
                                 crash-point-atom history-atom)]
      (if (:sync? env)
        blob
        (go blob))))

  (-delete-blob [_this store-key env]
    (swap! state-atom
           (fn [state]
             (-> state
                 (update :synced-blobs dissoc store-key)
                 (update :pending-blobs dissoc store-key)
                 (update :pending-new-files dissoc store-key))))
    (if (:sync? env) nil (go-try- nil)))

  (-blob-exists? [_this store-key env]
    (let [state @state-atom
          exists? (or (contains? (:synced-blobs state) store-key)
                      (contains? (:pending-blobs state) store-key)
                      (contains? (:pending-new-files state) store-key))]
      (if (:sync? env)
        exists?
        (go exists?))))

  (-migratable [_this _key _store-key env]
    (if (:sync? env) nil (go nil)))

  (-migrate [_this _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go nil)))

  (-copy [_this from to env]
    (swap! state-atom
           (fn [state]
             (let [data (or (get (:pending-blobs state) from)
                            (get (:pending-new-files state) from)
                            (get (:synced-blobs state) from))]
               (if data
                 (update state :pending-blobs assoc to (vec data))
                 state))))
    (if (:sync? env) nil (go-try- nil)))

  (-atomic-move [_this from to env]
    ;; Atomic move: from pending-new-files to synced (simulating rename)
    ;; Track old value for crash recovery
    ;; Check resource limits before committing
    (let [state @state-atom
          data (or (get (:pending-new-files state) from)
                   (get (:pending-blobs state) from))
          old-data (get (:synced-blobs state) to)
          new-key? (nil? old-data)
          data-size (if data (count data) 0)
          ;; For updates, we only add net new bytes
          net-bytes (if old-data
                      (- data-size (count old-data))
                      data-size)]
      ;; Check limits before swap
      (when data
        (check-resource-limits! state net-bytes new-key?)))

    (swap! state-atom
           (fn [state]
             (let [old-data (get (:synced-blobs state) to)]  ; Get existing value at target
               (if-let [data (get (:pending-new-files state) from)]
                 ;; Move .new file to final location
                 (-> state
                     (update :pending-new-files dissoc from)
                     ;; Track as pending move with old value for crash recovery
                     (update :pending-moves conj {:from from :to to :data data :old-data old-data})
                     ;; But also make it visible immediately (as real filesystem would)
                     (update :synced-blobs assoc to data))
                 ;; No pending-new-file, check if it's in pending-blobs
                 (if-let [data (get (:pending-blobs state) from)]
                   (-> state
                       (update :pending-blobs dissoc from)
                       (update :pending-moves conj {:from from :to to :data data :old-data old-data})
                       (update :synced-blobs assoc to data))
                   state)))))

    ;; Check for crash after atomic-move
    (when (= @crash-point-atom :after-atomic-move)
      (throw (crash-exception :after-atomic-move to)))

    (if (:sync? env) nil (go-try- nil)))

  (-create-store [_this env]
    (swap! state-atom assoc :store-exists? true)
    (if (:sync? env) nil (go-try- nil)))

  (-delete-store [_this env]
    (reset! state-atom (create-crash-state))
    (if (:sync? env) nil (go-try- nil)))

  (-store-exists? [_this env]
    (let [exists? (:store-exists? @state-atom)]
      (if (:sync? env)
        exists?
        (go exists?))))

  (-sync-store [_this env]
    ;; Sync store commits pending moves
    (swap! state-atom
           (fn [state]
             (-> state
                 ;; Clear pending moves (they're now durable)
                 (assoc :pending-moves [])
                 ;; Clear pending backup deletes (they're now durable)
                 (assoc :pending-backup-deletes #{}))))

    ;; Check for crash after sync-store
    (when (= @crash-point-atom :after-sync-store)
      (throw (crash-exception :after-sync-store nil)))

    (if (:sync? env) nil (go-try- nil)))

  (-keys [_this env]
    (let [state @state-atom
          ;; Return only synced keys (consistent view)
          keys-set (set (keys (:synced-blobs state)))]
      (if (:sync? env)
        (vec keys-set)
        (go (vec keys-set)))))

  (-handle-foreign-key [_this _migration-key _serializer _read-handlers _write-handlers env]
    (if (:sync? env) [] (go []))))

;; =============================================================================
;; Crash Simulation API
;; =============================================================================

(defn create-crash-aware-store
  "Create a new crash-aware backing store.

   Options:
     :max-total-bytes - Maximum total storage in bytes (nil = unlimited)
     :max-keys        - Maximum number of keys (nil = unlimited)

   Returns a map with:
     :backing - CrashAwareBackingStore implementing PBackingStore
     :state-atom - Atom containing crash state for inspection
     :crash-point-atom - Atom to set crash injection point
     :history-atom - Atom with operation history

   Usage:
     (def store-info (create-crash-aware-store))
     (def backing (:backing store-info))
     ;; Set crash point
     (reset! (:crash-point-atom store-info) :after-write-value)
     ;; Operations will now crash at that point

   With resource limits:
     (def store-info (create-crash-aware-store {:max-total-bytes 10000
                                                 :max-keys 100}))"
  ([] (create-crash-aware-store {}))
  ([{:keys [max-total-bytes max-keys]}]
   (let [state-atom (atom (create-crash-state max-total-bytes max-keys))
         crash-point-atom (atom nil)
         blob-locks-atom (atom {})
         history-atom (atom [])]
     {:backing (->CrashAwareBackingStore state-atom crash-point-atom
                                         blob-locks-atom history-atom)
      :state-atom state-atom
      :crash-point-atom crash-point-atom
      :history-atom history-atom})))

(defn simulate-crash!
  "Simulate a crash by discarding all pending (unsynced) data.

   This restores the store to its last consistent state:
   - Discards pending-blobs (writes not synced)
   - Discards pending-new-files (synced but not atomic-moved)
   - Reverts pending-moves: restores old values if they existed

   Returns the state before crash for verification."
  [state-atom]
  (let [before-state @state-atom]
    (swap! state-atom
           (fn [state]
             (-> state
                 ;; Discard pending writes
                 (assoc :pending-blobs {})
                 ;; Discard pending .new files
                 (assoc :pending-new-files {})
                 ;; Revert pending moves - restore old values or remove key
                 (update :synced-blobs
                         (fn [synced]
                           (reduce (fn [s {:keys [to old-data]}]
                                     (if old-data
                                       ;; Restore old value
                                       (assoc s to old-data)
                                       ;; No old value - this was a new key, remove it
                                       (dissoc s to)))
                                   synced
                                   (:pending-moves state))))
                 ;; Clear pending moves
                 (assoc :pending-moves [])
                 ;; Note: pending-backup-deletes don't affect data integrity
                 (assoc :pending-backup-deletes #{}))))
    before-state))

(defn set-crash-point!
  "Set the crash injection point.

   Valid points:
   - :after-write-header
   - :after-write-meta
   - :after-write-value
   - :after-sync
   - :after-atomic-move
   - :after-sync-store
   - nil (no crash injection)"
  [crash-point-atom point]
  (when (and point (not (contains? crash-points point)))
    (throw (ex-info "Invalid crash point"
                    {:valid-points crash-points
                     :provided point})))
  (reset! crash-point-atom point))

(defn clear-crash-point!
  "Clear crash injection (no crashes will be triggered)."
  [crash-point-atom]
  (reset! crash-point-atom nil))

(defn get-synced-data
  "Get currently synced (durable) data for inspection.
   Returns map of {store-key -> blob-data-vec}."
  [state-atom]
  (:synced-blobs @state-atom))

(defn get-pending-data
  "Get pending (not yet synced) data for inspection.
   Returns map of {store-key -> blob-data-vec}."
  [state-atom]
  (:pending-blobs @state-atom))

(defn get-pending-new-files
  "Get pending .new files (synced but not atomic-moved).
   Returns map of {store-key -> blob-data-vec}."
  [state-atom]
  (:pending-new-files @state-atom))

(defn has-orphan-files?
  "Check if there are orphan .new files after a crash.
   In real filesystem, these would be left behind."
  [state-atom]
  (boolean (seq (:pending-new-files @state-atom))))

;; =============================================================================
;; Test Helpers
;; =============================================================================

(defn verify-no-partial-data
  "Verify that no partial data is visible.
   Returns true if data integrity is maintained."
  [state-atom store-key expected-header-size expected-meta-size]
  (let [state @state-atom
        data (get (:synced-blobs state) store-key)]
    (if (nil? data)
      true  ; No data is valid (old value preserved)
      (let [total-size (count data)
            min-valid-size (+ expected-header-size expected-meta-size 1)]
        ;; Data must be either empty or complete
        (or (zero? total-size)
            (>= total-size min-valid-size))))))

(defn verify-atomicity
  "Verify that a write either fully completed or had no effect.
   Compares before and after states."
  [before-state after-state store-key]
  (let [before-data (get (:synced-blobs before-state) store-key)
        after-data (get (:synced-blobs after-state) store-key)]
    ;; After crash, data should be unchanged from before the write attempt
    (= before-data after-data)))
