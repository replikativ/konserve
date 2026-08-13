(ns ^:no-doc konserve.simulation.backing
  "INTERNAL. Fault-injection wrapper for any konserve `PBackingStore`.

   Not public API: this namespace ships in the jar so that backends and
   downstream projects (datahike, stratum) can test against it, but it carries
   no compatibility guarantee and may change or move in any release. Depend on
   it with that understood.

   Wraps any `PBackingStore` implementation and intercepts each protocol method
   to optionally inject faults — errors, corruption or crashes — according to a
   configuration of per-operation rates. Fault decisions are drawn from a
   caller-supplied `SplittableRandom`, so a seed reproduces a run exactly.

   Use cases:
   - Test DefaultStore error handling
   - Verify error propagation from low-level to high-level
   - Simulate crash scenarios
   - Test recovery behavior

   Example:
     (def backing (wrap-backing-store real-backing
                                      chaos-fault-config
                                      (rng 42)
                                      (atom [])))

     ;; Use with connect-default-store
     (connect-default-store backing {...})"
  (:require [konserve.impl.storage-layout :as sl]
            [superv.async :refer [go-try-]]
            [clojure.core.async :refer [go <!]])
  (:import [java.util SplittableRandom]))

;; =============================================================================
;; Randomness
;; =============================================================================

(defn rng
  "Create a `SplittableRandom` for reproducible fault injection.

   The same seed replays the same sequence of fault decisions, so a failing
   run is reproduced by re-running with its seed."
  ^SplittableRandom [seed]
  (SplittableRandom. (long seed)))

;; =============================================================================
;; Fault Configuration
;; =============================================================================

(def default-fault-config
  "Default fault configuration - low probability faults for stress testing."
  {:create-blob-fault-rate     0.01
   :delete-blob-fault-rate     0.01
   :blob-exists-fault-rate     0.01
   :copy-fault-rate            0.01
   :atomic-move-fault-rate     0.01
   :sync-store-fault-rate      0.01
   :keys-fault-rate            0.01
   ;; Blob-level faults
   :read-header-fault-rate     0.01
   :read-meta-fault-rate       0.01
   :read-value-fault-rate      0.01
   :write-header-fault-rate    0.01
   :write-meta-fault-rate      0.01
   :write-value-fault-rate     0.01
   :sync-blob-fault-rate       0.01
   :close-fault-rate           0.01
   :get-lock-fault-rate        0.01
   ;; Corruption (data returned is corrupted)
   :read-header-corrupt-rate   0.0
   :read-meta-corrupt-rate     0.0
   :read-value-corrupt-rate    0.0
   ;; Crash simulation (throws special crash exception)
   :crash-before-atomic-move   0.0
   :crash-after-atomic-move    0.0
   :crash-after-write          0.0})

(def chaos-fault-config
  "High fault rates for stress testing."
  {:create-blob-fault-rate     0.1
   :delete-blob-fault-rate     0.1
   :blob-exists-fault-rate     0.05
   :copy-fault-rate            0.1
   :atomic-move-fault-rate     0.1
   :sync-store-fault-rate      0.1
   :keys-fault-rate            0.05
   :read-header-fault-rate     0.1
   :read-meta-fault-rate       0.1
   :read-value-fault-rate      0.1
   :write-header-fault-rate    0.1
   :write-meta-fault-rate      0.1
   :write-value-fault-rate     0.1
   :sync-blob-fault-rate       0.1
   :close-fault-rate           0.05
   :get-lock-fault-rate        0.1
   :read-header-corrupt-rate   0.05
   :read-meta-corrupt-rate     0.05
   :read-value-corrupt-rate    0.05
   :crash-before-atomic-move   0.05
   :crash-after-atomic-move    0.05
   :crash-after-write          0.05})

(def no-faults-config
  "No faults - pass through to real backing store."
  {:create-blob-fault-rate     0.0
   :delete-blob-fault-rate     0.0
   :blob-exists-fault-rate     0.0
   :copy-fault-rate            0.0
   :atomic-move-fault-rate     0.0
   :sync-store-fault-rate      0.0
   :keys-fault-rate            0.0
   :read-header-fault-rate     0.0
   :read-meta-fault-rate       0.0
   :read-value-fault-rate      0.0
   :write-header-fault-rate    0.0
   :write-meta-fault-rate      0.0
   :write-value-fault-rate     0.0
   :sync-blob-fault-rate       0.0
   :close-fault-rate           0.0
   :get-lock-fault-rate        0.0
   :read-header-corrupt-rate   0.0
   :read-meta-corrupt-rate     0.0
   :read-value-corrupt-rate    0.0
   :crash-before-atomic-move   0.0
   :crash-after-atomic-move    0.0
   :crash-after-write          0.0})

;; =============================================================================
;; Fault Injection Helpers
;; =============================================================================

(defn maybe-fault!
  "Check if we should inject a fault based on rate. Returns true if fault triggered."
  [^SplittableRandom rng rate]
  (when (pos? rate)
    (< (.nextDouble rng) rate)))

(defn inject-fault!
  "Throw a simulated fault exception."
  [operation store-key]
  (throw (ex-info (str "Simulated " (name operation) " fault")
                  {:type :simulated-fault
                   :operation operation
                   :store-key store-key})))

(defn inject-crash!
  "Throw a simulated crash exception (special type for crash scenarios)."
  [phase store-key]
  (throw (ex-info (str "Simulated crash: " (name phase))
                  {:type :simulated-crash
                   :phase phase
                   :store-key store-key})))

(defn corrupt-bytes
  "Corrupt a byte array by flipping random bits."
  [^SplittableRandom rng ^bytes arr]
  (when (and arr (pos? (alength arr)))
    (let [num-corruptions (inc (.nextInt rng (min 5 (alength arr))))]
      (dotimes [_ num-corruptions]
        (let [idx (.nextInt rng (alength arr))
              old-byte (aget arr idx)
              new-byte (unchecked-byte (bit-xor old-byte (bit-shift-left 1 (.nextInt rng 8))))]
          (aset arr idx new-byte)))))
  arr)

(defn record-operation!
  "Record an operation to history for analysis."
  [history-atom op-type operation store-key result]
  (when history-atom
    (swap! history-atom conj
           {:time (System/nanoTime)
            :op-type op-type
            :operation operation
            :store-key store-key
            :result result})))

;; =============================================================================
;; SimulatedBlob - Wraps PBackingBlob with fault injection
;; =============================================================================

(defrecord SimulatedBlob [real-blob store-key fault-config rng history-atom]
  sl/PBackingBlob

  (-sync [_this env]
    (record-operation! history-atom :invoke :sync-blob store-key nil)
    (if (maybe-fault! rng (:sync-blob-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :sync-blob store-key :fault)
        (if (:sync? env)
          (inject-fault! :sync-blob store-key)
          (go-try- (inject-fault! :sync-blob store-key))))
      (let [result (sl/-sync real-blob env)]
        (record-operation! history-atom :ok :sync-blob store-key nil)
        result)))

  (-close [_this env]
    (record-operation! history-atom :invoke :close-blob store-key nil)
    (if (maybe-fault! rng (:close-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :close-blob store-key :fault)
        (if (:sync? env)
          (inject-fault! :close-blob store-key)
          (go-try- (inject-fault! :close-blob store-key))))
      (let [result (sl/-close real-blob env)]
        (record-operation! history-atom :ok :close-blob store-key nil)
        result)))

  (-get-lock [_this env]
    (record-operation! history-atom :invoke :get-lock store-key nil)
    (if (maybe-fault! rng (:get-lock-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :get-lock store-key :fault)
        (if (:sync? env)
          (inject-fault! :get-lock store-key)
          (go-try- (inject-fault! :get-lock store-key))))
      (let [result (sl/-get-lock real-blob env)]
        (record-operation! history-atom :ok :get-lock store-key nil)
        result)))

  (-read-header [_this env]
    (record-operation! history-atom :invoke :read-header store-key nil)
    (if (maybe-fault! rng (:read-header-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :read-header store-key :fault)
        (if (:sync? env)
          (inject-fault! :read-header store-key)
          (go-try- (inject-fault! :read-header store-key))))
      (if (:sync? env)
        (let [result (sl/-read-header real-blob env)]
          (if (maybe-fault! rng (:read-header-corrupt-rate fault-config 0.0))
            (do
              (record-operation! history-atom :ok :read-header store-key :corrupted)
              (corrupt-bytes rng result))
            (do
              (record-operation! history-atom :ok :read-header store-key nil)
              result)))
        (go
          (let [result (<! (sl/-read-header real-blob env))]
            (if (maybe-fault! rng (:read-header-corrupt-rate fault-config 0.0))
              (do
                (record-operation! history-atom :ok :read-header store-key :corrupted)
                (corrupt-bytes rng result))
              (do
                (record-operation! history-atom :ok :read-header store-key nil)
                result)))))))

  (-read-meta [_this meta-size env]
    (record-operation! history-atom :invoke :read-meta store-key nil)
    (if (maybe-fault! rng (:read-meta-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :read-meta store-key :fault)
        (if (:sync? env)
          (inject-fault! :read-meta store-key)
          (go-try- (inject-fault! :read-meta store-key))))
      (if (:sync? env)
        (let [result (sl/-read-meta real-blob meta-size env)]
          (if (maybe-fault! rng (:read-meta-corrupt-rate fault-config 0.0))
            (do
              (record-operation! history-atom :ok :read-meta store-key :corrupted)
              (corrupt-bytes rng result))
            (do
              (record-operation! history-atom :ok :read-meta store-key nil)
              result)))
        (go
          (let [result (<! (sl/-read-meta real-blob meta-size env))]
            (if (maybe-fault! rng (:read-meta-corrupt-rate fault-config 0.0))
              (do
                (record-operation! history-atom :ok :read-meta store-key :corrupted)
                (corrupt-bytes rng result))
              (do
                (record-operation! history-atom :ok :read-meta store-key nil)
                result)))))))

  (-read-value [_this meta-size env]
    (record-operation! history-atom :invoke :read-value store-key nil)
    (if (maybe-fault! rng (:read-value-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :read-value store-key :fault)
        (if (:sync? env)
          (inject-fault! :read-value store-key)
          (go-try- (inject-fault! :read-value store-key))))
      (if (:sync? env)
        (let [result (sl/-read-value real-blob meta-size env)]
          (if (maybe-fault! rng (:read-value-corrupt-rate fault-config 0.0))
            (do
              (record-operation! history-atom :ok :read-value store-key :corrupted)
              (corrupt-bytes rng result))
            (do
              (record-operation! history-atom :ok :read-value store-key nil)
              result)))
        (go
          (let [result (<! (sl/-read-value real-blob meta-size env))]
            (if (maybe-fault! rng (:read-value-corrupt-rate fault-config 0.0))
              (do
                (record-operation! history-atom :ok :read-value store-key :corrupted)
                (corrupt-bytes rng result))
              (do
                (record-operation! history-atom :ok :read-value store-key nil)
                result)))))))

  (-read-binary [_this meta-size locked-cb env]
    (record-operation! history-atom :invoke :read-binary store-key nil)
    (if (maybe-fault! rng (:read-value-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :read-binary store-key :fault)
        (if (:sync? env)
          (inject-fault! :read-binary store-key)
          (go-try- (inject-fault! :read-binary store-key))))
      (let [result (sl/-read-binary real-blob meta-size locked-cb env)]
        (record-operation! history-atom :ok :read-binary store-key nil)
        result)))

  (-write-header [_this header-arr env]
    (record-operation! history-atom :invoke :write-header store-key nil)
    (if (maybe-fault! rng (:write-header-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :write-header store-key :fault)
        (if (:sync? env)
          (inject-fault! :write-header store-key)
          (go-try- (inject-fault! :write-header store-key))))
      (let [result (sl/-write-header real-blob header-arr env)]
        (record-operation! history-atom :ok :write-header store-key nil)
        result)))

  (-write-meta [_this meta-arr env]
    (record-operation! history-atom :invoke :write-meta store-key nil)
    (if (maybe-fault! rng (:write-meta-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :write-meta store-key :fault)
        (if (:sync? env)
          (inject-fault! :write-meta store-key)
          (go-try- (inject-fault! :write-meta store-key))))
      (let [result (sl/-write-meta real-blob meta-arr env)]
        (record-operation! history-atom :ok :write-meta store-key nil)
        result)))

  (-write-value [_this value-arr meta-size env]
    (record-operation! history-atom :invoke :write-value store-key nil)
    (if (maybe-fault! rng (:write-value-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :write-value store-key :fault)
        (if (:sync? env)
          (inject-fault! :write-value store-key)
          (go-try- (inject-fault! :write-value store-key))))
      (let [result (sl/-write-value real-blob value-arr meta-size env)]
        (when (maybe-fault! rng (:crash-after-write fault-config 0.0))
          (inject-crash! :after-write store-key))
        (record-operation! history-atom :ok :write-value store-key nil)
        result)))

  (-write-binary [_this meta-size blob env]
    (record-operation! history-atom :invoke :write-binary store-key nil)
    (if (maybe-fault! rng (:write-value-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :write-binary store-key :fault)
        (if (:sync? env)
          (inject-fault! :write-binary store-key)
          (go-try- (inject-fault! :write-binary store-key))))
      (let [result (sl/-write-binary real-blob meta-size blob env)]
        (record-operation! history-atom :ok :write-binary store-key nil)
        result))))

;; =============================================================================
;; SimulatedBackingStore - Wraps PBackingStore with fault injection
;; =============================================================================

(defrecord SimulatedBackingStore [real-backing fault-config rng history-atom]
  sl/PBackingStore

  (-create-blob [_this store-key env]
    (record-operation! history-atom :invoke :create-blob store-key nil)
    (if (maybe-fault! rng (:create-blob-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :create-blob store-key :fault)
        (if (:sync? env)
          (inject-fault! :create-blob store-key)
          (go-try- (inject-fault! :create-blob store-key))))
      (if (:sync? env)
        (let [real-blob (sl/-create-blob real-backing store-key env)]
          (record-operation! history-atom :ok :create-blob store-key nil)
          (->SimulatedBlob real-blob store-key fault-config rng history-atom))
        (go
          (let [real-blob (<! (sl/-create-blob real-backing store-key env))]
            (record-operation! history-atom :ok :create-blob store-key nil)
            (->SimulatedBlob real-blob store-key fault-config rng history-atom))))))

  (-delete-blob [_this store-key env]
    (record-operation! history-atom :invoke :delete-blob store-key nil)
    (if (maybe-fault! rng (:delete-blob-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :delete-blob store-key :fault)
        (if (:sync? env)
          (inject-fault! :delete-blob store-key)
          (go-try- (inject-fault! :delete-blob store-key))))
      (let [result (sl/-delete-blob real-backing store-key env)]
        (record-operation! history-atom :ok :delete-blob store-key nil)
        result)))

  (-blob-exists? [_this store-key env]
    (record-operation! history-atom :invoke :blob-exists store-key nil)
    (if (maybe-fault! rng (:blob-exists-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :blob-exists store-key :fault)
        (if (:sync? env)
          (inject-fault! :blob-exists store-key)
          (go-try- (inject-fault! :blob-exists store-key))))
      (let [result (sl/-blob-exists? real-backing store-key env)]
        (record-operation! history-atom :ok :blob-exists store-key nil)
        result)))

  (-migratable [_this key store-key env]
    ;; Pass through - migration is not fault-injected
    (sl/-migratable real-backing key store-key env))

  (-migrate [_this migration-key key-vec serializer read-handlers write-handlers env]
    ;; Pass through - migration is not fault-injected
    (sl/-migrate real-backing migration-key key-vec serializer read-handlers write-handlers env))

  (-copy [_this from to env]
    (record-operation! history-atom :invoke :copy from nil)
    (if (maybe-fault! rng (:copy-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :copy from :fault)
        (if (:sync? env)
          (inject-fault! :copy from)
          (go-try- (inject-fault! :copy from))))
      (let [result (sl/-copy real-backing from to env)]
        (record-operation! history-atom :ok :copy from nil)
        result)))

  (-atomic-move [_this from to env]
    (record-operation! history-atom :invoke :atomic-move from nil)
    ;; Check for crash BEFORE atomic move
    (when (maybe-fault! rng (:crash-before-atomic-move fault-config 0.0))
      (record-operation! history-atom :crash :atomic-move from :before)
      (inject-crash! :before-atomic-move from))
    (if (maybe-fault! rng (:atomic-move-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :atomic-move from :fault)
        (if (:sync? env)
          (inject-fault! :atomic-move from)
          (go-try- (inject-fault! :atomic-move from))))
      (let [result (sl/-atomic-move real-backing from to env)]
        ;; Check for crash AFTER atomic move
        (when (maybe-fault! rng (:crash-after-atomic-move fault-config 0.0))
          (record-operation! history-atom :crash :atomic-move from :after)
          (inject-crash! :after-atomic-move from))
        (record-operation! history-atom :ok :atomic-move from nil)
        result)))

  (-create-store [_this env]
    (record-operation! history-atom :invoke :create-store nil nil)
    (let [result (sl/-create-store real-backing env)]
      (record-operation! history-atom :ok :create-store nil nil)
      result))

  (-delete-store [_this env]
    (record-operation! history-atom :invoke :delete-store nil nil)
    (let [result (sl/-delete-store real-backing env)]
      (record-operation! history-atom :ok :delete-store nil nil)
      result))

  (-store-exists? [_this env]
    (sl/-store-exists? real-backing env))

  (-sync-store [_this env]
    (record-operation! history-atom :invoke :sync-store nil nil)
    (if (maybe-fault! rng (:sync-store-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :sync-store nil :fault)
        (if (:sync? env)
          (inject-fault! :sync-store nil)
          (go-try- (inject-fault! :sync-store nil))))
      (let [result (sl/-sync-store real-backing env)]
        (record-operation! history-atom :ok :sync-store nil nil)
        result)))

  (-keys [_this env]
    (record-operation! history-atom :invoke :keys nil nil)
    (if (maybe-fault! rng (:keys-fault-rate fault-config 0.0))
      (do
        (record-operation! history-atom :fail :keys nil :fault)
        (if (:sync? env)
          (inject-fault! :keys nil)
          (go-try- (inject-fault! :keys nil))))
      (let [result (sl/-keys real-backing env)]
        (record-operation! history-atom :ok :keys nil nil)
        result)))

  (-handle-foreign-key [_this migration-key serializer read-handlers write-handlers env]
    ;; Pass through - migration is not fault-injected
    (sl/-handle-foreign-key real-backing migration-key serializer read-handlers write-handlers env)))

;; =============================================================================
;; Factory Functions
;; =============================================================================

(defn wrap-backing-store
  "Wrap a PBackingStore with fault injection.

   Args:
     real-backing  - The real PBackingStore to wrap
     fault-config  - Fault configuration map (see default-fault-config)
     rng           - SplittableRandom for deterministic fault injection
     history-atom  - Atom to record operation history (optional)

   Returns: SimulatedBackingStore"
  [real-backing fault-config ^SplittableRandom rng & [history-atom]]
  (->SimulatedBackingStore real-backing fault-config rng (or history-atom (atom []))))

(defn get-history
  "Get the operation history from a SimulatedBackingStore."
  [simulated-backing]
  @(:history-atom simulated-backing))

(defn count-faults
  "Count how many faults were injected."
  [simulated-backing]
  (count (filter #(= :fail (:op-type %)) (get-history simulated-backing))))
