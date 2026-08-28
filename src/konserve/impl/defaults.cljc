(ns konserve.impl.defaults
  "Default implementation of the high level protocol given a binary backing implementation as defined in the storage-layout namespace."
  (:require
   [clojure.core.async :refer [<! timeout] :as async]
   [clojure.string :refer [ends-with?]]
   [hasch.core :refer [uuid]]
   [konserve.serializers :refer [key->serializer]]
   [konserve.compressor :refer [get-compressor null-compressor]]
   [konserve.encryptor :refer [get-encryptor null-encryptor]]
   [konserve.protocols :as protocols :refer [PEDNKeyValueStore
                                             PBinaryKeyValueStore
                                             -serialize -deserialize
                                             PAssocSerializers
                                             PKeyIterable
                                             PMultiKeySupport PConditionalWrite
                                             PMultiKeyEDNValueStore
                                             PWriteHookStore]]
   #?(:clj [konserve.nio-helpers :as nio])
   [konserve.impl.storage-layout :refer [-streaming-binary-write? -atomic-move -create-store -store-exists?
                                         PBackingOpen -open-store
                                         -copy -create-blob -delete-blob -blob-exists?
                                         -keys -sync-store
                                         -migratable -migrate -handle-foreign-key
                                         -close -get-lock -sync
                                         -read-header -read-meta -read-value -read-binary
                                         -write-header -write-meta -write-value -write-binary
                                         PBackingLock -release
                                         PMultiWriteBackingStore -multi-write-blobs -multi-delete-blobs
                                         PMultiReadBackingStore -multi-read-blobs
                                         PReadMissSafe store-key-not-found?
                                         default-version
                                         parse-header create-header header-size]]
   [konserve.utils  #?@(:clj [:refer [async+sync *default-sync-translation*]]
                        :cljs [:refer [*default-sync-translation*] :refer-macros [async+sync]])]
   [superv.async :refer [go-try- <?-]]
   [replikativ.logging :as log])
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
    ;; The two write artifacts, exactly: `<key>.ksv.<nonce>.new` — a staging
    ;; file, nonce per writer, see `update-blob` — and `<key>.ksv.backup`.
    :else
    (if-let [[_ k] (re-matches #"(.+)\.ksv(?:\.[^.]+\.new|\.backup)" store-key)]
      k
      (throw (ex-info (str "Invalid konserve store key: " store-key)
                      {:key store-key})))))

#?(:cljs (extend-type js/Uint8Array ICounted (-count [this] (alength this))))

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
          to-array #?(:cljs
                      (fn [value]
                        (-serialize ((encryptor  (:encryptor config)) (compressor serializer)) nil write-handlers value))
                      :clj
                      (fn [value]
                        (let [bos (ByteArrayOutputStream.)]
                          (try (-serialize ((encryptor (:encryptor config)) (compressor serializer))
                                           bos write-handlers value)
                               (.toByteArray bos)
                               (finally
                                 (.close bos))))))

          meta  (up-fn-meta old-meta)
          value (when (= operation :write-edn)
                  (if-not (empty? rkey)
                    (update-in old-value rkey up-fn)
                    (up-fn old-value)))
          ;; The staging file is PER WRITER. A fixed `<key>.new` is a shared
          ;; path with no exclusion around it: two writers to one key — two
          ;; processes, or two stores connected to one path in one JVM —
          ;; interleave header, meta and value writes into the same file, and
          ;; whichever moves first can be overwritten in place by the other's
          ;; remaining positional writes, AFTER it has reported success. That
          ;; is a torn value, not the last-writer-wins lost update the design
          ;; accepts for unfenced writes. The target's FileLock used to narrow
          ;; that window incidentally; it is released before the move now (it
          ;; never guarded the move — see `cas-lock-suffix`), so the staging
          ;; file has to be exclusive on its own terms. It also makes the
          ;; filestore's move retry unambiguous: it retries OUR file by name.
          new-store-key (if (:in-place? config)
                          store-key
                          (str store-key "." (random-uuid) ".new"))
          moved?        (atom false)
          backup-store-key (str store-key ".backup")
          _ (when (and (:in-place? config) (not (:no-backup? config))) ;; let's back things up before writing then
              (log/trace :konserve/backup-blob {:backup-store-key backup-store-key :key key})
              (<?- (-copy backing store-key backup-store-key env)))
          meta-arr             (to-array meta)
          meta-size            (count meta-arr)
          header               (create-header version
                                              serializer compressor encryptor meta-size)
          new-blob             (<?- (-create-blob backing new-store-key env))
          new-blob-closed?     (atom false)]
      (try
        (<?- (-write-header new-blob header env))
        (<?- (-write-meta new-blob meta-arr env))
        (if (= operation :write-binary)
          ;; Normalize here so no backing has to. `bassoc` documents five input
          ;; shapes; before this, only the filestore handled any but bytes, and
          ;; every other backing silently mishandled the rest — konserve-s3
          ;; stashes the blob and later calls `(.write baos value)`, which needs
          ;; a byte array. A backing that can drain a stream says so and gets
          ;; the caller's input untouched, which is what lets a value larger
          ;; than the heap be written at all: Lucene's DEFAULT merge policy
          ;; tops out at 5 GB per segment, and a byte array at 2 GB.
          (<?- (-write-binary new-blob meta-size
                              (if (-streaming-binary-write? new-blob)
                                input
                                #?(:clj (nio/blob->bytes input) :cljs input))
                              env))
          (let [value-arr (to-array value)]
            (<?- (-write-value new-blob value-arr meta-size env))))

        (when (:sync-blob? config)
          (log/trace :konserve/syncing-blob {:key key})
          (<?- (-sync new-blob env)))
        ;; Marked before the call: a close that throws after releasing the
        ;; resource must not be closed again from the `finally`.
        (reset! new-blob-closed? true)
        (<?- (-close new-blob env))

        (when-not (:in-place? config)
          (log/trace :konserve/moving-blob {:key key})
          (<?- (-atomic-move backing new-store-key store-key env))
          (reset! moved? true))

        (when (:sync-blob? config)
          (log/trace :konserve/syncing-store {:key key})
          (<?- (-sync-store backing env)))

        ;; Clean up backup after successful write
        (when (and (:in-place? config) (not (:no-backup? config)))
          (log/trace :konserve/deleting-backup-blob {:backup-store-key backup-store-key :key key})
          (<?- (-delete-blob backing backup-store-key env)))

        (if (= operation :write-edn) [old-value value] true)
        (finally
          ;; Once. `-close` is idempotent on a FileChannel by spec, but that is
          ;; the filestore's property, not the protocol's.
          (when-not @new-blob-closed?
            (<?- (-close new-blob env)))
          ;; A staging file that never got moved is ours to remove: with a
          ;; per-writer name nothing later overwrites it, so a failed write
          ;; would otherwise leave a complete-looking `.new` behind for good.
          ;; Best effort — this runs while an exception may be propagating,
          ;; and a cleanup failure must not replace it.
          (when (and (not (:in-place? config)) (not @moved?))
            (try (<?- (-delete-blob backing new-store-key env))
                 (catch #?(:clj Exception :cljs js/Error) _ nil)))))))))

(defn read-header [ac serializers env]
  (let [{:keys [sync? store-key]} env]
    (async+sync sync? *default-sync-translation*
                (go-try-
                 (let [arr (<?- (-read-header ac env))]
                   (try
                     (parse-header arr serializers)
                     (catch #?(:clj Exception :cljs js/Error) e
                       (throw (ex-info "Header parsing error."
                                       {:error e
                                        :store-key store-key
                                        :arr (seq arr)})))))))))

(defn read-blob
  "Read meta, edn or binary from blob."
  [blob read-handlers serializers {:keys [sync? operation locked-cb config _store-key] :as env}]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [[_ serializer compressor encryptor meta-size header-size]
          (<?- (read-header blob serializers env))
          env (assoc env :header-size header-size)
          fn-read (partial -deserialize
                           ;; Encryptor OUTERMOST, matching the write path.
                           ;;
                           ;; Bytes on disk are encrypt(compress(serialize(v))),
                           ;; so reading them must decrypt BEFORE decompressing.
                           ;; This nested the other way -- compressor outermost --
                           ;; and so tried to decompress ciphertext. Any
                           ;; compressor combined with any encryptor failed:
                           ;; zstd+aes with "Unknown frame descriptor", lz4+aes
                           ;; with "Stream unsupported". Invisible while either
                           ;; side is the null implementation, which is the
                           ;; default and is what the tests used.
                           ((encryptor (:encryptor config)) (compressor serializer))
                           read-handlers)]
      (case operation
        :read-meta #?(:cljs (fn-read (<?- (-read-meta blob meta-size env)))
                      :clj
                      (let [bais-read (ByteArrayInputStream.
                                       (<?- (-read-meta blob meta-size env)))
                            value     (fn-read bais-read)
                            _         (.close bais-read)]
                        value))
        :read-edn #?(:cljs (fn-read (<?- (-read-value blob meta-size env)))
                     :clj
                     (let [bais-read (ByteArrayInputStream.
                                      (<?- (-read-value blob meta-size env)))
                           value     (fn-read bais-read)
                           _         (.close bais-read)]
                       value))
        :write-binary #?(:cljs
                         (let [meta (fn-read (<?- (-read-meta blob meta-size env)))]
                           [meta nil])
                         :clj
                         (let [bais-read (ByteArrayInputStream.
                                          (<?- (-read-meta blob meta-size env)))
                               meta      (fn-read bais-read)
                               _         (.close bais-read)]
                           [meta nil]))
        :write-edn #?(:cljs
                      (let [meta  (fn-read (<?- (-read-meta blob meta-size env)))
                            value (fn-read (<?- (-read-value blob meta-size env)))]
                        [meta value])
                      :clj
                      (let [bais-meta  (ByteArrayInputStream.
                                        (<?- (-read-meta blob meta-size env)))
                            meta       (fn-read bais-meta)
                            _          (.close bais-meta)
                            bais-value (ByteArrayInputStream.
                                        (<?- (-read-value blob meta-size env)))
                            value     (fn-read bais-value)
                            _          (.close bais-value)]
                        [meta value]))
        ;; Both segments, from the one pass the blob is already open for. The
        ;; write path has always done this (`:write-edn` below); a READ needs it
        ;; so a caller can obtain a value AND the revision it is at without a
        ;; second round-trip — which on a remote store is an extra GET, and worse,
        ;; is RACY: between reading the value and reading its revision another
        ;; writer can move both, and the caller would fence against a revision
        ;; that never belonged to the value it computed from.
        :read-edn-meta #?(:cljs
                          (let [meta  (fn-read (<?- (-read-meta blob meta-size env)))
                                value (fn-read (<?- (-read-value blob meta-size env)))]
                            [meta value])
                          :clj
                          (let [bais-meta  (ByteArrayInputStream.
                                            (<?- (-read-meta blob meta-size env)))
                                meta       (fn-read bais-meta)
                                _          (.close bais-meta)
                                bais-value (ByteArrayInputStream.
                                            (<?- (-read-value blob meta-size env)))
                                value      (fn-read bais-value)
                                _          (.close bais-value)]
                            [meta value]))
        :read-binary (<?- (-read-binary blob meta-size locked-cb env)))))))

(declare get-lock cas-lock-suffix)

(defn- needs-sidecar-lock?
  "Does konserve itself have to serialize conditional writes on this backing?

   Only a backing that does NOT fence its own writes, and so relies on the
   compare-and-write in `io-operation` being made atomic by a lock. Asked as a
   mechanism question, not inferred from the domain — the domain says how far
   a guarantee reaches, not who evaluates it; see `PSelfConditionalWrite`."
  [backing config]
  (and (:lock-blob? config)
       (satisfies? protocols/PConditionalWrite backing)
       (some? (protocols/-conditional-write-domain backing))
       (not (satisfies? protocols/PSelfConditionalWrite backing))))

(defn delete-blob
  "Remove/Delete key-value pair of backing store by given key. Returns true if the
   key existed and was deleted, false if it was absent.

   The -blob-exists? probe reports existed?/false-for-missing (konserve's contract,
   enforced by the compliance suite). Callers that DON'T need that boolean — e.g.
   datahike GC's bulk sweep — can pass `:ignore-existence? true` in opts: on a
   PReadMissSafe backing (whose -delete-blob is idempotent) this skips the probe and
   returns true, saving a round-trip (an S3 HEAD before the DELETE). On other
   backings the hint is ignored and the probe stays (a local stat is cheap, and
   -delete-blob there is not guaranteed idempotent)."
  [backing env]
  (async+sync
   (:sync? env) *default-sync-translation*
   (go-try-
    (let [{:keys [key-vec base ignore-existence? config]} env
          key          (first key-vec)
          store-key    (key->store-key key)
          on-error     (fn [e] (ex-info "Could not delete key."
                                        {:key key :base base :exception e}))
          ;; A delete is a write. On a FENCEABLE key — one with a sidecar — it
          ;; has to take the sidecar's lock like every other writer, or a fenced
          ;; write can pass `check-revision!`, this delete can land, and the
          ;; fenced value can then land on top: both report success and a
          ;; deleted key reappears, with no serialization consistent with the
          ;; revision the writer was promised. Same probe the write path pays.
          cas-store-key (str store-key cas-lock-suffix)
          cas-blob      (when (and (needs-sidecar-lock? backing config)
                                   (<?- (-blob-exists? backing cas-store-key env)))
                          (<?- (-create-blob backing cas-store-key env)))
          delete!       (fn [] (go-try-
                                (try (<?- (-delete-blob backing store-key env)) true
                                     (catch #?(:clj Exception :cljs js/Error) e (throw (on-error e))))))]
      (try
        (let [cas-lock (when cas-blob (<?- (get-lock cas-blob key env)))]
          (try
            (cond
              ;; opt-in fast path: no existed? boolean needed, idempotent delete.
              (and ignore-existence? (satisfies? PReadMissSafe backing)) (<?- (delete!))
              (<?- (-blob-exists? backing store-key env))                 (<?- (delete!))
              :else                                                      false)
            (finally
              (when cas-lock (<?- (-release cas-lock env))))))
        (finally
          (when cas-blob (<?- (-close cas-blob env)))))))))

(def ^:const max-lock-attempts 100)

(def ^:private not-found-sentinel
  "Internal marker distinguishing a missing key from a stored nil value on the
  read path. Threaded through `io-operation` as `:not-found` so `-get-in` can
  avoid a separate existence probe (an extra HEAD round-trip per read on
  remote backends such as S3)."
  #?(:clj (Object.) :cljs (js-obj)))

(def absent
  "The `:expected-revision` that means THE KEY MUST NOT EXIST — the create half of
   a conditional write. Distinct from `nil`, which means \"unconditional\": a
   caller that passed nil for \"I read nothing there\" would silently get an
   unconditional overwrite, which is the failure this whole mechanism exists to
   remove."
  ::absent)

(def ^:const cas-lock-suffix
  "Suffix of the sidecar blob a FENCED write takes its lock on.

   Its own blob rather than the value's because a lock lives on an INODE, and the
   write replaces the value blob by rename — which orphans a lock taken on it, so
   two writers could each hold a lock on a different inode and both believe they
   were serialized. The sidecar is never renamed, so it is a stable thing to lock.

   It therefore PERSISTS after the write, and must be recognised as konserve's own
   bookkeeping wherever store keys are enumerated. See `internal-artifact?`."
  ".cas")

(defn internal-artifact?
  "Is `store-key` konserve's own bookkeeping rather than a stored value?

   `.new` and `.backup` are transient write artifacts; `.cas` is the fenced-write
   lock sidecar, which is permanent.

   This matters more than it looks. An unrecognised name falls through to
   `-handle-foreign-key`, whose job is to migrate a value written in an older
   layout — so the sidecar was read as if it were a value, and `k/keys` (and
   `konserve.gc/sweep!` through it) THREW from the first fenced write onwards, on
   every default-backing store, permanently: the file is not transient and
   `dissoc`ing the key does not remove it. Backends that filter enumeration
   themselves must include this suffix too."
  [store-key]
  (or (ends-with? store-key ".new")
      (ends-with? store-key ".backup")
      (ends-with? store-key cas-lock-suffix)))

(defn check-revision!
  "Throw unless the stored revision is the one the caller derived its value from.

   `old-meta` is nil when the key does not exist. The comparison is `=` on an
   OPAQUE token: for this backing it is an integer counter kept in the metadata,
   for others it can be whatever their storage gives (an S3 ETag, a row version).
   Callers must treat it as opaque and pass back what they read.

   A key never written since revisions were introduced has no `:revision`, so it
   reads as nil and a caller that read nil and passes nil back still matches."
  [key expected old-meta]
  (when (and (some? old-meta) (not (contains? old-meta :revision)))
    ;; Written before revisions existed. There is no token to compare, so there
    ;; is no way to tell whether it changed — and answering "unchanged" would let
    ;; the caller overwrite a value it never saw. Refuse; one unconditional write
    ;; gives the key a revision and it is fenceable from then on.
    (throw (ex-info "This value predates revisions, so a conditional write cannot be evaluated against it. Write it once unconditionally to give it a revision."
                    {:type :konserve/revision-unavailable
                     :key  key})))
  (let [actual (if (nil? old-meta) absent (:revision old-meta))]
    (when-not (= expected actual)
      (throw (ex-info "Conditional write rejected: the stored revision is not the one this value was derived from."
                      {:type     :konserve/revision-mismatch
                       :key      key
                       :expected expected
                       :actual   actual})))))

(defn- release-held-lock!
  "Release the lock in `held` (an atom) once; later calls are no-ops.

   A function, not inline code: `io-operation`'s go block sits close to the
   JVM's 64KB method limit, and the early release below is a second call site
   for what the `finally` already does. Each call is one park on this channel
   rather than the whole body expanded into the state machine."
  [held key env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try-
               (when-let [l @held]
                 (reset! held nil)
                 (log/trace :konserve/releasing-blob-lock {:key key})
                 (<?- (-release l env))))))

(defn- close-held-blob!
  "Close the blob in `held` (an atom) once; later calls are no-ops. See
   `release-held-lock!` for why this is a function."
  [held env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try-
               (when-let [b @held]
                 (reset! held nil)
                 (<?- (-close b env))))))

(defn get-lock [this store-key env]
  (async+sync
   (:sync? env)
   *default-sync-translation*
   (go-try-
    (loop [i 0]
      (let [[l e] (try
                    [(<?- (-get-lock this env)) nil]
                    (catch #?(:clj Exception :cljs js/Error) e
                      (log/trace :konserve/lock-acquire-failed {:error e})
                      [nil e]))]

        (if-not (nil? l)
          l
          (do
            #?(:cljs
               (when-not (:sync? env)
                 ;; cannot blocking sleep in sync nodejs w/o package
                 (<! (timeout (rand-int 20))))
               :clj
               (if (:sync? env)
                 (Thread/sleep (long (rand-int 20)))
                 (<! (timeout (rand-int 20)))))
            (if (> i max-lock-attempts)
              (throw (ex-info (str "Failed to acquire lock after " i " iterations.")
                              {:type :file-lock-acquisition-error
                               :error e
                               :store-key store-key}))
              (recur (inc i))))))))))

(defn io-operation
  "Read/Write blob. For better understanding use the flow-chart of konserve."
  [{:keys [backing]} serializers read-handlers write-handlers
   {:keys [key-vec operation default-serializer sync? config expected-revision] :as env}]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [key           (first  key-vec)
          ;; A CONDITIONAL write has to see the old value to compare against it,
          ;; so it cannot take the full-overwrite shortcut that skips the read.
          ;; Cleared here rather than at every call site: `assoc` sets
          ;; `:overwrite? true` for any top-level key, which is exactly the shape
          ;; a fenced pointer write has.
          overwrite?    (and (:overwrite? env) (nil? expected-revision))
          env           (assoc env :overwrite? overwrite?)
          store-key     (key->store-key key)
          env           (assoc env :store-key store-key :header-size header-size)
          serializer    (get serializers default-serializer)
          migration-key (<?- (-migratable backing key store-key env))
          read-op?      (or (= :read-edn operation) (= :read-binary operation) (= :read-meta operation)
                            (= :read-edn-meta operation))
          write-op?     (or (= :write-edn operation) (= :write-binary operation))
          ;; A read that hands the caller a revision token makes the key
          ;; FENCEABLE — it gets a sidecar from here on, so the fenced write
          ;; that follows is ordered against every other writer and deleter.
          ;; Both token sources: `-get-in` with `:with-revision?` selects
          ;; `:read-edn-meta`; `k/revision` goes through `:read-meta` and
          ;; says so with `:revision-read?`. Computed ONCE and used by both
          ;; sidecar sites below — they had drifted, and the drift was a hole.
          revision-bearing? (or (= :read-edn-meta operation) (:revision-read? env))
          ;; A PReadMissSafe backing reports an absent key cleanly from the read
          ;; itself (an absent key throws store-key-not-found-ex), so the
          ;; -blob-exists? probe is a wasted round-trip (an S3 HEAD) whenever we
          ;; touch the blob anyway. Requires no pending migration (that branch
          ;; needs the existence answer independently).
          ;; `-migratable` returns a migration key (truthy) or a falsy no-migration
          ;; marker — some backends use nil, some false — so test truthiness, not nil?.
          miss-safe?    (and (satisfies? PReadMissSafe backing) (not migration-key))
          ;; Skip the probe when its answer is not needed:
          ;; - a full-overwrite WRITE writes regardless and never reads the old value;
          ;; - a READ on a miss-safe backing learns existence from the read itself;
          ;; - a NON-overwrite WRITE (update-in / update / nested assoc-in / bassoc)
          ;;   reads the old value regardless, and on a miss-safe backing that read
          ;;   establishes existence — so the probe is redundant there too (HEAD+GET+PUT
          ;;   collapses to GET+PUT on a hit). The old-read below becomes read-first.
          skip-read-probe?  (and read-op? miss-safe?)
          skip-write-probe? (and write-op? (not overwrite?) miss-safe?)
          skip-exists?  (or (and overwrite? write-op? (not migration-key))
                            skip-read-probe?
                            skip-write-probe?)
          store-key-exists? (when-not skip-exists?
                              (<?- (-blob-exists? backing store-key env)))
          max-retries (get-in config [:optimistic-locking-retries] 0)
          ;; WHO NEEDS THE SIDECAR AT ALL. Only a backing that does NOT fence
          ;; itself, and therefore needs konserve to provide the mechanism: the
          ;; compare-and-write in this function, made atomic by a lock.
          ;;
          ;; Asked as a mechanism question, not inferred from the domain. The
          ;; domain says how far a guarantee reaches, which is a different thing
          ;; from who evaluates it — see `PSelfConditionalWrite`, where the cases
          ;; that broke the old inference are written out.
          ;;
          ;; The cost of getting this wrong is not only tidiness. The probe below
          ;; is an existence check, and on a `PReadMissSafe` backing that is
          ;; exactly the round trip the read-miss-safe design exists to remove — a
          ;; billed HEAD on every write to S3, a `.getKey` transaction on every
          ;; write to IndexedDB — plus a sidecar blob written into a storage layer
          ;; that has no use for one.
          lock-based-fencing? (needs-sidecar-lock? backing config)]
      (cond
        ;; Read-first (PReadMissSafe): no existence probe — read the blob and
        ;; treat an absent key (store-key-not-found-ex) as the caller's
        ;; not-found. One round-trip on remote stores instead of HEAD + GET. A
        ;; genuinely stored nil still comes back as nil (the read succeeds),
        ;; distinct from the not-found sentinel.
        skip-read-probe?
        ;; A REVISION-BEARING READ takes the sidecar too, for two reasons.
        ;;
        ;; It makes the read ATOMIC against writers: the caller is reading a
        ;; value and the token they will fence its successor on, and those two
        ;; must come from the same state — reading them across another writer's
        ;; rename would hand back a revision that never belonged to the value.
        ;;
        ;; And it makes the key FENCEABLE from the first read rather than the
        ;; first write, which is what closes the residual window in the scheme
        ;; described at `cas-blob` below: a caller that re-reads a pointer before
        ;; each commit (datahike re-reads the branch head) has the sidecar in
        ;; place before it ever issues a conditional write, so unconditional
        ;; writers are excluded from that point on.
        (let [cas-store-key (str store-key cas-lock-suffix)
              cas-blob (when (and lock-based-fencing? revision-bearing?)
                         (<?- (-create-blob backing cas-store-key env)))]
          ;; Keep each acquisition INSIDE the try that releases the preceding
          ;; resource. A failure opening or locking the value blob must not strand
          ;; the sidecar's JVM-wide FileLock; one leaked revision read otherwise
          ;; wedges every later read and write of this key until the JVM exits.
          (try
            (let [cas-lock (when cas-blob
                             (log/trace :konserve/acquiring-cas-lock {:key key})
                             (<?- (get-lock cas-blob (first key-vec) env)))]
              (try
                (let [blob (<?- (-create-blob backing store-key env))]
                  (try
                    (let [lock (when (:lock-blob? config)
                                 (log/trace :konserve/acquiring-blob-lock {:key key :blob (str blob)})
                                 (<?- (get-lock blob (first key-vec) env)))]
                      (try
                        (<?- (read-blob blob read-handlers serializers env))
                        (catch #?(:clj Exception :cljs js/Error) e
                          (if (store-key-not-found? e) (:not-found env) (throw e)))
                        (finally
                          (when lock
                            (log/trace :konserve/releasing-blob-lock
                                       {:key (first key-vec) :blob (str blob)})
                            (<?- (-release lock env))))))
                    (finally
                      (<?- (-close blob env)))))
                (finally
                  (when cas-lock (<?- (-release cas-lock env))))))
            (finally
              (when cas-blob (<?- (-close cas-blob env))))))

        (and (not store-key-exists?) migration-key)
        (<?- (-migrate backing migration-key key-vec serializer read-handlers write-handlers env))

        (or store-key-exists? write-op?)
          ;; Retry loop for optimistic locking conflicts
        (loop [attempt 0]
          (let [result
                (try
                  (let [;; A FENCED write serializes on a SIDECAR whose inode never
                        ;; moves, acquired BEFORE the blob is even opened.
                        ;;
                        ;; The blob lock alone cannot do this. It is held on an
                        ;; INODE, and a non-in-place write replaces the file by
                        ;; rename — so a writer that opened the blob, waited for the
                        ;; lock, and found the file renamed underneath it holds a
                        ;; lock on an orphan and reads stale metadata. Its compare
                        ;; then passes against content that already changed.
                        ;; Reproduced exactly that way from two JVMs: both writers
                        ;; fenced on one revision, both succeeded, the first write
                        ;; was lost.
                        ;;
                        ;; Writing IN PLACE would also keep the inode stable, and
                        ;; was tried — but it gives up the atomic rename, and a torn
                        ;; write to a mutable POINTER is a corrupt database. That is
                        ;; a worse failure than the race, so the pointer keeps its
                        ;; rename and the exclusion moves to a file that has none.
                        ;;
                        ;; One extra object per FENCED key, not per key: fencing is
                        ;; for mutable pointers (a branch head), of which there are
                        ;; a handful, not for the content-addressed values that make
                        ;; up the bulk of a store.
                        cas-store-key (str store-key cas-lock-suffix)
                        ;; WHO TAKES THE SIDECAR. Every write to a FENCEABLE key,
                        ;; not merely every fenced write — otherwise the lock
                        ;; excludes the wrong set of writers and the guarantee is
                        ;; not what `:machine` says.
                        ;;
                        ;; An unconditional write renames a NEW inode over this
                        ;; key. A fenced write that opened the old inode before
                        ;; taking its lock then holds a lock on a DETACHED file,
                        ;; reads the pre-write value through it, compares the
                        ;; revision against that, PASSES, and renames its own
                        ;; result over the top. The fence does not merely fail to
                        ;; exclude the other writer: it grants a false pass and
                        ;; loses their committed value. (S3 does not have this
                        ;; problem — If-Match is evaluated by S3 at write time, so
                        ;; an intervening unconditional PUT correctly REJECTS the
                        ;; fenced write. Without this the filestore would be
                        ;; strictly weaker than S3 under the same API.)
                        ;;
                        ;; A key is fenceable once a sidecar exists for it, and
                        ;; only fenced writes and revision-bearing reads create
                        ;; one. So the cost stays where the original design put
                        ;; it: mutable pointers, of which a store has a handful,
                        ;; pay an extra file; the content-addressed values that
                        ;; make up the bulk of a store pay one `exists?` probe
                        ;; (~1us against a ~200us write) and no extra file at all.
                        ;;
                        ;; RESIDUAL, and it is worth stating: the FIRST fenced
                        ;; write to a key can still race an unconditional one,
                        ;; because that probe misses until the sidecar exists. It
                        ;; is self-healing — once created, the hole is closed for
                        ;; that key forever — and revision-bearing reads create it
                        ;; too, so a reader that fences (datahike re-reads the
                        ;; branch head before every commit) closes it before its
                        ;; first write.
                        cas-blob (when (and lock-based-fencing?
                                            (or (some? expected-revision)
                                                revision-bearing?
                                                (<?- (-blob-exists? backing cas-store-key env))))
                                   (<?- (-create-blob backing cas-store-key env)))]
                    ;; THREE NESTED try/finally, one per thing acquired, because a
                    ;; `let` binding that throws skips every `finally` written
                    ;; below it. The sidecar used to be taken up here and released
                    ;; in the innermost `finally`, so anything that threw in
                    ;; between — an IOException opening the value blob, the
                    ;; existence probe, `get-lock` giving up after ~1s of
                    ;; contention — leaked a `FileLock`. That lock is held by the
                    ;; OS on behalf of the whole JVM, so ONE transient error made
                    ;; the key unreadable and unwritable for every process on the
                    ;; machine until this one exited. Reads take the sidecar too,
                    ;; so a branch head would simply stop responding.
                    (try
                      (let [cas-lock (when cas-blob
                                       (log/trace :konserve/acquiring-cas-lock {:key key})
                                       (<?- (get-lock cas-blob (first key-vec) env)))]
                        (try
                          ;; Probed HERE, under the cas lock, because `-create-blob`
                          ;; creates the file: after it, "does this key exist" can no
                          ;; longer be asked. The pre-lock probe is not evidence
                          ;; either — a competing create can land between it and the
                          ;; lock, and trusting it would let a create-if-absent see
                          ;; ::absent and overwrite the other create.
                          (let [exists-under-lock? (when expected-revision
                                                     (<?- (-blob-exists? backing store-key env)))
                                ;; NOTHING TO READ MEANS NOTHING TO CREATE. A fenced
                                ;; write to a key that does not exist either fails its
                                ;; check (no revision to match) or is a create, and
                                ;; `update-blob` makes its own blob either way — so
                                ;; opening this one would only produce an empty file
                                ;; we might never write.
                                ;;
                                ;; That empty file was the "ghost", and the cleanup
                                ;; written to remove it deleted BY PATH: on a backing
                                ;; that takes no sidecar (every `:global` one, where
                                ;; `lock-based-fencing?` is false) it ran unlocked and
                                ;; unlinked whatever was at the path — which is the
                                ;; winner's value in an ordinary create-if-absent
                                ;; race. Reproduced against MinIO: 10 of 10 keys, one
                                ;; peer told its fenced write SUCCEEDED and the key
                                ;; then missing. Worse on S3 than on a filestore,
                                ;; because there `-create-blob` writes nothing
                                ;; remotely, so there was never a ghost to collect and
                                ;; the delete was pure destruction.
                                ;;
                                ;; Not creating it retires the ghost, the cleanup, and
                                ;; the race in the cleanup together.
                                skip-blob? (and expected-revision (not exists-under-lock?))
                                blob (when-not skip-blob?
                                       (<?- (-create-blob backing store-key env)))
                                ;; In an atom, not a plain binding: the target is
                                ;; handed back EARLY in rename mode (below), and
                                ;; the `finally` must then find nothing to close.
                                held-blob (atom blob)]
                            ;; The value blob gets the same treatment as the
                            ;; sidecar, one level down, and for the same reason: it
                            ;; was opened in a `let` binding and closed in a
                            ;; `finally` below the lock acquisition, so a `get-lock`
                            ;; that threw — about a second of contention is enough —
                            ;; leaked the handle. Measured one file descriptor per
                            ;; failed attempt, which is a slow EMFILE for a process
                            ;; that retries. Fixing this at the sidecar and not here
                            ;; was an incomplete fix, not a different bug.
                            (try
                              (let [lock (when (and blob (:lock-blob? config))
                                           (log/trace :konserve/acquiring-blob-lock {:key key :blob (str blob)})
                                           (<?- (get-lock blob (first key-vec) env)))
                                    held-lock (atom lock)]
                                (try
                                  (let [old (cond
                                          ;; A FENCED write decides from the existence
                                          ;; probe taken UNDER the lock, not from the
                                          ;; pre-lock one.
                                              expected-revision
                                              (if exists-under-lock?
                                                (<?- (read-blob blob read-handlers serializers env))
                                                [nil nil])
                                          ;; full overwrite never needs the old value
                                              overwrite? [nil nil]
                                          ;; miss-safe non-overwrite write: no probe was done, so
                                          ;; read-first and treat an absent key as a fresh write.
                                              skip-write-probe?
                                              (try (<?- (read-blob blob read-handlers serializers env))
                                                   (catch #?(:clj Exception :cljs js/Error) e
                                                     (if (store-key-not-found? e) [nil nil] (throw e))))
                                          ;; probe said the key exists (or this is a retry): read old
                                              (or store-key-exists? (pos? attempt))
                                              (<?- (read-blob blob read-handlers serializers env))
                                              :else [nil nil])]
                                    (when expected-revision
                                      (check-revision! key expected-revision (first old)))
                                    ;; RENAME MODE: hand the target back BEFORE `update-blob`
                                    ;; moves `.new` over it. Everything the target was
                                    ;; opened for is done — `old` is read, the revision
                                    ;; checked. Holding it across the move is what made
                                    ;; the write fail on Windows, which refuses to replace
                                    ;; a file that has any open handle (POSIX renames
                                    ;; underneath one). And the lock released here never
                                    ;; guarded the move: it lives on the inode the rename
                                    ;; detaches — see `cas-lock-suffix` — which is why the
                                    ;; sidecar exists for the writes that need serializing,
                                    ;; and that sidecar's lock stays held. In-place mode
                                    ;; edits this very file and keeps both.
                                    (when (and write-op? (not (:in-place? config)))
                                      (<?- (release-held-lock! held-lock (first key-vec) env))
                                      (<?- (close-held-blob! held-blob env)))
                                    (if write-op?
                                  ;; The meta is computed ONCE and the same value is both
                                  ;; written and reported. Calling the meta-fn a second time
                                  ;; to learn the revision was correct only while the
                                  ;; revision was a counter derived from `old`; a minted
                                  ;; token differs on every call, so the caller was handed a
                                  ;; revision that had never been stored — and every chained
                                  ;; fenced write then failed against a head nobody else had
                                  ;; touched. Measured 60/60 wrong before this.
                                      (let [new-meta (when (:with-revision? env)
                                                       ((:up-fn-meta env) (first old)))
                                            env      (cond-> env new-meta (assoc :up-fn-meta (constantly new-meta)))
                                            res      (<?- (update-blob backing store-key serializer write-handlers env old))]
                                        (if new-meta
                                          [res (:revision new-meta)]
                                          res))
                                      old))
                                  (finally
                                    (<?- (release-held-lock! held-lock (first key-vec) env)))))
                              (finally
                                (<?- (close-held-blob! held-blob env)))))
                          (finally
                            (when cas-lock (<?- (-release cas-lock env))))))
                      (finally
                        (when cas-blob (<?- (-close cas-blob env))))))
                  (catch #?(:clj Exception :cljs js/Error) e
                    ;; DELIBERATELY not retried: `:konserve/revision-mismatch`.
                    ;; That conflict belongs to the CALLER — retrying would re-run
                    ;; `up-fn` against a value they never agreed to, which is the
                    ;; silent drift `:expected-revision` exists to prevent. Only a
                    ;; backend's own internal lock contention is retried here.
                    (if (and (pos? max-retries)
                             (= :optimistic-lock-conflict (:type (ex-data e)))
                             (< attempt max-retries))
                      ::retry
                      (throw e))))]
            (if (= result ::retry)
              (do
                (log/trace :konserve/optimistic-lock-retry {:key key :attempt (inc attempt) :max-retries max-retries})
                (recur (inc attempt)))
              result)))

        :else
        ;; Key is missing (and not migratable). Read callers that need to
        ;; distinguish a missing key from a stored nil pass a `:not-found`
        ;; sentinel; everyone else keeps getting nil as before.
        (:not-found env))))))

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
            (internal-artifact? store-key)
            (recur keys store-keys)

            (ends-with? store-key ".ksv")
            (let [keys-new (try
                             (let [blob        (<?- (-create-blob backing store-key env))
                                   env         (update-in env [:msg :keys] (fn [_] store-key))
                                   env    (assoc env :store-key store-key)
                                   lock   (when (and (:in-place? config) (:lock-blob? config))
                                            (log/trace :konserve/acquiring-blob-lock {:store-key store-key :blob (str blob)})
                                            (<?- (-get-lock blob env)))
                                   keys-new (try (conj keys (<?- (read-blob blob read-handlers serializers env)))
                                                     ;; it can be that the blob has been deleted, ignore reading errors
                                                 (catch #?(:clj Exception :cljs js/Error) _
                                                   keys)
                                                 (finally
                                                   (<?- (-release lock env))
                                                   (<?- (-close blob env))))]
                               keys-new)
                             (catch #?(:clj Exception :cljs js/Error) e
                               ;; If anything fails during key enumeration (blob creation, read, or cleanup),
                               ;; skip this key and continue. This handles concurrent deletes/modifications.
                               (log/trace :konserve/skipping-key-enumeration {:store-key store-key :error (ex-message e)})
                               keys))]
              (recur keys-new store-keys))

            :else ;; needs migration
            (let [additional-keys (<?- (-handle-foreign-key backing store-key serializer read-handlers write-handlers env))]
              (recur (into keys additional-keys) store-keys)))
          keys))))))

(defn prepare-multi-assoc
  "Prepares multiple key-value pairs for writing to the backing store.
   Handles serialization, metadata updates, and key translation."
  [backing serializers read-handlers write-handlers
   {:keys [kvs meta-up-fn default-serializer compressor encryptor version config] :as env}]
  (async+sync
   (:sync? env) *default-sync-translation*
   (go-try-
    (let [serializer (get serializers default-serializer)
          to-array #?(:cljs
                      (fn [value]
                        (-serialize ((encryptor  (:encryptor config)) (compressor serializer)) nil write-handlers value))
                      :clj
                      (fn [value]
                        (let [bos (ByteArrayOutputStream.)]
                          (try (-serialize ((encryptor (:encryptor config)) (compressor serializer))
                                           bos write-handlers value)
                               (.toByteArray bos)
                               (finally
                                 (.close bos))))))

          ;; Process each key-value pair
          results (loop [pairs []
                         pending-entries (seq kvs)]
                    (if-let [[key val] (first pending-entries)]
                      (let [store-key (key->store-key key)

                            ;; no reading, we will just reset it with fresh metadata here
                            old-meta nil

                            ;; Prepare serialized data
                            meta (meta-up-fn key :edn old-meta)
                            meta-arr (to-array meta)
                            meta-size (count meta-arr)
                            value-arr (to-array val)
                            header (create-header version
                                                  serializer compressor encryptor meta-size)

                            ;; Create serialized data structure
                            data {:store-key store-key
                                  :header header
                                  :meta-arr meta-arr
                                  :value-arr value-arr
                                  :meta-size meta-size
                                  :key key}]

                        (recur (conj pairs data) (rest pending-entries)))
                      pairs))

          ;; Map to format expected by backing store
          store-key-values (map (fn [{:keys [store-key header meta-arr value-arr]}]
                                  [store-key {:header header
                                              :meta meta-arr
                                              :value value-arr}])
                                results)]

      ;; Return the prepared data for backend
      {:store-key-values store-key-values
       :processed-pairs results}))))

(defrecord DefaultStore [version backing serializers default-serializer compressor encryptor
                         read-handlers write-handlers buffer-size locks config write-hooks]
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
    (let [{:keys [sync? with-revision?]} opts]
      (async+sync
       sync?
       *default-sync-translation*
       (go-try-
        ;; No upfront -exists? probe: io-operation already checks blob
        ;; existence (and migratability) exactly once and returns the
        ;; :not-found sentinel for missing keys. The previous double probe
        ;; cost an extra HEAD request per read on remote backends.
        (let [a (<?-
                 (io-operation this serializers read-handlers write-handlers
                               {:key-vec key-vec
                                :operation (if with-revision? :read-edn-meta :read-edn)
                                :compressor compressor
                                :encryptor encryptor
                                :format    :data
                                :version version
                                :sync? sync?
                                :buffer-size buffer-size
                                :config config
                                :default-serializer default-serializer
                                :not-found not-found-sentinel
                                :msg       {:type :read-edn-error
                                            :key  key}}))]
          (if with-revision?
            ;; [value revision] — the absent key reports the absent sentinel as its
            ;; revision, which is exactly what a create-if-absent write expects.
            (if (identical? a not-found-sentinel)
              [not-found absent]
              (let [[meta value] a]
                [(clojure.core/get-in value (rest key-vec)) (:revision meta)]))
            (if (identical? a not-found-sentinel)
              not-found
              (clojure.core/get-in a (rest key-vec)))))))))
  (-get-meta [this key opts]
    (let [{:keys [sync? revision-read?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :operation :read-meta
                     :revision-read? revision-read?
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
    (let [{:keys [sync? expected-revision with-revision?]} opts
          key (first key-vec)]
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
                     :overwrite? (empty? (rest key-vec))
                     :expected-revision expected-revision
                     :with-revision? with-revision?
                     :msg        {:type :write-edn-error
                                  :key  key}})))

  (-update-in [this key-vec meta-up up-fn opts]
    ;; `:with-revision?` is forwarded here for the same reason as in `-assoc-in`:
    ;; a caller that fences with `:expected-revision` needs the revision its write
    ;; PRODUCED in order to chain the next one. Omitting it here meant `update-in`
    ;; silently answered with the plain `[old new]` shape however the caller
    ;; asked, so a chained fenced update-in had nothing to fence against.
    (let [{:keys [sync? expected-revision with-revision?]} opts
          key (first key-vec)]
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
                     :expected-revision expected-revision
                     :with-revision? with-revision?
                     :msg        {:type :write-edn-error
                                  :key  key}})))
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
                  :ignore-existence? (:ignore-existence? opts)
                  :buffer-size buffer-size
                  :msg        {:type :deletion-error
                               :key  key}}))

  PBinaryKeyValueStore
  (-bget [this key locked-cb opts]
    (let [{:keys [sync? streaming?]} opts]
      (io-operation this serializers read-handlers write-handlers
                    {:key-vec [key]
                     :operation :read-binary
                     :default-serializer default-serializer
                     :compressor compressor
                     :encryptor encryptor
                     :config    config
                     :version version
                     :sync? sync?
                     :streaming? streaming?
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

  PAssocSerializers
  (-assoc-serializers [this serializers]
    (assoc this :serializers serializers))

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
                  :msg {:type :read-all-keys-error}})))

  PConditionalWrite
  (-conditional-write-domain [_]
    ;; ASK THE BACKING, do not infer from `:lock-blob?`. That flag says a lock is
    ;; requested, not that one exists: konserve's IndexedDB backing sets it and
    ;; implements `-get-lock` as a NO-OP ("the alternative is to overwrite
    ;; defaults/update-blob"), so inferring a domain from the flag would tell two
    ;; browser tabs they are fenced when nothing serializes them at all — precisely
    ;; the lie this capability exists to prevent. A backing that does not declare a
    ;; domain gets none, and `:expected-revision` is refused.
    (let [declared (when (satisfies? protocols/PConditionalWrite backing)
                     (protocols/-conditional-write-domain backing))]
      ;; `:lock-blob?` can revoke a claim that RESTS on konserve's lock, and only
      ;; such a claim. Without the flag there is no lock, the compare and the write
      ;; stop being one step, and the claim is void however sincerely it is made.
      ;;
      ;; A backing that fences ITSELF keeps its domain whatever the flag says: the
      ;; flag is about a lock it never uses. That used to be decided by testing the
      ;; domain for `:global`, which held only because the one self-fencing backing
      ;; happened to be global — see `PSelfConditionalWrite` for the stores that
      ;; fence themselves without reaching that far, which the old test would have
      ;; silently disarmed.
      ;; `:in-place?` revokes a SELF-fenced claim, and only that kind. The
      ;; storage layer evaluates the precondition inside `-sync`, but under
      ;; `:in-place? false` `update-blob` syncs to `<store-key>.new` and then
      ;; renames it into place — so the comparison is made against a key that by
      ;; construction does not exist (every such write is a create, and creates
      ;; succeed), and the `-atomic-move` that follows compares nothing at all. A
      ;; caller would be told its conditional write landed while no condition was
      ;; ever evaluated.
      ;;
      ;; A LOCK-based claim survives that layout, which is why this is not
      ;; hoisted out of the branch: there konserve holds the lock and evaluates
      ;; `check-revision!` itself, across the write AND the rename. The filestore
      ;; is `:in-place? false` by default and is fenced correctly; moving this
      ;; test above the `if` would silently disarm it.
      (if (satisfies? protocols/PSelfConditionalWrite backing)
        (when (:in-place? config) declared)
        (when (:lock-blob? config) declared))))
  (-revision [this key opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try- (let [m (<?- (protocols/-get-meta this key (assoc opts :revision-read? true)))]
                           (cond
                             (nil? m) absent
                             ;; A KEY WITH NO REVISION MUST NOT ANSWER nil. Every
                             ;; value written by this version has one, but a store
                             ;; upgraded from an earlier konserve does not, and
                             ;; neither does a key rebuilt by `migrate-file-v1`.
                             ;; Handing back nil looked like a token and was then
                             ;; accepted as one — nil is falsy, so it sailed past
                             ;; every conditional gate and wrote UNCONDITIONALLY,
                             ;; while the caller believed they had fenced. That is
                             ;; the documented read-then-hand-it-back pattern
                             ;; silently overwriting exactly the keys someone would
                             ;; try it on first. Refuse instead: rewrite the key
                             ;; once (an ordinary write mints a revision) and fence
                             ;; from there.
                             (nil? (:revision m))
                             (throw (ex-info (str "This key has no revision: it was written before konserve "
                                                  "recorded them, so there is nothing to fence against. Write "
                                                  "it once unconditionally to mint one.")
                                             {:type :konserve/revision-unavailable
                                              :key  key}))
                             :else (:revision m))))))

  PMultiKeySupport
  (-supports-multi-key? [_]
    (and (satisfies? PMultiWriteBackingStore backing)
         (satisfies? PMultiReadBackingStore backing)))

  PMultiKeyEDNValueStore
  (-multi-assoc [this kvs meta-up-fn opts]
    (let [{:keys [sync?]} opts]
      ;; First check if the backing store supports multi-writes
      (when-not (satisfies? PMultiWriteBackingStore backing)
        (throw (ex-info "Backing store does not support multi-key operations"
                        {:store-type (type backing)
                         :type :not-supported})))

      (let [env (merge opts
                       {:kvs kvs
                        :meta-up-fn meta-up-fn
                        :compressor compressor
                        :encryptor encryptor
                        :version version
                        :default-serializer default-serializer
                        :config config
                        :buffer-size buffer-size
                        :sync? sync?
                        :operation :write-edn
                        :msg {:type :multi-write-edn-error}})]

        (async+sync
         sync? *default-sync-translation*
         (go-try-
          ;; 1. Prepare the data for multi-key storage
          (let [prepared-data (<?- (prepare-multi-assoc backing serializers read-handlers write-handlers env))
                {:keys [store-key-values processed-pairs]} prepared-data

                ;; 2. Use the backing store's multi-write capability
                multi-result (<?- (-multi-write-blobs backing store-key-values env))]

            ;; 3. Map the results back to original keys
            (into {} (map (fn [{:keys [key store-key]}]
                            [key (get multi-result store-key true)])
                          processed-pairs))))))))

  (-multi-dissoc [this keys opts]
    (let [{:keys [sync?]} opts]
      ;; Check if backing store supports multi-writes (even though we're deleting)
      (when-not (satisfies? PMultiWriteBackingStore backing)
        (throw (ex-info "Backing store does not support multi-key operations"
                        {:store-type (type backing)
                         :type :not-supported})))

      (async+sync
       sync? *default-sync-translation*
       (go-try-
        ;; Convert keys to store-keys
        (let [store-keys (map key->store-key keys)
              env (merge opts {:sync? sync?})

              ;; Use backing store's multi-delete capability
              result (<?- (-multi-delete-blobs backing store-keys env))]

          ;; Map results back from store-keys to original keys
          (into {} (map (fn [key store-key]
                          [key (get result store-key false)])
                        keys store-keys)))))))

  (-multi-get [this keys opts]
    (let [{:keys [sync?]} opts]
      ;; Check if backing store supports multi-reads
      (when-not (satisfies? PMultiReadBackingStore backing)
        (throw (ex-info "Backing store does not support multi-key read operations"
                        {:store-type (type backing)
                         :type :not-supported})))

      (async+sync
       sync? *default-sync-translation*
       (go-try-
        ;; Convert keys to store-keys and track the mapping
        (let [keys-and-store-keys (map (fn [k] [k (key->store-key k)]) keys)
              store-keys (map second keys-and-store-keys)
              env (merge opts {:sync? sync?})

              ;; Use backing store's multi-read capability to get blobs
              store-key-to-blob (<?- (-multi-read-blobs backing store-keys env))]

          ;; Deserialize each blob and build result map (sparse - only found keys)
          (loop [result {}
                 pending keys-and-store-keys]
            (if-let [[key store-key] (first pending)]
              (if-let [blob (get store-key-to-blob store-key)]
                ;; Blob exists, deserialize it
                (let [read-env (assoc env :store-key store-key
                                      :operation :read-edn
                                      :config config)
                      value (<?- (read-blob blob read-handlers serializers read-env))]
                  (recur (assoc result key value) (rest pending)))
                ;; Blob doesn't exist, skip this key (sparse map)
                (recur result (rest pending)))
              result)))))))

  PWriteHookStore
  (-get-write-hooks [_] write-hooks)
  (-set-write-hooks! [this hooks-atom]
    (assoc this :write-hooks hooks-atom)))

(def ^:const encoding-keys
  "What `:config :encoding` may contain.

  The first three ARE the blob header -- serializer, compressor and encryptor
  are bytes 1, 2 and 3 of every blob, and a later reader dispatches on them.
  That is what makes this group real rather than a tidy-up: get one wrong and
  the bytes on disk mean something else.

  The handlers and the serializer registry are not in the header. They are
  here because they are about turning a value into bytes, and because keeping
  them next to `:serializer` is what makes \"your handlers must match what was
  written\" legible -- separating them is how a record wire-name change went
  unnoticed until a test asserted equality with the input."
  #{:serializer :compressor :encryptor :serializers :read-handlers :write-handlers})

(defn- deprecated!
  [from to]
  (log/warn :konserve/deprecated-config
            {:msg (str "konserve: " from " is deprecated; use " to
                       ". Both work for now.")
             :from from :to to}))

(defn normalize-store-config
  "A store config in canonical shape: everything about value-to-bytes under
  `:config :encoding`.

      {:backend :file :path \"...\" :id #uuid \"...\"      ; which store
       :config {:encoding {:serializer :BoringSerializer  ; header byte 1
                           :compressor {:type :zstd}      ; header byte 2
                           :encryptor  {:type :aes}}      ; header byte 3
                :sync-blob? true                          ; local policy
                :in-place?  false}}

  Three levels, three rules: the top says WHICH store, `:encoding` says how a
  value becomes bytes and is durable, the rest of `:config` is local policy you
  can change between runs without anything on disk caring.

  OLD SPELLINGS STILL WORK and warn. `:default-serializer` and the handler maps
  used to sit at the top level while `:compressor` sat inside `:config`, with
  no rule saying why -- which is how five backends each guessed differently and
  three of them silently ignored what they were handed.

  Canonical wins if both are present, so a caller migrating one key at a time
  never gets a surprise from the leftover."
  [params]
  (let [enc (get-in params [:config :encoding] {})
        ;; Old top-level spellings. These DID work through `connect-fs-store`,
        ;; so they are the real deprecation surface; the same keys through the
        ;; lifecycle API were dropped and cannot have been relied on.
        enc (reduce (fn [e [old new]]
                      (if (and (contains? params old) (not (contains? e new)))
                        (do (deprecated! old (str ":config :encoding " new))
                            (assoc e new (get params old)))
                        e))
                    enc
                    [[:default-serializer :serializer]
                     [:serializers :serializers]
                     [:read-handlers :read-handlers]
                     [:write-handlers :write-handlers]])
        ;; And the old `:config` spellings, which are what works today.
        enc (reduce (fn [e k]
                      (if (and (contains? (:config params) k) (not (contains? e k)))
                        (do (deprecated! (str ":config " k) (str ":config :encoding " k))
                            (assoc e k (get-in params [:config k])))
                        e))
                    enc
                    [:compressor :encryptor])
        bad (remove encoding-keys (keys enc))]
    (when (seq bad)
      (throw (ex-info (str "konserve: unknown key(s) in :config :encoding: "
                           (pr-str (vec bad)) ". Accepted: "
                           (pr-str (vec (sort encoding-keys))) ".")
                      {:type :store-configuration-error :unknown (vec bad)})))
    (-> params
        (dissoc :default-serializer :serializers :read-handlers :write-handlers)
        (update :config #(-> (or % {}) (dissoc :compressor :encryptor)
                             (assoc :encoding enc))))))

(defn assert-encoding-supported!
  "Throw unless the config's encoding is one `backend-name` can honour.

  `supported` is `{:serializers #{...} :compressors #{...} :encryptors #{...}}`
  of ACCEPTED values; `nil` for a slot means anything goes. Compressor and
  encryptor are named by `:type` keyword, with `:none` standing for absent.

  For backends that cannot honour an arbitrary encoding -- konserve-lmdb uses
  its own buffer format, and others hardcoded a serializer for years -- so that
  they REFUSE rather than accept and quietly write something else. Every config
  bug this shape has produced was a backend accepting a setting it then
  ignored; a shared helper means the refusal reads the same everywhere instead
  of being spelled five ways or not at all."
  [backend-name config {:keys [serializers compressors encryptors]}]
  (let [enc (get-in config [:config :encoding] {})
        chk (fn [allowed got what]
              (when (and allowed (not (contains? allowed got)))
                (throw (ex-info (str "konserve: " backend-name " does not support "
                                     what " " (pr-str got) ". Supported: "
                                     (pr-str (vec (sort-by str allowed))) ".")
                                {:type :store-configuration-error
                                 :backend backend-name what got}))))]
    (chk serializers (or (:serializer enc) :FressianSerializer) :serializer)
    (chk compressors (or (get-in enc [:compressor :type]) :none) :compressor)
    (chk encryptors  (or (get-in enc [:encryptor :type]) :none) :encryptor))
  config)

(defn connect-default-store
  "Create general store in given base of backing store."
  [backing
   {:keys [default-serializer serializers
           read-handlers write-handlers
           buffer-size config opts]
    :or   {default-serializer :FressianSerializer
           read-handlers      (atom {})
           write-handlers     (atom {})
           buffer-size        (* 1024 1024)
           opts               {:sync? false}}
    :as   params}]
  ;; NORMALISED FIRST, so everything below reads one shape. `params` is what
  ;; the caller wrote, in any accepted spelling; `p` is canonical.
  (let [p (normalize-store-config params)
        config (:config p)
        enc (:encoding config)
        default-serializer (or (:serializer enc) default-serializer)
        serializers (or (:serializers enc) serializers)
        read-handlers (or (:read-handlers enc) read-handlers)
        write-handlers (or (:write-handlers enc) write-handlers)
        ;; `:compressor`/`:encryptor` are ALSO left at `:config` level, because
        ;; the use sites read them there: `update-blob` applies the encryptor
        ;; to `(:encryptor config)`, which is where its KEY lives, not just its
        ;; type. Moving them under `:encoding` and stopping there silently
        ;; stripped the encryption key -- 34 tests in the compressor/encryptor
        ;; matrix caught it. `:encoding` is where a caller CONFIGURES them;
        ;; this keeps the existing readers working until they are migrated too.
        complete-config (merge {:sync-blob? true
                                :in-place? false
                                :lock-blob? true}
                               config
                               (select-keys enc [:compressor :encryptor]))
        compressor (get-compressor (get-in enc [:compressor :type]))
        encryptor (get-encryptor (get-in enc [:encryptor :type]))]
    ;; A top-level `:compressor`/`:encryptor` is REFUSED rather than ignored.
    ;; Both are read from `config` above, so passing a function at the top
    ;; level -- which reads exactly like it should work, and which
    ;; `connect-fs-store` itself used to hand us as a default -- did nothing
    ;; at all: the store came back on `null-compressor` and wrote a 0 into
    ;; every blob header while the caller believed their data was compressed.
    ;; Silence is the wrong answer for a durable property nobody re-checks.
    ;; Only a MEANINGFUL one is refused. konserve-rocksdb passes
    ;; `:compressor null-compressor` and `:encryptor null-encryptor` as dead
    ;; defaults -- the same pattern konserve's own filestore carried until it
    ;; was removed -- and throwing on those would break a maintained backend on
    ;; upgrade for a value that asks for nothing. Passing `lz4-compressor`,
    ;; which is someone actually trying to configure compression and silently
    ;; getting none, is what has to be loud.
    (when (or (and (contains? params :compressor)
                   (not= (:compressor params) null-compressor))
              (and (contains? params :encryptor)
                   (not= (:encryptor params) null-encryptor)))
      (throw (ex-info (str "konserve: :compressor and :encryptor are set under "
                           ":config, not at the top level. Write "
                           "{:config {:compressor {:type :lz4}}} -- a TYPE "
                           "keyword, not a function. Accepted types: :lz4, "
                           ":zstd, or omit for none.")
                      {:type :store-configuration-error
                       :got (select-keys params [:compressor :encryptor])})))
    (async+sync
     (:sync? opts) *default-sync-translation*
     (go-try-
      (if (and (:in-place? complete-config) (not (:lock-blob? complete-config)))
        (throw (ex-info "You need to activate file-locking for in-place mode."
                        {:type :store-configuration-error
                         :config complete-config}))
        (let [;; ENSURE means make-it-so, not write-regardless. This used to
              ;; call `-create-store` unconditionally on every connect, which on
              ;; an object-store backing is a PUT of the store marker — so every
              ;; connect to an EXISTING store performed a write it had no reason
              ;; to perform. Two practical consequences, both measured against
              ;; S3 (replikativ/datahike-serverless#6): a reader holding
              ;; read-only credentials could not connect at all, and a cold
              ;; open cost 2 PUTs (datahike probes and then connects, so the
              ;; marker was written twice) where the correct number is zero.
              ;;
              ;; Probing first makes the existing-store path READ-ONLY — a
              ;; HEAD/stat instead of a write — while auto-create semantics are
              ;; untouched for a missing store. The first-connect race is
              ;; benign and no worse than before: two concurrent creators both
              ;; write a marker whose content is constant, and `-create-store`
              ;; has always had to be idempotent — until this change it ran on
              ;; EVERY connect.
              ;; A backing whose create IS its open (IndexedDB sets its db
              ;; handle in -create-store) still has to open an existing store:
              ;; skipping create there left the handle nil and every later
              ;; transaction failed. `PBackingOpen` is the read-only half.
              _                  (if (<?- (-store-exists? backing opts))
                                   (when (satisfies? PBackingOpen backing)
                                     (<?- (-open-store backing opts)))
                                   (<?- (-create-store backing opts)))
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
                                                     :config              complete-config
                                                     :write-hooks         (atom {})})]
          store))))))
