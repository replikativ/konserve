# Changelog

All notable, user-visible changes to konserve are documented here.

## Unreleased

### Added
- **Conditional writes (`:expected-revision`), with an explicit capability.** A
  write can now be made conditional on the revision the caller read: it lands
  only if the stored revision is still that one, and otherwise raises
  `{:type :konserve/revision-mismatch}` having written nothing and — for
  `update-in` — WITHOUT running `up-fn`. Pass
  `konserve.core/absent` to mean "only if this key does not exist".
  `:with-revision? true` reports the revision a read saw or a write produced, so
  a fencing caller can chain writes without a re-read; on a read it returns
  `[value revision]`, on a write `[[old new] revision]`.

  The capability is EXPLICIT and a store that lacks it REFUSES rather than
  ignoring the option — silently degrading to an unconditional write is worse
  than having no fencing, because the caller asked for a guarantee and got a knob
  that reads as handled. `conditional-write-domain` reports how far a store's
  guarantee reaches (`:process`, `:machine`, `:global`) rather than a boolean,
  since the API is uniform and the guarantee is not: `true` would let someone
  running two processes against a memory store, or two machines against a
  filestore, believe they were fenced when they were only serialized against a
  narrower set of writers. `conditional-write?` compares against a domain you
  need. Backed by `konserve.compliance-test/conditional-write-compliance-test`,
  which any backend can call — a store without the capability passes it by
  refusing.
- **Simulation testing harness** (`konserve.simulation.backing`, `.crash`,
  `.memory`). A fault-injecting wrapper around any `PBackingStore` (19
  per-operation error and corruption rates, seeded from a `SplittableRandom` so
  runs replay exactly), a crash simulator that tracks sync points and discards
  unsynced state at each of the six steps of the `DefaultStore` write path, and
  an in-memory backing store for tests that should not touch a filesystem. Comes
  with 99 tests / 2427 assertions covering error propagation, atomicity,
  durability across reopen, orphaned `.new`/`.backup` handling, GC under crash,
  and concurrency and resource-exhaustion stress. Two properties carry most of
  the weight: corruption is never silently absorbed (a read returns exactly what
  was written or fails), and acknowledged writes are durable under chaos, swept
  across seeds. The suite is mutation-tested — removing fault injection, crash
  injection, resource limits, deletes or konserve's per-key lock each produces
  failures. Folded in from an out-of-tree repository so the backing
  implementations track the protocols they wrap. See
  [doc/simulation-testing.md](doc/simulation-testing.md).

  These namespaces are marked `^:no-doc` and are **internal**: they ship in the
  jar so backends and downstream projects can test against them, but they carry
  no compatibility guarantee and may change or move in any release.
- **Monotonic write stamps** — `:last-write` metadata is now issued by a
  process-global monotone clock (`utils/now` = `max(wall-clock,
  previous-stamp)`): stamps never go backwards under wall-clock retreat
  (NTP step-backs, VM suspend/resume, manual clock sets) — the stamp holds
  at its high-water mark until real time catches up. This makes
  `konserve.gc/sweep!`'s safety argument hold by construction: an object
  written under a collector's guard can no longer be stamped *before* the
  cutoff that protects it and be deleted while live. Deliberately
  non-strict (no `+1` per stamp): a strict clock would run ahead of
  physical time above 1000 writes/second and stall collection after a
  restart following a bulk import; same-millisecond ties remain possible
  and are fail-safe (the sweep spares equality — garbage retained one
  cycle, never a live deletion). Stamps still read as wall time (`Date`);
  external collectors must obtain their cutoffs from
  `utils/now`/`utils/monotonic-now-ms` (the same source) rather than raw
  clock reads. Single-process only, as before.
- **`PReadMissSafe` marker protocol** (`konserve.impl.storage-layout`). A backing
  store implements it to declare that a read of an absent key is side-effect-free
  and reports the miss cleanly — its `-read-header` throws
  `(store-key-not-found-ex store-key)` on an absent key, with no side effect. When
  a backing declares it, `io-operation` learns existence from the read itself and
  skips the separate `-blob-exists?` probe, removing a redundant round-trip (an S3
  `HEAD` before the `GET`). The default filestore deliberately does **not**
  implement it — its `-create-blob` opens with `CREATE` and would materialise an
  empty blob on a probe-free missing read — so filestore behaviour is unchanged.
  New helpers: `store-key-not-found-ex`, `store-key-not-found?`,
  `store-key-not-found`.
- **`dissoc` opt `:ignore-existence?`.** `dissoc` normally probes with
  `-blob-exists?` so it can return whether the key existed (`true`) or was absent
  (`false`) — konserve's contract, enforced by the compliance suite. A caller that
  does not need that boolean (e.g. a GC bulk sweep) can pass
  `{:ignore-existence? true}` to skip the probe on a `PReadMissSafe` backing (whose
  delete is idempotent), returning `true`. On non-miss-safe backings the hint is
  ignored and the probe stays.

- **The IndexedDB backend implements `PReadMissSafe`.** A browser read was two
  IndexedDB transactions — `.getKey` (the `-blob-exists?` probe) then `.get` — and
  is now a single `.get` (read-modify-write ops drop their `.getKey` too). Its
  `-create-blob` is side-effect-free and `read-blob` now signals
  `store-key-not-found-ex` on an absent key. (`dissoc`'s single-key fast path also
  honours `:ignore-existence?`; the multi-key GC delete path is a separate
  follow-up.)
- **`:BoringSerializer` (serializer byte 3)** — [boring](https://github.com/replikativ/boring),
  an RFC 8949 CBOR codec that runs on the JVM *and* ClojureScript from one
  implementation. Unlike `:CBORSerializer` (byte 2, clj-cbor) it accepts read
  handlers rather than throwing on them, and unlike `:FressianSerializer` it is
  not JVM-only. Because the payload is standard CBOR, a store written by
  Clojure can be read by any language with a CBOR library:
  `interop/read_konserve_blob.py` is the whole format in one file, and
  `konserve.interop-python-test` runs it against a real blob so it cannot drift
  from what konserve writes.
- **`:zstd` compressor (compressor byte 2)**, via the optional dependency
  `com.github.luben/zstd-jni`. Loaded reflectively so the native binaries stay
  out of every user's dependency graph; when absent, byte 2 resolves to a
  compressor that throws an actionable message instead of the namespace failing
  to load. On one 512-datom blob zstd-3 was 23x faster than LZ4-HC *and* about
  half the size, which is why `:lz4` stays the fast compressor rather than
  being switched to the high one.

### Fixed
- **`multi-get`, `multi-dissoc` and a copy-and-rename layout no longer drop
  `:expected-revision` silently.** Three ways the option could be asked for and
  not honoured, each of which now raises instead:

  - `multi-get` forwarded it to the backing. A read cannot be fenced, so on its
    own that is merely meaningless — but a backing that fences ITSELF has to
    remember, for the duration of one conditional write, the metadata konserve
    read under the lock, because `-sync` runs on a different blob record than the
    read did. `multi-get` takes no per-key lock and closes no rows, so such a
    read looked exactly like the read belonging to an in-flight fenced write and
    replaced the metadata that write was about to compare against. Reproduced on
    konserve-jdbc: the fenced write reported SUCCESS and overwrote the value that
    had committed in between.
  - `multi-dissoc` forwarded it into `-multi-delete-blobs`, where every backing
    ignores it, so a caller asking for a fenced batch delete was told it
    happened. `multi-assoc` has always refused; this is the same rule.
  - A backing that declares `PSelfConditionalWrite` now LOSES its domain when the
    store is configured `:in-place? false`. That layout writes `<store-key>.new`
    and renames it into place, so the storage layer's precondition is evaluated
    against a key that by construction does not exist — every such write is a
    create, and creates succeed — while the rename that follows compares nothing.
    A lock-based claim is unaffected and must be: there konserve holds the lock
    and evaluates the revision itself, across the write and the rename, which is
    how the filestore (`:in-place? false` by default) is fenced correctly.

  Every released self-fencing backend sets `:in-place? true`, so no default
  deployment was affected; `connect-default-store`'s own default is `false`, and
  a caller's `:config` is merged over a backend's, so the guarantee rested on a
  flag rather than on anything structural.

- **Compression combined with encryption never worked.** The read path nested
  the wrappers the opposite way round from the write path, so it tried to
  *decompress ciphertext*: `zstd` + `aes` failed with "Unknown frame
  descriptor", `lz4` + `aes` with "Stream unsupported". Both defaults are null,
  and null is identity, so the order was invisible in every configuration the
  tests covered — while the README documents compression and encryption
  configured together. A new `konserve.compressor-encryptor-matrix-test` walks
  every compressor × encryptor × serializer combination through a real store,
  and asserts that a compressed+encrypted blob is smaller than an
  encrypted-only one, which pins the order rather than just the round trip.
- **The LZ4 compressor truncated its own frames.** It called `.flush` rather
  than `.close`, and `LZ4FrameOutputStream.flush` does not write the frame's
  EndMark. Fressian never noticed, because it stops reading at the end of a
  value; the CBOR serializer reads to EOF and hit the truncation with "Stream
  ended prematurely". A compressor bug that only one serializer could see,
  which is why the matrix runs over both.
- **LZ4 was broken on every GraalVM JDK, not just native images.**
  `native-image-build?` tested only whether `org.graalvm.nativeimage.ImageInfo`
  was on the classpath — and that class ships with every GraalVM JDK, so an
  ordinary JVM run on GraalVM took the native-image path: NPE on write, throw
  on read. It now asks `ImageInfo/inImageCode`, which is the question that was
  meant.
- **A `:frontend-only` tiered store no longer deletes the shared backend.** `-delete-store
  :tiered` deleted the backend unconditionally. Under `:write-policy :frontend-only` the
  store is a read-through **cache** over a backend that another peer OWNS and that this
  one must never write — deleting is the most destructive write there is, so a cache peer
  calling `delete-store` (or Datahike's `delete-database`) would take the authoritative
  data with it. It now deletes only its own cache; under every other policy the store owns
  its backend and both tiers go. This was latent while tiered delete silently did nothing
  (see below) — fixing the missing await made it reachable.
- **Node file backend: `delete-store-async` was broken in three ways, and never ran.**
  Wiring `-delete-store :file` to the async variant (above) exposed it. `iofs/arm-r`
  yields **`[?err]`** — a vector — but it was bound as a bare `?err`, so the success
  value `[nil]` was truthy and the function **always took the error branch**: it
  returned `[nil]` *as if it were an error* and never reached the fsync at all. Once
  that was fixed, two more surfaced: it fsynced `base` — the directory `arm-r` had just
  deleted — where the sync twin correctly fsyncs the **parent**; and `sync-base-async`
  called `.force` on the result of `open-async-file-channel` without checking whether
  it was an `Error`. It now returns nil on success and the error on failure, matching
  the sync `delete-store`.
- **`delete-store` now honours `:sync?` — and `:tiered` actually deletes.**
  `-delete-store` was the one store method that ignored its `opts`: `:memory` and
  `:file` (JVM and Node) returned a plain value whatever `:sync?` said, so an async
  caller could not await the deletion — and `delete-store` defaults to
  `{:sync? false}`, so async is the *common* path. Worse, `:tiered` called
  `(delete-store backend-config)` with **no opts** — the async default — and then
  dropped the returned channel, so **deleting a tiered store over an async backend
  (e.g. S3) removed nothing at all**, silently, with any error swallowed into a
  channel nobody read. All four implementations now follow the same contract every
  other store method obeys: a value under `{:sync? true}`, otherwise a channel that
  delivers when the deletion is *complete*. The Node file backend now also uses its
  existing non-blocking `delete-store-async` on the async path.
  The contract is documented on the `-delete-store` multimethod and pinned by tests
  (previously `memory-store-delete` *asserted* the broken behaviour).

### Changed
- **A fenced key carries a sidecar lock file (`<key>.cas`).** It exists because
  konserve replaces a value by renaming a new file over it, which orphans a lock
  taken on the old one; the sidecar is never renamed, so it is a stable thing to
  lock. Every write to a key that has one takes it, so the fence excludes
  unconditional writers too — without that it would grant a false pass and lose
  their write. A key gets a sidecar the first time a conditional write or a
  revision-bearing read touches it, so keys that are never fenced cost one
  existence probe and no extra file. Backends that filter their own key
  enumeration must skip this suffix (see `konserve.impl.defaults/internal-artifact?`).
- **Fencing orders the writers that participate.** Conditional writes are
  optimistic concurrency control: a writer that writes unconditionally overwrites
  whatever is there, here as on S3 or against a row-version column. Fencing a key
  means every writer to it fences. Konserve does go further than that where it
  can — once a key has a sidecar, unconditional writers take its lock too — but a
  deployment that mixes fenced and unfenced writers to one key is not protected
  by any of this, and should not be read as if it were.
- **Every value's metadata now carries a `:revision`.** It is what
  `:expected-revision` compares and what `konserve.core/revision` returns — an
  OPAQUE token, minted per write, to be passed back rather than interpreted. It
  is deliberately not `:last-write`: that clock is non-decreasing and admits
  same-millisecond ties, so two writes could share one value and a fence built on
  it would pass when it should fail. The visible consequence is that metadata
  maps have one more key, so a test or tool comparing them whole (`k/keys`,
  `get-meta`) should strip `:revision` alongside `:last-write`.
- **Probe-elision now covers non-overwrite writes, not only reads.** On a
  `PReadMissSafe` backing, `update-in` / `update` / nested `assoc-in` / `bassoc`
  read the old value *read-first* (an absent key → a fresh write) instead of a
  `HEAD` probe followed by the read. A read-modify-write on an existing key drops
  from `HEAD` + `GET` + `PUT` to `GET` + `PUT`. Pure reads (`get` / `bget` /
  `get-meta`) were already a single `GET` on a miss-safe backing.
- **`konserve.gc/sweep!`** passes `:ignore-existence?` on its single-key delete
  fallback, so GC on a miss-safe store deletes each dead key without a per-key
  `HEAD` probe (the batch `multi-dissoc` path was already probe-free).
