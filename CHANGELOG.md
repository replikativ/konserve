# Changelog

All notable, user-visible changes to konserve are documented here.

## Unreleased

### Added
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

### Fixed
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
- **Probe-elision now covers non-overwrite writes, not only reads.** On a
  `PReadMissSafe` backing, `update-in` / `update` / nested `assoc-in` / `bassoc`
  read the old value *read-first* (an absent key → a fresh write) instead of a
  `HEAD` probe followed by the read. A read-modify-write on an existing key drops
  from `HEAD` + `GET` + `PUT` to `GET` + `PUT`. Pure reads (`get` / `bget` /
  `get-meta`) were already a single `GET` on a miss-safe backing.
- **`konserve.gc/sweep!`** passes `:ignore-existence?` on its single-key delete
  fallback, so GC on a miss-safe store deletes each dead key without a per-key
  `HEAD` probe (the batch `multi-dissoc` path was already probe-free).
