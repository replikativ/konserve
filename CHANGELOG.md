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
