# Changelog

All notable, user-visible changes to konserve are documented here.

## Unreleased

### Added
- **`:aes-gcm` encryptor** — AES-256-GCM authenticated encryption (header byte 2),
  via geheimnis v2. Unlike the old `:aes`, it *detects* tampering: a flipped,
  truncated or relocated blob fails its authentication tag and raises instead of
  being deserialized into a value. Each written blob gets a fresh CSPRNG salt from
  which a per-blob key is derived (HKDF-SHA-256), so no key ever encrypts more than
  one message and GCM's nonce-reuse failure mode cannot arise. The associated data
  binds each ciphertext to its layout version, store-key and slot (`:meta` /
  `:value`), so a blob cannot be moved to another key or slot and still verify. The
  format is byte-identical on JVM and ClojureScript.

  ```clojure
  {:encryptor {:type :aes-gcm :key (konserve.encryptor/generate-key)}}
  ```

  The key must be 256 bits — 32 raw bytes or a 64-character hex string. konserve
  does not stretch passphrases; a guessable one would be brute-forced offline
  against the stored blobs.

  On ClojureScript `:aes-gcm` requires the asynchronous API: Web Crypto exposes no
  synchronous cipher, so `{:sync? true}` is rejected. The JVM supports both.

- **`konserve.protocols/PEncryptor`** — encryption is now its own protocol
  (`-encrypt` / `-decrypt` over whole byte arrays) rather than a `PStoreSerializer`
  decorator. An AEAD cipher has to verify its tag over the complete ciphertext
  before any plaintext may reach the deserializer, so there was nothing to stream.
  Custom encryptors implement `PEncryptor`; the ops return a channel, or the byte
  array directly when `(:sync? env)`.

- **`konserve.core/seal` and `unseal`** — encrypt raw bytes under the store's own
  key, bound to a konserve key. konserve does not encrypt binary values (see below),
  so if you want a `bassoc`'d payload encrypted, this is how to do it without
  managing a second key. The ciphertext is bound to the key and layout version, so
  bytes sealed under one key do not unseal under another, and (with `:aes-gcm`)
  tampering is rejected rather than returned as garbage.

  ```clojure
  (k/bassoc store :thumb (<? (k/seal store :thumb raw-bytes)) {:raw? true})
  (k/bget store :thumb
          (fn [{is :input-stream}] (go (<? (k/unseal store :thumb (slurp-bytes is)))))
          {:raw? true})
  ```

- **`:raw?` opt on `bassoc` / `bget`** — the explicit opt-out for binary values on
  an encrypted store: "I own this format and its confidentiality." Required there,
  since binary bytes pass through unencrypted (below). It keeps its meaning if
  binary later becomes encrypted by default.

- **`konserve.core/encrypted?`** — whether a store encrypts the values it writes.

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
- **Binary values on an encrypted store now require an explicit choice.**
  `bassoc`/`bget` pass your bytes to storage untouched — the format is yours, and
  konserve has never encrypted them. On a store with an encryptor configured that
  used to happen silently, which is not something a user of an encrypted store would
  expect. Both now raise unless you say which you meant: `seal`/`unseal` the payload
  under the store's key, or pass `{:raw? true}` to own it yourself. Encrypting binary
  by default needs chunked AEAD framing to keep `bassoc` streaming, which is a
  separate change; `:raw?` survives it unchanged.

### Fixed
- **The IndexedDB store ignored `:config` entirely.** `connect-idb-store` dropped the
  caller's `:config` (`(dissoc params :config)`) and always used its own hardcoded
  map, so `{:encryptor {...}}` and `{:compressor {...}}` were silently discarded —
  a browser store asked for encryption got none. It now merges the caller's config
  over the defaults, as both filestores already did. This means the existing
  IndexedDB `:aes` encryptor test had been passing vacuously against an unencrypted
  store; it now exercises the cipher for real.
- **`:lz4` combined with an encryptor was unreadable.** Writes nested the encryptor
  outside the compressor (compress, then encrypt) while reads nested them the other
  way round, so a read tried to LZ4-decompress the ciphertext. The `PEncryptor`
  split makes the order explicit and symmetric. Only stores using both features were
  affected; the default compressor is the null one.

### Deprecated
- **The `:aes` encryptor** (unauthenticated AES-256-CBC, geheimnis v1). It cannot
  detect tampering, and its per-blob salt comes from `Math.random` on ClojureScript
  rather than a CSPRNG. Existing blobs remain readable and writable — the on-disk
  format is unchanged, pinned by a golden vector in the test suite — but new stores
  should use `:aes-gcm`. Note also that its JVM and JS salt encodings disagree, so a
  blob written on one platform fails to decrypt on the other roughly one time in
  five; this is preserved rather than fixed, since fixing it would break the stores
  it currently works for.
