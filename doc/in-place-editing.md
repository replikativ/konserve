# Reading and editing a value without decoding it

`konserve.mmap` reaches into a stored value **without materialising it**. A blob
the boring serializer wrote is ordinary CBOR at a known offset in a file, so it
can be memory-mapped and walked — reaching one field, or changing one field,
without decoding (or re-encoding) the rest. On a large value that is the
difference between microseconds and milliseconds, and between a page write and a
whole-file rewrite.

Two halves, both here:

- **Read** — navigate to a field and realise only that (`with-mmap-value`,
  `value-location`, `navigable?`).
- **Write** — `update-in!` / `assoc-in!` / `dissoc-in!` edit a value in place or
  by splicing only the changed bytes.

> **EXPERIMENTAL, filestore-only.** The shape of this API may change. The write
> ops that edit in place need JDK 22+ (`java.lang.foreign`); without it they fall
> back to a whole-file rewrite, which needs nothing.

---

## What a value must be, and the fallback when it is not

Both halves work only on a blob that is:

- **boring** (serializer id 3) — a Fressian or clj-cbor blob is not CBOR a cursor
  can walk;
- **uncompressed** and **unencrypted** (compressor 0, encryptor 0) — either would
  have to be undone whole first, which is the cost this avoids;
- **not stringref-wrapped** — a stringref value's byte lengths depend on
  everything encoded before it, so it cannot be edited in isolation.

Encoding is **per blob, not per store** (the header records it — see
[Reading a store from another language](../README.org)), so a store may hold a
mix. `navigable?` and `edit-eligible?` answer the question for one key without
throwing; every write op checks it and, when the blob is ineligible, **falls back
to the ordinary `konserve.core` op**. Dispatch is therefore always safe — the
worst case is a correct answer at full cost, never a wrong one.

### Configuring a store so its values are editable

boring is stringref-off and deterministic only under `:archival` (or
`:canonical`). Configure the serializer by NAME (konserve keys serializers by
keyword, not by byte):

```clojure
(require '[konserve.filestore :refer [connect-fs-store]]
         '[konserve.serializers :as ser]
         '[boring.core :as boring])

(connect-fs-store
  "/path/to/store"
  :serializers {:BoringSerializer
                (ser/boring-serializer (boring/tag-registry) {:profile :archival})}
  :default-serializer :BoringSerializer
  :config {:id (random-uuid)}
  :opts {:sync? true})
```

A store left on boring's default writes stringref blobs, which these ops
correctly refuse (falling back) — so nothing breaks, the fast path just never
engages.

---

## Reading without decoding

```clojure
(require '[konserve.mmap :as kmm] '[boring.nav :as nav])

(kmm/with-mmap-value [c store "customers"]
  (nav/value (get-in c ["customer-137" "name"])))
;; walks the wire format to that one key; the other customers are never built
```

- **`with-mmap-value`** binds a `boring.nav` cursor over the value and closes the
  mapping after the body. Do not let the cursor escape the body — the mapping is
  gone, and touching it afterward throws a typed FFM error rather than reading
  freed memory.
- **`mmap-value`** is the same without the macro's scope, for a caller that must
  manage the arena lifetime itself.
- **`value-location`** returns `[path offset]` — the file and the byte offset the
  value begins at — for a caller who wants to hand the offset to something other
  than a cursor (a ranged reader, a checksum).
- **`navigable?`** is the boolean form: cheap to ask (reads the 20-byte header),
  and the natural guard for a mixed store —

  ```clojure
  (if (kmm/navigable? store k)
    (kmm/with-mmap-value [c store k] (nav/value (get-in c path)))
    (get-in (k/get store k nil {:sync? true}) path))
  ```

See [`boring`'s reading docs](https://github.com/replikativ/boring/blob/main/doc/INDEX.md)
for how an offset index turns a walk into a jump.

---

## Editing without decoding

`update-in!` / `assoc-in!` / `dissoc-in!` mirror their `konserve.core`
counterparts but edit the blob directly. `key-vec` is `[store-key & path]` — the
first element names the value, the rest is the path into it, resolved on the
container like `clojure.core/get-in` (a map key of any type, a vector index).

```clojure
(kmm/assoc-in!  store [:doc :section :field] 42)
(kmm/update-in! store [:doc :counter] inc)
(kmm/dissoc-in! store [:doc :section :stale])
```

- A **same-length** change is a *poke* — the value's bytes are overwritten in
  place, any offset index stays valid.
- A **size-changing leaf** change is a *splice* — only the altered bytes move.
- A **structural** change (a new key, a removed key) re-encodes just the parent
  container.

Which of those runs is chosen automatically. What you pick is **durability**.

### Durability

```clojure
(kmm/assoc-in! store [:doc :field] v {:durability :checked})
```

| `:durability` | how | crash behaviour | cost |
|---|---|---|---|
| **`:rename`** (default) | never mutates in place; writes `.new` + fsync + atomic rename + directory fsync | crash-safe by construction | O(file) |
| **`:checked`** | edits in place (no copy), guarded by a dirty marker | a crash is **detectable** via `torn?`; reconstruct from source | O(dirty pages) |
| **`:raw`** | edits in place, no marker | relies on nothing | cheapest |

The instant same-length poke and the no-copy splice happen only under `:checked`
and `:raw`; `:rename` trades that speed for crash-safety and is the default.

`:checked` marks an in-progress edit with a sidecar `<blob>.dirty` file —
deliberately **not** a header byte, because konserve reads a nonzero byte in the
header's spare region as a legacy-header signature and would then read the value
from the wrong offset. **`torn?` is a manual signal**: nothing consults it
automatically; a caller using `:checked` checks it on read and reconstructs a
value that was interrupted mid-edit.

```clojure
(when (kmm/torn? store k)
  ;; the value may be half-written -- rebuild it from source
  (k/assoc-in store [k] (recompute k) {:sync? true}))
```

### Locking and concurrency

Write ops take the store/key **in-process lock** by default — the same per-key
lock `konserve.core` writes use — so an mmap edit serialises against ordinary
konserve writes and against other mmap edits on that key. It costs about a
microsecond uncontended, negligible against any file write; `:lock? false` opts
out for a caller that guarantees its own single-writer discipline.

```clojure
(kmm/update-in! store [:doc :counter] inc {:durability :checked :lock? false})
```

Reads (`with-mmap-value`) stay **lock-free**: a mapping is a point-in-time
snapshot of the inode, and the default `:rename` never mutates a live value. If
you use `:checked`/`:raw` in-place writes and also read the same key
concurrently, a reader can observe a half-written value — coordinate, or keep the
default.

---

## Performance

On a 33.8 MB value, an in-place `:checked` `update-in!` is **70–360×** a
`konserve.core/update-in`, because the value is never decoded or re-encoded. A
same-length poke is **position-independent** — a field update costs a page write
whether the value is a kilobyte or a gigabyte, provided the blob is indexed so
the field can be located without a scan (the boring serializer indexes by
default). The `:rename` path is O(file) but still skips the decode/encode object
graph and maintains the index instead of rebuilding it.

---

## The full surface

| | |
|---|---|
| `navigable?` / `value-location` | is a key's blob navigable, and where its value begins |
| `with-mmap-value` / `mmap-value` | a scoped `boring.nav` cursor over the value |
| `update-in!` / `assoc-in!` / `dissoc-in!` | edit without decoding; `:durability`, `:lock?` |
| `edit-eligible?` | whether a key's blob can be edited (navigable and not stringref) |
| `torn?` | whether a `:checked` edit was interrupted and the value needs recovery |

`boring`'s own [`doc/EDITING.md`](https://github.com/replikativ/boring/blob/main/doc/EDITING.md)
documents the byte-level engine these sit on — the count-not-byte-length
invariant, the poke/splice/maintain semantics, and what is refused.
