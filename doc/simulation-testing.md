# Simulation testing

konserve ships a simulation harness for testing the storage layer under
conditions that are hard to reach with ordinary tests: I/O errors at arbitrary
points, byte-level corruption, resource exhaustion, and process crashes at each
step of the write path.

The harness lives in three namespaces under `konserve.simulation`. They are
marked `^:no-doc` and are **internal**: they ship in the jar so that backend
implementations and downstream projects can test against them, but they carry
no compatibility guarantee and may change or move in any release.

| namespace | what it is |
|---|---|
| `konserve.simulation.backing` | fault-injecting wrapper around any `PBackingStore` |
| `konserve.simulation.crash` | crash simulation with sync-point tracking |
| `konserve.simulation.memory` | in-memory `PBackingStore`, no filesystem I/O |

All three implement or wrap `konserve.impl.storage-layout` protocols, so they
compose with `connect-default-store` exactly like a real backing store:

```
[k/assoc] → [DefaultStore] → [SimulatedBackingStore] → [MemoryBackingStore | BackingFilestore]
```

## Fault injection

`konserve.simulation.backing/wrap-backing-store` intercepts every
`PBackingStore` and `PBackingBlob` method and decides, per call, whether to
inject a fault. The configuration is a map of per-operation probabilities — 19
knobs in total: 16 error-injection rates (`:create-blob-fault-rate`,
`:atomic-move-fault-rate`, `:write-value-fault-rate`, `:get-lock-fault-rate`, …),
3 corruption rates that flip bits in returned byte arrays rather than throwing,
plus three crash probabilities.

Three presets are provided: `no-faults-config`, `default-fault-config` (1% per
operation) and `chaos-fault-config`.

Decisions are drawn from a caller-supplied `java.util.SplittableRandom`, so a
seed reproduces a run exactly:

```clojure
(require '[konserve.simulation.backing :as sim]
         '[konserve.impl.defaults :refer [connect-default-store]])

(let [history (atom [])
      backing (sim/wrap-backing-store real-backing
                                      sim/chaos-fault-config
                                      (sim/rng 42)
                                      history)]
  (connect-default-store backing {...})
  ;; ... exercise the store ...
  (sim/count-faults @history))
```

Every intercepted operation is appended to the history atom, so a failing run
can be replayed and inspected.

## Crash simulation

`konserve.simulation.crash` models a crash as *loss of everything not yet
synced*. It tracks pending versus synced blob state, promotes pending to synced
on `-sync`/`-sync-store`, and on crash discards the pending set and restores the
synced snapshot — including pending `.new` files, pending atomic moves, and
pending backup deletions.

Crash points are chosen explicitly, not at random; there is no RNG in this
namespace, so a scenario is deterministic by construction. The six points
correspond to the steps of the `DefaultStore` write path:

| point | crash between |
|---|---|
| `:after-write-header` | `-write-header` and `-write-meta` |
| `:after-write-meta` | `-write-meta` and `-write-value` |
| `:after-write-value` | `-write-value` and `-sync` |
| `:after-sync` | `-sync` and `-atomic-move` |
| `:after-atomic-move` | `-atomic-move` and `-sync-store` |
| `:after-sync-store` | `-sync-store` and `-delete-blob` |

The choice to enumerate sync points rather than sample random interruptions
follows the crash-consistency literature (SQLite, RocksDB, CrashMonkey): crash
bugs cluster immediately after fsync-like calls, and most reproduce in a
handful of operations.

The harness also enforces resource limits (`max-total-bytes`, `max-keys`) so
exhaustion paths can be exercised without filling a disk.

## What is covered

95 tests, 2,409 assertions.

**`simulation_backing_test`** (23) — error propagation from each `PBackingStore`
method up to the public API, in both sync and async modes; atomicity (a failed
write leaves no trace, a failed atomic move does not corrupt the previous
value); durability across store reopen; that orphaned `.new` and `.backup`
files left by a failed write neither break subsequent operations nor appear in
`keys`; history recording; chaos-mode survival.

**`simulation_crash_test`** (20) — a crash at each of the six points, with
recovery checked afterwards; repeated crash/recovery cycles; the no-partial-data
and atomicity invariants; crashes concurrent with other writers; empty values,
large values, and first writes to a new key.

**`simulation_gc_test`** (14) — `konserve.gc/sweep!` with whitelist and
timestamp cutoffs; a crash during sweep deletion; that recovery leaves GC
idempotent; writes and reads concurrent with a sweep; nested data; repeated
cycles.

**`simulation_stress_test`** (38) — storage and key limits and reclamation;
concurrent writers to one key and to many; read/write contention; 1000+
operation stability runs; binary blobs to 100 KB and mixed with EDN; 200-cycle
rapid delete/recreate on one key, including concurrently; `update-in`
atomicity; store close/reopen, including after a crash; `keys` enumeration
during concurrent modification; delete racing against read and update;
`append` ordering, concurrency and crash behaviour.

## Limits of the harness

Worth stating plainly, so results are not read as stronger than they are:

- **Single process, JVM only.** These namespaces are `.clj`, use
  `SplittableRandom` and `java.io`, and model one process crashing. Multi-node
  and multi-process failure modes are out of scope.
- **No linearizability checker.** The recorded history is deliberately
  Elle-shaped (`:invoke`/`:ok`/`:fail` with process ids), but no checker is run
  against it. The consistency claims here rest on hand-written invariants, not
  on a model checker.
- **Two tests are characterizations, not assertions.**
  `update-in-concurrent-test` runs 5 threads × 20 `update-in`s and only asserts
  the result is positive, printing any lost updates rather than failing. No
  lost updates are observed on the current implementation, so this could be
  tightened to an equality assertion — it has not been, so it would not catch a
  regression. `delete-during-update-test` similarly asserts only that no errors
  occur while delete races update, deliberately accepting either winner.
- **Crash simulation models sync semantics, not the filesystem.** It reproduces
  what konserve's write path promises about sync points; it does not model
  reordering within a physical device or partial sector writes.

## Why it lives here

This harness was developed out-of-tree, in a separate repository used to
validate konserve during early 2026, and was folded in so that the
backing-store implementations track the protocols they depend on. The
out-of-tree copy silently broke when `BackingFilestore` gained a `:filesystem`
argument for jimfs support, and that went unnoticed for months. In this
repository it runs in CI on every commit, and a change to
`konserve.impl.storage-layout` updates its wrappers in the same commit.

An earlier version also carried a deterministic-simulation layer built on a
reactive runtime — virtual time, O(1) forking of simulation state, per-fork
process ids. No test exercised it and it was dropped. The idea it reached for
remains sound: `konserve.simulation.crash` hand-rolls its synced/pending
snapshots, which is exactly what a copy-on-write overlay would provide for
free, and would turn "re-run the scenario once per crash point" into a
forkable state tree.
