# Directory sync is required by default (JVM file store)

With the default `:sync-blob? true`, successful write completion requires both
file and directory sync. Directory-open and directory-force failures propagate
on every OS, including Windows. Async API calls report the failure on their
result channel; blocking calls throw.

The default non-in-place sequence is: write segments, force the file, close it,
atomic replacement, then force the store directory. A failure after replacement
can leave the new value visible. Failure is not rollback: reconcile/retry and
withhold transaction-success and durability receipts.

## Explicitly unsafe compatibility

This deliberately changes 0.9.392, which silently ignored directory-open
AccessDeniedException, including on POSIX. Windows FileChannel directory opens
may be unsupported. Synced writes now fail in that case. NTFS journaling is not
treated as a substitute for the missing barrier.

For disposable data, development or tests only, a caller can explicitly choose:

```clojure
(connect-fs-store path
  :config {:allow-unsafe-directory-sync? true})
```

This boolean option emits a warning at connection and permits skipping directory
sync for custom filesystems (including in-memory Jimfs) or a Windows directory
open AccessDeniedException. POSIX open errors and force errors still propagate.
**Acknowledged writes may be lost after an OS crash or power loss.** Do not use
this option for durable database commits or replication receipts.
Alternatively, existing `:sync-blob? false` explicitly disables sync altogether
and likewise does not promise crash durability. Custom filesystems without
directory-sync support must choose one of these weaker modes explicitly.

The earlier unreleased `:strict-directory-sync?` option is rejected: safety is
now the default, not an extra setting consumers must discover and enable.

## Qualification boundaries

This does not establish hardware power-loss guarantees, durability of ancestor
directory provisioning, immutable-write semantics, retention or consensus.
Administrative `delete-store` remains separate best-effort cleanup. Complete
repository qualification still requires filesystem/device and provisioning tests
plus the repository's marker/receipt ordering tests.

```sh
clojure -X:test :nses '[konserve.directory-sync-test konserve.filestore-test konserve.simulation-crash-test]'
```

These tests inject open/force errors, exercise sync/async writes and successful
retry/reopen. Platform decisions are injected on Linux, not a Windows machine.
The crash simulator tests storage ordering, not actual power-cut behavior.
