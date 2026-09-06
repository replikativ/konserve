# Directory sync is required by default (JVM file store)

With the default `:sync-blob? true`, successful write completion requires both
file and directory sync. Directory-open and directory-flush failures propagate
on every OS, including Windows. Windows now uses the system DLL through the
JDK 22+ FFM binding (GENERIC_WRITE + BACKUP_SEMANTICS, FlushFileBuffers, CloseHandle),
not a Java FileChannel directory open. Async API calls report the failure on their
result channel; blocking calls throw.

The default non-in-place sequence is: write segments, force the file, close it,
atomic replacement, then force the store directory. A failure after replacement
can leave the new value visible. Failure is not rollback: reconcile/retry and
withhold transaction-success and durability receipts.

## Explicitly unsafe compatibility

This deliberately changes 0.9.392, which silently ignored directory-open
AccessDeniedException, including on POSIX. Windows FileChannel directory opens
may be unsupported; the native Windows binding supplies the barrier instead.
NTFS journaling is not treated as a substitute for a failed flush.

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

The mmap directory barrier now uses the same platform implementation and does
not swallow IOExceptions. The experimental mmap mutation API does not inherit
the unsafe file-store override.

### Packaging and merge gates

The candidate release compiles the Windows binding with `javac --release 22`.
Unix JVMs below 22 can still load the Clojure namespaces without loading that
class. A Windows JVM below 22 fails explicitly; the supported Windows JVM floor
must be agreed before merging. GraalVM 25 Windows/x64 is the native test target;
older Native Image toolchains are not qualified by this increment.

Run JVMs with `--enable-native-access=ALL-UNNAMED`. The jar includes the native
image foreign-call metadata and runtime-initialization rule for the DLL handles.
Release publication rejects jars missing the binding class. Jar staging is
separate from compiler output, so packaging cannot shadow edited Clojure sources.

The Windows batch now runs the real Konserve directory, filestore, mmap and crash
simulator tests after the binding probes. A separate real child-process kill test
checks that acknowledged payloads and roots survive process termination and
reopen with a complete root closure. This is not an OS crash or power cut.
Datahike native-image integration and initial directory provisioning remain
merge gates; a successful binding survey alone is insufficient.

```sh
clojure -X:test :nses '[konserve.directory-sync-test konserve.filestore-test konserve.simulation-crash-test]'
```

These tests inject open/force errors, exercise sync/async writes and successful
retry/reopen. Platform decisions are injected on Linux, not a Windows machine.
The crash simulator tests storage ordering, not actual power-cut behavior.
