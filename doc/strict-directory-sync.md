# Strict directory sync (JVM file store)

Applications that must not acknowledge a write after skipping directory sync can
opt in when connecting a file store:

```clojure
(connect-fs-store path
  :config {:sync-blob? true
           :in-place? false
           :strict-directory-sync? true})
```

The non-in-place write sequence is: write segments, force the file, close it,
atomic replacement, then force the store directory. Strict mode propagates
directory-open and directory-force failures, including on Windows. The option
must be boolean, requires `:sync-blob? true`, and rejects custom filesystems
whose directory-sync operation is not implemented. It does not require blocking
API calls: async completion carries the failure through its result channel.

Without strict mode, only Windows directory-open `AccessDeniedException` keeps
the historical compatibility fallback. POSIX open failures and force failures
on all platforms propagate. Custom filesystems retain their non-strict behavior.

A failure after replacement can leave the new value visible. A failed write is
not a rollback: callers must reconcile/retry and withhold durability receipts.
This option does not establish hardware power-loss guarantees, durability of
externally provisioned ancestor directories, immutable-write semantics, replica
retention, or distributed consensus. Administrative `delete-store` retains its
separate best-effort cleanup semantics. Qualifying a complete durable repository
still requires testing provisioning, the filesystem/device and the repository's
own marker/receipt ordering.

The focused gate injects open and force errors for sync/async writes, checks
platform-policy decisions, and verifies successful retry/reopen:

```sh
clojure -X:test :nses '[konserve.directory-sync-test konserve.filestore-test konserve.simulation-crash-test]'
```

The crash simulator tests storage ordering independently of the OS. These tests
are not an actual power-cut experiment or execution on a Windows machine.
