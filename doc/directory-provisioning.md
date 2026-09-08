# Explicit durable directory provisioning

`konserve.directory-sync/provision-directory!` is an opt-in synchronous helper
for a caller-owned local directory tree. It does not change `connect-fs-store`
or make existing read-only connections perform writes.

```clojure
(require '[konserve.directory-sync :as directory-sync])
(directory-sync/provision-directory! "/srv/durable-pool" "/srv/durable-pool/checkpoints/node-a")
```

The caller supplies an existing, independently provisioned durable ancestor.
The helper cannot establish the durability of that ancestor's own name. Both
arguments must be nonempty paths on the default filesystem; the normalized
target must lie beneath the ancestor. It never creates the ancestor implicitly.

For each descendant it creates or validates the directory, flushes the parent
entry, and flushes the child. Retries repeat all barriers even if every directory
now exists. Existence after a failed operation does not mean its name was made
durable. IO failures propagate; partially created directories are left in place
for retry, not automatically removed. Windows uses the JDK 22+ native directory
barrier; Unix uses its directory force implementation. There is no unsafe mode.

The caller must prevent concurrent rename, deletion, symlink replacement, and
changes to ancestor resolution during provisioning and subsequent use. Existing
symlink components within the managed path are rejected, but path checks are not
a sandbox against hostile filesystem races. No permissions or ownership are
changed. This operation does not freeze the tree or establish a distributed
retention policy.

Tests inject a failure at each barrier, retry existing-but-unconfirmed paths,
check real directory barriers, and reject outside paths, missing ancestors,
regular files and symlink components. Symlink creation tests are Unix-only;
ordinary and injected barrier cases are included in the Windows artifact suite.
These checks are not OS-crash/power-cut qualification.

Netz adoption must bind the ancestor to its owned repository configuration and
cover all newly created repository subdirectories before issuing durable
receipts. Adding this helper alone does not qualify arbitrary caller-provided
Konserve stores or repair existing unsynced ancestors outside the chosen boundary.
