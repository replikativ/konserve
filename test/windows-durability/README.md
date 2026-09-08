# Windows persistence regression tests

From the repository root, with GraalVM 25 Windows/x64 and its native compiler:

```powershell
New-Item -ItemType Directory -Force probe-results | Out-Null
./test/windows-durability/RunKonserve.ps1 -ScratchParent $env:TEMP
```

The runner builds the release jar and tests it without loose production sources,
classes or metadata on the test classpath. It runs JVM/native binding tests,
file-store/mmap/error-propagation tests, simulated storage-crash tests, and real
process-kill recovery checks. Errors fail the runner; results are in `probe-results`.
The build cleans `target`. Scratch directories are retained for inspection.

`prepare-classpath.clj` resolves dependencies using checksum-pinned Clojure tools;
`build-artifact.clj` runs the release builder in the resulting build classpath.
They are separate because they execute in different JVM dependency environments.
`WindowsDirectorySyncTest.java` covers handle/error handling and Windows path
behavior. `process-crash.clj` checks acknowledged payload/root recovery.

These tests do not establish OS-crash or power-loss durability and do not replace
Datahike native integration. See [the persistence contract](../../doc/strict-directory-sync.md).
The exploratory API surveys have been removed; their history remains in Git.
