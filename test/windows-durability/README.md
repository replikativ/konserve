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

## Older Unix runtime qualification

The same packaged jar was tested on Linux with JDK 21 (65 tests, 804 assertions)
and GraalVM CE 21.0.2: the native Unix directory-barrier executable built and ran
with the jar's Windows classes and native metadata present. This is a focused
native compatibility check, not a full Datahike native build or qualification of
every older JVM. The ordinary directory-sync suite additionally checks the typed
unavailable-Windows-binding error on JVMs below 22.

After the artifact build above, `probe-results/artifact.cp` contains the jar and
test dependencies, with loose production sources/classes/resources excluded.
To repeat the native check on Unix, use JDK/GraalVM 21 executables on PATH:

```sh
mkdir -p probe-results/unix-aot
artifact_cp=$(< probe-results/artifact.cp)
java -cp "test/windows-durability:$artifact_cp" clojure.main -e \
  "(binding [*compile-path* \"probe-results/unix-aot\"] (compile 'unix-directory-smoke))"
native-image -O0 --no-fallback --initialize-at-build-time -J-Xmx3g \
  -cp "probe-results/unix-aot:$artifact_cp" unix_directory_smoke probe-results/unix-directory-smoke
./probe-results/unix-directory-smoke
```

Compile the Clojure entry point with the target JDK, not the newer release-build
JDK. No Windows native configuration is stripped from the jar for this check.
