# Windows persistence regression checks

Windows file stores require JDK 22+. Native checks target GraalVM 25 Windows/x64.
Run from the repository root on Windows with GraalVM 25 and its native compiler:

```powershell
New-Item -ItemType Directory -Force probe-results | Out-Null
./interop/windows-durability/RunKonserve.ps1 -ScratchParent $env:TEMP
```

The runner resolves dependencies using checksum-pinned Clojure tools, builds the
release jar through `build/jar`, and tests that artifact, not loose source/classes:

- JVM and native binding lifecycle/error handling, Unicode and long paths;
- file-store, mmap, directory-sync and simulated storage-crash regression tests;
- child-process kill/reopen checks after acknowledged payload and root writes.

The native binding check discovers foreign-call metadata and initialization
settings from the jar. No separate test metadata or binding source compilation
can mask packaging omissions. Failures fail the runner; text results go into
`probe-results`. The build cleans the repository's `target` directory.

These checks do **not** qualify OS-crash or power-loss durability, nor replace an
actual Datahike native-image integration test. See the
[persistence contract](../../doc/strict-directory-sync.md).

## Optional diagnostic survey

`DurabilityProbe.java` and `NativeProbe.ps1 -SurveyOnly` retain the original
Java/Win32 API comparison for manual diagnosis through the first release.
Expected API refusals are recorded as JSONL data, not regression successes.

```powershell
javac -d probe-results interop/windows-durability/DurabilityProbe.java
java -cp probe-results DurabilityProbe $env:TEMP manual-jdk
./interop/windows-durability/NativeProbe.ps1 -ScratchParent $env:TEMP -SurveyOnly
```

Scratch directories are uniquely named and retained for inspection. Wine cannot
qualify Windows kernel/NTFS durability. The existing workflow temporarily invokes
the regression runner after the survey for compatibility; the prepared focused
workflow removes this coupling from regular CI.
