# Batched Windows persistence API probes

Run the `Windows durability API probes` workflow on this PR. One Windows 2025
runner executes JDK 21, GraalVM JDK 25, the same Java source compiled to a tiny
native executable, and direct Win32 calls through PowerShell/.NET P/Invoke.
No Datahike build, Clojure dependencies, or new runtime library dependency.
All operations are batched, with JSONL artifacts and one Actions summary table.

Java compares forced staging + atomic rename, post-rename destination force,
directory force, and SYNC staging + post-rename force. Each runs for a new target,
replacement and an open reader. Win32 compares ordinary/write-through rename
and post-rename flush, including readers with and without delete sharing; it also
tests directory open + flush with BACKUP_SEMANTICS under three access masks.
Cross-volume copy fallback is never enabled.

An API error is a measurement, not a failed experiment. Incomplete output, build
failures and readback mismatches fail the job. Expected sharing violations and
unsupported directory operations remain visible as error rows. A green workflow
means a complete compatibility experiment, **not a qualified durability recipe**.
No process-kill, OS-crash or power-cut test is included in this first gate.

Local JVM invocation (use an existing scratch parent):

```sh
javac -d /tmp interop/windows-durability/DurabilityProbe.java
java -cp /tmp DurabilityProbe /tmp local-jdk
```

Each invocation creates its own unique scratch directory, retained for inspection.
The workflow runner owns cleanup. Do not point a destructive cleanup command at
the caller-provided parent. Wine can run the Windows executable for compatibility
diagnosis but cannot qualify Windows kernel/NTFS durability.

## Decision gate

Compare all rows before changing production code. Narrow to one viable recipe,
then validate the documented persistence semantics and add process-crash plus
Konserve/Datahike integration tests for that recipe. Keep PR #190 unmerged until
Windows support has an agreed implementation; do not enable unsafe fallback just
to make a build pass. If a native primitive is required, its packaging and GraalVM
integration need their own validation: PowerShell P/Invoke is a probe, not the
production binding.

## Experimental FFM binding increment

`WindowsDirectorySync.java` calls CreateFileW with GENERIC_WRITE, share
read/write/delete, OPEN_EXISTING and FILE_FLAG_BACKUP_SEMANTICS, then
FlushFileBuffers and CloseHandle. Native errors are captured at the downcall
boundary with GetLastError, before Java or another call can overwrite them.
Flush errors survive close errors; close errors are never silently discarded.
Native-image foreign-call metadata is supplied explicitly. Long and Unicode paths
are covered. No JNA or bundled JNI library is introduced.

The existing Windows survey step invokes `RunBinding.ps1` afterward, so no workflow
edit is needed. It builds and runs both JDK 25 and GraalVM native binding checks,
uploads separate text artifacts and fails on any binding error. Linux can run the
six injected handle-lifecycle cases using `DirectorySyncBindingProbe --self-test`.
This adds one small native build, not a full Datahike build.

This is deliberately outside production source: FFM requires JDK 22+ and the
native gate targets GraalVM 25 Windows/x64. Konserve's Java baseline is unchanged.
Support for older JVMs requires a separate binding/packaging decision; do not
silently enable unsafe mode there. Initial directory provisioning, mmap, store
integration and crash recovery are not covered by these binding checks.

Microsoft documents directory handles via BACKUP_SEMANTICS and write access for
FlushFileBuffers. These establish the API/access requirements, not by themselves
the complete recovery guarantee for our multi-file publication sequence:

* https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew
* https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers
* https://www.graalvm.org/latest/reference-manual/native-image/native-code-interoperability/ffm-api/
