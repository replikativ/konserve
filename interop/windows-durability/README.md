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
