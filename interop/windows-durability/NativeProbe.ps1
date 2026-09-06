param([Parameter(Mandatory=$true)][string]$ScratchParent)
$ErrorActionPreference = 'Stop'
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
public static class DurabilityWin32 {
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool MoveFileEx(string source, string target, uint flags);
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    public static extern SafeFileHandle CreateFile(string path, uint access, uint share,
        IntPtr security, uint disposition, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError=true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool FlushFileBuffers(SafeFileHandle handle);
}
'@
function Row($recipe, $scenario, $outcome, $detail) {
    [ordered]@{runtime='win32-pinvoke';recipe=$recipe;scenario=$scenario;outcome=$outcome;detail=$detail} |
        ConvertTo-Json -Compress
}
function Write-Forced($path, $value) {
    $stream = [IO.File]::Open($path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::Read)
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($value)
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    } finally { $stream.Dispose() }
}
$base = Join-Path $ScratchParent ('win32-probe-' + [guid]::NewGuid())
$null = New-Item -ItemType Directory -Path $base
Row 'environment' 'metadata' 'info' ([Environment]::OSVersion.ToString() + '; scratch=' + $base)
foreach ($recipe in @('move', 'move-write-through', 'move-write-through-flush')) {
    foreach ($scenario in @('create', 'replace', 'held-reader', 'deny-delete-reader')) {
        $dir = Join-Path $base ($recipe + '-' + $scenario)
        $null = New-Item -ItemType Directory -Path $dir
        $target = Join-Path $dir 'value'
        $stage = Join-Path $dir 'stage'
        $reader = $null
        $phase = 'seed'
        try {
            if ($scenario -ne 'create') { Write-Forced $target 'old-complete-value' }
            if ($scenario -in @('held-reader', 'deny-delete-reader')) {
                $share = [IO.FileShare]::ReadWrite
                if ($scenario -eq 'held-reader') { $share = $share -bor [IO.FileShare]::Delete }
                $reader = [IO.File]::Open($target, [IO.FileMode]::Open, [IO.FileAccess]::Read, $share)
            }
            $phase = 'write-and-force-stage'
            Write-Forced $stage 'new-complete-value'
            $phase = 'move'
            [uint32]$flags = 1 # REPLACE_EXISTING; never COPY_ALLOWED
            if ($recipe -ne 'move') { $flags = $flags -bor 8 } # WRITE_THROUGH
            if (-not [DurabilityWin32]::MoveFileEx($stage, $target, $flags)) {
                throw [ComponentModel.Win32Exception]::new([Runtime.InteropServices.Marshal]::GetLastWin32Error())
            }
            if ($recipe -eq 'move-write-through-flush') {
                $phase = 'flush-destination'
                $stream = [IO.File]::Open($target, [IO.FileMode]::Open, [IO.FileAccess]::Write, [IO.FileShare]::ReadWrite)
                try { $stream.Flush($true) } finally { $stream.Dispose() }
            }
            $phase = 'verify'
            if ([IO.File]::ReadAllText($target) -ne 'new-complete-value') { throw 'destination bytes differ' }
            Row $recipe $scenario 'ok' 'API completion and readback only'
        } catch {
            Row $recipe $scenario 'error' ($phase + ': ' + $_.Exception.ToString())
        } finally {
            if ($null -ne $reader) { $reader.Dispose() }
        }
    }
}
# Opening a directory successfully does not imply FlushFileBuffers supports it.
foreach ($access in @([uint32]2147483648, [uint32]1073741824, [uint32]3221225472)) {
    $handle = [DurabilityWin32]::CreateFile($base, $access, 7, [IntPtr]::Zero, 3, 0x02000000, [IntPtr]::Zero)
    try {
        if ($handle.IsInvalid) {
            Row 'directory-backup-semantics' "$access" 'error' ('open Win32=' + [Runtime.InteropServices.Marshal]::GetLastWin32Error())
        } elseif (-not [DurabilityWin32]::FlushFileBuffers($handle)) {
            Row 'directory-backup-semantics' "$access" 'error' ('flush Win32=' + [Runtime.InteropServices.Marshal]::GetLastWin32Error())
        } else { Row 'directory-backup-semantics' "$access" 'ok' 'open and flush succeeded; not a durability proof' }
    } finally { $handle.Dispose() }
}
