param([Parameter(Mandatory=$true)][string]$ScratchParent)
# Diagnostic branch only; not a replacement for the persistence regression.
$ErrorActionPreference = 'Stop'
$probeDir = Join-Path $ScratchParent ([guid]::NewGuid().ToString())
New-Item -ItemType Directory -Path $probeDir | Out-Null
$zip = Join-Path $probeDir 'bb.zip'
Invoke-WebRequest 'https://github.com/babashka/babashka/releases/download/v1.12.208/babashka-1.12.208-windows-amd64.zip' -OutFile $zip
Expand-Archive $zip -DestinationPath $probeDir
$bb = Join-Path $probeDir 'bb.exe'
& $bb --config "$PSScriptRoot/empty.edn" "$PSScriptRoot/process-probe.clj" $bb "$PSScriptRoot/empty.edn" 2>&1 | Tee-Object probe-results/process-probe.log
if ($LASTEXITCODE -ne 0) { throw "Process probe failed: $LASTEXITCODE" }
