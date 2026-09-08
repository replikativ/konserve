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
$datahikeCheckout = Join-Path $probeDir 'datahike'
git clone --depth 1 --branch fix/windows-process-completion https://github.com/replikativ/datahike.git $datahikeCheckout
if ($LASTEXITCODE -ne 0) { throw 'Datahike checkout failed' }
$actualCommit = git -C $datahikeCheckout rev-parse HEAD
if ($actualCommit -ne '12b44be7a71cebf766890b1390e1a7424b2f2b78') { throw "Unexpected Datahike revision: $actualCommit" }
$lifecycleLog = Join-Path (Get-Location) 'probe-results/datahike-lifecycle.log'
$env:PATH = "$probeDir;$env:PATH"
Push-Location $datahikeCheckout
try {
  & $bb --config bb/scratch.edn -m tools.scratch-test 2>&1 | Tee-Object $lifecycleLog
  if ($LASTEXITCODE -ne 0) { throw "Datahike lifecycle tests failed: $LASTEXITCODE" }
} finally { Pop-Location }
