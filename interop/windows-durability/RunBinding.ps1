param([Parameter(Mandatory=$true)][string]$ScratchParent)
$ErrorActionPreference = 'Stop'
$classes = Join-Path $ScratchParent ('ffm-classes-' + [guid]::NewGuid())
$null = New-Item -ItemType Directory -Path $classes
$source = $PSScriptRoot
javac -d $classes "$source/WindowsDirectorySync.java" "$source/DirectorySyncBindingProbe.java"
if ($LASTEXITCODE -ne 0) { throw 'FFM binding compilation failed' }
java --enable-native-access=ALL-UNNAMED -cp $classes DirectorySyncBindingProbe $ScratchParent |
    Tee-Object probe-results/ffm-jdk-25.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM JVM binding checks failed' }
$executable = Join-Path $classes 'directory-sync-binding'
native-image -O0 --no-fallback --enable-native-access=ALL-UNNAMED `
    '-H:+UnlockExperimentalVMOptions' "-H:ConfigurationFileDirectories=$source/ffm-config" `
    '-H:-UnlockExperimentalVMOptions' '--initialize-at-run-time=WindowsDirectorySync$Native' `
    -cp $classes DirectorySyncBindingProbe $executable |
    Tee-Object probe-results/ffm-native-build.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM native-image compilation failed' }
& "$executable.exe" $ScratchParent | Tee-Object probe-results/ffm-native-25.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM native-image binding checks failed' }
if ($env:GITHUB_STEP_SUMMARY) {
    '## Experimental Windows FFM binding' >> $env:GITHUB_STEP_SUMMARY
    'JDK 25 and native-image binding checks passed: handle lifecycle/failure propagation, create/replace/readback, Unicode/long paths and missing-directory error codes. Not crash/power-loss qualification; not activated in Konserve.' >> $env:GITHUB_STEP_SUMMARY
}
