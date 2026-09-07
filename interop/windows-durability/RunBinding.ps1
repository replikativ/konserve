param([Parameter(Mandatory=$true)][string]$ScratchParent,
      [Parameter(Mandatory=$true)][string]$Artifact)
$ErrorActionPreference = 'Stop'
$classes = Join-Path $ScratchParent ('ffm-classes-' + [guid]::NewGuid())
$null = New-Item -ItemType Directory -Path $classes
$source = $PSScriptRoot
javac -cp $Artifact -d $classes "$source/DirectorySyncBindingProbe.java"
if ($LASTEXITCODE -ne 0) { throw 'FFM binding compilation failed' }
$cp = "$classes$([IO.Path]::PathSeparator)$Artifact"
java --enable-native-access=ALL-UNNAMED -cp $cp konserve.internal.DirectorySyncBindingProbe $ScratchParent |
    Tee-Object probe-results/ffm-jdk-25.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM JVM binding checks failed' }
$executable = Join-Path $classes 'directory-sync-binding'
# Native configuration must be discovered inside the release jar.
native-image -O0 --no-fallback `
    -cp $cp konserve.internal.DirectorySyncBindingProbe $executable |
    Tee-Object probe-results/ffm-native-build.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM native-image compilation failed' }
& "$executable.exe" $ScratchParent | Tee-Object probe-results/ffm-native-25.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM native-image binding checks failed' }
if ($env:GITHUB_STEP_SUMMARY) {
    '## Windows FFM binding' >> $env:GITHUB_STEP_SUMMARY
    'JDK 25 and native-image binding checks passed: handle lifecycle/failure propagation, create/replace/readback, Unicode/long paths and missing-directory error codes. These checks alone are not crash/power-loss qualification.' >> $env:GITHUB_STEP_SUMMARY
}
