param([Parameter(Mandatory=$true)][string]$ScratchParent)
$ErrorActionPreference = 'Stop'
# Use the pinned tools jar to resolve dependencies without installing a global
# Clojure CLI or modifying the workflow. Verify bytes before executing it.
$tools = Join-Path $ScratchParent ('clojure-tools-' + [guid]::NewGuid())
$zip = "$tools.zip"
Invoke-WebRequest https://download.clojure.org/install/clojure-tools-1.12.2.1565.zip -OutFile $zip
if ((Get-FileHash $zip -Algorithm SHA256).Hash -ne '4d488e73314e368752d8f3f2db29d0592bb2203b8f7a46cf9ae6b6044f45c8c4') {
    throw 'Clojure tools checksum mismatch'
}
Expand-Archive $zip -DestinationPath $tools
$toolsJar = Join-Path $tools 'ClojureTools/clojure-tools-1.12.2.1565.jar'
java -cp $toolsJar clojure.main "$PSScriptRoot/prepare-classpath.clj"
if ($LASTEXITCODE -ne 0) { throw 'Konserve test dependency resolution failed' }
$buildCp = (Get-Content probe-results/build.cp -Raw).Trim()
java -cp $buildCp clojure.main "$PSScriptRoot/build-artifact.clj"
if ($LASTEXITCODE -ne 0) { throw 'Konserve release artifact build failed' }
$artifact = (Get-Content probe-results/artifact.path -Raw).Trim()
$classes = Join-Path $ScratchParent ('ffm-classes-' + [guid]::NewGuid())
$null = New-Item -ItemType Directory -Path $classes
$source = $PSScriptRoot
javac -cp $artifact -d $classes "$source/WindowsDirectorySyncTest.java"
if ($LASTEXITCODE -ne 0) { throw 'FFM binding compilation failed' }
$cp = "$classes$([IO.Path]::PathSeparator)$artifact"
java --enable-native-access=ALL-UNNAMED -cp $cp konserve.internal.WindowsDirectorySyncTest $ScratchParent |
    Tee-Object probe-results/ffm-jdk-25.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM JVM binding checks failed' }
$executable = Join-Path $classes 'directory-sync-binding'
# Native configuration must be discovered inside the release jar.
native-image -O0 --no-fallback `
    -cp $cp konserve.internal.WindowsDirectorySyncTest $executable |
    Tee-Object probe-results/ffm-native-build.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM native-image compilation failed' }
& "$executable.exe" $ScratchParent | Tee-Object probe-results/ffm-native-25.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'FFM native-image binding checks failed' }
if ($env:GITHUB_STEP_SUMMARY) {
    '## Windows FFM binding' >> $env:GITHUB_STEP_SUMMARY
    'JDK 25 and native-image binding checks passed: handle lifecycle/failure propagation, create/replace/readback, Unicode/long paths and missing-directory error codes. These checks alone are not crash/power-loss qualification.' >> $env:GITHUB_STEP_SUMMARY
}

$cp = (Get-Content probe-results/artifact.cp -Raw).Trim()
java --enable-native-access=ALL-UNNAMED -cp $cp clojure.main -e `
    "(require 'konserve.directory-sync-test 'konserve.directory-provisioning-test 'konserve.filestore-test 'konserve.mmap-test 'konserve.simulation-crash-test) (let [r (clojure.test/run-tests 'konserve.directory-sync-test 'konserve.directory-provisioning-test 'konserve.filestore-test 'konserve.mmap-test 'konserve.simulation-crash-test)] (shutdown-agents) (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))" |
    Tee-Object probe-results/konserve-windows-integration.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Konserve Windows integration failed' }
java --enable-native-access=ALL-UNNAMED -cp $cp clojure.main "$PSScriptRoot/process-crash.clj" |
    Tee-Object probe-results/konserve-process-crash.txt | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Konserve process-crash recovery failed' }
if ($env:GITHUB_STEP_SUMMARY) {
    '## Konserve Windows integration' >> $env:GITHUB_STEP_SUMMARY
    'Directory-sync, filestore, mmap and storage crash-simulator tests passed with the production Windows binding and no unsafe compatibility setting.' >> $env:GITHUB_STEP_SUMMARY
}
