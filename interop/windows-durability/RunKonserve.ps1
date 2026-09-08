param([Parameter(Mandatory=$true)][string]$ScratchParent)
# Compatibility entry point until the GitHub workflow uses test/windows-durability.
& "$PSScriptRoot/../../test/windows-durability/RunKonserve.ps1" -ScratchParent $ScratchParent
