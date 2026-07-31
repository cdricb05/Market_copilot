<#
.SYNOPSIS
    One-time secure BEA (U.S. Bureau of Economic Analysis) API UserID setup for
    the Alpha Agent Stage 8 data-acquisition layer.

.DESCRIPTION
    Prompts interactively for the FREE BEA API UserID (registered at
    bea.gov/API/signup) via Read-Host -AsSecureString so it never appears on
    screen. It DPAPI-encrypts the UserID for the CURRENT Windows user
    (ConvertFrom-SecureString, no external key) and stores the encrypted blob
    OUTSIDE the repository under C:\Users\<you>\.paper_trader\alpha_agent_bea\.

    The UserID is NEVER printed, NEVER written to the repo, NEVER placed in a
    command-line argument, an environment variable, a .env file, a temporary
    file, JSON config, logs, report files or PROJECT_STATE.md, and is never
    exposed to the LLM. Only the DPAPI-encrypted blob is written to disk. The
    BeaCollector resolves + decrypts this blob at runtime.

    This script performs NO network call and acquires NO data.

.NOTES
    Windows PowerShell 5.1. Run interactively in YOUR own session. Prints exactly
    BEA_CONFIGURED on success or BEA_CONFIGURATION_FAILED - <safe reason> on
    failure. The UserID itself is never displayed.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_bea",
    [string]$KeyFile = "bea_userid.dpapi"
)

$ErrorActionPreference = "Stop"

function Fail {
    param([string]$Reason)
    Write-Host "BEA_CONFIGURATION_FAILED - $Reason"
    exit 1
}

# --------------------------------------------------------------------------- #
# BEA UserID (secret) - read as a SecureString so it never echoes.
# --------------------------------------------------------------------------- #
$SecureInput = Read-Host "BEA API UserID (free, from bea.gov/API/signup)" -AsSecureString
if ($null -eq $SecureInput) { Fail "no BEA UserID was entered" }

if (-not (Test-Path -LiteralPath $CredentialDir)) {
    New-Item -ItemType Directory -Path $CredentialDir -Force | Out-Null
}
$KeyPath = Join-Path $CredentialDir $KeyFile

$InBstr = [IntPtr]::Zero
$OutBstr = [IntPtr]::Zero
$Normalized = $null
try {
    # Decrypt the entered SecureString in memory ONLY to validate its shape.
    $InBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureInput)
    $Raw = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($InBstr)
    $Normalized = ($Raw -replace '\s', '')
    if ([string]::IsNullOrEmpty($Normalized)) { Fail "the BEA UserID was blank" }
    # BEA UserIDs are commonly a 36-char GUID, but any non-trivial token is
    # accepted (BEA does not publish a fixed format).
    if ($Normalized.Length -lt 8) { Fail "the BEA UserID is implausibly short" }

    # DPAPI-encrypt the UserID for the current Windows user.
    $SecureNormalized = ConvertTo-SecureString -String $Normalized -AsPlainText -Force
    $Encrypted = ConvertFrom-SecureString -SecureString $SecureNormalized
    Set-Content -Path $KeyPath -Value $Encrypted -Encoding ASCII -Force

    # Lock ACLs to the current user where possible (best-effort).
    try {
        $Me = "$env:USERDOMAIN\$env:USERNAME"
        icacls $KeyPath /inheritance:r /grant:r "${Me}:F" | Out-Null
    } catch { Write-Host "[configure] ACL hardening skipped." }

    # Verify round-trip decryption WITHOUT printing the UserID.
    $Roundtrip = ConvertTo-SecureString -String ((Get-Content -LiteralPath $KeyPath -Raw).Trim())
    $OutBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Roundtrip)
    $PlainLen = ([Runtime.InteropServices.Marshal]::PtrToStringBSTR($OutBstr)).Length
    if ($PlainLen -lt 8) { Fail "stored BEA UserID did not decrypt correctly" }

    # Safe metadata only (never the UserID).
    $EncItem = Get-Item -LiteralPath $KeyPath
    $Sha = (Get-FileHash -LiteralPath $KeyPath -Algorithm SHA256).Hash
    Write-Host "===== BEA CREDENTIAL METADATA (non-secret) ====="
    Write-Host ("  credential_dir : {0}" -f $CredentialDir)
    Write-Host ("  key_file       : {0}" -f $KeyPath)
    Write-Host ("  key_exists     : {0}" -f (Test-Path -LiteralPath $KeyPath))
    Write-Host ("  key_size       : {0}" -f $EncItem.Length)
    Write-Host ("  key_lastwrite  : {0}" -f $EncItem.LastWriteTime.ToString('s'))
    Write-Host ("  key_sha256     : {0}" -f $Sha)
    Write-Host "  next           : run scripts\diagnose_alpha_agent_bea.ps1 to probe the API read-only"

    Write-Host "BEA_CONFIGURED"
    exit 0
}
finally {
    $Raw = $null
    $Normalized = $null
    $SecureNormalized = $null
    if ($InBstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($InBstr)
    }
    if ($OutBstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($OutBstr)
    }
    [GC]::Collect()
}
