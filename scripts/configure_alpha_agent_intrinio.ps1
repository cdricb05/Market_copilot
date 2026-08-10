<#
.SYNOPSIS
    One-time secure Intrinio TRIAL API key setup for the Alpha Agent research
    data-acquisition layer (US Fundamentals + Zacks current-data trial).

.DESCRIPTION
    Prompts interactively for the Intrinio TRIAL API key via
    Read-Host -AsSecureString so it never appears on screen. It DPAPI-encrypts the
    key for the CURRENT Windows user (ConvertFrom-SecureString, no external key) and
    stores the encrypted blob OUTSIDE the repository under
    C:\Users\<you>\.paper_trader\alpha_agent_intrinio\.

    The key is NEVER printed, NEVER written to the repo, NEVER placed in a
    command-line argument, an environment variable file, a temporary file, JSON
    config, logs, report files or PROJECT_STATE.md, and is never exposed to the
    LLM. Only the DPAPI-encrypted blob is written to disk. IntrinioCollector
    resolves + decrypts this blob at runtime (env var INTRINIO_API_KEY takes
    precedence if you prefer that lane).

    This script performs NO network call and acquires NO data. It does NOT
    activate, subscribe to, or pay any provider — it only stores a key the
    operator already obtained from Intrinio.

.NOTES
    Windows PowerShell 5.1. Run interactively in YOUR own session. Prints exactly
    INTRINIO_CONFIGURED on success or INTRINIO_CONFIGURATION_FAILED - <safe reason>
    on failure. The key itself is never displayed.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_intrinio",
    [string]$KeyFile = "intrinio_api_key.dpapi"
)

$ErrorActionPreference = "Stop"

function Fail {
    param([string]$Reason)
    Write-Host "INTRINIO_CONFIGURATION_FAILED - $Reason"
    exit 1
}

# --------------------------------------------------------------------------- #
# Intrinio TRIAL API key (secret) - read as a SecureString so it never echoes.
# --------------------------------------------------------------------------- #
$SecureInput = Read-Host "Intrinio TRIAL API key" -AsSecureString
if ($null -eq $SecureInput) { Fail "no Intrinio API key was entered" }

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
    if ([string]::IsNullOrEmpty($Normalized)) { Fail "the Intrinio API key was blank" }
    # Intrinio keys are non-trivial tokens; accept any plausible token length.
    if ($Normalized.Length -lt 16) { Fail "the Intrinio API key is implausibly short" }

    # DPAPI-encrypt the key for the current Windows user.
    $SecureNormalized = ConvertTo-SecureString -String $Normalized -AsPlainText -Force
    $Encrypted = ConvertFrom-SecureString -SecureString $SecureNormalized
    Set-Content -Path $KeyPath -Value $Encrypted -Encoding ASCII -Force

    # Lock ACLs to the current user where possible (best-effort).
    try {
        $Me = "$env:USERDOMAIN\$env:USERNAME"
        icacls $KeyPath /inheritance:r /grant:r "${Me}:F" | Out-Null
    } catch { Write-Host "[configure] ACL hardening skipped." }

    # Verify round-trip decryption WITHOUT printing the key.
    $Roundtrip = ConvertTo-SecureString -String ((Get-Content -LiteralPath $KeyPath -Raw).Trim())
    $OutBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Roundtrip)
    $PlainLen = ([Runtime.InteropServices.Marshal]::PtrToStringBSTR($OutBstr)).Length
    if ($PlainLen -lt 16) { Fail "stored Intrinio API key did not decrypt correctly" }

    # Safe metadata only (never the key).
    $EncItem = Get-Item -LiteralPath $KeyPath
    $Sha = (Get-FileHash -LiteralPath $KeyPath -Algorithm SHA256).Hash
    Write-Host "===== INTRINIO CREDENTIAL METADATA (non-secret) ====="
    Write-Host ("  credential_dir : {0}" -f $CredentialDir)
    Write-Host ("  key_file       : {0}" -f $KeyPath)
    Write-Host ("  key_exists     : {0}" -f (Test-Path -LiteralPath $KeyPath))
    Write-Host ("  key_size       : {0}" -f $EncItem.Length)
    Write-Host ("  key_lastwrite  : {0}" -f $EncItem.LastWriteTime.ToString('s'))
    Write-Host ("  key_sha256     : {0}" -f $Sha)
    Write-Host "  next           : run .venv-win\Scripts\python.exe scripts\intrinio_entitlement_probe.py to probe entitlement read-only"

    Write-Host "INTRINIO_CONFIGURED"
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
