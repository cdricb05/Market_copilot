<#
.SYNOPSIS
    One-time secure Gmail SMTP App Password setup for the Alpha Agent Stage 4
    runtime.

.DESCRIPTION
    Prompts interactively for the Gmail account and a dedicated Google App
    Password (via Read-Host -AsSecureString so it never appears on screen),
    normalises the App Password IN MEMORY (removes the spaces Google shows and
    requires exactly 16 characters), encrypts it with Windows DPAPI for the
    CURRENT Windows user (ConvertFrom-SecureString, no external key) and stores
    the encrypted blob outside the repository under
    C:\Users\<you>\.paper_trader\alpha_agent_email\.

    The App Password is NEVER printed, NEVER written to the repo, NEVER placed in
    a command-line argument, an environment variable, a temporary file, JSON
    config, logs or report files, and is never exposed to the LLM. Only the
    DPAPI-encrypted blob and the (non-secret) account address are written to
    disk.

    This is the SMTP replacement for the retired Gmail API OAuth transport. It
    does NOT send an email.

.NOTES
    Windows PowerShell 5.1. Run interactively in YOUR own session. Prints exactly
    GMAIL_SMTP_CONFIGURED on success or GMAIL_SMTP_CONFIGURATION_FAILED -
    <safe reason> on failure. The App Password itself is never displayed.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_email",
    [string]$AppPasswordFile = "gmail_smtp_app_password.dpapi",
    [string]$AccountFile = "gmail_smtp_account.txt",
    [string]$DefaultAccount = "binisti@gmail.com"
)

$ErrorActionPreference = "Stop"

function Fail {
    param([string]$Reason)
    Write-Host "GMAIL_SMTP_CONFIGURATION_FAILED - $Reason"
    exit 1
}

# --------------------------------------------------------------------------- #
# 1. Gmail account (non-secret). Default to binisti@gmail.com.
# --------------------------------------------------------------------------- #
$Account = Read-Host "Gmail account [$DefaultAccount]"
if ([string]::IsNullOrWhiteSpace($Account)) { $Account = $DefaultAccount }
$Account = $Account.Trim()
if ($Account -notmatch '^[^@\s]+@[^@\s]+\.[^@\s]+$') {
    Fail "the Gmail account address is not a valid email address"
}

# --------------------------------------------------------------------------- #
# 2. App Password (secret) - read as a SecureString so it never echoes.
# --------------------------------------------------------------------------- #
$SecureInput = Read-Host "Gmail App Password (16 chars, spaces allowed)" -AsSecureString
if ($null -eq $SecureInput) { Fail "no App Password was entered" }

if (-not (Test-Path -LiteralPath $CredentialDir)) {
    New-Item -ItemType Directory -Path $CredentialDir -Force | Out-Null
}

$AppPasswordPath = Join-Path $CredentialDir $AppPasswordFile
$AccountPath = Join-Path $CredentialDir $AccountFile

$InBstr = [IntPtr]::Zero
$OutBstr = [IntPtr]::Zero
$Normalized = $null
try {
    # Decrypt the entered SecureString in memory ONLY to normalise + validate.
    $InBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureInput)
    $Raw = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($InBstr)

    # Google shows App Passwords in four space-separated groups; strip ALL
    # whitespace, then require exactly 16 alphanumeric characters.
    $Normalized = ($Raw -replace '\s', '')
    if ([string]::IsNullOrEmpty($Normalized)) { Fail "the App Password was blank" }
    if ($Normalized.Length -ne 16) {
        Fail "the App Password must be exactly 16 characters after removing spaces"
    }
    if ($Normalized -notmatch '^[A-Za-z0-9]{16}$') {
        Fail "the App Password must be 16 letters/digits (Google App Password format)"
    }

    # DPAPI-encrypt the NORMALISED App Password for the current Windows user.
    # ConvertFrom-SecureString with no -Key uses the Windows Data Protection API
    # scoped to the current account.
    $SecureNormalized = ConvertTo-SecureString -String $Normalized -AsPlainText -Force
    $Encrypted = ConvertFrom-SecureString -SecureString $SecureNormalized

    Set-Content -Path $AppPasswordPath -Value $Encrypted -Encoding ASCII -Force
    Set-Content -Path $AccountPath -Value $Account -Encoding ASCII -Force

    # Lock the ACL down to the current user where possible (best-effort).
    try {
        $Me = "$env:USERDOMAIN\$env:USERNAME"
        icacls $AppPasswordPath /inheritance:r /grant:r "${Me}:F" | Out-Null
        icacls $AccountPath /inheritance:r /grant:r "${Me}:F" | Out-Null
    }
    catch {
        Write-Host "[configure] ACL hardening skipped."
    }

    # Verify round-trip decryption WITHOUT printing the App Password.
    $Roundtrip = ConvertTo-SecureString -String ((Get-Content -LiteralPath $AppPasswordPath -Raw).Trim())
    $OutBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Roundtrip)
    $PlainLen = ([Runtime.InteropServices.Marshal]::PtrToStringBSTR($OutBstr)).Length
    if ($PlainLen -ne 16) { Fail "stored App Password did not decrypt to 16 characters" }

    # --------------------------------------------------------------------- #
    # Safe metadata only (never the App Password).
    # --------------------------------------------------------------------- #
    $EncItem = Get-Item -LiteralPath $AppPasswordPath
    $Sha = (Get-FileHash -LiteralPath $AppPasswordPath -Algorithm SHA256).Hash
    Write-Host "===== SMTP CREDENTIAL METADATA (non-secret) ====="
    Write-Host ("  credential_dir : {0}" -f $CredentialDir)
    Write-Host ("  account        : {0}" -f $Account)
    Write-Host ("  smtp_host      : smtp.gmail.com")
    Write-Host ("  smtp_port      : 587 (STARTTLS)")
    Write-Host ("  enc_file       : {0}" -f $AppPasswordPath)
    Write-Host ("  enc_exists     : {0}" -f (Test-Path -LiteralPath $AppPasswordPath))
    Write-Host ("  enc_size       : {0}" -f $EncItem.Length)
    Write-Host ("  enc_lastwrite  : {0}" -f $EncItem.LastWriteTime.ToString('s'))
    Write-Host ("  enc_sha256     : {0}" -f $Sha)

    Write-Host "GMAIL_SMTP_CONFIGURED"
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
