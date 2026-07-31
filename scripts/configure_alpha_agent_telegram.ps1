<#
.SYNOPSIS
    One-time secure Telegram control-plane setup for the Alpha Agent Stage 8
    autonomous runtime.

.DESCRIPTION
    Prompts interactively for the Telegram BOT TOKEN (via Read-Host
    -AsSecureString so it never appears on screen), the ONE allowed numeric
    Telegram user id and the ONE allowed private chat id. It DPAPI-encrypts the
    bot token for the CURRENT Windows user (ConvertFrom-SecureString, no
    external key) and stores the encrypted blob outside the repository under
    C:\Users\<you>\.paper_trader\alpha_agent_telegram\. The allowed numeric ids
    are written as a NON-SECRET json file in the same external directory (never
    in the repo).

    The bot token is NEVER printed, NEVER written to the repo, NEVER placed in a
    command-line argument, an environment variable, a temporary file, JSON
    config, logs, report files or PROJECT_STATE.md, and is never exposed to the
    LLM. Only the DPAPI-encrypted blob and the (non-secret) numeric allowlist
    are written to disk.

    This script does NOT start polling and does NOT send any Telegram message.

.NOTES
    Windows PowerShell 5.1. Run interactively in YOUR own session. Prints exactly
    TELEGRAM_CONFIGURED on success or TELEGRAM_CONFIGURATION_FAILED - <safe
    reason> on failure. The bot token itself is never displayed.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_telegram",
    [string]$TokenFile = "telegram_bot_token.dpapi",
    [string]$AllowedIdsFile = "telegram_allowed_ids.json"
)

$ErrorActionPreference = "Stop"

function Fail {
    param([string]$Reason)
    Write-Host "TELEGRAM_CONFIGURATION_FAILED - $Reason"
    exit 1
}

# --------------------------------------------------------------------------- #
# 1. Allowed numeric user id + private chat id (non-secret).
# --------------------------------------------------------------------------- #
# Telegram ids can exceed the signed 32-bit range (e.g. 8284912423), so they are
# NEVER cast to [int]/[System.Int32]. They are validated as decimal strings and
# persisted as normalized decimal strings. For this private-chat configuration
# both the user id and the private chat id must be positive.
$UserIdRaw = (Read-Host "Allowed Telegram numeric USER id").Trim()
if ($UserIdRaw -notmatch '^\d{3,}$') { Fail "the user id must be a positive decimal number (no letters, no decimals)" }
$ChatIdRaw = (Read-Host "Allowed Telegram private CHAT id (often same as user id)").Trim()
if ($ChatIdRaw -notmatch '^\d{3,}$') { Fail "the private chat id must be a positive decimal number (no letters, no decimals)" }

# --------------------------------------------------------------------------- #
# 2. Bot token (secret) - read as a SecureString so it never echoes.
# --------------------------------------------------------------------------- #
$SecureInput = Read-Host "Telegram BOT TOKEN (from BotFather)" -AsSecureString
if ($null -eq $SecureInput) { Fail "no bot token was entered" }

if (-not (Test-Path -LiteralPath $CredentialDir)) {
    New-Item -ItemType Directory -Path $CredentialDir -Force | Out-Null
}

$TokenPath = Join-Path $CredentialDir $TokenFile
$AllowedIdsPath = Join-Path $CredentialDir $AllowedIdsFile

$InBstr = [IntPtr]::Zero
$OutBstr = [IntPtr]::Zero
$Normalized = $null
try {
    # Decrypt the entered SecureString in memory ONLY to validate its shape.
    $InBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureInput)
    $Raw = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($InBstr)
    $Normalized = ($Raw -replace '\s', '')
    if ([string]::IsNullOrEmpty($Normalized)) { Fail "the bot token was blank" }
    # BotFather tokens look like 123456789:AA...(35 url-safe chars).
    if ($Normalized -notmatch '^\d{6,}:[A-Za-z0-9_-]{30,}$') {
        Fail "the bot token is not in the expected BotFather format"
    }

    # DPAPI-encrypt the token for the current Windows user.
    $SecureNormalized = ConvertTo-SecureString -String $Normalized -AsPlainText -Force
    $Encrypted = ConvertFrom-SecureString -SecureString $SecureNormalized
    Set-Content -Path $TokenPath -Value $Encrypted -Encoding ASCII -Force

    # Write the NON-SECRET allowlist json (outside the repo). Ids are stored as
    # normalized decimal STRINGS - never [int]/[System.Int32] (which overflows
    # at 2147483647) and never a JSON number (which loses precision above
    # 2**53). The forced single-element array keeps the shape stable for a
    # one-element allowlist.
    $Allow = [ordered]@{
        allowed_user_ids = @([string]$UserIdRaw)
        allowed_chat_ids = @([string]$ChatIdRaw)
        configured_at    = (Get-Date).ToString('s')
    }
    ($Allow | ConvertTo-Json -Compress) | Set-Content -Path $AllowedIdsPath -Encoding ASCII -Force

    # Lock ACLs to the current user where possible (best-effort).
    try {
        $Me = "$env:USERDOMAIN\$env:USERNAME"
        icacls $TokenPath /inheritance:r /grant:r "${Me}:F" | Out-Null
        icacls $AllowedIdsPath /inheritance:r /grant:r "${Me}:F" | Out-Null
    } catch { Write-Host "[configure] ACL hardening skipped." }

    # Verify round-trip decryption WITHOUT printing the token.
    $Roundtrip = ConvertTo-SecureString -String ((Get-Content -LiteralPath $TokenPath -Raw).Trim())
    $OutBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Roundtrip)
    $PlainLen = ([Runtime.InteropServices.Marshal]::PtrToStringBSTR($OutBstr)).Length
    if ($PlainLen -lt 20) { Fail "stored bot token did not decrypt correctly" }

    # Safe metadata only (never the token).
    $EncItem = Get-Item -LiteralPath $TokenPath
    $Sha = (Get-FileHash -LiteralPath $TokenPath -Algorithm SHA256).Hash
    Write-Host "===== TELEGRAM CREDENTIAL METADATA (non-secret) ====="
    Write-Host ("  credential_dir : {0}" -f $CredentialDir)
    Write-Host ("  allowed_user   : {0}" -f $UserIdRaw)
    Write-Host ("  allowed_chat   : {0}" -f $ChatIdRaw)
    Write-Host ("  token_file     : {0}" -f $TokenPath)
    Write-Host ("  token_exists   : {0}" -f (Test-Path -LiteralPath $TokenPath))
    Write-Host ("  token_size     : {0}" -f $EncItem.Length)
    Write-Host ("  token_lastwrite: {0}" -f $EncItem.LastWriteTime.ToString('s'))
    Write-Host ("  token_sha256   : {0}" -f $Sha)
    Write-Host ("  allowlist_file : {0}" -f $AllowedIdsPath)

    Write-Host "TELEGRAM_CONFIGURED"
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
