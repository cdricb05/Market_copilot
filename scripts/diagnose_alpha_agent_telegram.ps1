<#
Alpha Agent Stage 8 - Telegram control-plane DIAGNOSTIC wrapper (READ-ONLY).

Purpose: verify the Telegram bot token authenticates (getMe) WITHOUT sending any
message or exposing the token. It reports non-secret credential metadata (paths,
sizes, timestamps, SHA-256 fingerprints, allowed ids), then decrypts the Windows
DPAPI bot token IN MEMORY and hands it to the Python control module
(paper_trader.alpha_agent.telegram_control --mode diagnose) over a REDIRECTED
STDIN pipe only. The bot token never appears in a command-line argument, an
environment variable, a file, or console output.

The probe performs exactly ONE read-only getMe call; it never calls getUpdates
with side effects, never sends a message, and never writes to the credential
store or the repository.

Prints a metadata block, then the probe's single JSON result, then one
machine-readable classification token on its own line.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_telegram",
    [string]$TokenFile = "telegram_bot_token.dpapi",
    [string]$AllowedIdsFile = "telegram_allowed_ids.json",
    [string]$Config
)

$ErrorActionPreference = "Stop"

$Repo = Split-Path -Parent $PSScriptRoot
$PythonParent = Split-Path -Parent $Repo
$Python = Join-Path $Repo ".venv-win\Scripts\python.exe"
if (-not $Config) { $Config = Join-Path $Repo "configs\alpha_agent\stage8_autonomy.json" }

$TokenPath = Join-Path $CredentialDir $TokenFile
$AllowedIdsPath = Join-Path $CredentialDir $AllowedIdsFile

function Show-FileMeta {
    param([string]$Label, [string]$Path)
    if (Test-Path -LiteralPath $Path) {
        $f = Get-Item -LiteralPath $Path
        $sha = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash
        Write-Host ("  {0,-14} exists size={1} lastwrite={2} sha256={3}" -f `
            $Label, $f.Length, $f.LastWriteTime.ToString('s'), $sha)
        return
    }
    Write-Host ("  {0,-14} MISSING ({1})" -f $Label, $Path)
}

Write-Host "===== TELEGRAM CREDENTIAL METADATA (non-secret) ====="
Write-Host ("  credential_dir : {0}" -f $CredentialDir)
Show-FileMeta "token" $TokenPath
Show-FileMeta "allowlist" $AllowedIdsPath
if (Test-Path -LiteralPath $AllowedIdsPath) {
    try {
        $ids = Get-Content -LiteralPath $AllowedIdsPath -Raw | ConvertFrom-Json
        Write-Host ("  allowed_users  : {0}" -f ($ids.allowed_user_ids -join ','))
        Write-Host ("  allowed_chats  : {0}" -f ($ids.allowed_chat_ids -join ','))
    } catch { }
}

if (-not (Test-Path -LiteralPath $TokenPath)) {
    Write-Host '{"classification":"TELEGRAM_TOKEN_MISSING"}'
    Write-Host "TELEGRAM_TOKEN_MISSING"
    exit 1
}

$Encrypted = (Get-Content -LiteralPath $TokenPath -Raw).Trim()
$Secure = ConvertTo-SecureString -String $Encrypted
$Bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
$Plain = $null

Write-Host "`n===== TELEGRAM AUTH PROBE (getMe, no message sent) ====="
try {
    $Plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($Bstr)
    if ([string]::IsNullOrEmpty($Plain)) {
        Write-Host '{"classification":"TELEGRAM_TOKEN_MISSING"}'
        Write-Host "TELEGRAM_TOKEN_MISSING"
        exit 1
    }

    $StartInfo = New-Object System.Diagnostics.ProcessStartInfo
    $StartInfo.FileName = $Python
    $StartInfo.RedirectStandardInput = $true
    $StartInfo.RedirectStandardOutput = $true
    $StartInfo.RedirectStandardError = $true
    $StartInfo.UseShellExecute = $false
    $StartInfo.CreateNoWindow = $true
    # CWD = repo parent so 'paper_trader' is importable via -m.
    $StartInfo.WorkingDirectory = $PythonParent
    $StartInfo.Arguments = (
        "-m paper_trader.alpha_agent.telegram_control --mode diagnose " +
        "--config " + ('"' + ($Config -replace '"', '""') + '"'))

    $Process = New-Object System.Diagnostics.Process
    $Process.StartInfo = $StartInfo
    [void]$Process.Start()
    $Process.StandardInput.WriteLine($Plain)
    $Process.StandardInput.Close()
    $StdOut = $Process.StandardOutput.ReadToEnd()
    $null = $Process.StandardError.ReadToEnd()
    $Process.WaitForExit()

    $JsonLine = $null
    foreach ($Line in ($StdOut -split "`r?`n")) {
        $Trimmed = $Line.Trim()
        if ($Trimmed.StartsWith("{") -and $Trimmed.EndsWith("}")) { $JsonLine = $Trimmed }
    }
    if ($JsonLine) {
        Write-Host $JsonLine
        try { $Result = $JsonLine | ConvertFrom-Json; Write-Host ([string]$Result.classification) }
        catch { Write-Host "TELEGRAM_UNREACHABLE" }
    } else {
        Write-Host "TELEGRAM_UNREACHABLE"
    }
}
finally {
    $Plain = $null
    if ($Bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($Bstr)
    }
}
