<#
Alpha Agent Stage 4 — Gmail OAuth credential DIAGNOSTIC wrapper (READ-ONLY).

Purpose: make a Gmail delivery failure DISTINGUISHABLE without sending an email,
opening a browser, or exposing any secret. It reports non-secret credential
metadata (paths, sizes, timestamps, SHA-256 fingerprints, path/profile
consistency), then decrypts the Windows DPAPI refresh token IN MEMORY and hands
it to the standalone probe (diagnose_alpha_agent_gmail.py) over a REDIRECTED
STDIN pipe only. The token, client secret and DPAPI plaintext never appear in a
command-line argument, an environment variable, a file, or console output.

Performs exactly ONE OAuth token-exchange (no email send, no authorization). It
never writes to the credential store or the repository.

Prints a human-readable metadata block, then the probe's single JSON result, then
one machine-readable classification token on its own line.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_email",
    [string]$OAuthClientFile = "google_oauth_client.json",
    [string]$RefreshTokenFile = "gmail_oauth_refresh_token.dpapi",
    [string]$AccountFile = "gmail_oauth_account.txt",
    [string]$TokenEndpoint = "https://oauth2.googleapis.com/token",
    [int]$TimeoutSeconds = 30
)

$ErrorActionPreference = "Stop"

$Repo = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Repo ".venv-win\Scripts\python.exe"
$Probe = Join-Path $Repo "scripts\diagnose_alpha_agent_gmail.py"

$ClientPath = Join-Path $CredentialDir $OAuthClientFile
$TokenPath = Join-Path $CredentialDir $RefreshTokenFile
$AccountPath = Join-Path $CredentialDir $AccountFile

function Quote-Arg {
    param([string]$Value)
    return '"' + ($Value -replace '"', '""') + '"'
}

function Show-FileMeta {
    param([string]$Label, [string]$Path)
    if (Test-Path -LiteralPath $Path) {
        $f = Get-Item -LiteralPath $Path
        $sha = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash
        Write-Host ("  {0,-14} exists size={1} lastwrite={2} sha256={3}" -f `
            $Label, $f.Length, $f.LastWriteTime.ToString('s'), $sha)
        return $f.LastWriteTime
    }
    Write-Host ("  {0,-14} MISSING ({1})" -f $Label, $Path)
    return $null
}

Write-Host "===== CREDENTIAL METADATA (non-secret) ====="
Write-Host ("  credential_dir : {0}" -f $CredentialDir)
Write-Host ("  configure_dir  : {0}\.paper_trader\alpha_agent_email" -f $env:USERPROFILE)
$tokMtime = Show-FileMeta "refresh_token" $TokenPath
$acctMtime = Show-FileMeta "account" $AccountPath
$null = Show-FileMeta "oauth_client" $ClientPath

if (-not (Test-Path -LiteralPath $TokenPath)) {
    Write-Host '{"classification":"TOKEN_FILE_NOT_FOUND"}'
    Write-Host "TOKEN_FILE_NOT_FOUND"
    exit 1
}

# The configure script writes the token and account files TOGETHER; equal
# last-write times mean the token has not been altered since configuration. A
# token file newer than the account file was rewritten out-of-band.
if ($tokMtime -ne $null -and $acctMtime -ne $null) {
    $delta = [math]::Abs(($tokMtime - $acctMtime).TotalSeconds)
    if ($delta -gt 5) {
        Write-Host ("  NOTE: TOKEN_FILE_CHANGED_AFTER_CONFIGURATION (token/account write times differ by {0}s)" -f [math]::Round($delta))
    } else {
        Write-Host "  token/account write times consistent (single configuration)"
    }
}

if (-not (Test-Path -LiteralPath $ClientPath) -or -not (Test-Path -LiteralPath $AccountPath)) {
    Write-Host '{"classification":"TOKEN_FILE_NOT_FOUND"}'
    Write-Host "TOKEN_FILE_NOT_FOUND"
    exit 1
}

$Account = (Get-Content -LiteralPath $AccountPath -Raw).Trim()
$Encrypted = (Get-Content -LiteralPath $TokenPath -Raw).Trim()
$Secure = ConvertTo-SecureString -String $Encrypted
$Bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
$Plain = $null

Write-Host "`n===== TOKEN-EXCHANGE PROBE (no email sent) ====="
try {
    $Plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($Bstr)
    if ([string]::IsNullOrEmpty($Plain)) {
        Write-Host '{"classification":"TOKEN_FILE_NOT_FOUND","google_error_description":"DPAPI token did not decrypt."}'
        Write-Host "TOKEN_FILE_NOT_FOUND"
        exit 1
    }

    $StartInfo = New-Object System.Diagnostics.ProcessStartInfo
    $StartInfo.FileName = $Python
    $StartInfo.RedirectStandardInput = $true
    $StartInfo.RedirectStandardOutput = $true
    $StartInfo.RedirectStandardError = $true
    $StartInfo.UseShellExecute = $false
    $StartInfo.CreateNoWindow = $true
    $StartInfo.WorkingDirectory = $Repo
    $StartInfo.Arguments = (
        (Quote-Arg $Probe) +
        " --oauth-client-path " + (Quote-Arg $ClientPath) +
        " --expected-account " + (Quote-Arg $Account) +
        " --token-endpoint " + (Quote-Arg $TokenEndpoint) +
        " --timeout-seconds " + (Quote-Arg ([string]$TimeoutSeconds))
    )

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
        catch { Write-Host "TOKEN_EXCHANGE_UNREACHABLE" }
    } else {
        Write-Host "TOKEN_EXCHANGE_UNREACHABLE"
    }
}
finally {
    $Plain = $null
    if ($Bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($Bstr)
    }
}
