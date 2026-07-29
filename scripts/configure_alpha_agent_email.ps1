<#
.SYNOPSIS
    One-time secure Gmail API OAuth setup for the Alpha Agent Stage 4 runtime.

.DESCRIPTION
    Runs the standalone Gmail API OAuth Desktop authorization flow
    (scripts\authorize_alpha_agent_gmail.py) in your own interactive session,
    captures ONLY the resulting refresh token from the child's stdout (never
    displaying it), encrypts it with Windows DPAPI for the CURRENT Windows user
    (ConvertFrom-SecureString, no external key) and stores the encrypted blob
    outside the repository under
    C:\Users\<you>\.paper_trader\alpha_agent_email\. The refresh token is never
    printed, never written to the repo, never placed in JSON config, logs or
    report files, and is never exposed to the LLM.

    Legacy password-based email delivery is not used anywhere. The Desktop OAuth
    client JSON must already exist at the credential directory
    (google_oauth_client.json).

.NOTES
    Windows PowerShell 5.1. Run interactively in YOUR own session; a browser
    window opens for Google consent. Prints exactly GMAIL_OAUTH_CONFIGURED on
    success or GMAIL_OAUTH_CONFIGURATION_FAILED — <safe reason> on failure.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_email",
    [string]$OAuthClientFile = "google_oauth_client.json",
    [string]$RefreshTokenFile = "gmail_oauth_refresh_token.dpapi",
    [string]$AccountFile = "gmail_oauth_account.txt",
    [string]$Account = "binisti@gmail.com",
    [string]$Scope = "https://www.googleapis.com/auth/gmail.send",
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = "Stop"

function Fail {
    param([string]$Reason)
    Write-Host "GMAIL_OAUTH_CONFIGURATION_FAILED — $Reason"
    exit 1
}

function Quote-Arg {
    # .NET Framework has no ProcessStartInfo.ArgumentList; build one quoted
    # string. Inputs (file paths, an email address, a scope URL, integers) never
    # contain embedded quotes.
    param([string]$Value)
    return '"' + ($Value -replace '"', '""') + '"'
}

$Repo = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Repo ".venv-win\Scripts\python.exe"
$AuthorizeScript = Join-Path $Repo "scripts\authorize_alpha_agent_gmail.py"
$ClientPath = Join-Path $CredentialDir $OAuthClientFile

if (-not (Test-Path -LiteralPath $Python)) { Fail "repository Python is missing" }
if (-not (Test-Path -LiteralPath $AuthorizeScript)) { Fail "authorization script is missing" }
if (-not (Test-Path -LiteralPath $ClientPath)) { Fail "OAuth client secrets file is missing" }
if (-not (Test-Path -LiteralPath $CredentialDir)) {
    New-Item -ItemType Directory -Path $CredentialDir -Force | Out-Null
}

# Run the authorization flow and capture its stdout WITHOUT displaying it.
$StartInfo = New-Object System.Diagnostics.ProcessStartInfo
$StartInfo.FileName = $Python
$StartInfo.RedirectStandardOutput = $true
$StartInfo.RedirectStandardError = $true
$StartInfo.UseShellExecute = $false
$StartInfo.CreateNoWindow = $true
$StartInfo.WorkingDirectory = $Repo
$StartInfo.Arguments = (
    (Quote-Arg $AuthorizeScript) +
    " --client-secrets-path " + (Quote-Arg $ClientPath) +
    " --account " + (Quote-Arg $Account) +
    " --scope " + (Quote-Arg $Scope) +
    " --timeout-seconds " + (Quote-Arg ([string]$TimeoutSeconds))
)

$Process = New-Object System.Diagnostics.Process
$Process.StartInfo = $StartInfo
[void]$Process.Start()
$StdOut = $Process.StandardOutput.ReadToEnd()
$null = $Process.StandardError.ReadToEnd()
$Process.WaitForExit()

# Parse the single JSON result line WITHOUT echoing stdout (it holds the token).
$JsonLine = $null
foreach ($Line in ($StdOut -split "`r?`n")) {
    $Trimmed = $Line.Trim()
    if ($Trimmed.StartsWith("{") -and $Trimmed.EndsWith("}")) {
        $JsonLine = $Trimmed
    }
}
if (-not $JsonLine) { Fail "authorization returned no result" }

try { $Result = $JsonLine | ConvertFrom-Json }
catch { Fail "authorization result was unparseable" }

if ($Result.status -ne "GMAIL_AUTHORIZATION_OK") {
    $Reason = if ($Result.diagnostic) { [string]$Result.diagnostic }
    else { [string]$Result.status }
    Fail $Reason
}

$RefreshToken = [string]$Result.refresh_token
if ([string]::IsNullOrWhiteSpace($RefreshToken)) { Fail "no refresh token was returned" }

$Bstr = [IntPtr]::Zero
try {
    # DPAPI-encrypt the refresh token for the current user. ConvertFrom-Secure
    # String with no -Key uses the Windows Data Protection API scoped to the
    # current Windows account.
    $Secure = ConvertTo-SecureString -String $RefreshToken -AsPlainText -Force
    $Encrypted = ConvertFrom-SecureString -SecureString $Secure
    $TokenPath = Join-Path $CredentialDir $RefreshTokenFile
    $AccountPath = Join-Path $CredentialDir $AccountFile
    Set-Content -Path $TokenPath -Value $Encrypted -Encoding ASCII -Force
    Set-Content -Path $AccountPath -Value $Account -Encoding ASCII -Force

    # Lock the ACL down to the current user where possible (best-effort).
    try {
        $Me = "$env:USERDOMAIN\$env:USERNAME"
        icacls $TokenPath /inheritance:r /grant:r "${Me}:F" | Out-Null
        icacls $AccountPath /inheritance:r /grant:r "${Me}:F" | Out-Null
    }
    catch {
        Write-Host "[configure] ACL hardening skipped."
    }

    # Verify round-trip decryption WITHOUT printing the token.
    $Roundtrip = ConvertTo-SecureString -String ((Get-Content -LiteralPath $TokenPath -Raw).Trim())
    $Bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Roundtrip)
    $PlainLen = ([Runtime.InteropServices.Marshal]::PtrToStringBSTR($Bstr)).Length
    if ($PlainLen -le 0) { Fail "stored refresh token did not decrypt" }

    Write-Host "GMAIL_OAUTH_CONFIGURED"
    exit 0
}
finally {
    $RefreshToken = $null
    $Result = $null
    $JsonLine = $null
    $StdOut = $null
    if ($Bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($Bstr)
    }
    [GC]::Collect()
}
