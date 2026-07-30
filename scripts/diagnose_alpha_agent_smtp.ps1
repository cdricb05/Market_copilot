<#
Alpha Agent Stage 4 — Gmail SMTP credential DIAGNOSTIC wrapper (READ-ONLY).

Purpose: verify the Gmail SMTP App Password authenticates WITHOUT sending an
email or exposing any secret. It reports non-secret credential metadata (paths,
sizes, timestamps, SHA-256 fingerprints), then decrypts the Windows DPAPI App
Password IN MEMORY and hands it to the standalone probe
(diagnose_alpha_agent_smtp.py) over a REDIRECTED STDIN pipe only. The App
Password, the SMTP AUTH exchange and the DPAPI plaintext never appear in a
command-line argument, an environment variable, a file, or console output.

Performs exactly ONE SMTP authentication (ehlo -> starttls -> ehlo -> login ->
NOOP -> quit); it never issues MAIL/RCPT/DATA and never writes to the credential
store or the repository.

Prints a human-readable metadata block, then the probe's single JSON result, then
one machine-readable classification token on its own line.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_email",
    [string]$AppPasswordFile = "gmail_smtp_app_password.dpapi",
    [string]$AccountFile = "gmail_smtp_account.txt",
    [string]$SmtpHost = "smtp.gmail.com",
    [int]$SmtpPort = 587,
    [int]$TimeoutSeconds = 30
)

$ErrorActionPreference = "Stop"

$Repo = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Repo ".venv-win\Scripts\python.exe"
$Probe = Join-Path $Repo "scripts\diagnose_alpha_agent_smtp.py"

$AppPasswordPath = Join-Path $CredentialDir $AppPasswordFile
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
        return
    }
    Write-Host ("  {0,-14} MISSING ({1})" -f $Label, $Path)
}

Write-Host "===== SMTP CREDENTIAL METADATA (non-secret) ====="
Write-Host ("  credential_dir : {0}" -f $CredentialDir)
Write-Host ("  smtp_host      : {0}" -f $SmtpHost)
Write-Host ("  smtp_port      : {0} (STARTTLS)" -f $SmtpPort)
Show-FileMeta "app_password" $AppPasswordPath
Show-FileMeta "account" $AccountPath

if (-not (Test-Path -LiteralPath $AppPasswordPath) -or
    -not (Test-Path -LiteralPath $AccountPath)) {
    Write-Host '{"classification":"SMTP_CREDENTIAL_MISSING"}'
    Write-Host "SMTP_CREDENTIAL_MISSING"
    exit 1
}

$Account = (Get-Content -LiteralPath $AccountPath -Raw).Trim()
$Encrypted = (Get-Content -LiteralPath $AppPasswordPath -Raw).Trim()
$Secure = ConvertTo-SecureString -String $Encrypted
$Bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
$Plain = $null

Write-Host "`n===== SMTP AUTH PROBE (no email sent) ====="
try {
    $Plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($Bstr)
    if ([string]::IsNullOrEmpty($Plain)) {
        Write-Host '{"classification":"SMTP_CREDENTIAL_MISSING"}'
        Write-Host "SMTP_CREDENTIAL_MISSING"
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
        " --account " + (Quote-Arg $Account) +
        " --smtp-host " + (Quote-Arg $SmtpHost) +
        " --smtp-port " + (Quote-Arg ([string]$SmtpPort)) +
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
        catch { Write-Host "SMTP_CONNECTION_FAILED" }
    } else {
        Write-Host "SMTP_CONNECTION_FAILED"
    }
}
finally {
    $Plain = $null
    if ($Bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($Bstr)
    }
}
