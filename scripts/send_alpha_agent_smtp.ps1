<#
Alpha Agent Stage 4 — Gmail SMTP sender wrapper (App Password).

This is a SMALL wrapper. It uses Gmail SMTP (smtp.gmail.com:587, STARTTLS)
exclusively — the replacement for the retired Gmail API OAuth transport. It does
NOT embed Python source and NEVER invokes python with -c. It decrypts the Windows
DPAPI Gmail App Password in memory and hands it to the standalone sender
(scripts\send_alpha_agent_smtp.py) through a REDIRECTED STDIN pipe only. The App
Password never appears in a command-line argument, an environment variable, a
file, or a log line.

The child prints exactly one JSON result line; this wrapper parses it, prints the
machine-readable status (plus one safe diagnostic on failure) and the generated
RFC Message-ID, then moves the outbox job atomically to sent\ or failed\.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$JobPath,

    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_email",
    [string]$AppPasswordFile = "gmail_smtp_app_password.dpapi",
    [string]$AccountFile = "gmail_smtp_account.txt",
    [string]$SmtpHost = "smtp.gmail.com",
    [int]$SmtpPort = 587,
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = "Stop"

$Repo = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Repo ".venv-win\Scripts\python.exe"
$Sender = Join-Path $Repo "scripts\send_alpha_agent_smtp.py"

$AppPasswordPath = Join-Path $CredentialDir $AppPasswordFile
$AccountPath = Join-Path $CredentialDir $AccountFile

function Quote-Arg {
    # Windows PowerShell 5.1 runs on .NET Framework, which has no
    # ProcessStartInfo.ArgumentList; arguments are one string. Wrap each value in
    # double quotes so paths with spaces survive. Our inputs (file paths, an
    # email address, a host, integers) never contain embedded quotes.
    param([string]$Value)
    return '"' + ($Value -replace '"', '""') + '"'
}

function Move-OutboxJob {
    param([string]$Path, [string]$Status)
    try {
        if (-not (Test-Path -LiteralPath $Path)) { return }
        $Item = Get-Item -LiteralPath $Path
        $OutboxRoot = $Item.Directory.Parent.FullName
        $Folder = "failed"
        if ($Status -eq "EMAIL_SENT") { $Folder = "sent" }
        $DestinationDirectory = Join-Path $OutboxRoot $Folder
        New-Item -ItemType Directory -Path $DestinationDirectory -Force | Out-Null
        $Destination = Join-Path $DestinationDirectory $Item.Name
        Move-Item -LiteralPath $Item.FullName -Destination $Destination -Force
    }
    catch {
        # A secondary outbox move failure must not mask the delivery result; the
        # Python runtime also reconciles outbox transitions.
    }
}

if (-not (Test-Path -LiteralPath $Python)) {
    Write-Host "EMAIL_SEND_FAILED"
    Write-Host "[smtp] Python executable is missing."
    exit 1
}
if (-not (Test-Path -LiteralPath $Sender)) {
    Write-Host "EMAIL_SEND_FAILED"
    Write-Host "[smtp] Standalone SMTP sender script is missing."
    exit 1
}
if (-not (Test-Path -LiteralPath $JobPath)) {
    Write-Host "EMAIL_SEND_FAILED"
    Write-Host "[smtp] Outbox job is missing."
    exit 1
}
if (-not (Test-Path -LiteralPath $AppPasswordPath) -or
    -not (Test-Path -LiteralPath $AccountPath)) {
    Write-Host "EMAIL_SMTP_CREDENTIAL_MISSING"
    Write-Host "[smtp] Gmail SMTP App Password is not configured."
    Move-OutboxJob -Path $JobPath -Status "EMAIL_SMTP_CREDENTIAL_MISSING"
    exit 0
}

$Account = (Get-Content -LiteralPath $AccountPath -Raw).Trim()
$Encrypted = (Get-Content -LiteralPath $AppPasswordPath -Raw).Trim()

$Secure = ConvertTo-SecureString -String $Encrypted
$Bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
$Plain = $null

try {
    $Plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($Bstr)

    if ([string]::IsNullOrEmpty($Plain)) {
        Write-Host "EMAIL_SMTP_CREDENTIAL_MISSING"
        Write-Host "[smtp] Stored App Password did not decrypt."
        Move-OutboxJob -Path $JobPath -Status "EMAIL_SMTP_CREDENTIAL_MISSING"
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

    # Non-secret arguments only, as one quoted string. The App Password is NEVER
    # passed here — it goes only through the redirected stdin pipe below.
    $StartInfo.Arguments = (
        (Quote-Arg $Sender) + " --job-path " + (Quote-Arg $JobPath) +
        " --account " + (Quote-Arg $Account) +
        " --smtp-host " + (Quote-Arg $SmtpHost) +
        " --smtp-port " + (Quote-Arg ([string]$SmtpPort)) +
        " --timeout-seconds " + (Quote-Arg ([string]$TimeoutSeconds))
    )

    $Process = New-Object System.Diagnostics.Process
    $Process.StartInfo = $StartInfo
    [void]$Process.Start()

    # Pass the App Password ONLY through redirected stdin, then close it at once.
    $Process.StandardInput.WriteLine($Plain)
    $Process.StandardInput.Close()

    $StdOut = $Process.StandardOutput.ReadToEnd()
    $StdErr = $Process.StandardError.ReadToEnd()
    $Process.WaitForExit()

    $Status = "EMAIL_SEND_FAILED"
    $Diagnostic = "The SMTP sender returned no parsable result."
    $MessageId = $null

    $JsonLine = $null
    foreach ($Line in ($StdOut -split "`r?`n")) {
        $Trimmed = $Line.Trim()
        if ($Trimmed.StartsWith("{") -and $Trimmed.EndsWith("}")) {
            $JsonLine = $Trimmed
        }
    }

    if ($JsonLine) {
        try {
            $Result = $JsonLine | ConvertFrom-Json
            if ($Result.status) { $Status = [string]$Result.status }
            if ($Result.diagnostic) { $Diagnostic = [string]$Result.diagnostic }
            if ($Result.message_id) { $MessageId = [string]$Result.message_id }
        }
        catch {
            $Status = "EMAIL_SEND_FAILED"
            $Diagnostic = "The SMTP sender result could not be parsed."
        }
    }

    # Echo the sender's SAFE JSON result (status + RFC Message-ID + fixed
    # diagnostic only — never a secret) so the runtime can preserve the message
    # id and diagnostic, then the machine-readable status token.
    if ($JsonLine) { Write-Host $JsonLine }
    Write-Host $Status
    if ($Status -ne "EMAIL_SENT") {
        Write-Host "[smtp] $Diagnostic"
        if ($StdErr -and $StdErr.Trim().Length -gt 0) {
            Write-Host "[smtp] The SMTP sender reported an unexpected error."
        }
    }

    Move-OutboxJob -Path $JobPath -Status $Status

    if ($Status -eq "EMAIL_SENT") { exit 0 }
    exit 1
}
finally {
    $Plain = $null
    if ($Bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($Bstr)
    }
}
