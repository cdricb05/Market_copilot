<#
Alpha Agent Stage 8 - Telegram control-plane RUNNER (long polling).

This is the command the AlphaAgent-Telegram scheduled task runs. It decrypts the
Windows DPAPI bot token IN MEMORY and hands it to the Python control module
(paper_trader.alpha_agent.telegram_control --mode poll) over a REDIRECTED STDIN
pipe only, then long-polls the Telegram Bot API for messages from the ONE allowed
user in the ONE allowed private chat.

The control plane is READ-ONLY plus bounded research-request enqueue: it can
answer evidence questions or queue a bounded research job. It can NEVER run a
shell command, execute Python or SQL, delete a file, create an order / fill /
trade decision / signal, promote a model, or mutate holdings or cash.

The bot token never appears in a command-line argument, an environment variable,
a file, or console output. No public webhook is used (long polling only).

Usage:
    powershell -ExecutionPolicy Bypass -File run_alpha_agent_telegram.ps1
    -MaxCycles N   run N poll cycles then exit (0 = run until stopped)
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_telegram",
    [string]$TokenFile = "telegram_bot_token.dpapi",
    [string]$Config,
    [int]$MaxCycles = 0,
    [ValidateSet("poll", "deliver")]
    [string]$Mode = "poll"
)

$ErrorActionPreference = "Stop"

$Repo = Split-Path -Parent $PSScriptRoot
$PythonParent = Split-Path -Parent $Repo
$Python = Join-Path $Repo ".venv-win\Scripts\python.exe"
if (-not $Config) { $Config = Join-Path $Repo "configs\alpha_agent\stage8_autonomy.json" }

$TokenPath = Join-Path $CredentialDir $TokenFile

if (-not (Test-Path -LiteralPath $Python)) {
    Write-Host "TELEGRAM_RUN_FAILED - python not found: $Python"; exit 1
}
if (-not (Test-Path -LiteralPath $TokenPath)) {
    Write-Host "TELEGRAM_TOKEN_MISSING"
    Write-Host "Run: powershell -ExecutionPolicy Bypass -File `"$Repo\scripts\configure_alpha_agent_telegram.ps1`""
    exit 1
}

$Encrypted = (Get-Content -LiteralPath $TokenPath -Raw).Trim()
$Secure = ConvertTo-SecureString -String $Encrypted
$Bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
$Plain = $null

try {
    $Plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($Bstr)
    if ([string]::IsNullOrEmpty($Plain)) { Write-Host "TELEGRAM_TOKEN_MISSING"; exit 1 }

    $StartInfo = New-Object System.Diagnostics.ProcessStartInfo
    $StartInfo.FileName = $Python
    $StartInfo.RedirectStandardInput = $true
    $StartInfo.RedirectStandardOutput = $true
    $StartInfo.RedirectStandardError = $true
    $StartInfo.UseShellExecute = $false
    $StartInfo.CreateNoWindow = $true
    $StartInfo.WorkingDirectory = $PythonParent
    $StartInfo.Arguments = (
        "-m paper_trader.alpha_agent.telegram_control --mode " + $Mode + " " +
        "--max-cycles " + ([string]$MaxCycles) + " " +
        "--config " + ('"' + ($Config -replace '"', '""') + '"'))

    $Process = New-Object System.Diagnostics.Process
    $Process.StartInfo = $StartInfo
    [void]$Process.Start()
    $Process.StandardInput.WriteLine($Plain)
    $Process.StandardInput.Close()
    # Stream the child's stdout while it polls.
    while (-not $Process.HasExited) {
        $Line = $Process.StandardOutput.ReadLine()
        if ($null -ne $Line) { Write-Host $Line }
    }
    Write-Host $Process.StandardOutput.ReadToEnd()
    $Process.WaitForExit()
    exit $Process.ExitCode
}
finally {
    $Plain = $null
    if ($Bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($Bstr)
    }
}
