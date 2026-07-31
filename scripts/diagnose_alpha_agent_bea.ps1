<#
.SYNOPSIS
    Read-only BEA API diagnostic for the Alpha Agent Stage 8 data-acquisition
    layer. Decrypts the DPAPI-protected BEA UserID in memory and performs ONE
    read-only GetData probe against the official BEA API.

.DESCRIPTION
    Resolves the DPAPI-encrypted UserID written by configure_alpha_agent_bea.ps1,
    decrypts it in memory (never to disk, never printed), and issues a single
    read-only Invoke-RestMethod GetData request for a small NIPA table. It prints
    ONLY non-secret result metadata (row counts, dataset/table, HTTP result
    class) plus BEA_DIAGNOSTIC_OK or BEA_DIAGNOSTIC_FAILED - <safe reason>. The
    UserID and the full request URL (which contains the UserID) are never
    printed. This script writes nothing and acquires no data into the store.

.NOTES
    Windows PowerShell 5.1. Read-only. Run interactively in YOUR own session.
#>
[CmdletBinding()]
param(
    [string]$CredentialDir = "$env:USERPROFILE\.paper_trader\alpha_agent_bea",
    [string]$KeyFile = "bea_userid.dpapi",
    [string]$BaseUrl = "https://apps.bea.gov/api/data",
    [string]$Dataset = "NIPA",
    [string]$TableName = "T10101",
    [string]$Frequency = "Q",
    [string]$Year = "2025"
)

$ErrorActionPreference = "Stop"

function Fail {
    param([string]$Reason)
    Write-Host "BEA_DIAGNOSTIC_FAILED - $Reason"
    exit 1
}

$KeyPath = Join-Path $CredentialDir $KeyFile
if (-not (Test-Path -LiteralPath $KeyPath)) {
    Fail "no DPAPI credential configured; run scripts\configure_alpha_agent_bea.ps1 first (BEA_CREDENTIAL_SETUP_REQUIRED)"
}

$OutBstr = [IntPtr]::Zero
$UserId = $null
try {
    # Decrypt the DPAPI blob to a plaintext UserID in memory ONLY.
    $Secure = ConvertTo-SecureString -String ((Get-Content -LiteralPath $KeyPath -Raw).Trim())
    $OutBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    $UserId = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($OutBstr)
    if ([string]::IsNullOrEmpty($UserId)) { Fail "stored UserID did not decrypt" }

    # Build the read-only GetData URL in memory (the UserID is embedded; the full
    # URL is NEVER printed).
    $Query = "?&UserID=$UserId&method=GetData&datasetname=$Dataset&TableName=$TableName&Frequency=$Frequency&Year=$Year&ResultFormat=JSON"
    $Url = $BaseUrl + $Query

    try {
        $Resp = Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 30
    } catch {
        Fail "BEA API request failed (network or HTTP error)"
    }

    $Api = $Resp.BEAAPI
    if ($null -eq $Api) { Fail "unexpected BEA response shape (no BEAAPI node)" }
    $Results = $Api.Results
    if ($Results -and $Results.Error) {
        # An APIErrorCode here usually means an invalid UserID or bad parameters.
        Fail "BEA API returned an error result (check the UserID / parameters)"
    }
    $Data = $null
    if ($Results) { $Data = $Results.Data }
    $RowCount = 0
    if ($Data) { $RowCount = @($Data).Count }

    Write-Host "===== BEA DIAGNOSTIC (non-secret) ====="
    Write-Host ("  dataset       : {0}" -f $Dataset)
    Write-Host ("  table         : {0}" -f $TableName)
    Write-Host ("  frequency     : {0}" -f $Frequency)
    Write-Host ("  year          : {0}" -f $Year)
    Write-Host ("  data_rows     : {0}" -f $RowCount)
    Write-Host ("  userid_printed: never")
    if ($RowCount -gt 0) {
        Write-Host "BEA_DIAGNOSTIC_OK"
        exit 0
    }
    Fail "BEA API returned no data rows for the probe table"
}
finally {
    $UserId = $null
    if ($OutBstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($OutBstr)
    }
    [GC]::Collect()
}
