param(
    [string]$Uri = "http://localhost:8080/transaction",
    [string]$InputFile = ".\data\test-transactions.txt",
    [int]$DelayMs = 0,
    [switch]$StopOnError
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -Path $InputFile)) {
    throw "Input file not found: $InputFile"
}

$headers = @{ "Content-Type" = "application/json" }
$sent = 0
$failed = 0

Get-Content -Path $InputFile | ForEach-Object {
    $line = $_.Trim()

    if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) {
        return
    }

    try {
        $response = Invoke-RestMethod -Method POST -Uri $Uri -Headers $headers -Body $line
        $sent++

        $txId = ""
        try {
            $txId = (ConvertFrom-Json -InputObject $line).transaction_id
        }
        catch {
            $txId = "<unknown>"
        }

        Write-Host "[$sent] Sent transaction: $txId"
        if ($null -ne $response) {
            Write-Host "      Response: $($response | ConvertTo-Json -Compress)"
        }
    }
    catch {
        $failed++
        Write-Warning "Failed to send line: $line"
        Write-Warning "Error: $($_.Exception.Message)"

        if ($StopOnError) {
            throw
        }
    }

    if ($DelayMs -gt 0) {
        Start-Sleep -Milliseconds $DelayMs
    }
}

Write-Host "Done. Sent=$sent Failed=$failed"
