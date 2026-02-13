param(
    [string]$TaskName = "AlphaScreenerHourlyOptimizer"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ScriptPath = Join-Path $ProjectRoot "hourly_optimize_restart.ps1"

if (-not (Test-Path $ScriptPath)) {
    throw "Script not found: $ScriptPath"
}

$taskCommand = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

schtasks /Create `
    /TN $TaskName `
    /TR $taskCommand `
    /SC HOURLY `
    /MO 1 `
    /F | Out-Null

Write-Output "Created/updated task: $TaskName"
Write-Output "Task command: $taskCommand"
