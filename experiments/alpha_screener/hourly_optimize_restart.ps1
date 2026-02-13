param(
    [string]$ApiBase = "http://127.0.0.1:8000",
    [int]$AutopilotIntervalSec = 3600,
    [int]$AutopilotWindowSec = 3600,
    [int]$AutopilotNoTradeWindowSec = 3600
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$StartScript = Join-Path $ProjectRoot "start_guarded.bat"
$AiScript = Join-Path $ProjectRoot "hourly_ai_optimizer.py"
$LogDir = Join-Path $ProjectRoot "logs"
$LogFile = Join-Path $LogDir "hourly_optimizer.log"

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $LogFile -Value "[$ts] $Message"
}

function Invoke-Json {
    param(
        [string]$Method,
        [string]$Url,
        [object]$Body = $null
    )
    $client = [System.Net.Http.HttpClient]::new()
    $client.Timeout = [TimeSpan]::FromSeconds(10)
    try {
        $httpMethod = [System.Net.Http.HttpMethod]::new($Method.ToUpperInvariant())
        $request = [System.Net.Http.HttpRequestMessage]::new($httpMethod, $Url)
        if ($Body -ne $null) {
            $json = ($Body | ConvertTo-Json -Depth 8 -Compress)
            $request.Content = [System.Net.Http.StringContent]::new($json, [System.Text.Encoding]::UTF8, "application/json")
        }
        $response = $client.SendAsync($request).GetAwaiter().GetResult()
        $text = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) {
            throw "HTTP $([int]$response.StatusCode): $text"
        }
        if ([string]::IsNullOrWhiteSpace($text)) {
            return $null
        }
        try {
            return ($text | ConvertFrom-Json)
        }
        catch {
            return $text
        }
    }
    finally {
        $client.Dispose()
    }
}

function Is-Healthy {
    try {
        $null = Invoke-Json -Method "GET" -Url "$ApiBase/api/autopilot"
        return $true
    }
    catch {
        return $false
    }
}

function Is-HealthyWithRetry {
    param(
        [int]$Attempts = 3,
        [int]$DelaySec = 4
    )
    $attemptsSafe = [Math]::Max(1, [int]$Attempts)
    for ($i = 0; $i -lt $attemptsSafe; $i++) {
        if (Is-Healthy) {
            return $true
        }
        if ($i -lt ($attemptsSafe - 1)) {
            Start-Sleep -Seconds ([Math]::Max(1, [int]$DelaySec))
        }
    }
    return $false
}

function Stop-AlphaProcesses {
    try {
        $procs = Get-CimInstance Win32_Process | Where-Object {
            ($_.CommandLine -like "*alpha_screener*server_guardian.py*") -or
            ($_.CommandLine -like "*alpha_screener*main:app*") -or
            ($_.CommandLine -like "*alpha_screener*uvicorn*")
        }
        foreach ($proc in $procs) {
            try {
                Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
                Write-Log "Stopped process pid=$($proc.ProcessId)"
            }
            catch {
                Write-Log "Failed stopping pid=$($proc.ProcessId): $($_.Exception.Message)"
            }
        }
    }
    catch {
        Write-Log "Process scan failed: $($_.Exception.Message)"
    }
}

function Start-Alpha {
    if (-not (Test-Path $StartScript)) {
        throw "Start script not found: $StartScript"
    }
    Start-Process -FilePath $StartScript -WorkingDirectory $ProjectRoot -WindowStyle Hidden | Out-Null
    Write-Log "Launched start_guarded.bat"
}

function Invoke-AiOptimizer {
    if (-not (Test-Path $AiScript)) {
        Write-Log "AI optimizer script not found: $AiScript"
        return
    }
    try {
        $output = & py -3 $AiScript --api-base $ApiBase --window-sec $AutopilotWindowSec 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Log "AI optimizer exited non-zero: $LASTEXITCODE"
        }
        if ($output) {
            foreach ($line in $output) {
                $msg = [string]$line
                if (-not [string]::IsNullOrWhiteSpace($msg)) {
                    Write-Log "AI optimizer: $msg"
                }
            }
        }
    }
    catch {
        Write-Log "AI optimizer failed: $($_.Exception.Message)"
    }
}

Write-Log "Hourly optimizer tick started"

$healthy = Is-HealthyWithRetry -Attempts 3 -DelaySec 3
$needsRestart = $false

if (-not $healthy) {
    Write-Log "Health check failed at start"
    $needsRestart = $true
}
else {
    try {
        $currentSettings = Invoke-Json -Method "GET" -Url "$ApiBase/api/settings"
        $solMin = [double]($currentSettings.sol_amount_min)
        $solMax = [double]($currentSettings.sol_amount_max)
        if ($solMin -le 0 -or $solMax -le 0) {
            throw "Invalid current sol_amount values from settings endpoint"
        }
        $settingsPatch = @{
            sol_amount_min                = $solMin
            sol_amount_max                = $solMax
            autopilot_enabled             = $true
            autopilot_interval_sec        = $AutopilotIntervalSec
            autopilot_window_sec          = $AutopilotWindowSec
            autopilot_no_trade_window_sec = $AutopilotNoTradeWindowSec
        }
        $null = Invoke-Json -Method "POST" -Url "$ApiBase/api/settings" -Body $settingsPatch
        Write-Log "Applied autopilot hourly settings"
    }
    catch {
        Write-Log "Failed applying settings: $($_.Exception.Message)"
    }

    Invoke-AiOptimizer

    try {
        $state = Invoke-Json -Method "GET" -Url "$ApiBase/api/autopilot"
        $lastRunTs = [double]($state.last_run_ts)
        if ($lastRunTs -gt 0) {
            $ageSec = [int]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds() - $lastRunTs)
            Write-Log "Autopilot last_run age=${ageSec}s action=$($state.last_action)"
            if ($ageSec -gt ([int]($AutopilotIntervalSec * 2))) {
                Write-Log "Autopilot appears stale"
                $needsRestart = $true
            }
        }
        else {
            Write-Log "Autopilot last_run_ts missing/zero"
            $needsRestart = $true
        }
    }
    catch {
        Write-Log "Failed reading autopilot state: $($_.Exception.Message)"
        $needsRestart = $true
    }
}

if ($needsRestart) {
    Write-Log "Restart required; cycling server"
    Stop-AlphaProcesses
    Start-Sleep -Seconds 2
    Start-Alpha
    if (Is-HealthyWithRetry -Attempts 7 -DelaySec 4) {
        Write-Log "Server healthy after restart"
    }
    else {
        Write-Log "Server still unhealthy after restart"
    }
}
else {
    Write-Log "No restart needed; server healthy"
}

Write-Log "Hourly optimizer tick finished"
