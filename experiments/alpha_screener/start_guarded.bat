@echo off
setlocal

cd /d c:\Users\joshz\OneDrive\Documents\Antigravity\experiments\alpha_screener

REM Birdeye API key fallback pool (primary + backup).
if defined BIRDEYE_API_KEY (
  set BIRDEYE_API_KEYS=%BIRDEYE_API_KEY%,10f33a9d5c564580aa5e4772c4e2da3f
) else (
  set BIRDEYE_API_KEY=10f33a9d5c564580aa5e4772c4e2da3f
  set BIRDEYE_API_KEYS=10f33a9d5c564580aa5e4772c4e2da3f
)

REM Prevent duplicate guardians on this port.
powershell -NoProfile -Command "if (Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'server_guardian.py' -and $_.CommandLine -match '--port 8000' }) { exit 1 } else { exit 0 }"
if %ERRORLEVEL% EQU 1 (
  echo Guardian already running on port 8000. Exiting.
  goto :eof
)

echo Starting Alpha Screener with guardian auto-restart...
echo.
echo Tip: set BIRDEYE_API_KEY before launch for Birdeye-first mode.
echo.

py -3 server_guardian.py ^
  --app main:app ^
  --host 127.0.0.1 ^
  --port 8000 ^
  --health-path /api/autopilot ^
  --startup-grace-sec 90 ^
  --health-interval-sec 20 ^
  --health-timeout-sec 15 ^
  --max-health-failures 3 ^
  --restart-backoff-sec 3 ^
  --restart-backoff-max-sec 60

endlocal
