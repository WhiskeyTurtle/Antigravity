@echo off
REM Alpha Screener Startup Script with Bird Credentials

REM Set Twitter auth for Bird CLI
set TWITTER_AUTH_TOKEN=ee444e5131aadee5ee5942163000208c825c1eaa
set CT0=993b026d2c58272b65019a1379f4bd6d705516d470193f82a85ce17f049c863c58be7e1db131440d1858945afcd9531e44f455a85051723efdbff1f5b33c8229e917ccdb17fff06eaac97c07b63d4ae5
set BIRDEYE_API_KEY=10f33a9d5c564580aa5e4772c4e2da3f
set BIRDEYE_API_KEYS=10f33a9d5c564580aa5e4772c4e2da3f

REM Start the server
echo Starting Alpha Screener with Live Twitter Monitoring...
cd /d c:\Users\joshz\OneDrive\Documents\Antigravity
REM NOTE: do not use --reload in live trading; it can duplicate background tasks/signals.
C:\Users\joshz\AppData\Local\Programs\Python\Python312\python.exe -m uvicorn experiments.alpha_screener.main:app --host 0.0.0.0 --port 8000
