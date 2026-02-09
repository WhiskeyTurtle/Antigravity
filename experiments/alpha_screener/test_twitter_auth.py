
import asyncio
import subprocess
import os

BIRD_CMD = r"C:\Users\joshz\AppData\Roaming\npm\bird.cmd"
AUTH_TOKEN = "ec35e4dc84ec7b2cd5a9cefd07157f4a345c2090"
CT0 = "9f1398a6d5f1071609ef4e7fb22d5868de481a65e2c3e8b0d3396a2ca4d1d9c1692cebfc42bf44407eedf076ad5c69dc730ced1d5971963fa78cd28210ac7f320a34f3dc7fd851cfda6954bbf7d8810b"

async def test_auth():
    env = os.environ.copy()
    env["AUTH_TOKEN"] = AUTH_TOKEN
    env["CT0"] = CT0
    
    print(f"Testing bird whoami with provided tokens...")
    result = subprocess.run(
        [BIRD_CMD, "whoami"],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=env
    )
    
    if result.returncode == 0:
        print(f"✅ Auth Successful: {result.stdout.strip()}")
    else:
        print(f"❌ Auth Failed: {result.stderr.strip()}")

    print(f"\nTesting bird user-tweets for @RoaringKitty...")
    result = subprocess.run(
        [BIRD_CMD, "user-tweets", "RoaringKitty", "-n", "1", "--json"],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=env
    )
    
    if result.returncode == 0:
        print(f"✅ Tweet Fetch Successful!")
        # print(result.stdout[:500])
    else:
        print(f"❌ Tweet Fetch Failed: {result.stderr.strip()}")

if __name__ == "__main__":
    asyncio.run(test_auth())
