import json
import os
import re
import shutil
import subprocess

SOL_ADDRESS_REGEX = r"[1-9A-HJ-NP-Za-km-z]{32,44}"


def run_bird_integration_check() -> int:
    """Manual integration check for Bird CLI."""
    bird_path = shutil.which("bird")
    if not bird_path:
        print("Bird CLI not found in PATH. Skipping manual integration check.")
        return 0

    auth_token = os.getenv("TWITTER_AUTH_TOKEN")
    ct0 = os.getenv("CT0")
    if auth_token:
        os.environ["TWITTER_AUTH_TOKEN"] = auth_token
    if ct0:
        os.environ["CT0"] = ct0

    print("Testing Bird CLI integration...")
    print("=" * 60)

    print("\n1. Testing authentication...")
    result = subprocess.run([bird_path, "whoami"], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Auth failed: {result.stderr}")
        return result.returncode

    print(f"Auth OK: {result.stdout.strip()}")

    print("\n2. Fetching recent tweets from @aeyakovenko...")
    result = subprocess.run(
        [bird_path, "user-tweets", "aeyakovenko", "-n", "10", "--json"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        print(f"Failed: {result.stderr}")
        return result.returncode

    tweets = json.loads(result.stdout)
    print(f"Fetched {len(tweets)} tweets")

    print("\n3. Scanning for Solana addresses...")
    found_count = 0
    for tweet in tweets:
        text = tweet.get("text", "") or tweet.get("full_text", "")
        addresses = re.findall(SOL_ADDRESS_REGEX, text)
        if addresses:
            found_count += 1
            print(f"\nFound in tweet {tweet.get('id')}:")
            print(f"  Text: {text[:100]}...")
            print(f"  Addresses: {addresses}")

    if found_count == 0:
        print("No Solana addresses found in recent tweets (normal for many timelines).")
    else:
        print(f"\nFound {found_count} tweets with Solana addresses.")

    print("\n" + "=" * 60)
    print("Bird CLI integration test complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_bird_integration_check())
