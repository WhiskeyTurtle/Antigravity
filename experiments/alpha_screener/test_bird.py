import subprocess
import json
import re
import os

# Set credentials
os.environ['TWITTER_AUTH_TOKEN'] = 'ee444e5131aadee5ee5942163000208c825c1eaa'
os.environ['CT0'] = '993b026d2c58272b65019a1379f4bd6d705516d470193f82a85ce17f049c863c58be7e1db131440d1858945afcd9531e44f455a85051723efdbff1f5b33c8229e917ccdb17fff06eaac97c07b63d4ae5'

SOL_ADDRESS_REGEX = r'[1-9A-HJ-NP-Za-km-z]{32,44}'

print("🐦 Testing Bird CLI Integration...")
print("=" * 60)

# Test whoami
print("\n1. Testing authentication...")
result = subprocess.run(["bird", "whoami"], capture_output=True, text=True)
if result.returncode == 0:
    print(f"✅ Auth OK: {result.stdout.strip()}")
else:
    print(f"❌ Auth failed: {result.stderr}")
    exit(1)

# Test fetching tweets from Anatoly (Solana founder - more likely to mention tokens)
print("\n2. Fetching recent tweets from @aeyakovenko...")
result = subprocess.run(
    ["bird", "user-tweets", "aeyakovenko", "-n", "10", "--json"],
    capture_output=True,
    text=True,
    timeout=30
)

if result.returncode != 0:
    print(f"❌ Failed: {result.stderr}")
    exit(1)

tweets = json.loads(result.stdout)
print(f"✅ Fetched {len(tweets)} tweets")

# Look for Solana addresses
print("\n3. Scanning for Solana addresses...")
found_count = 0
for tweet in tweets:
    text = tweet.get("text", "") or tweet.get("full_text", "")
    addresses = re.findall(SOL_ADDRESS_REGEX, text)
    
    if addresses:
        found_count += 1
        print(f"\n🚀 FOUND in tweet {tweet.get('id')}:")
        print(f"   Text: {text[:100]}...")
        print(f"   Addresses: {addresses}")

if found_count == 0:
    print("ℹ️ No Solana addresses found in recent tweets")
    print("   (This is normal - influencers don't always mention tokens)")
else:
    print(f"\n✅ Found {found_count} tweets with Solana addresses!")

print("\n" + "=" * 60)
print("🎉 Bird CLI integration test complete!")
