
import asyncio
import re
import logging
import random
# pip install telethon
from telethon import TelegramClient, events

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SocialMonitor")

# ---------------------------------------------------------
# 1. SETUP CREDENTIALS
# Go to https://my.telegram.org/apps to get API_ID and API_HASH
# ---------------------------------------------------------
API_ID = 123456  # REPLACE ME
API_HASH = "your_api_hash_here" # REPLACE ME
PHONE_NUMBER = "+1234567890" # REPLACE ME

# Channels to Monitor (Username or ID)
CHANNELS = [
    "solana_calls", 
    "bonkbot_alerts",
    # Add your favorite channels here
]

# Solana Address Regex (Base58, 32-44 chars)
SOL_ADDRESS_REGEX = r'[1-9A-HJ-NP-Za-km-z]{32,44}'

# Bird CLI path (Windows npm global install)
BIRD_CMD = r"C:\Users\joshz\AppData\Roaming\npm\bird.cmd"

# ---------------------------------------------------------
# MOCK TWITTER STREAM (Simulation for Strategy Validation)
# ---------------------------------------------------------
class MockTwitterStream:
    def __init__(self, callback):
        self.callback = callback
        self.influencers = [
            {"handle": "@elonmusk", "name": "Elon Musk", "impact": 100},
            {"handle": "@realDonaldTrump", "name": "Donald J. Trump", "impact": 95},
            {"handle": "@VitalikButerin", "name": "Vitalik Buterin", "impact": 90},
            {"handle": "@cz_binance", "name": "CZ", "impact": 85},
            {"handle": "@aeyakovenko", "name": "Anatoly Yakovenko", "impact": 80}
        ]
        self.running = False

    async def start(self):
        self.running = True
        logger.info("🐦 Mock Twitter Stream Started (Listening for Alpha...)")
        
        # Trigger immediate test tweet
        await asyncio.sleep(2)
        await self._emit_mock_tweet()
        
        while self.running:
            await asyncio.sleep(60) 
            if random.random() < 0.1: 
                await self._emit_mock_tweet()

    async def _emit_mock_tweet(self):
        influencer = random.choice(self.influencers)
        mock_addr = f"Mock{random.randint(1000,9999)}SocialAlpha{random.randint(100,999)}"
        text = f"This is interesting. Checking out {mock_addr} #Solana"
        logger.info(f"🐦 [TWEET] {influencer['handle']}: {text}")
        
        await self.callback({
            "type": "social",
            "source": "TWITTER",
            "author": influencer["handle"],
            "author_name": influencer["name"],
            "impact_score": influencer["impact"],
            "text": text,
            "signature": mock_addr,
            "token": mock_addr
        })

# ---------------------------------------------------------
# BIRD CLI TWITTER STREAM (Live Monitoring via steipete/bird)
# ---------------------------------------------------------
import subprocess
import json

class BirdTwitterStream:
    """
    Uses the 'bird' CLI (https://github.com/steipete/bird) to monitor
    high-impact X/Twitter accounts for token mentions.
    Requires: npm install -g @steipete/bird
    Auth: Export cookies from your logged-in X session to ~/.bird/cookies.json
    """
    def __init__(self, callback):
        self.callback = callback
        self.influencers = [
            {"handle": "0x3ddy", "name": "0x3ddy", "impact": 100},
            {"handle": "daumenxyz", "name": "Daumen", "impact": 95},
            {"handle": "RoaringKitty", "name": "Roaring Kitty", "impact": 90},
            {"handle": "TmsCrypto10", "name": "TMS Crypto", "impact": 85},
            {"handle": "Bullrun_Gravano", "name": "Bullrun Gravano", "impact": 80},
            {"handle": "cryptorover", "name": "Crypto Rover", "impact": 75},
            {"handle": "bschizojew", "name": "bschizojew", "impact": 70},
        ]
        self.running = False
        self.seen_tweets = set()  # Prevent duplicate processing
        self.poll_interval = 120  # Check every 2 minutes

    async def start(self):
        self.running = True
        logger.info("🐦 Bird Twitter Stream Started (Live X Monitoring...)")
        
        # Check if bird is available
        try:
            result = subprocess.run(
                [BIRD_CMD, "whoami"], 
                capture_output=True, 
                text=True, 
                encoding='utf-8',
                errors='replace',
                timeout=10
            )
            if result.returncode == 0:
                logger.info(f"🔑 Bird Auth OK: {result.stdout.strip()}")
            else:
                logger.warning(f"⚠️ Bird auth issue: {result.stderr}. Falling back to Mock.")
                # Fall back to mock stream
                mock = MockTwitterStream(self.callback)
                await mock.start()
                return
        except FileNotFoundError:
            logger.error("❌ 'bird' CLI not found. Run: npm install -g @steipete/bird")
            return
        except Exception as e:
            logger.error(f"❌ Bird check failed: {e}")
            return

        while self.running:
            for influencer in self.influencers:
                try:
                    await self._poll_influencer(influencer)
                except Exception as e:
                    logger.error(f"Error polling {influencer['handle']}: {e}")
                await asyncio.sleep(5)  # Small delay between users
            
            await asyncio.sleep(self.poll_interval)

    async def _poll_influencer(self, influencer):
        """Fetch recent tweets from an influencer and look for Solana addresses."""
        handle = influencer["handle"]
        
        try:
            # Run bird user-tweets command
            result = subprocess.run(
                [BIRD_CMD, "user-tweets", handle, "-n", "5", "--json"],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=30
            )
            
            if result.returncode != 0:
                logger.warning(f"bird user-tweets failed for @{handle}: {result.stderr}")
                return
            
            # Parse JSON output
            tweets = json.loads(result.stdout)
            
            for tweet in tweets:
                tweet_id = tweet.get("id") or tweet.get("rest_id")
                if not tweet_id or tweet_id in self.seen_tweets:
                    continue
                
                self.seen_tweets.add(tweet_id)
                text = tweet.get("text", "") or tweet.get("full_text", "")
                
                # Look for Solana addresses in tweet text
                addresses = re.findall(SOL_ADDRESS_REGEX, text)
                
                if addresses:
                    for addr in addresses:
                        logger.info(f"🚀 [ALPHA] @{handle} mentioned: {addr}")
                        await self.callback({
                            "type": "social",
                            "source": "TWITTER",
                            "author": f"@{handle}",
                            "author_name": influencer["name"],
                            "impact_score": influencer["impact"],
                            "text": text[:200],  # Truncate for display
                            "signature": addr,
                            "token": addr,
                            "tweet_id": tweet_id
                        })
                        
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error for @{handle}: {e}")
        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout polling @{handle}")

    def stop(self):
        self.running = False


class SocialMonitor:
    def __init__(self, callback, use_live=True):
        self.callback = callback
        self.client = TelegramClient('anon', API_ID, API_HASH)
        self.use_live = use_live
        
        if use_live:
            self.twitter_stream = BirdTwitterStream(callback)
        else:
            self.twitter_stream = MockTwitterStream(callback)

    async def start(self):
        # Run both monitors
        await asyncio.gather(
            self._start_telegram(),
            self.twitter_stream.start()
        )

    async def _start_telegram(self):
        try:
            logger.info("📱 Social Monitor (Telegram) starting...")
            # Telegram logic (stubbed for now if no creds)
            if API_ID == 123456:
                logger.warning("⚠️ No valid Telegram API_ID. Skipping Telegram connection.")
                return

            @self.client.on(events.NewMessage(chats=CHANNELS))
            async def handler(event):
                # ... existing logic ...
                pass
                
            await self.client.start(phone=PHONE_NUMBER)
            await self.client.run_until_disconnected()
        except Exception as e:
            logger.error(f"Telegram Monitor Failed: {e}")

# Standalone test
if __name__ == "__main__":
    async def dump_callback(event):
        print(f"Callback Event: {event}")
        
    mon = SocialMonitor(dump_callback)
    try:
        asyncio.run(mon.start())
    except KeyboardInterrupt:
        pass
