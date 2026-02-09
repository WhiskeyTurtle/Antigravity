import asyncio
import json
import logging
import os
import random
import re
import subprocess
import threading
import time

from telethon import TelegramClient, events

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SocialMonitor")

API_ID = 123456
API_HASH = "your_api_hash_here"
PHONE_NUMBER = "+1234567890"

CHANNELS = [
    "solana_calls",
    "bonkbot_alerts",
]

SOL_ADDRESS_REGEX = r"[1-9A-HJ-NP-Za-km-z]{32,44}"
BIRD_CMD = r"C:\Users\joshz\AppData\Roaming\npm\bird.cmd"

_BIRD_LOCK = threading.Lock()
_BIRD_FAIL_COUNT = 0
_BIRD_DISABLE_UNTIL = 0.0


def _run_bird_guarded(cmd_args, env, timeout):
    """
    Serialize Bird CLI calls and apply a circuit breaker on repeated failures.
    Helps avoid Node/libuv assertion crashes on Windows under heavy subprocess churn.
    """
    global _BIRD_FAIL_COUNT, _BIRD_DISABLE_UNTIL
    now = time.time()
    if now < _BIRD_DISABLE_UNTIL:
        return None

    with _BIRD_LOCK:
        now = time.time()
        if now < _BIRD_DISABLE_UNTIL:
            return None
        try:
            result = subprocess.run(
                cmd_args,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                timeout=timeout,
            )
            if result.returncode == 0:
                _BIRD_FAIL_COUNT = 0
                return result

            _BIRD_FAIL_COUNT += 1
            if _BIRD_FAIL_COUNT >= 3:
                _BIRD_DISABLE_UNTIL = time.time() + 120
                logger.warning("Temporarily disabling Bird CLI calls for 120s due to repeated failures.")
            return result
        except Exception:
            _BIRD_FAIL_COUNT += 1
            if _BIRD_FAIL_COUNT >= 3:
                _BIRD_DISABLE_UNTIL = time.time() + 120
                logger.warning("Temporarily disabling Bird CLI calls for 120s due to repeated exceptions.")
            return None


class MockTwitterStream:
    def __init__(self, callback):
        self.callback = callback
        self.influencers = [
            {"handle": "@elonmusk", "name": "Elon Musk", "impact": 100},
            {"handle": "@realDonaldTrump", "name": "Donald J. Trump", "impact": 95},
            {"handle": "@VitalikButerin", "name": "Vitalik Buterin", "impact": 90},
            {"handle": "@cz_binance", "name": "CZ", "impact": 85},
            {"handle": "@aeyakovenko", "name": "Anatoly Yakovenko", "impact": 80},
        ]
        self.running = False

    async def start(self):
        self.running = True
        logger.info("Mock Twitter stream started")
        await asyncio.sleep(2)
        await self._emit_mock_tweet()
        while self.running:
            await asyncio.sleep(60)
            if random.random() < 0.1:
                await self._emit_mock_tweet()

    async def _emit_mock_tweet(self):
        influencer = random.choice(self.influencers)
        mock_addr = f"Mock{random.randint(1000, 9999)}SocialAlpha{random.randint(100, 999)}"
        text = f"This is interesting. Checking out {mock_addr} #Solana"
        await self.callback(
            {
                "type": "social",
                "source": "TWITTER",
                "author": influencer["handle"],
                "author_name": influencer["name"],
                "impact_score": influencer["impact"],
                "text": text,
                "signature": mock_addr,
                "token": mock_addr,
            }
        )


class BirdTwitterStream:
    def __init__(self, callback, auth_token=None, ct0=None):
        self.callback = callback
        self.auth_token = auth_token
        self.ct0 = ct0
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
        self.seen_tweets = set()
        self.poll_interval = 120

    def _bird_env(self):
        env = os.environ.copy()
        if self.auth_token:
            env["AUTH_TOKEN"] = self.auth_token
        if self.ct0:
            env["CT0"] = self.ct0
        return env

    async def start(self):
        self.running = True
        logger.info("Bird Twitter stream started")

        env = self._bird_env()
        if not os.path.exists(BIRD_CMD):
            logger.error("bird CLI not found. Install with: npm install -g @steipete/bird")
            return

        result = _run_bird_guarded([BIRD_CMD, "whoami"], env=env, timeout=10)
        if result is None or result.returncode != 0:
            logger.warning("Bird auth failed/unavailable. Falling back to mock stream.")
            mock = MockTwitterStream(self.callback)
            await mock.start()
            return

        while self.running:
            for influencer in self.influencers:
                try:
                    await self._poll_influencer(influencer)
                except Exception as e:
                    logger.error(f"Error polling {influencer['handle']}: {e}")
                await asyncio.sleep(5)
            await asyncio.sleep(self.poll_interval)

    async def _poll_influencer(self, influencer):
        handle = influencer["handle"]
        result = _run_bird_guarded(
            [BIRD_CMD, "user-tweets", handle, "-n", "5", "--json"],
            env=self._bird_env(),
            timeout=30,
        )
        if result is None or result.returncode != 0:
            return

        try:
            tweets = json.loads(result.stdout)
        except Exception:
            return

        for tweet in tweets:
            tweet_id = tweet.get("id") or tweet.get("rest_id")
            if not tweet_id or tweet_id in self.seen_tweets:
                continue
            self.seen_tweets.add(tweet_id)

            text = tweet.get("text", "") or tweet.get("full_text", "")
            addresses = re.findall(SOL_ADDRESS_REGEX, text)
            for addr in addresses:
                await self.callback(
                    {
                        "type": "social",
                        "source": "TWITTER",
                        "author": f"@{handle}",
                        "author_name": influencer["name"],
                        "impact_score": influencer["impact"],
                        "text": text[:200],
                        "signature": addr,
                        "token": addr,
                        "tweet_id": tweet_id,
                    }
                )

    def stop(self):
        self.running = False


class SocialMonitor:
    def __init__(self, callback, use_live=True, auth_token=None, ct0=None):
        self.callback = callback
        self.client = TelegramClient("anon", API_ID, API_HASH)
        self.use_live = use_live
        self.auth_token = auth_token
        self.ct0 = ct0

        self._mention_cache = {}
        self._creator_cache = {}
        self._mention_ttl_sec = 120
        self._creator_ttl_sec = 300

        if use_live:
            self.twitter_stream = BirdTwitterStream(callback, auth_token=auth_token, ct0=ct0)
        else:
            self.twitter_stream = MockTwitterStream(callback)

    def _bird_env(self):
        env = os.environ.copy()
        if self.auth_token:
            env["AUTH_TOKEN"] = self.auth_token
        if self.ct0:
            env["CT0"] = self.ct0
        return env

    async def get_token_mention_count(self, token_mint_or_ca: str, timeframe: str = "24h") -> dict:
        token_q = (token_mint_or_ca or "").lower().strip()
        now = time.time()
        if not token_q:
            return {"count": 0, "timeframe": timeframe, "scraped_at": now}

        cached = self._mention_cache.get(token_q)
        if cached and now - cached["scraped_at"] < self._mention_ttl_sec:
            return cached

        if not self.use_live or not os.path.exists(BIRD_CMD):
            out = {"count": 0, "timeframe": timeframe, "scraped_at": now}
            self._mention_cache[token_q] = out
            return out

        count = 0
        seen_tweets = set()
        influencers = getattr(self.twitter_stream, "influencers", [])[:4]

        for influencer in influencers:
            handle = influencer.get("handle")
            if not handle:
                continue

            result = _run_bird_guarded(
                [BIRD_CMD, "user-tweets", handle, "-n", "15", "--json"],
                env=self._bird_env(),
                timeout=25,
            )
            if result is None or result.returncode != 0:
                continue

            try:
                tweets = json.loads(result.stdout)
            except Exception:
                continue

            for tweet in tweets:
                tweet_id = tweet.get("id") or tweet.get("rest_id")
                if tweet_id in seen_tweets:
                    continue
                seen_tweets.add(tweet_id)
                text = (tweet.get("text", "") or tweet.get("full_text", "")).lower()
                if token_q in text:
                    count += 1

        out = {"count": count, "timeframe": timeframe, "scraped_at": now}
        self._mention_cache[token_q] = out
        return out

    async def get_creator_metrics(self, twitter_handle: str) -> dict:
        handle = (twitter_handle or "").strip().lstrip("@").lower()
        now = time.time()
        if not handle:
            return {}

        cached = self._creator_cache.get(handle)
        if cached and now - cached.get("scraped_at", 0) < self._creator_ttl_sec:
            return cached

        if not os.path.exists(BIRD_CMD):
            out = {"username": handle, "followers": 0, "scraped_at": now}
            self._creator_cache[handle] = out
            return out

        result = _run_bird_guarded(
            [BIRD_CMD, "user", handle, "--json"],
            env=self._bird_env(),
            timeout=20,
        )
        if result is None or result.returncode != 0:
            out = {"username": handle, "followers": 0, "scraped_at": now}
            self._creator_cache[handle] = out
            return out

        try:
            payload = json.loads(result.stdout)
            user = payload if isinstance(payload, dict) else {}
            followers = (
                user.get("followers_count")
                or user.get("followersCount")
                or user.get("legacy", {}).get("followers_count", 0)
            )
            username = (
                user.get("screen_name")
                or user.get("username")
                or user.get("legacy", {}).get("screen_name")
                or handle
            )
            out = {"username": username, "followers": int(followers or 0), "scraped_at": now}
        except Exception:
            out = {"username": handle, "followers": 0, "scraped_at": now}

        self._creator_cache[handle] = out
        return out

    async def start(self):
        await asyncio.gather(self._start_telegram(), self.twitter_stream.start())

    async def _start_telegram(self):
        try:
            logger.info("Social Monitor (Telegram) starting...")
            if API_ID == 123456:
                logger.warning("No valid Telegram API_ID. Skipping Telegram connection.")
                return

            @self.client.on(events.NewMessage(chats=CHANNELS))
            async def handler(event):
                pass

            await self.client.start(phone=PHONE_NUMBER)
            await self.client.run_until_disconnected()
        except Exception as e:
            logger.error(f"Telegram Monitor Failed: {e}")


if __name__ == "__main__":
    async def dump_callback(event):
        print(f"Callback Event: {event}")

    mon = SocialMonitor(dump_callback)
    try:
        asyncio.run(mon.start())
    except KeyboardInterrupt:
        pass
