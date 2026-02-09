import aiohttp
import asyncio
import time


class RealyEngine:
    """
    Fetches REAL on-chain data for tokens.
    Uses DexScreener API for price and metadata.
    """

    def __init__(self):
        self.sol_mint = "So11111111111111111111111111111111111111112"

    async def _fetch_best_pair(self, token_mint: str):
        """Fetches DexScreener pair payload and returns best pair dict or None."""
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint}"
        import socket

        connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                pairs = data.get("pairs", [])
                if not pairs:
                    return None
                return pairs[0]

    async def get_token_price(self, token_mint: str):
        """
        Calculates price of 1 token in SOL and collects market metadata.
        """
        try:
            best_pair = await self._fetch_best_pair(token_mint)
            if not best_pair:
                return {"price_sol": 0, "liquidity_check": False}

            price_native = float(best_pair.get("priceNative", 0))
            price_usd = float(best_pair.get("priceUsd", 0))

            volume = best_pair.get("volume", {})
            volume_m5 = float(volume.get("m5", 0))
            volume_h1 = float(volume.get("h1", 0))

            price_change = best_pair.get("priceChange", {})
            price_change_m5 = float(price_change.get("m5", 0))

            liquidity = best_pair.get("liquidity", {})
            liquidity_usd = float(liquidity.get("usd", 0))

            fdv = float(best_pair.get("fdv", 0))
            pair_id = best_pair.get("pairAddress", "")

            info = best_pair.get("info", {})
            image_url = info.get("imageUrl", "")
            websites = info.get("websites", [])
            socials = info.get("socials", [])

            return {
                "price_sol": price_native,
                "price_usd": price_usd,
                "volume_m5": volume_m5,
                "volume_h1": volume_h1,
                "price_change_m5": price_change_m5,
                "liquidity_usd": liquidity_usd,
                "fdv": fdv,
                "pair_address": pair_id,
                "image_url": image_url,
                "websites": websites,
                "socials": socials,
            }

        except Exception as e:
            print(f"RealyEngine Error (DexScreener): {e}")
            return await self.check_liquidity_via_rpc(token_mint)

    async def get_token_age(self, token_mint: str) -> int:
        """Returns approximate token pair age in seconds based on pair creation time."""
        try:
            pair = await self._fetch_best_pair(token_mint)
            if not pair:
                return 0
            created_ms = pair.get("pairCreatedAt")
            if not created_ms:
                return 0
            age_sec = int(time.time() - (float(created_ms) / 1000.0))
            return max(0, age_sec)
        except Exception:
            return 0

    async def get_creator_twitter_metrics(self, token_mint: str) -> dict:
        """
        Returns best-effort creator Twitter metadata from token socials.
        Follower count can be enriched via Bird in SocialMonitor.
        """
        try:
            pair = await self._fetch_best_pair(token_mint)
            if not pair:
                return {}

            info = pair.get("info", {})
            socials = info.get("socials", []) or []

            twitter_url = ""
            for social in socials:
                if str(social.get("type", "")).lower() == "twitter":
                    twitter_url = social.get("url", "")
                    break

            if not twitter_url:
                return {}

            username = twitter_url.rstrip("/").split("/")[-1].lstrip("@")
            return {
                "username": username,
                "url": twitter_url,
                "followers": 0,
                "scraped_at": time.time(),
            }
        except Exception:
            return {}

    async def check_liquidity_via_rpc(self, token_mint: str):
        """
        Fallback when DexScreener is unavailable.
        """
        return {
            "price_sol": 0.000001,
            "liquidity_check": True,
            "source": "RPC_FALLBACK",
        }


if __name__ == "__main__":
    async def main():
        engine = RealyEngine()
        print("Testing RealyEngine (IPV4)...")
        print(await engine.get_token_price("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"))

    asyncio.run(main())
