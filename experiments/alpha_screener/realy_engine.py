import aiohttp
import asyncio
import os
import time
from typing import List, Optional


class RealyEngine:
    """
    Fetches REAL on-chain data for tokens.
    Uses DexScreener API for price and metadata.
    """

    def __init__(self):
        self.sol_mint = "So11111111111111111111111111111111111111112"
        self.usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        self.usdt_mint = "Es9vMFrzaCER7w6fC4M1m9P8f1sM9vDOMkMt2rt7NmBG"
        primary_key = os.getenv("BIRDEYE_API_KEY", "").strip()
        keys_raw = os.getenv("BIRDEYE_API_KEYS", "").strip()
        keys = []
        if primary_key:
            keys.append(primary_key)
        if keys_raw:
            for part in keys_raw.replace(";", ",").split(","):
                k = str(part or "").strip()
                if k and k not in keys:
                    keys.append(k)
        self.birdeye_api_keys = keys
        self._birdeye_key_cursor = 0
        self._birdeye_key_cooldown_until = {}
        self.birdeye_base_url = "https://public-api.birdeye.so"
        self.birdeye_timeout_sec = 1.2
        self._birdeye_warned_missing = False
        # Short-lived market cache to smooth transient API zero-value glitches.
        self.market_cache = {}
        self.market_cache_ttl_sec = 20.0
        self.sol_usd_cache_ttl_sec = 15.0
        if not self.birdeye_api_keys:
            print("RealyEngine: Birdeye key(s) not set; using DexScreener/Gecko fallback path.")
            self._birdeye_warned_missing = True

    def _cached_quote_or_none(self, token_mint: str, source_suffix: str = "cache") -> Optional[dict]:
        cache = self.market_cache.get(token_mint, {}) or {}
        updated_at = self._to_float(cache.get("updated_at", 0.0), 0.0)
        if updated_at <= 0:
            return None
        # Allow slightly stale cache during provider outages; values are still real snapshots.
        age_sec = max(0.0, time.time() - updated_at)
        if age_sec > max(60.0, float(self.market_cache_ttl_sec or 20.0) * 3.0):
            return None

        price_sol = self._to_float(cache.get("price_sol", 0.0), 0.0)
        liquidity_usd = self._to_float(cache.get("liquidity_usd", 0.0), 0.0)
        mcap = self._to_float(cache.get("mcap_usd", 0.0), 0.0)
        if price_sol <= 0 and liquidity_usd <= 0 and mcap <= 0:
            return None

        return {
            "price_sol": price_sol,
            "price_usd": 0.0,
            "volume_m5": 0.0,
            "volume_h1": 0.0,
            "price_change_m5": 0.0,
            "liquidity_usd": liquidity_usd,
            "liquidity_known": liquidity_usd > 0,
            "fdv": mcap,
            "mcap_usd": mcap,
            "mcap_known": mcap > 0,
            "market_cap": mcap,
            "fdv_raw": mcap,
            "pair_address": "",
            "market_data_source": f"{source_suffix}_stale",
            "image_url": "",
            "websites": [],
            "socials": [],
        }

    def _to_float(self, value, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _is_known_numeric(self, value) -> bool:
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        return True

    def _pair_sort_key(self, pair: dict, token_mint: str):
        chain = str(pair.get("chainId", "")).lower()
        is_solana = 1 if chain == "solana" else 0

        quote_addr = str((pair.get("quoteToken", {}) or {}).get("address", "") or "")
        base_addr = str((pair.get("baseToken", {}) or {}).get("address", "") or "")

        quote_pref = 0
        if quote_addr == self.sol_mint:
            quote_pref = 3
        elif quote_addr == self.usdc_mint:
            quote_pref = 2
        elif quote_addr == self.usdt_mint:
            quote_pref = 1

        base_pref = 1 if base_addr == token_mint else 0

        liq = self._to_float((pair.get("liquidity", {}) or {}).get("usd", 0))
        vol_h1 = self._to_float((pair.get("volume", {}) or {}).get("h1", 0))
        vol_h24 = self._to_float((pair.get("volume", {}) or {}).get("h24", 0))
        mcap = self._to_float(pair.get("marketCap", 0))
        fdv = self._to_float(pair.get("fdv", 0))
        cap = mcap if mcap > 0 else fdv
        age = self._to_float(pair.get("pairCreatedAt", 0))

        return (
            is_solana,
            quote_pref,
            base_pref,
            liq,
            vol_h1,
            vol_h24,
            cap,
            age,
        )

    def _select_best_pair(self, pairs: List[dict], token_mint: str) -> Optional[dict]:
        if not pairs:
            return None

        sol_pairs = [p for p in pairs if str(p.get("chainId", "")).lower() == "solana"]
        candidates = sol_pairs if sol_pairs else pairs
        candidates = [p for p in candidates if isinstance(p, dict)]
        if not candidates:
            return None

        best = max(candidates, key=lambda p: self._pair_sort_key(p, token_mint))
        return best

    def _select_best_liquidity_pair(self, pairs: List[dict], token_mint: str, exclude_pair_id: str = "") -> Optional[dict]:
        if not pairs:
            return None

        candidates = []
        for p in pairs:
            if not isinstance(p, dict):
                continue
            if str(p.get("chainId", "")).lower() != "solana":
                continue
            if exclude_pair_id and str(p.get("pairAddress", "") or "") == exclude_pair_id:
                continue
            base_addr = str((p.get("baseToken", {}) or {}).get("address", "") or "")
            if base_addr != token_mint:
                continue
            liq = self._to_float((p.get("liquidity", {}) or {}).get("usd", 0.0), 0.0)
            if liq <= 0:
                continue
            vol_h1 = self._to_float((p.get("volume", {}) or {}).get("h1", 0.0), 0.0)
            candidates.append((liq, vol_h1, p))

        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return candidates[0][2]

    async def _fetch_pairs(self, token_mint: str) -> List[dict]:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint}"
        import socket

        last_exc = None
        for attempt in range(2):
            try:
                connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
                timeout = aiohttp.ClientTimeout(total=3.5)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    async with session.get(url) as response:
                        if response.status != 200:
                            return []
                        data = await response.json()
                        pairs = data.get("pairs", [])
                        if not pairs:
                            return []
                        return [p for p in pairs if isinstance(p, dict)]
            except Exception as exc:
                last_exc = exc
                if attempt == 0:
                    await asyncio.sleep(0.15)
                    continue
                raise
        if last_exc:
            raise last_exc
        return []

    async def _fetch_best_pair(self, token_mint: str):
        """Fetches DexScreener pair payload and returns best pair dict or None."""
        pairs = await self._fetch_pairs(token_mint)
        if not pairs:
            return None
        return self._select_best_pair(pairs, token_mint)

    async def _fetch_gecko_pool(self, pair_address: str) -> dict:
        if not pair_address:
            return {}
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pair_address}"
        import socket

        connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
        timeout = aiohttp.ClientTimeout(total=1.2)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return {}
                data = await response.json()
                return (data.get("data", {}) or {}).get("attributes", {}) or {}

    async def _fetch_birdeye_json(self, path: str, params: dict = None) -> dict:
        """Best-effort Birdeye GET helper. Returns parsed dict or {}."""
        if not self.birdeye_api_keys:
            return {}
        url = f"{self.birdeye_base_url}{path}"
        timeout = aiohttp.ClientTimeout(total=float(self.birdeye_timeout_sec or 1.2))
        now = time.time()
        n = len(self.birdeye_api_keys)
        if n <= 0:
            return {}
        ordered_keys = [
            self.birdeye_api_keys[(self._birdeye_key_cursor + i) % n]
            for i in range(n)
        ]
        for key in ordered_keys:
            cooldown_until = float(self._birdeye_key_cooldown_until.get(key, 0.0) or 0.0)
            if now < cooldown_until:
                continue
            headers = {
                "X-API-KEY": key,
                "accept": "application/json",
                "x-chain": "solana",
            }
            try:
                import socket
                connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
                    async with session.get(url, params=(params or {})) as response:
                        if response.status == 200:
                            data = await response.json()
                            if isinstance(data, dict):
                                self._birdeye_key_cursor = (self.birdeye_api_keys.index(key) + 1) % max(1, n)
                                return data
                            continue
                        if response.status in (401, 403):
                            self._birdeye_key_cooldown_until[key] = time.time() + (12 * 3600)
                            continue
                        if response.status == 429:
                            self._birdeye_key_cooldown_until[key] = time.time() + (20 * 60)
                            continue
                        continue
            except Exception:
                continue
        return {}

    async def _fetch_sol_usd_cached(self) -> float:
        """Returns SOL/USD from Birdeye with short cache; 0 when unavailable."""
        cache = self.market_cache.get("__sol_usd__", {}) or {}
        now = time.time()
        cache_age = now - float(cache.get("updated_at", 0.0) or 0.0)
        if cache_age <= self.sol_usd_cache_ttl_sec:
            cached = self._to_float(cache.get("price_usd", 0.0), 0.0)
            if cached > 0:
                return cached

        sol_usd = 0.0
        for attempt in range(2):
            payload = await self._fetch_birdeye_json("/defi/price", {"address": self.sol_mint})
            data = (payload or {}).get("data", {}) if isinstance(payload, dict) else {}
            sol_usd = self._to_float((data or {}).get("value", 0.0), 0.0)
            if sol_usd > 0:
                break
            if attempt == 0:
                await asyncio.sleep(0.12)
        if sol_usd > 0:
            self.market_cache["__sol_usd__"] = {"updated_at": now, "price_usd": sol_usd}
            return sol_usd

        # Fallback 1: CoinGecko public quote.
        try:
            import socket
            connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
            timeout = aiohttp.ClientTimeout(total=1.8)
            url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        payload = await response.json()
                        sol_usd = self._to_float(((payload or {}).get("solana", {}) or {}).get("usd", 0.0), 0.0)
        except Exception:
            sol_usd = 0.0
        if sol_usd > 0:
            self.market_cache["__sol_usd__"] = {"updated_at": now, "price_usd": sol_usd}
            return sol_usd

        # Fallback 2: DexScreener SOL token snapshot.
        try:
            pairs = await self._fetch_pairs(self.sol_mint)
            best = self._select_best_pair(pairs, self.sol_mint)
            if best:
                sol_usd = self._to_float(best.get("priceUsd", 0.0), 0.0)
        except Exception:
            sol_usd = 0.0
        if sol_usd > 0:
            self.market_cache["__sol_usd__"] = {"updated_at": now, "price_usd": sol_usd}
            return sol_usd

        cached = self._to_float(cache.get("price_usd", 0.0), 0.0)
        return cached if cached > 0 else 0.0

    async def _fetch_birdeye_market(self, token_mint: str) -> dict:
        """
        Fetches Birdeye market data and normalizes to internal shape.
        Returns {} when unavailable.
        """
        payload = await self._fetch_birdeye_json("/defi/v3/token/market-data", {"address": token_mint})
        if not payload:
            await asyncio.sleep(0.12)
            payload = await self._fetch_birdeye_json("/defi/v3/token/market-data", {"address": token_mint})
        if not payload:
            return {}
        data = (payload or {}).get("data", {}) if isinstance(payload, dict) else {}
        if not isinstance(data, dict) or not data:
            return {}

        price_usd = self._to_float(data.get("price", 0.0), 0.0)
        liquidity_usd = self._to_float(data.get("liquidity", 0.0), 0.0)
        market_cap = self._to_float(data.get("market_cap", 0.0), 0.0)
        fdv_raw = self._to_float(data.get("fdv", 0.0), 0.0)
        mcap = market_cap if market_cap > 0 else fdv_raw
        mcap_known = market_cap > 0 or fdv_raw > 0
        liquidity_known = liquidity_usd > 0

        price_sol = 0.0
        if price_usd > 0:
            sol_usd = await self._fetch_sol_usd_cached()
            if sol_usd > 0:
                price_sol = price_usd / sol_usd

        return {
            "price_sol": price_sol,
            "price_usd": price_usd,
            "liquidity_usd": liquidity_usd,
            "liquidity_known": liquidity_known,
            "mcap_usd": mcap,
            "mcap_known": mcap_known,
            "market_cap": market_cap,
            "fdv_raw": fdv_raw if fdv_raw > 0 else mcap,
            "fdv": mcap,
            "market_data_source": "birdeye",
        }

    async def get_token_price(self, token_mint: str):
        """
        Calculates price of 1 token in SOL and collects market metadata.
        """
        try:
            birdeye = await self._fetch_birdeye_market(token_mint)
            used_birdeye = bool(birdeye)
            if used_birdeye:
                be_price_sol = self._to_float(birdeye.get("price_sol", 0.0), 0.0)
                be_price_usd = self._to_float(birdeye.get("price_usd", 0.0), 0.0)
                be_liq = self._to_float(birdeye.get("liquidity_usd", 0.0), 0.0)
                be_mcap = self._to_float(birdeye.get("mcap_usd", 0.0), 0.0)
                if be_price_sol > 0 and (be_liq > 0 or be_mcap > 0):
                    # Fast-path for open-position marks: avoid waiting on DexScreener when
                    # Birdeye already has a valid quote.
                    self.market_cache[token_mint] = {
                        "updated_at": time.time(),
                        "liquidity_usd": be_liq,
                        "mcap_usd": be_mcap,
                        "price_sol": be_price_sol,
                    }
                    return {
                        "price_sol": be_price_sol,
                        "price_usd": be_price_usd,
                        "volume_m5": 0.0,
                        "volume_h1": 0.0,
                        "price_change_m5": 0.0,
                        "liquidity_usd": be_liq,
                        "liquidity_known": be_liq > 0,
                        "fdv": be_mcap,
                        "mcap_usd": be_mcap,
                        "mcap_known": be_mcap > 0,
                        "market_cap": self._to_float(birdeye.get("market_cap", 0.0), 0.0),
                        "fdv_raw": self._to_float(birdeye.get("fdv_raw", be_mcap), be_mcap),
                        "pair_address": "",
                        "market_data_source": "birdeye_fast",
                        "image_url": "",
                        "websites": [],
                        "socials": [],
                    }
            pairs = await self._fetch_pairs(token_mint)
            best_pair = self._select_best_pair(pairs, token_mint)
            if not best_pair:
                if birdeye:
                    return {
                        "price_sol": self._to_float(birdeye.get("price_sol", 0.0), 0.0),
                        "price_usd": self._to_float(birdeye.get("price_usd", 0.0), 0.0),
                        "volume_m5": 0.0,
                        "volume_h1": 0.0,
                        "price_change_m5": 0.0,
                        "liquidity_usd": self._to_float(birdeye.get("liquidity_usd", 0.0), 0.0),
                        "liquidity_known": bool(birdeye.get("liquidity_known", False)),
                        "fdv": self._to_float(birdeye.get("fdv", 0.0), 0.0),
                        "mcap_usd": self._to_float(birdeye.get("mcap_usd", 0.0), 0.0),
                        "mcap_known": bool(birdeye.get("mcap_known", False)),
                        "market_cap": self._to_float(birdeye.get("market_cap", 0.0), 0.0),
                        "fdv_raw": self._to_float(birdeye.get("fdv_raw", 0.0), 0.0),
                        "pair_address": "",
                        "market_data_source": "birdeye_only",
                        "image_url": "",
                        "websites": [],
                        "socials": [],
                    }
                cached = self._cached_quote_or_none(token_mint, source_suffix="cache_no_pair")
                if cached:
                    return cached
                return {
                    "price_sol": 0.0,
                    "price_usd": 0.0,
                    "volume_m5": 0.0,
                    "volume_h1": 0.0,
                    "price_change_m5": 0.0,
                    "liquidity_usd": 0.0,
                    "liquidity_known": False,
                    "fdv": 0.0,
                    "mcap_usd": 0.0,
                    "mcap_known": False,
                    "market_cap": 0.0,
                    "fdv_raw": 0.0,
                    "pair_address": "",
                    "market_data_source": "dexscreener_missing",
                    "image_url": "",
                    "websites": [],
                    "socials": [],
                }

            price_native = self._to_float(best_pair.get("priceNative", 0))
            price_usd = self._to_float(best_pair.get("priceUsd", 0))

            volume = best_pair.get("volume", {})
            volume_m5 = self._to_float(volume.get("m5", 0))
            volume_h1 = self._to_float(volume.get("h1", 0))

            price_change = best_pair.get("priceChange", {})
            price_change_m5 = self._to_float(price_change.get("m5", 0))

            liquidity = best_pair.get("liquidity", {})
            raw_liq_usd = liquidity.get("usd", None)
            liquidity_usd = self._to_float(raw_liq_usd, 0.0)
            liquidity_known = self._is_known_numeric(raw_liq_usd)

            raw_fdv = best_pair.get("fdv", None)
            raw_market_cap = best_pair.get("marketCap", None)
            fdv_raw = self._to_float(raw_fdv, 0.0)
            market_cap = self._to_float(raw_market_cap, 0.0)
            mcap = market_cap if market_cap > 0 else fdv_raw
            mcap_known = self._is_known_numeric(raw_market_cap) or self._is_known_numeric(raw_fdv)
            pair_id = best_pair.get("pairAddress", "")
            market_data_sources = {"dexscreener"}

            if used_birdeye:
                market_data_sources.add("birdeye")
                be_mcap = self._to_float(birdeye.get("mcap_usd", 0.0), 0.0)
                if be_mcap > 0:
                    mcap = be_mcap
                    mcap_known = True
                    be_market_cap = self._to_float(birdeye.get("market_cap", 0.0), 0.0)
                    if be_market_cap > 0:
                        market_cap = be_market_cap
                    be_fdv_raw = self._to_float(birdeye.get("fdv_raw", 0.0), 0.0)
                    if be_fdv_raw > 0:
                        fdv_raw = be_fdv_raw

                be_price_sol = self._to_float(birdeye.get("price_sol", 0.0), 0.0)
                be_price_usd = self._to_float(birdeye.get("price_usd", 0.0), 0.0)
                if be_price_sol > 0:
                    price_native = be_price_sol
                if be_price_usd > 0:
                    price_usd = be_price_usd

                be_liq = self._to_float(birdeye.get("liquidity_usd", 0.0), 0.0)
                if be_liq > 0:
                    liquidity_usd = be_liq
                    liquidity_known = True

            # If selected pair has transient liq=0, quickly recover from other DexScreener
            # pairs in the same response (no extra network call).
            if (not liquidity_known or liquidity_usd <= 0) and pairs:
                alt_pair = self._select_best_liquidity_pair(pairs, token_mint, exclude_pair_id=pair_id)
                if alt_pair:
                    alt_liq = self._to_float((alt_pair.get("liquidity", {}) or {}).get("usd", 0.0), 0.0)
                    if alt_liq > 0:
                        liquidity_usd = alt_liq
                        liquidity_known = True
                        market_data_sources.add("dexscreener_alt_liq")

            # Use recent known-good values when APIs momentarily return zeros.
            cache = self.market_cache.get(token_mint, {})
            cache_age = time.time() - float(cache.get("updated_at", 0.0) or 0.0)
            cache_fresh = cache_age <= self.market_cache_ttl_sec
            if cache_fresh:
                cached_liq = self._to_float(cache.get("liquidity_usd", 0.0), 0.0)
                cached_mcap = self._to_float(cache.get("mcap_usd", 0.0), 0.0)
                cached_price_sol = self._to_float(cache.get("price_sol", 0.0), 0.0)
                if (not liquidity_known or liquidity_usd <= 0) and cached_liq > 0:
                    liquidity_usd = cached_liq
                    liquidity_known = True
                    market_data_sources.add("cache_liq")
                if ((not mcap_known) or mcap <= 0) and cached_mcap > 0:
                    mcap = cached_mcap
                    mcap_known = True
                    market_data_sources.add("cache_mcap")
                if price_native <= 0 and cached_price_sol > 0:
                    price_native = cached_price_sol
                    market_data_sources.add("cache_price")

            # Fresh launches frequently return missing/zero values for a few seconds.
            if pair_id and ((not liquidity_known) or liquidity_usd <= 0 or (not mcap_known) or mcap <= 0):
                try:
                    gecko = await self._fetch_gecko_pool(pair_id)
                except Exception:
                    gecko = {}
                if gecko:
                    gecko_liq = self._to_float(gecko.get("reserve_in_usd", 0), 0.0)
                    gecko_mcap = self._to_float(gecko.get("market_cap_usd", 0), 0.0)
                    gecko_fdv = self._to_float(gecko.get("fdv_usd", 0), 0.0)
                    gecko_price_usd = self._to_float(gecko.get("base_token_price_usd", 0), 0.0)
                    if gecko_liq > 0 and (not liquidity_known or liquidity_usd <= 0):
                        liquidity_usd = gecko_liq
                        liquidity_known = True
                        market_data_sources.add("gecko_liq")
                    gecko_cap = gecko_mcap if gecko_mcap > 0 else gecko_fdv
                    if gecko_cap > 0 and ((not mcap_known) or mcap <= 0):
                        mcap = gecko_cap
                        market_cap = gecko_mcap if gecko_mcap > 0 else market_cap
                        if gecko_fdv > 0:
                            fdv_raw = gecko_fdv
                        mcap_known = True
                        market_data_sources.add("gecko_mcap")
                    if gecko_price_usd > 0 and price_usd <= 0:
                        price_usd = gecko_price_usd
                        market_data_sources.add("gecko_price")

            if used_birdeye and "dexscreener" in market_data_sources:
                if (not mcap_known) or mcap <= 0 or price_native <= 0:
                    print(f"RealyEngine: token {token_mint[:8]} used Dex/Gecko fallback after Birdeye partial data.")

            info = best_pair.get("info", {})
            image_url = info.get("imageUrl", "")
            websites = info.get("websites", [])
            socials = info.get("socials", [])

            # Refresh cache with any positive values.
            self.market_cache[token_mint] = {
                "updated_at": time.time(),
                "liquidity_usd": liquidity_usd if liquidity_usd > 0 else self._to_float(cache.get("liquidity_usd", 0.0), 0.0),
                "mcap_usd": mcap if mcap > 0 else self._to_float(cache.get("mcap_usd", 0.0), 0.0),
                "price_sol": price_native if price_native > 0 else self._to_float(cache.get("price_sol", 0.0), 0.0),
            }
            market_data_source = "+".join(sorted(market_data_sources))

            return {
                "price_sol": price_native,
                "price_usd": price_usd,
                "volume_m5": volume_m5,
                "volume_h1": volume_h1,
                "price_change_m5": price_change_m5,
                "liquidity_usd": liquidity_usd,
                "liquidity_known": bool(liquidity_known),
                "fdv": mcap,
                "mcap_usd": mcap,
                "mcap_known": bool(mcap_known),
                "market_cap": market_cap,
                "fdv_raw": fdv_raw,
                "pair_address": pair_id,
                "market_data_source": market_data_source,
                "image_url": image_url,
                "websites": websites,
                "socials": socials,
            }

        except Exception as e:
            print(f"RealyEngine Error (DexScreener) [{type(e).__name__}]: {e!r}")
            cached = self._cached_quote_or_none(token_mint, source_suffix="cache_error")
            if cached:
                return cached
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
        Fallback when market data providers are unavailable.
        Never fabricate executable prices; return zeroed quote fields so
        downstream PnL logic does not create synthetic gains/losses.
        """
        cached = self._cached_quote_or_none(token_mint, source_suffix="cache_rpc_fallback")
        if cached:
            return cached
        return {
            "price_sol": 0.0,
            "price_usd": 0.0,
            "volume_m5": 0.0,
            "volume_h1": 0.0,
            "price_change_m5": 0.0,
            "liquidity_usd": 0.0,
            "liquidity_known": False,
            "fdv": 0.0,
            "mcap_usd": 0.0,
            "mcap_known": False,
            "market_cap": 0.0,
            "fdv_raw": 0.0,
            "pair_address": "",
            "market_data_source": "rpc_fallback",
            "image_url": "",
            "websites": [],
            "socials": [],
            "liquidity_check": True,
            "source": "RPC_FALLBACK",
        }


if __name__ == "__main__":
    async def main():
        engine = RealyEngine()
        print("Testing RealyEngine (IPV4)...")
        print(await engine.get_token_price("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"))

    asyncio.run(main())
