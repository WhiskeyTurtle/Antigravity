import asyncio
import aiohttp
import time
import statistics
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set


class HolderIntel:
    """
    Computes holder concentration and heuristic bundler/sniper holding percentages.

    Notes:
    - `top10_holding_pct` is exact from RPC largest token accounts.
    - `snipers_holding_pct` and `bundlers_holding_pct` are heuristic, using Gecko trades
      and wallet balances from RPC.
    """

    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url
        self.gecko_base = "https://api.geckoterminal.com/api/v2"
        self.wallet_age_sample_size = 12
        self.wallet_age_signature_limit = 200

    async def _rpc_call(self, method: str, params: list):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.rpc_url, json=payload, timeout=20) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data.get("result")

    async def _get_total_supply(self, token_mint: str) -> float:
        result = await self._rpc_call("getTokenSupply", [token_mint])
        if not result:
            return 0.0
        value = result.get("value", {})
        try:
            ui_amount = value.get("uiAmount")
            if ui_amount is not None:
                return float(ui_amount)
            amount = float(value.get("amount", 0))
            decimals = int(value.get("decimals", 0))
            return amount / (10 ** decimals) if decimals >= 0 else 0.0
        except Exception:
            return 0.0

    async def _get_top10_holding_pct(self, token_mint: str, total_supply: float) -> float:
        if total_supply <= 0:
            return 0.0
        result = await self._rpc_call("getTokenLargestAccounts", [token_mint])
        if not result:
            return 0.0
        accounts = result.get("value", [])[:10]
        total = 0.0
        for a in accounts:
            try:
                ui_amount = a.get("uiAmount")
                if ui_amount is not None:
                    total += float(ui_amount)
                else:
                    amount = float(a.get("amount", 0))
                    decimals = int(a.get("decimals", 0))
                    total += amount / (10 ** decimals) if decimals >= 0 else 0.0
            except Exception:
                continue
        return (total / total_supply) * 100.0

    async def _get_top_holder_wallets(self, token_mint: str, limit: int = 20) -> List[str]:
        result = await self._rpc_call("getTokenLargestAccounts", [token_mint])
        if not result:
            return []
        token_accounts = []
        for a in (result.get("value", []) or []):
            addr = str(a.get("address", "") or "").strip()
            if addr:
                token_accounts.append(addr)
            if len(token_accounts) >= max(1, int(limit)):
                break
        if not token_accounts:
            return []

        multi = await self._rpc_call(
            "getMultipleAccounts",
            [
                token_accounts,
                {"encoding": "jsonParsed"},
            ],
        )
        wallets: List[str] = []
        seen: Set[str] = set()
        values = ((multi or {}).get("value", []) if isinstance(multi, dict) else []) or []
        for row in values:
            try:
                owner = str(
                    (((row or {}).get("data", {}) or {}).get("parsed", {}) or {})
                    .get("info", {})
                    .get("owner", "")
                    or ""
                ).strip()
                if owner and owner not in seen:
                    wallets.append(owner)
                    seen.add(owner)
            except Exception:
                continue
        return wallets

    async def _get_pool_data(self, token_mint: str) -> Optional[dict]:
        url = f"{self.gecko_base}/networks/solana/tokens/{token_mint}/pools"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=20) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                pools = data.get("data", [])
                if not pools:
                    return None
                attrs = pools[0].get("attributes", {})
                return {
                    "pool_address": attrs.get("address", ""),
                    "pool_created_at": attrs.get("pool_created_at", ""),
                }

    async def _get_trades(self, pool_address: str, trade_limit: int = 300) -> List[dict]:
        url = (
            f"{self.gecko_base}/networks/solana/pools/{pool_address}/trades"
            f"?limit={trade_limit}"
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=25) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                items = data.get("data", [])
                return [i.get("attributes", {}) for i in items]

    def _to_unix(self, iso_str: str) -> float:
        if not iso_str:
            return 0.0
        try:
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            return dt.timestamp()
        except Exception:
            return 0.0

    def _classify_wallet_sets(self, trades: List[dict], pool_created_ts: float):
        snipers: Set[str] = set()
        bundlers: Set[str] = set()

        if not trades:
            return snipers, bundlers

        # Define early windows.
        sniper_window_sec = 120
        bundle_window_sec = 15 * 60

        bucket_to_wallets: Dict[int, Set[str]] = {}
        wallet_early_trade_count: Dict[str, int] = {}

        for t in trades:
            wallet = t.get("tx_from_address")
            block_ts = t.get("block_timestamp")
            side = str(t.get("kind", "")).lower()
            if not wallet or not block_ts:
                continue
            try:
                block_ts = float(block_ts)
            except Exception:
                continue
            if pool_created_ts <= 0:
                # Fallback: use newest trades heuristic if creation time unavailable.
                continue

            dt = block_ts - pool_created_ts
            if dt < 0:
                continue

            is_buy_like = side in ("buy", "swap") or not side
            if not is_buy_like:
                continue

            if dt <= sniper_window_sec:
                snipers.add(wallet)

            if dt <= bundle_window_sec:
                wallet_early_trade_count[wallet] = wallet_early_trade_count.get(wallet, 0) + 1
                sec_bucket = int(dt)
                bucket_to_wallets.setdefault(sec_bucket, set()).add(wallet)

        # Bundler heuristic A: repeated early activity from same wallet.
        for wallet, cnt in wallet_early_trade_count.items():
            if cnt >= 2:
                bundlers.add(wallet)

        # Bundler heuristic B: synchronized wallets in same-second buckets.
        for wallets in bucket_to_wallets.values():
            if len(wallets) >= 3:
                bundlers.update(wallets)

        return snipers, bundlers

    async def _wallet_holding_for_mint(self, wallet: str, token_mint: str) -> float:
        result = await self._rpc_call(
            "getTokenAccountsByOwner",
            [
                wallet,
                {"mint": token_mint},
                {"encoding": "jsonParsed"},
            ],
        )
        if not result:
            return 0.0

        total = 0.0
        for acc in result.get("value", []):
            try:
                parsed = acc.get("account", {}).get("data", {}).get("parsed", {})
                amt = parsed.get("info", {}).get("tokenAmount", {}).get("uiAmount", 0)
                total += float(amt or 0)
            except Exception:
                continue
        return total

    async def _wallet_first_seen_ts(self, wallet: str, sig_limit: int = 200) -> float:
        result = await self._rpc_call(
            "getSignaturesForAddress",
            [
                wallet,
                {"limit": max(1, int(sig_limit))},
            ],
        )
        if not isinstance(result, list) or not result:
            return 0.0
        oldest = 0.0
        for row in result:
            try:
                ts = float((row or {}).get("blockTime", 0) or 0)
            except Exception:
                ts = 0.0
            if ts <= 0:
                continue
            oldest = ts if oldest <= 0 else min(oldest, ts)
        return oldest

    async def _wallet_age_cluster_metrics(self, wallets: List[str]) -> dict:
        sampled = [str(w or "").strip() for w in (wallets or []) if str(w or "").strip()]
        sampled = sampled[: max(1, int(self.wallet_age_sample_size))]
        if not sampled:
            return {
                "sample_size": 0,
                "ages_hours": [],
                "recent_24h_share_pct": 0.0,
                "recent_48h_share_pct": 0.0,
                "same_hour_cluster_pct": 0.0,
                "median_age_hours": 0.0,
            }

        first_seen_rows = await asyncio.gather(
            *[self._wallet_first_seen_ts(w, sig_limit=self.wallet_age_signature_limit) for w in sampled],
            return_exceptions=True,
        )
        now = time.time()
        first_seen_ts: List[float] = []
        for ts in first_seen_rows:
            if isinstance(ts, Exception):
                continue
            tsv = float(ts or 0.0)
            if tsv > 0:
                first_seen_ts.append(tsv)

        if not first_seen_ts:
            return {
                "sample_size": 0,
                "ages_hours": [],
                "recent_24h_share_pct": 0.0,
                "recent_48h_share_pct": 0.0,
                "same_hour_cluster_pct": 0.0,
                "median_age_hours": 0.0,
            }

        ages_hours = [max(0.0, (now - ts) / 3600.0) for ts in first_seen_ts]
        sample_size = len(ages_hours)
        recent_24h = sum(1 for h in ages_hours if h <= 24.0)
        recent_48h = sum(1 for h in ages_hours if h <= 48.0)

        hour_buckets: Dict[int, int] = {}
        for ts in first_seen_ts:
            bucket = int(ts // 3600)
            hour_buckets[bucket] = int(hour_buckets.get(bucket, 0) or 0) + 1
        same_hour_cluster = (max(hour_buckets.values()) / sample_size * 100.0) if hour_buckets else 0.0
        median_age = float(statistics.median(ages_hours)) if ages_hours else 0.0

        return {
            "sample_size": sample_size,
            "ages_hours": [round(h, 2) for h in ages_hours],
            "recent_24h_share_pct": round(recent_24h / sample_size * 100.0, 2),
            "recent_48h_share_pct": round(recent_48h / sample_size * 100.0, 2),
            "same_hour_cluster_pct": round(same_hour_cluster, 2),
            "median_age_hours": round(median_age, 2),
        }

    async def _group_holding_pct(self, wallets: Set[str], token_mint: str, total_supply: float) -> float:
        if total_supply <= 0 or not wallets:
            return 0.0

        # Limit to keep RPC cost bounded.
        sampled = list(wallets)[:30]
        balances = await asyncio.gather(
            *[self._wallet_holding_for_mint(w, token_mint) for w in sampled],
            return_exceptions=True,
        )
        held = 0.0
        for b in balances:
            if isinstance(b, Exception):
                continue
            held += float(b or 0)
        return (held / total_supply) * 100.0

    async def get_holder_intel(self, token_mint: str, pair_address: str = "") -> dict:
        started_at = time.time()
        total_supply = await self._get_total_supply(token_mint)
        top10_pct = await self._get_top10_holding_pct(token_mint, total_supply)

        pool_created_ts = 0.0
        pool_addr = pair_address or ""
        if not pool_addr:
            pool_data = await self._get_pool_data(token_mint)
            if pool_data:
                pool_addr = pool_data.get("pool_address", "")
                pool_created_ts = self._to_unix(pool_data.get("pool_created_at", ""))

        snipers_pct = 0.0
        bundlers_pct = 0.0
        sniper_wallets = 0
        bundler_wallets = 0

        if pool_addr:
            if pool_created_ts <= 0:
                pool_data = await self._get_pool_data(token_mint)
                if pool_data:
                    pool_created_ts = self._to_unix(pool_data.get("pool_created_at", ""))

            trades = await self._get_trades(pool_addr)
            snipers, bundlers = self._classify_wallet_sets(trades, pool_created_ts)
            sniper_wallets = len(snipers)
            bundler_wallets = len(bundlers)

            snipers_pct = await self._group_holding_pct(snipers, token_mint, total_supply)
            bundlers_pct = await self._group_holding_pct(bundlers, token_mint, total_supply)

        wallet_age = {"sample_size": 0, "ages_hours": [], "recent_24h_share_pct": 0.0, "recent_48h_share_pct": 0.0, "same_hour_cluster_pct": 0.0, "median_age_hours": 0.0}
        try:
            top_wallets = await self._get_top_holder_wallets(token_mint, limit=max(12, self.wallet_age_sample_size))
            wallet_age = await self._wallet_age_cluster_metrics(top_wallets)
        except Exception:
            pass

        return {
            "top10_holding_pct": round(top10_pct, 2),
            "snipers_holding_pct": round(snipers_pct, 2),
            "bundlers_holding_pct": round(bundlers_pct, 2),
            "sniper_wallet_count": sniper_wallets,
            "bundler_wallet_count": bundler_wallets,
            "total_supply": total_supply,
            "wallet_age_sample_size": int(wallet_age.get("sample_size", 0) or 0),
            "wallet_age_hours_sample": list(wallet_age.get("ages_hours", []) or []),
            "wallet_age_recent_24h_share_pct": float(wallet_age.get("recent_24h_share_pct", 0.0) or 0.0),
            "wallet_age_recent_48h_share_pct": float(wallet_age.get("recent_48h_share_pct", 0.0) or 0.0),
            "wallet_age_same_hour_cluster_pct": float(wallet_age.get("same_hour_cluster_pct", 0.0) or 0.0),
            "wallet_age_median_hours": float(wallet_age.get("median_age_hours", 0.0) or 0.0),
            "source": "geckoterminal+rpc_heuristic",
            "calculated_at": time.time(),
            "latency_sec": round(time.time() - started_at, 3),
        }
