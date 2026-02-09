import asyncio
import aiohttp
import time
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

        return {
            "top10_holding_pct": round(top10_pct, 2),
            "snipers_holding_pct": round(snipers_pct, 2),
            "bundlers_holding_pct": round(bundlers_pct, 2),
            "sniper_wallet_count": sniper_wallets,
            "bundler_wallet_count": bundler_wallets,
            "total_supply": total_supply,
            "source": "geckoterminal+rpc_heuristic",
            "calculated_at": time.time(),
            "latency_sec": round(time.time() - started_at, 3),
        }
