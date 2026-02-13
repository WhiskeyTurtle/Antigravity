import asyncio
import aiohttp
import time
from typing import List


class CandleAnalyzer:
    """Computes candle-derived signals from GeckoTerminal OHLCV data."""

    def __init__(self):
        self.base = "https://api.geckoterminal.com/api/v2"
        self._pool_cache = {}
        self._pool_cache_ts = {}
        self._pool_miss_cache = {}
        self._pool_cache_ttl_sec = 300.0
        self._pool_miss_ttl_sec = 120.0
        self._ohlcv_cache = {}
        self._ohlcv_cache_ttl_sec = 12.0
        self._last_ohlcv_rate_limited = False
        self._next_gecko_request_ts = 0.0
        self._min_gecko_request_gap_sec = 0.35

    async def _throttle_gecko(self):
        now = time.time()
        wait = self._next_gecko_request_ts - now
        if wait > 0:
            await asyncio.sleep(wait)
        self._next_gecko_request_ts = max(time.time(), self._next_gecko_request_ts) + self._min_gecko_request_gap_sec

    async def _fetch_ohlcv(self, pool_address: str, timeframe: str = "minute", aggregate: int = 1, limit: int = 120):
        if not pool_address:
            return []
        key = (str(pool_address), str(timeframe), int(aggregate), int(limit))
        now = time.time()
        cached = self._ohlcv_cache.get(key)
        if cached:
            age = now - float(cached.get("ts", 0.0) or 0.0)
            if age <= self._ohlcv_cache_ttl_sec:
                return list(cached.get("data", []) or [])

        url = (
            f"{self.base}/networks/solana/pools/{pool_address}/ohlcv/{timeframe}"
            f"?aggregate={aggregate}&limit={limit}"
        )
        saw_rate_limit = False
        for attempt in range(4):
            try:
                await self._throttle_gecko()
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=20) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            candles = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
                            if isinstance(candles, list):
                                self._ohlcv_cache[key] = {"ts": now, "data": candles}
                                return candles
                            return []
                        # Gecko intermittently rate-limits on bursty reads.
                        if resp.status == 429 and attempt < 3:
                            saw_rate_limit = True
                            await asyncio.sleep(0.35 * (attempt + 1))
                            continue
                        if resp.status == 429:
                            saw_rate_limit = True
                        break
            except Exception:
                if attempt < 3:
                    await asyncio.sleep(0.35 * (attempt + 1))
                    continue
                break

        # Fall back to stale cache if available instead of returning empty immediately.
        cached = self._ohlcv_cache.get(key)
        if cached:
            return list(cached.get("data", []) or [])
        if saw_rate_limit:
            self._last_ohlcv_rate_limited = True
        return []

    async def _resolve_pool_address_from_token(self, token_mint: str) -> str:
        if not token_mint:
            return ""
        now = time.time()
        cached = str(self._pool_cache.get(token_mint, "") or "")
        cache_age = now - float(self._pool_cache_ts.get(token_mint, 0.0) or 0.0)
        if cached and cache_age <= self._pool_cache_ttl_sec:
            return cached
        miss_age = now - float(self._pool_miss_cache.get(token_mint, 0.0) or 0.0)
        if miss_age <= self._pool_miss_ttl_sec:
            return ""

        url = f"{self.base}/networks/solana/tokens/{token_mint}/pools?page=1"
        for attempt in range(4):
            try:
                await self._throttle_gecko()
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=20) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            pools = data.get("data", []) or []
                            if not pools:
                                self._pool_miss_cache[token_mint] = now
                                return ""
                            attrs = (pools[0] or {}).get("attributes", {}) or {}
                            resolved = str(attrs.get("address", "") or "")
                            if not resolved:
                                pool_id = str((pools[0] or {}).get("id", "") or "")
                                if pool_id.startswith("solana_"):
                                    resolved = pool_id.split("solana_", 1)[1]
                            if resolved:
                                self._pool_cache[token_mint] = resolved
                                self._pool_cache_ts[token_mint] = now
                                return resolved
                            self._pool_miss_cache[token_mint] = now
                            return ""
                        if resp.status == 429 and attempt < 3:
                            await asyncio.sleep(0.35 * (attempt + 1))
                            continue
                        self._pool_miss_cache[token_mint] = now
                        return ""
            except Exception:
                if attempt < 3:
                    await asyncio.sleep(0.35 * (attempt + 1))
                    continue
                break
        self._pool_miss_cache[token_mint] = now
        return ""

    def _ema(self, values: List[float], period: int) -> float:
        if not values:
            return 0.0
        k = 2 / (period + 1)
        ema = values[0]
        for v in values[1:]:
            ema = (v * k) + (ema * (1 - k))
        return ema

    def _aggregate_ohlcv(self, ohlcv_old_to_new: List[list], aggregate: int = 5) -> List[list]:
        """
        Aggregates 1m candles into N-minute candles.
        Input/output format: [ts, open, high, low, close, volume], oldest -> newest.
        """
        agg = max(1, int(aggregate or 1))
        if agg <= 1 or not ohlcv_old_to_new:
            return list(ohlcv_old_to_new or [])

        out = []
        bucket = []
        for row in ohlcv_old_to_new:
            bucket.append(row)
            if len(bucket) >= agg:
                ts = int(bucket[-1][0])
                o = float(bucket[0][1])
                h = max(float(x[2]) for x in bucket)
                l = min(float(x[3]) for x in bucket)
                c = float(bucket[-1][4])
                v = sum(float(x[5]) for x in bucket)
                out.append([ts, o, h, l, c, v])
                bucket = []

        if bucket:
            ts = int(bucket[-1][0])
            o = float(bucket[0][1])
            h = max(float(x[2]) for x in bucket)
            l = min(float(x[3]) for x in bucket)
            c = float(bucket[-1][4])
            v = sum(float(x[5]) for x in bucket)
            out.append([ts, o, h, l, c, v])
        return out

    def _atr(self, ohlcv: List[list], period: int = 14) -> float:
        if len(ohlcv) < period + 1:
            return 0.0
        # ohlcv format: [ts, open, high, low, close, volume]
        trs = []
        for i in range(1, min(len(ohlcv), period + 1)):
            high = float(ohlcv[i][2])
            low = float(ohlcv[i][3])
            prev_close = float(ohlcv[i - 1][4])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if not trs:
            return 0.0
        return sum(trs) / len(trs)

    def _ema_tail(self, values: List[float], period: int) -> float:
        if not values:
            return 0.0
        p = max(2, min(int(period or 2), len(values)))
        window = values[-p:] if len(values) > p else list(values)
        return self._ema(window, p)

    def _atr_pct(self, ohlcv: List[list], period: int = 14) -> float:
        atr = self._atr(ohlcv, period=period)
        last_close = float(ohlcv[-1][4]) if ohlcv and float(ohlcv[-1][4]) != 0 else 1.0
        return (atr / last_close) * 100.0 if last_close > 0 else 0.0

    async def analyze(self, pool_address: str, config: dict = None) -> dict:
        if not pool_address:
            return {"score_adjustment": 0, "reason": "no_pool", "reason_code": "NO_POOL"}
        self._last_ohlcv_rate_limited = False
        cfg = dict(config or {})
        token_mint = str(cfg.get("token_mint", "") or "")
        lookback = max(5, int(cfg.get("breakout_lookback_bars", 20) or 20))
        min_volume_multiple = max(0.1, float(cfg.get("min_volume_multiple", 1.8) or 1.8))
        require_retest = bool(cfg.get("require_retest", False))
        stop_atr_mult = max(0.1, float(cfg.get("stop_atr_mult", 1.2) or 1.2))
        tp1_r = max(0.1, float(cfg.get("tp1_r", 1.0) or 1.0))
        tp2_r = max(tp1_r, float(cfg.get("tp2_r", 2.0) or 2.0))
        runner_trail_atr_mult = max(0.1, float(cfg.get("runner_trail_atr_mult", 1.5) or 1.5))

        one_min = await self._fetch_ohlcv(pool_address, timeframe="minute", aggregate=1, limit=120)
        resolved_pool_address = ""
        if (not one_min) and token_mint:
            try:
                resolved_pool_address = await self._resolve_pool_address_from_token(token_mint)
            except Exception:
                resolved_pool_address = ""
            if resolved_pool_address:
                one_min = await self._fetch_ohlcv(resolved_pool_address, timeframe="minute", aggregate=1, limit=120)

        # Support fresh launch pools: Chart Pro should still operate with short history.
        min_one_min_bars = 8
        if len(one_min) < min_one_min_bars:
            reason = "candles_rate_limited" if self._last_ohlcv_rate_limited else "insufficient_candles"
            reason_code = "RATE_LIMITED" if reason == "candles_rate_limited" else "INSUFFICIENT_CANDLES"
            return {"score_adjustment": 0, "reason": reason, "reason_code": reason_code}

        # Reverse to oldest -> newest for indicator math.
        one_min = list(reversed(one_min))
        five_min = self._aggregate_ohlcv(one_min, aggregate=5)
        min_five_min_bars = 2
        if len(five_min) < min_five_min_bars:
            reason = "candles_rate_limited" if self._last_ohlcv_rate_limited else "insufficient_candles"
            reason_code = "RATE_LIMITED" if reason == "candles_rate_limited" else "INSUFFICIENT_CANDLES"
            return {"score_adjustment": 0, "reason": reason, "reason_code": reason_code}

        closes_1m = [float(c[4]) for c in one_min]
        highs_1m = [float(c[2]) for c in one_min]
        lows_1m = [float(c[3]) for c in one_min]
        vols_1m = [float(c[5]) for c in one_min]

        closes_5m = [float(c[4]) for c in five_min]

        ema9_1m = self._ema_tail(closes_1m, 9)
        ema21_1m = self._ema_tail(closes_1m, 21)
        ema50_1m = self._ema_tail(closes_1m, 50)
        ema9_5m = self._ema_tail(closes_5m, 9)
        ema21_5m = self._ema_tail(closes_5m, 21)
        ema50_5m = self._ema_tail(closes_5m, 50)

        last_close = closes_1m[-1]
        trend_1m = ema9_1m > ema21_1m and last_close > ema9_1m
        trend_5m = ema9_5m > ema21_5m and closes_5m[-1] > ema9_5m

        if len(highs_1m) < 2 or len(lows_1m) < 2:
            return {"score_adjustment": 0, "reason": "insufficient_candles", "reason_code": "INSUFFICIENT_CANDLES"}
        effective_lookback = min(max(5, lookback), max(5, len(highs_1m) - 1))
        prior_slice = slice(-(effective_lookback + 1), -1) if len(highs_1m) > effective_lookback else slice(0, -1)
        prior_high = max(highs_1m[prior_slice]) if highs_1m[prior_slice] else max(highs_1m[:-1])
        prior_low = min(lows_1m[prior_slice]) if lows_1m[prior_slice] else min(lows_1m[:-1])
        breakout = last_close > prior_high

        vol_window = min(20, max(1, len(vols_1m) - 1))
        avg_vol_n = (sum(vols_1m[-(vol_window + 1):-1]) / float(vol_window)) if len(vols_1m) > 1 else 0.0
        volume_multiple = (vols_1m[-1] / avg_vol_n) if avg_vol_n > 0 else 0.0
        volume_spike = volume_multiple >= min_volume_multiple

        atr_abs = self._atr(one_min, period=14)
        atr_pct = self._atr_pct(one_min, period=14)
        overheated = atr_pct > 12.0

        bias_bullish = ema9_5m > ema21_5m and closes_5m[-1] > ema9_5m
        bias_bearish = ema9_5m < ema21_5m and closes_5m[-1] < ema9_5m
        trend_bias_5m = "bullish" if bias_bullish else ("bearish" if bias_bearish else "neutral")
        chop_score = abs(ema9_5m - ema21_5m) / max(closes_5m[-1], 1e-12)
        volatility_state = "high" if atr_pct >= 12.0 else ("normal" if atr_pct >= 5.0 else "low")
        # Relax chop gate slightly when 5m history is short.
        chop_threshold = 0.003 if len(closes_5m) >= 20 else 0.0015
        chop_ok = chop_score >= chop_threshold

        momentum_extension_ok = True
        if atr_abs > 0:
            momentum_extension_ok = (last_close - ema9_1m) <= (atr_abs * 2.5)

        retest_ok = True
        if require_retest:
            retest_ok = lows_1m[-1] <= prior_high and last_close > prior_high

        entry_ready = bool(bias_bullish and chop_ok and breakout and volume_spike and retest_ok and momentum_extension_ok and not overheated)

        stop_price = 0.0
        if atr_abs > 0:
            stop_price = max(prior_low, last_close - (atr_abs * stop_atr_mult))
        risk_per_unit = max(0.0, last_close - stop_price) if stop_price > 0 else 0.0
        tp1_price = last_close + (risk_per_unit * tp1_r) if risk_per_unit > 0 else 0.0
        tp2_price = last_close + (risk_per_unit * tp2_r) if risk_per_unit > 0 else 0.0

        score_adjustment = 0
        reasons = []

        if trend_1m and trend_5m:
            score_adjustment += 8
            reasons.append("trend_aligned")
        elif trend_1m or trend_5m:
            score_adjustment += 3
            reasons.append("trend_partial")
        else:
            score_adjustment -= 6
            reasons.append("trend_weak")

        if breakout and volume_spike:
            score_adjustment += 8
            reasons.append("breakout_volume")
        elif breakout:
            score_adjustment += 3
            reasons.append("breakout")

        if overheated:
            score_adjustment -= 6
            reasons.append("high_atr_risk")

        quality_score = 30.0 + float(score_adjustment)
        if entry_ready:
            quality_score += 20.0
        if not chop_ok:
            quality_score -= 10.0
        quality_score = max(0.0, min(100.0, quality_score))
        reason = ""
        reason_code = "OK"
        if not entry_ready:
            if quality_score < 45.0:
                reason = "low_score"
                reason_code = "LOW_SCORE"
            else:
                reason = "no_trigger"
                reason_code = "NO_TRIGGER"

        return {
            "score_adjustment": score_adjustment,
            "reason": reason,
            "reason_code": reason_code,
            "signals": {
                "trend_1m": trend_1m,
                "trend_5m": trend_5m,
                "breakout": breakout,
                "volume_spike": volume_spike,
                "atr_pct": round(atr_pct, 2),
                "ema9_1m": ema9_1m,
                "ema21_1m": ema21_1m,
                "ema50_1m": ema50_1m,
                "ema9_5m": ema9_5m,
                "ema21_5m": ema21_5m,
                "ema50_5m": ema50_5m,
            },
            "reasons": reasons,
            "regime": {
                "trend_bias_5m": trend_bias_5m,
                "volatility_state": volatility_state,
                "chop_score": round(chop_score, 6),
            },
            "setup": {
                "breakout_level": prior_high,
                "range_high": prior_high,
                "range_low": prior_low,
            },
            "trigger": {
                "entry_ready": entry_ready,
                "entry_price": last_close,
                "trigger_bar_ts": int(one_min[-1][0]) if one_min else 0,
            },
            "risk": {
                "atr_pct": round(atr_pct, 2),
                "atr_abs": atr_abs,
                "stop_price": stop_price,
                "risk_per_unit": risk_per_unit,
                "tp1_price": tp1_price,
                "tp2_price": tp2_price,
                "runner_trail_atr_mult": runner_trail_atr_mult,
            },
            "quality": {
                "score": quality_score,
                "reasons": reasons,
                "strictness_profile": "balanced",
            },
            "debug": {
                "volume_multiple": volume_multiple,
                "min_volume_multiple": min_volume_multiple,
                "require_retest": require_retest,
                "retest_ok": retest_ok,
                "momentum_extension_ok": momentum_extension_ok,
                "lookback_bars": lookback,
                "effective_lookback_bars": effective_lookback,
                "bars_1m": len(one_min),
                "bars_5m": len(five_min),
                "chop_threshold": chop_threshold,
                "resolved_pool_address": resolved_pool_address,
                "derived_5m_from_1m": True,
            },
        }
