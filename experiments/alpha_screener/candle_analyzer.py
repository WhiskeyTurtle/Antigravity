import aiohttp
from typing import List


class CandleAnalyzer:
    """Computes candle-derived signals from GeckoTerminal OHLCV data."""

    def __init__(self):
        self.base = "https://api.geckoterminal.com/api/v2"

    async def _fetch_ohlcv(self, pool_address: str, timeframe: str = "minute", aggregate: int = 1, limit: int = 120):
        url = (
            f"{self.base}/networks/solana/pools/{pool_address}/ohlcv/{timeframe}"
            f"?aggregate={aggregate}&limit={limit}"
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=20) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])

    def _ema(self, values: List[float], period: int) -> float:
        if not values:
            return 0.0
        k = 2 / (period + 1)
        ema = values[0]
        for v in values[1:]:
            ema = (v * k) + (ema * (1 - k))
        return ema

    def _atr_pct(self, ohlcv: List[list], period: int = 14) -> float:
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
        atr = sum(trs) / len(trs)
        last_close = float(ohlcv[-1][4]) if float(ohlcv[-1][4]) != 0 else 1.0
        return (atr / last_close) * 100.0

    async def analyze(self, pool_address: str) -> dict:
        if not pool_address:
            return {"score_adjustment": 0, "reason": "no_pool"}

        one_min = await self._fetch_ohlcv(pool_address, timeframe="minute", aggregate=1, limit=120)
        five_min = await self._fetch_ohlcv(pool_address, timeframe="minute", aggregate=5, limit=120)
        if len(one_min) < 30 or len(five_min) < 30:
            return {"score_adjustment": 0, "reason": "insufficient_candles"}

        # Reverse to oldest -> newest for indicator math.
        one_min = list(reversed(one_min))
        five_min = list(reversed(five_min))

        closes_1m = [float(c[4]) for c in one_min]
        highs_1m = [float(c[2]) for c in one_min]
        vols_1m = [float(c[5]) for c in one_min]

        closes_5m = [float(c[4]) for c in five_min]

        ema9_1m = self._ema(closes_1m[-40:], 9)
        ema21_1m = self._ema(closes_1m[-60:], 21)
        ema9_5m = self._ema(closes_5m[-40:], 9)
        ema21_5m = self._ema(closes_5m[-60:], 21)

        last_close = closes_1m[-1]
        trend_1m = ema9_1m > ema21_1m and last_close > ema9_1m
        trend_5m = ema9_5m > ema21_5m and closes_5m[-1] > ema9_5m

        prior20_high = max(highs_1m[-21:-1])
        breakout = last_close > prior20_high

        avg_vol_20 = sum(vols_1m[-21:-1]) / 20 if len(vols_1m) >= 21 else 0
        volume_spike = avg_vol_20 > 0 and vols_1m[-1] >= 1.8 * avg_vol_20

        atr_pct = self._atr_pct(one_min, period=14)
        overheated = atr_pct > 12.0

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

        return {
            "score_adjustment": score_adjustment,
            "signals": {
                "trend_1m": trend_1m,
                "trend_5m": trend_5m,
                "breakout": breakout,
                "volume_spike": volume_spike,
                "atr_pct": round(atr_pct, 2),
            },
            "reasons": reasons,
        }
