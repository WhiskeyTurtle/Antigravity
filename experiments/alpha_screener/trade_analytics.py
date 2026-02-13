import csv
import json
import os
import time
from typing import Dict, List

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYTICS_FILE = os.path.join(BASE_DIR, "trade_analytics.json")
CSV_EXPORT_FILE = os.path.join(BASE_DIR, "trade_analytics_export.csv")


class TradeAnalyticsStore:
    def __init__(self, analytics_file: str = ANALYTICS_FILE):
        self.analytics_file = analytics_file
        self._data = self._load()

    def _default(self) -> dict:
        return {
            "schema_version": 1,
            "updated_at": time.time(),
            "trades": [],
        }

    def _load(self) -> dict:
        if not os.path.exists(self.analytics_file):
            return self._default()
        try:
            with open(self.analytics_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return self._default()
            data.setdefault("schema_version", 1)
            data.setdefault("updated_at", time.time())
            data.setdefault("trades", [])
            return data
        except Exception:
            return self._default()

    def _save(self):
        self._data["updated_at"] = time.time()
        with open(self.analytics_file, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)

    def record_trade_analytics(self, closed_trade: dict, autosave: bool = True) -> bool:
        token = closed_trade.get("token", "")
        close_time = float(closed_trade.get("close_time", 0) or 0)
        trade_type = str(closed_trade.get("trade_type", "PAPER") or "PAPER")
        for t in self._data["trades"]:
            if (
                t.get("token") == token
                and str(t.get("trade_type", "PAPER") or "PAPER") == trade_type
                and float(t.get("close_time", 0) or 0) == close_time
            ):
                return False

        trade = {
            "trade_type": trade_type,
            "strategy_mode": closed_trade.get("strategy_mode", ""),
            "token": token,
            "entry_price": closed_trade.get("entry_price", 0.0),
            "exit_price": closed_trade.get("sell_price", 0.0),
            "pnl": closed_trade.get("pnl", 0.0),
            "pnl_pct": closed_trade.get("pnl_pct", 0.0),
            "close_reason": closed_trade.get("reason", "UNKNOWN"),
            "close_time": close_time or time.time(),
            "duration": closed_trade.get("duration", 0.0),
            "entry_sol_amount": closed_trade.get("entry_sol_amount", 0.0),
            "pair_address": closed_trade.get("pair_address", ""),
            "kol_signals": closed_trade.get("kol_signals", []),
            "ath_during_trade": closed_trade.get("ath_during_trade", {}),
            "token_age_seconds": closed_trade.get("token_age_seconds", 0),
            "creator_twitter": closed_trade.get("creator_twitter", {}),
            "token_mentions": closed_trade.get("token_mentions", {}),
            "holder_intel": closed_trade.get("holder_intel", {}),
            "candle_signals": closed_trade.get("candle_signals", {}),
            "ath_post_close_72h": closed_trade.get("ath_post_close_72h", {}),
        }

        self._data["trades"].append(trade)
        if autosave:
            self._save()
        return True

    def record_trade_analytics_many(self, closed_trades: List[dict]) -> int:
        """Bulk ingest trades and save once to avoid repeated full-file rewrites."""
        added = 0
        for trade in list(closed_trades or []):
            try:
                if self.record_trade_analytics(trade, autosave=False):
                    added += 1
            except Exception:
                continue
        if added > 0:
            self._save()
        return added

    def update_trade_post_close_ath(
        self,
        token: str,
        close_time: float,
        ath_post_close_72h: dict,
        autosave: bool = True,
    ) -> bool:
        for trade in self._data["trades"]:
            if trade.get("token") == token and float(trade.get("close_time", 0)) == float(close_time):
                trade["ath_post_close_72h"] = ath_post_close_72h
                if autosave:
                    self._save()
                return True
        return False

    def save(self):
        self._save()

    def get_trades(self) -> List[dict]:
        return self._data.get("trades", [])

    def purge_trade_type(self, trade_type: str) -> int:
        ttype = str(trade_type or "").strip().upper()
        if not ttype:
            return 0
        trades = list(self._data.get("trades", []))
        kept = [t for t in trades if str(t.get("trade_type", "PAPER") or "PAPER").strip().upper() != ttype]
        removed = len(trades) - len(kept)
        if removed > 0:
            self._data["trades"] = kept
            self._save()
        return removed

    def export_analytics_csv(self, output_file: str = CSV_EXPORT_FILE) -> str:
        trades = self.get_trades()
        fieldnames = [
            "trade_type",
            "strategy_mode",
            "token",
            "entry_price",
            "exit_price",
            "pnl",
            "pnl_pct",
            "close_reason",
            "close_time",
            "duration",
            "entry_sol_amount",
            "pair_address",
            "kol_count",
            "creator_username",
            "creator_followers",
            "token_mentions_24h",
            "ath_during_price",
            "ath_during_pnl_pct",
            "ath_post_close_price",
            "ath_post_close_potential_pnl_pct",
            "ath_post_close_monitoring_complete",
        ]

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for t in trades:
                ath_during = t.get("ath_during_trade", {})
                ath_post = t.get("ath_post_close_72h", {})
                creator = t.get("creator_twitter", {})
                mentions = t.get("token_mentions", {})
                writer.writerow(
                    {
                        "token": t.get("token", ""),
                        "trade_type": t.get("trade_type", "PAPER"),
                        "strategy_mode": t.get("strategy_mode", ""),
                        "entry_price": t.get("entry_price", 0.0),
                        "exit_price": t.get("exit_price", 0.0),
                        "pnl": t.get("pnl", 0.0),
                        "pnl_pct": t.get("pnl_pct", 0.0),
                        "close_reason": t.get("close_reason", "UNKNOWN"),
                        "close_time": t.get("close_time", 0),
                        "duration": t.get("duration", 0),
                        "entry_sol_amount": t.get("entry_sol_amount", 0.0),
                        "pair_address": t.get("pair_address", ""),
                        "kol_count": len(t.get("kol_signals", [])),
                        "creator_username": creator.get("username", ""),
                        "creator_followers": creator.get("followers", 0),
                        "token_mentions_24h": mentions.get("count", 0),
                        "ath_during_price": ath_during.get("price_sol", 0.0),
                        "ath_during_pnl_pct": ath_during.get("pnl_pct_at_ath", 0.0),
                        "ath_post_close_price": ath_post.get("price_sol", 0.0),
                        "ath_post_close_potential_pnl_pct": ath_post.get("potential_pnl_pct", 0.0),
                        "ath_post_close_monitoring_complete": ath_post.get("monitoring_complete", False),
                    }
                )

        return output_file

    def get_kol_performance_summary(self) -> Dict[str, dict]:
        summary: Dict[str, dict] = {}
        for trade in self.get_trades():
            seen_wallets = set()
            for signal in trade.get("kol_signals", []):
                wallet = signal.get("wallet")
                if not wallet or wallet in seen_wallets:
                    continue
                seen_wallets.add(wallet)
                label = signal.get("label") or wallet

                row = summary.setdefault(
                    wallet,
                    {
                        "label": label,
                        "trades": 0,
                        "wins": 0,
                        "total_pnl": 0.0,
                        "avg_pnl_pct": 0.0,
                    },
                )
                row["trades"] += 1
                row["wins"] += 1 if float(trade.get("pnl", 0.0)) >= 0 else 0
                row["total_pnl"] += float(trade.get("pnl", 0.0))
                row["avg_pnl_pct"] += float(trade.get("pnl_pct", 0.0))

        for row in summary.values():
            trades = max(1, row["trades"])
            row["win_rate"] = (row["wins"] / trades) * 100
            row["avg_pnl_pct"] = row["avg_pnl_pct"] / trades
        return summary

    def get_missed_opportunity_report(self, min_gap_pct: float = 20.0) -> List[dict]:
        report = []
        for trade in self.get_trades():
            realized = float(trade.get("pnl_pct", 0.0))
            post = float(trade.get("ath_post_close_72h", {}).get("potential_pnl_pct", realized))
            gap = post - realized
            if gap >= min_gap_pct:
                report.append(
                    {
                        "token": trade.get("token", ""),
                        "close_time": trade.get("close_time", 0),
                        "realized_pnl_pct": realized,
                        "potential_pnl_pct": post,
                        "missed_pct_points": gap,
                    }
                )
        report.sort(key=lambda x: x["missed_pct_points"], reverse=True)
        return report

    def get_overview(self) -> dict:
        trades = self.get_trades()
        total_trades = len(trades)
        wins = sum(1 for t in trades if float(t.get("pnl", 0.0)) >= 0)
        total_pnl = sum(float(t.get("pnl", 0.0)) for t in trades)
        avg_pnl_pct = (
            sum(float(t.get("pnl_pct", 0.0)) for t in trades) / total_trades
            if total_trades > 0
            else 0.0
        )

        biggest_win = None
        biggest_loss = None
        if trades:
            biggest_win = max(trades, key=lambda t: float(t.get("pnl", 0.0)))
            biggest_loss = min(trades, key=lambda t: float(t.get("pnl", 0.0)))

        missed = self.get_missed_opportunity_report(min_gap_pct=20.0)
        kol_summary = self.get_kol_performance_summary()

        monitored = 0
        monitoring_complete = 0
        for t in trades:
            aph = t.get("ath_post_close_72h", {})
            if aph:
                monitored += 1
                if aph.get("monitoring_complete", False):
                    monitoring_complete += 1

        return {
            "trades": {
                "total": total_trades,
                "wins": wins,
                "losses": total_trades - wins,
                "win_rate": (wins / total_trades * 100) if total_trades > 0 else 0.0,
            },
            "pnl": {
                "total_sol": total_pnl,
                "avg_pct": avg_pnl_pct,
            },
            "extremes": {
                "biggest_win": {
                    "token": biggest_win.get("token", ""),
                    "pnl": float(biggest_win.get("pnl", 0.0)),
                    "pnl_pct": float(biggest_win.get("pnl_pct", 0.0)),
                    "close_time": biggest_win.get("close_time", 0),
                } if biggest_win else None,
                "biggest_loss": {
                    "token": biggest_loss.get("token", ""),
                    "pnl": float(biggest_loss.get("pnl", 0.0)),
                    "pnl_pct": float(biggest_loss.get("pnl_pct", 0.0)),
                    "close_time": biggest_loss.get("close_time", 0),
                } if biggest_loss else None,
            },
            "kol": {
                "tracked_wallets": len(kol_summary),
                "top_by_pnl": sorted(
                    [
                        {"wallet": w, **v}
                        for w, v in kol_summary.items()
                    ],
                    key=lambda x: float(x.get("total_pnl", 0.0)),
                    reverse=True,
                )[:5],
            },
            "missed_opportunities": {
                "count_ge_20pct_gap": len(missed),
                "top": missed[:5],
            },
            "post_close_monitoring": {
                "tracked": monitored,
                "completed": monitoring_complete,
                "pending": max(0, monitored - monitoring_complete),
            },
            "updated_at": self._data.get("updated_at", 0),
        }
