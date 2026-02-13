import asyncio
import json
import os
import time
from typing import Dict, List, Optional

from realy_engine import RealyEngine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "paper_trade_state.json")
POST_CLOSE_MONITOR_WINDOW_SEC = 72 * 3600
TRADING_MODE_LAUNCH_BURST = "kol_launch_burst_scalper"
TRADING_MODE_CHART_PRO = "chart_pro_breakout_scalper"


def _legacy_state_candidates() -> List[str]:
    """Legacy locations used when app startup cwd varied."""
    candidates: List[str] = []
    cwd_state = os.path.join(os.getcwd(), "paper_trade_state.json")
    if cwd_state != STATE_FILE:
        candidates.append(cwd_state)
    candidates.append(os.path.join(os.path.dirname(BASE_DIR), "paper_trade_state.json"))
    candidates.append(os.path.join(os.path.dirname(os.path.dirname(BASE_DIR)), "paper_trade_state.json"))
    return candidates


class PaperTrader:
    def __init__(self):
        self.engine = RealyEngine()
        self.positions: Dict[str, dict] = {}
        self.closed_positions: List[dict] = []
        self.total_pnl_sol = 0.0

        # Risk Settings
        self.take_profit_pct = 0.50  # +50%
        self.stop_loss_pct = -0.20   # -20%
        self.time_stop_sec = 1800    # 30 mins (kept for UI status only)

        self.load_state()

    def _normalize_position(self, pos: dict) -> dict:
        entry_price = float(pos.get("entry_price", 0.0) or 0.0)
        amount = float(pos.get("amount", 0.0) or 0.0)
        pos.setdefault("entry_sol_amount", entry_price * amount)
        pos.setdefault("last_price", entry_price)
        pos.setdefault("last_fdv", float(pos.get("entry_fdv", 0.0) or 0.0))
        pos.setdefault("strategy_mode", "")
        pos.setdefault("strategy_meta", {})
        return pos

    def _normalize_closed_trade(self, trade: dict) -> dict:
        trade.setdefault("entry_price", 0.0)
        trade.setdefault("sell_price", trade.get("exit_price", 0.0))
        trade.setdefault("entry_sol_amount", 0.0)
        trade.setdefault("pair_address", "")
        trade.setdefault("close_time", time.time())
        trade.setdefault("reason", trade.get("close_reason", "UNKNOWN"))
        trade.setdefault("pnl", 0.0)
        trade.setdefault("pnl_pct", 0.0)
        trade.setdefault("sell_fraction", 1.0)
        trade.setdefault("is_partial", False)
        trade.setdefault("strategy_mode", "")
        trade.setdefault("kol_signals", [])
        trade.setdefault("ath_during_trade", {})

        if "ath_post_close_72h" not in trade:
            trade["ath_post_close_72h"] = {
                "price_sol": trade.get("sell_price", 0.0),
                "fdv": trade.get("sell_fdv", 0.0),
                "timestamp": trade.get("close_time", time.time()),
                "potential_pnl_pct": trade.get("pnl_pct", 0.0),
                "monitoring_complete": False,
            }
        else:
            aph = trade["ath_post_close_72h"]
            aph.setdefault("price_sol", trade.get("sell_price", 0.0))
            aph.setdefault("fdv", trade.get("sell_fdv", 0.0))
            aph.setdefault("timestamp", trade.get("close_time", time.time()))
            aph.setdefault("potential_pnl_pct", trade.get("pnl_pct", 0.0))
            aph.setdefault("monitoring_complete", False)

        return trade

    def load_state(self):
        """Loads trading state from disk."""
        state_path = None
        if os.path.exists(STATE_FILE):
            state_path = STATE_FILE
        else:
            legacy_paths = [p for p in _legacy_state_candidates() if os.path.exists(p)]
            if legacy_paths:
                state_path = max(legacy_paths, key=os.path.getmtime)

        if not state_path:
            return

        try:
            with open(state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.positions = {
                    k: self._normalize_position(v)
                    for k, v in data.get("positions", {}).items()
                    if isinstance(v, dict)
                }
                self.closed_positions = [
                    self._normalize_closed_trade(t)
                    for t in data.get("closed_positions", [])
                    if not str(t.get("token", "")).startswith("token_")
                ]
                self.total_pnl_sol = sum(float(t.get("pnl", 0.0)) for t in self.closed_positions)
                print(
                    f"Loaded state from {state_path}: "
                    f"{len(self.positions)} active, {len(self.closed_positions)} closed"
                )
                if state_path != STATE_FILE and not os.path.exists(STATE_FILE):
                    self.save_state()
        except Exception as e:
            print(f"Failed to load state: {e}")

    def save_state(self):
        """Saves trading state to disk."""
        try:
            state = {
                "positions": self.positions,
                "closed_positions": self.closed_positions,
                "total_pnl_sol": self.total_pnl_sol,
            }
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"Failed to save state: {e}")

    def _find_signature_by_token(self, token_mint: str) -> Optional[str]:
        for sig, pos in self.positions.items():
            if pos.get("token_mint") == token_mint:
                return sig
        return None

    def count_distinct_kols_for_token(self, token_mint: str) -> int:
        sig = self._find_signature_by_token(token_mint)
        if not sig:
            return 0
        wallets = {
            s.get("wallet")
            for s in self.positions[sig].get("kol_signals", [])
            if s.get("wallet")
        }
        return len(wallets)

    def add_kol_signal(
        self,
        signature: Optional[str] = None,
        token_mint: Optional[str] = None,
        kol_wallet: str = "",
        kol_label: str = "",
    ) -> bool:
        """Adds KOL attribution to an existing position. Returns True when added."""
        if not signature and token_mint:
            signature = self._find_signature_by_token(token_mint)
        if not signature or signature not in self.positions:
            return False

        position = self.positions[signature]
        wallet = kol_wallet or ""
        if not wallet:
            return False

        for signal in position.get("kol_signals", []):
            if signal.get("wallet") == wallet:
                return False

        position.setdefault("kol_signals", []).append(
            {
                "wallet": wallet,
                "label": kol_label or "Unknown",
                "timestamp": time.time(),
            }
        )
        self.save_state()
        print(f"Added KOL signal from {kol_label or 'Unknown'} to {position.get('token_mint', '')[:8]}")
        return True

    async def open_position(
        self,
        signature: str,
        token_mint: str,
        entry_price: float,
        amount_tokens: float,
        entry_fdv: float = 0,
        pair_address: str = "",
        kol_data: dict = None,
        cooldown_sec: int = 900,
        strategy_mode: str = "",
        strategy_meta: dict = None,
    ):
        """Opens a new paper position."""
        if signature in self.positions:
            return {"opened": False, "reason": "duplicate_signature", "signature": signature}

        # Check duplicate active position by mint.
        for existing_sig, pos in self.positions.items():
            if pos.get("token_mint") == token_mint:
                if kol_data:
                    self.add_kol_signal(
                        signature=existing_sig,
                        kol_wallet=kol_data.get("wallet", ""),
                        kol_label=kol_data.get("label", ""),
                    )
                return {"opened": False, "reason": "token_already_active", "signature": existing_sig}

        # Cooldown: prevent re-buying a recently closed token.
        cooldown_sec = max(0, int(cooldown_sec or 0))
        current_time = time.time()
        for trade in self.closed_positions:
            if trade.get("token") == token_mint:
                close_time = float(trade.get("close_time", 0))
                if current_time - close_time < cooldown_sec:
                    return {"opened": False, "reason": "cooldown", "signature": None}

        print(f"PAPER BUY: {token_mint[:8]} @ {entry_price:.9f} SOL (MCap: ${entry_fdv:,.0f})")

        kol_signals: List[dict] = []
        if kol_data and kol_data.get("wallet"):
            kol_signals.append(
                {
                    "wallet": kol_data.get("wallet", ""),
                    "label": kol_data.get("label", ""),
                    "timestamp": current_time,
                }
            )

        pos = {
            "token_mint": token_mint,
            "entry_price": entry_price,
            "entry_fdv": entry_fdv,
            "entry_sol_amount": entry_price * amount_tokens,
            "amount": amount_tokens,
            "start_time": current_time,
            "last_price": entry_price,
            "last_fdv": entry_fdv,
            "pair_address": pair_address,
            "kol_signals": kol_signals,
            "ath_during_trade": {
                "price_sol": entry_price,
                "fdv": entry_fdv,
                "timestamp": current_time,
                "pnl_pct_at_ath": 0.0,
            },
            "token_age_seconds": 0,
            "creator_twitter": {},
            "token_mentions": {},
            "holder_intel": {},
            "candle_signals": {},
            "take_profit_pct": self.take_profit_pct,
            "stop_loss_pct": self.stop_loss_pct,
            "max_hold_sec": self.time_stop_sec,
            "strategy_mode": strategy_mode or "",
            "strategy_meta": dict(strategy_meta or {}),
        }
        if strategy_mode == TRADING_MODE_LAUNCH_BURST:
            pos["strategy_meta"].setdefault("burst_tp1_hit", False)
            pos["strategy_meta"].setdefault("burst_tp2_hit", False)
            pos["strategy_meta"].setdefault("burst_runner_peak_price", entry_price)
            pos["strategy_meta"].setdefault("burst_peak_price", entry_price)
            pos["strategy_meta"].setdefault("burst_red_streak", 0)
            pos["strategy_meta"].setdefault("burst_breakout_price", entry_price)
            pos["strategy_meta"].setdefault("burst_addon_done", False)
            pos["strategy_meta"].setdefault("burst_strong_red_pct", 0.02)
        elif strategy_mode == TRADING_MODE_CHART_PRO:
            meta = pos["strategy_meta"]
            meta.setdefault("chart_entry_price", entry_price)
            meta.setdefault("chart_stop_price", 0.0)
            meta.setdefault("chart_risk_per_unit", 0.0)
            meta.setdefault("chart_risk_pct", 0.0)
            meta.setdefault("chart_tp1_price", 0.0)
            meta.setdefault("chart_tp2_price", 0.0)
            meta.setdefault("chart_tp1_r", 1.0)
            meta.setdefault("chart_tp2_r", 2.0)
            meta.setdefault("chart_runner_peak", entry_price)
            meta.setdefault("chart_tp1_hit", False)
            meta.setdefault("chart_tp2_hit", False)
            meta.setdefault("chart_setup_level", 0.0)
            meta.setdefault("chart_signal_version", "v1")
            meta.setdefault("chart_runner_trail_atr_mult", 1.5)
            meta.setdefault("chart_atr_abs", 0.0)

        self.positions[signature] = pos
        self.save_state()
        return {"opened": True, "reason": "opened", "signature": signature}

    def set_exit_profile(
        self,
        signature: str,
        take_profit_pct: float = None,
        stop_loss_pct: float = None,
        max_hold_sec: int = None,
    ) -> bool:
        if signature not in self.positions:
            return False
        pos = self.positions[signature]
        if take_profit_pct is not None:
            pos["take_profit_pct"] = float(take_profit_pct)
        if stop_loss_pct is not None:
            pos["stop_loss_pct"] = float(stop_loss_pct)
        if max_hold_sec is not None:
            pos["max_hold_sec"] = int(max_hold_sec)
        self.save_state()
        return True

    def set_strategy_meta(self, signature: str, updates: dict) -> bool:
        if signature not in self.positions:
            return False
        pos = self.positions[signature]
        meta = pos.setdefault("strategy_meta", {})
        meta.update(dict(updates or {}))
        self.save_state()
        return True

    def scale_in_position(
        self,
        signature: str,
        add_amount_tokens: float,
        add_price: float,
        add_fdv: float = 0.0,
    ) -> bool:
        if signature not in self.positions:
            return False
        add_amt = float(add_amount_tokens or 0.0)
        add_px = float(add_price or 0.0)
        if add_amt <= 0 or add_px <= 0:
            return False

        pos = self.positions[signature]
        old_amt = float(pos.get("amount", 0.0) or 0.0)
        old_sol = float(pos.get("entry_sol_amount", old_amt * float(pos.get("entry_price", 0.0) or 0.0)) or 0.0)
        new_amt = old_amt + add_amt
        new_sol = old_sol + (add_amt * add_px)

        if new_amt <= 0:
            return False

        pos["amount"] = new_amt
        pos["entry_sol_amount"] = new_sol
        pos["entry_price"] = new_sol / new_amt

        old_entry_fdv = float(pos.get("entry_fdv", 0.0) or 0.0)
        new_entry_fdv = float(add_fdv or 0.0)
        if new_entry_fdv > 0:
            pos["entry_fdv"] = ((old_entry_fdv * old_amt) + (new_entry_fdv * add_amt)) / new_amt if old_amt > 0 else new_entry_fdv
        pos["last_price"] = add_px
        if new_entry_fdv > 0:
            pos["last_fdv"] = new_entry_fdv

        self.save_state()
        return True

    def update_analytics_data(
        self,
        signature,
        token_age=None,
        twitter_data=None,
        mentions=None,
        holder_intel=None,
        candle_signals=None,
    ):
        """Updates async analytics data for a position."""
        if signature in self.positions:
            pos = self.positions[signature]
            if token_age is not None:
                pos["token_age_seconds"] = token_age
            if twitter_data:
                pos["creator_twitter"] = twitter_data
            if mentions:
                pos["token_mentions"] = mentions
            if holder_intel:
                pos["holder_intel"] = holder_intel
            if candle_signals:
                pos["candle_signals"] = candle_signals
            self.save_state()

    def update_ath(self, signature: str, current_price: float, current_fdv: float):
        if signature not in self.positions:
            return
        pos = self.positions[signature]
        ath = pos.setdefault(
            "ath_during_trade",
            {
                "price_sol": pos.get("entry_price", 0.0),
                "fdv": pos.get("entry_fdv", 0.0),
                "timestamp": pos.get("start_time", time.time()),
                "pnl_pct_at_ath": 0.0,
            },
        )

        if current_price > float(ath.get("price_sol", 0.0)):
            pnl_pct = 0.0
            entry = float(pos.get("entry_price", 0.0) or 0.0)
            if entry > 0:
                pnl_pct = ((current_price - entry) / entry) * 100
            ath["price_sol"] = current_price
            ath["fdv"] = current_fdv
            ath["timestamp"] = time.time()
            ath["pnl_pct_at_ath"] = pnl_pct

    def start_post_close_monitoring(self, closed_trade: dict):
        if "ath_post_close_72h" not in closed_trade:
            closed_trade["ath_post_close_72h"] = {
                "price_sol": closed_trade.get("sell_price", 0.0),
                "fdv": closed_trade.get("sell_fdv", 0.0),
                "timestamp": closed_trade.get("close_time", time.time()),
                "potential_pnl_pct": closed_trade.get("pnl_pct", 0.0),
                "monitoring_complete": False,
            }

    def get_pending_post_close_monitoring(self) -> List[dict]:
        now_ts = time.time()
        pending = []
        changed = False

        for trade in self.closed_positions:
            aph = trade.get("ath_post_close_72h", {})
            close_time = float(trade.get("close_time", 0))
            if close_time <= 0:
                continue

            if now_ts - close_time > POST_CLOSE_MONITOR_WINDOW_SEC:
                if not aph.get("monitoring_complete", False):
                    aph["monitoring_complete"] = True
                    changed = True
                continue

            if not aph.get("monitoring_complete", False):
                pending.append(trade)

        if changed:
            self.save_state()
        return pending

    def update_post_close_ath(self, token_mint: str, price: float, fdv: float):
        now_ts = time.time()
        changed = False

        for trade in self.closed_positions:
            if trade.get("token") != token_mint:
                continue

            close_time = float(trade.get("close_time", 0))
            if close_time <= 0:
                continue

            aph = trade.setdefault(
                "ath_post_close_72h",
                {
                    "price_sol": trade.get("sell_price", 0.0),
                    "fdv": trade.get("sell_fdv", 0.0),
                    "timestamp": close_time,
                    "potential_pnl_pct": trade.get("pnl_pct", 0.0),
                    "monitoring_complete": False,
                },
            )

            if now_ts - close_time > POST_CLOSE_MONITOR_WINDOW_SEC:
                if not aph.get("monitoring_complete", False):
                    aph["monitoring_complete"] = True
                    changed = True
                continue

            old_price = float(aph.get("price_sol", 0.0) or 0.0)
            if price > old_price:
                entry_price = float(trade.get("entry_price", 0.0) or 0.0)
                potential_pct = 0.0
                if entry_price > 0:
                    potential_pct = ((price - entry_price) / entry_price) * 100

                aph["price_sol"] = price
                aph["fdv"] = fdv
                aph["timestamp"] = now_ts
                aph["potential_pnl_pct"] = potential_pct
                changed = True

        if changed:
            self.save_state()

    def _apply_sell(
        self,
        signature: str,
        sell_fraction: float,
        current_price: float,
        current_fdv: float,
        reason: str,
    ):
        if signature not in self.positions:
            return None

        pos = self.positions[signature]
        amount_total = float(pos.get("amount", 0.0) or 0.0)
        if amount_total <= 0:
            return None

        frac = max(0.0, min(1.0, float(sell_fraction or 0.0)))
        if frac <= 0:
            return None

        sell_amount = amount_total * frac
        entry_sol_total = float(pos.get("entry_sol_amount", amount_total * float(pos.get("entry_price", 0.0) or 0.0)) or 0.0)
        cost_basis = entry_sol_total * frac
        sol_received = sell_amount * float(current_price or 0.0)
        pnl_sol = sol_received - cost_basis
        pnl_pct = (pnl_sol / cost_basis * 100.0) if cost_basis > 0 else 0.0
        close_time = time.time()

        trade_data = {
            "token": pos["token_mint"],
            "pnl": pnl_sol,
            "pnl_pct": pnl_pct,
            "entry_sol_amount": cost_basis,
            "reason": reason,
            "entry_time": float(pos.get("start_time", close_time) or close_time),
            "start_time": float(pos.get("start_time", close_time) or close_time),
            "duration": close_time - float(pos.get("start_time", close_time)),
            "close_time": close_time,
            "entry_price": float(pos.get("entry_price", 0.0) or 0.0),
            "sell_price": float(current_price or 0.0),
            "sell_fdv": float(current_fdv or 0.0),
            "pair_address": pos.get("pair_address", ""),
            "kol_signals": pos.get("kol_signals", []),
            "ath_during_trade": pos.get("ath_during_trade", {}),
            "token_age_seconds": pos.get("token_age_seconds", 0),
            "creator_twitter": pos.get("creator_twitter", {}),
            "token_mentions": pos.get("token_mentions", {}),
            "holder_intel": pos.get("holder_intel", {}),
            "candle_signals": pos.get("candle_signals", {}),
            "sold_amount_tokens": sell_amount,
            "sell_fraction": frac,
            "is_partial": frac < 0.999999,
            "signature": signature,
            "strategy_mode": pos.get("strategy_mode", ""),
            "strategy_meta": dict(pos.get("strategy_meta", {}) or {}),
            "source_type": str((pos.get("strategy_meta", {}) or {}).get("source_type", "") or ""),
            "source_label": str((pos.get("strategy_meta", {}) or {}).get("source_label", "") or ""),
            "source_author": str((pos.get("strategy_meta", {}) or {}).get("source_author", "") or ""),
            "source_author_name": str((pos.get("strategy_meta", {}) or {}).get("source_author_name", "") or ""),
            "source_tweet_id": str((pos.get("strategy_meta", {}) or {}).get("source_tweet_id", "") or ""),
        }

        self.closed_positions.append(trade_data)
        self.total_pnl_sol += pnl_sol

        if frac >= 0.999999:
            self.start_post_close_monitoring(trade_data)
            del self.positions[signature]
        else:
            remain_amount = amount_total - sell_amount
            remain_sol = max(0.0, entry_sol_total - cost_basis)
            pos["amount"] = remain_amount
            pos["entry_sol_amount"] = remain_sol
            if remain_amount > 0:
                pos["entry_price"] = remain_sol / remain_amount if remain_sol > 0 else float(pos.get("entry_price", 0.0) or 0.0)
            pos["last_price"] = float(current_price or 0.0)
            pos["last_fdv"] = float(current_fdv or 0.0)

        return trade_data

    async def update_positions(self):
        """Checks all active positions for TP/SL using parallel requests."""
        if not self.positions:
            return []

        closed_this_cycle = []

        sigs = list(self.positions.keys())
        tasks = [self.engine.get_token_price(self.positions[sig]["token_mint"]) for sig in sigs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, sig in enumerate(sigs):
            if sig not in self.positions:
                continue
            pos = self.positions[sig]
            quote = results[i]

            if isinstance(quote, Exception) or not quote:
                continue

            current_price = float(quote.get("price_sol", 0.0) or 0.0)
            current_fdv = float(quote.get("fdv", 0.0) or 0.0)
            if current_price == 0:
                continue

            prev_price = float(pos.get("last_price", pos.get("entry_price", current_price)) or current_price)
            pos["last_price"] = current_price
            pos["last_fdv"] = current_fdv
            self.update_ath(sig, current_price, current_fdv)

            amount_now = float(pos.get("amount", 0.0) or 0.0)
            if amount_now <= 0:
                continue
            entry_sol_amount_now = float(pos.get("entry_sol_amount", pos["entry_price"] * amount_now) or 0.0)
            avg_entry_price = (entry_sol_amount_now / amount_now) if amount_now > 0 else float(pos.get("entry_price", 0.0) or 0.0)
            if avg_entry_price <= 0:
                continue
            price_change_pct = (current_price - avg_entry_price) / avg_entry_price
            tp_pct = float(pos.get("take_profit_pct", self.take_profit_pct))
            sl_pct = float(pos.get("stop_loss_pct", self.stop_loss_pct))
            max_hold_sec = int(pos.get("max_hold_sec", self.time_stop_sec))
            duration_sec = time.time() - pos["start_time"]

            strategy_mode = str(pos.get("strategy_mode", "") or "")
            if strategy_mode == TRADING_MODE_CHART_PRO:
                meta = pos.setdefault("strategy_meta", {})
                chart_stop = float(meta.get("chart_stop_price", 0.0) or 0.0)
                tp1_price = float(meta.get("chart_tp1_price", 0.0) or 0.0)
                tp2_price = float(meta.get("chart_tp2_price", 0.0) or 0.0)
                tp1_r = max(0.1, float(meta.get("chart_tp1_r", 1.0) or 1.0))
                tp2_r = max(tp1_r, float(meta.get("chart_tp2_r", 2.0) or 2.0))
                tp1_hit = bool(meta.get("chart_tp1_hit", False))
                tp2_hit = bool(meta.get("chart_tp2_hit", False))
                runner_peak = float(meta.get("chart_runner_peak", current_price) or current_price)
                runner_peak = max(runner_peak, current_price)
                meta["chart_runner_peak"] = runner_peak
                atr_abs = float(meta.get("chart_atr_abs", 0.0) or 0.0)
                trail_mult = float(meta.get("chart_runner_trail_atr_mult", 1.5) or 1.5)
                trail_abs = atr_abs * trail_mult if atr_abs > 0 else max(current_price * 0.03, 0.0)
                chart_max_hold = int(pos.get("max_hold_sec", self.time_stop_sec) or self.time_stop_sec)
                chart_risk_pct = float(meta.get("chart_risk_pct", 0.0) or 0.0)
                if chart_risk_pct <= 0 and chart_stop > 0 and avg_entry_price > 0 and chart_stop < avg_entry_price:
                    chart_risk_pct = (avg_entry_price - chart_stop) / avg_entry_price
                if chart_risk_pct <= 0:
                    chart_risk_pct = 0.02
                chart_risk_pct = max(0.005, min(0.35, chart_risk_pct))
                meta["chart_risk_pct"] = chart_risk_pct
                if chart_stop <= 0 or chart_stop >= (avg_entry_price * 0.995):
                    chart_stop = avg_entry_price * (1.0 - chart_risk_pct)
                    meta["chart_stop_price"] = chart_stop
                if tp1_price <= avg_entry_price or tp1_price > (avg_entry_price * 2.5):
                    tp1_price = avg_entry_price * (1.0 + (chart_risk_pct * tp1_r))
                    meta["chart_tp1_price"] = tp1_price
                if tp2_price <= tp1_price or tp2_price > (avg_entry_price * 4.0):
                    tp2_price = avg_entry_price * (1.0 + (chart_risk_pct * tp2_r))
                    meta["chart_tp2_price"] = tp2_price

                full_reason = None
                if chart_stop > 0 and current_price <= chart_stop:
                    full_reason = "CHART_STOP_LOSS"
                elif duration_sec >= chart_max_hold:
                    full_reason = "CHART_TIME_EXIT"
                if full_reason:
                    trade_data = self._apply_sell(
                        signature=sig,
                        sell_fraction=1.0,
                        current_price=current_price,
                        current_fdv=current_fdv,
                        reason=full_reason,
                    )
                    if trade_data:
                        closed_this_cycle.append(trade_data)
                    continue

                if (not tp1_hit) and tp1_price > 0 and current_price >= tp1_price:
                    trade_data = self._apply_sell(
                        signature=sig,
                        sell_fraction=0.5,
                        current_price=current_price,
                        current_fdv=current_fdv,
                        reason="CHART_TP1_PARTIAL",
                    )
                    if trade_data:
                        meta["chart_tp1_hit"] = True
                        closed_this_cycle.append(trade_data)
                    if sig not in self.positions:
                        continue
                    pos = self.positions[sig]
                    meta = pos.setdefault("strategy_meta", meta)

                if sig in self.positions:
                    pos = self.positions[sig]
                    meta = pos.setdefault("strategy_meta", meta)
                    tp1_hit = bool(meta.get("chart_tp1_hit", False))
                    tp2_hit = bool(meta.get("chart_tp2_hit", False))
                    if tp1_hit and (not tp2_hit) and tp2_price > 0 and current_price >= tp2_price:
                        trade_data = self._apply_sell(
                            signature=sig,
                            sell_fraction=0.5,
                            current_price=current_price,
                            current_fdv=current_fdv,
                            reason="CHART_TP2_PARTIAL",
                        )
                        if trade_data:
                            meta["chart_tp2_hit"] = True
                            meta["chart_runner_peak"] = current_price
                            closed_this_cycle.append(trade_data)
                        if sig not in self.positions:
                            continue

                if sig in self.positions:
                    pos = self.positions[sig]
                    meta = pos.setdefault("strategy_meta", meta)
                    if bool(meta.get("chart_tp2_hit", False)):
                        runner_peak = float(meta.get("chart_runner_peak", current_price) or current_price)
                        runner_peak = max(runner_peak, current_price)
                        meta["chart_runner_peak"] = runner_peak
                        if trail_abs > 0 and current_price <= max(0.0, runner_peak - trail_abs):
                            trade_data = self._apply_sell(
                                signature=sig,
                                sell_fraction=1.0,
                                current_price=current_price,
                                current_fdv=current_fdv,
                                reason="CHART_TRAILING_STOP",
                            )
                            if trade_data:
                                closed_this_cycle.append(trade_data)
                continue

            if strategy_mode != TRADING_MODE_LAUNCH_BURST:
                reason = None
                if price_change_pct >= tp_pct:
                    reason = "TAKE_PROFIT"
                elif price_change_pct <= sl_pct:
                    reason = "STOP_LOSS"
                elif duration_sec >= max_hold_sec:
                    reason = "TIME_EXIT"

                if reason:
                    trade_data = self._apply_sell(
                        signature=sig,
                        sell_fraction=1.0,
                        current_price=current_price,
                        current_fdv=current_fdv,
                        reason=reason,
                    )
                    if trade_data:
                        closed_this_cycle.append(trade_data)
                continue

            # Launch-burst staged exits.
            meta = pos.setdefault("strategy_meta", {})
            tp1_pct = float(meta.get("burst_tp1_pct", 0.12) or 0.12)
            tp2_pct = float(meta.get("burst_tp2_pct", 0.20) or 0.20)
            trail_pct = float(meta.get("burst_runner_trail_pct", 0.08) or 0.08)
            strong_red_pct = float(meta.get("burst_strong_red_pct", 0.02) or 0.02)

            peak_price = float(meta.get("burst_peak_price", avg_entry_price) or avg_entry_price)
            peak_price = max(peak_price, current_price)
            meta["burst_peak_price"] = peak_price

            red_streak = int(meta.get("burst_red_streak", 0) or 0)
            if peak_price >= avg_entry_price * 1.08:
                if current_price <= prev_price * (1.0 - strong_red_pct):
                    red_streak += 1
                elif current_price >= prev_price:
                    red_streak = 0
            else:
                red_streak = 0
            meta["burst_red_streak"] = red_streak

            full_reason = None
            if price_change_pct <= sl_pct:
                full_reason = "STOP_LOSS"
            elif duration_sec >= max_hold_sec:
                full_reason = "TIME_EXIT"
            elif red_streak >= 2:
                full_reason = "EARLY_FAILURE"

            if full_reason:
                trade_data = self._apply_sell(
                    signature=sig,
                    sell_fraction=1.0,
                    current_price=current_price,
                    current_fdv=current_fdv,
                    reason=full_reason,
                )
                if trade_data:
                    closed_this_cycle.append(trade_data)
                continue

            tp1_hit = bool(meta.get("burst_tp1_hit", False))
            tp2_hit = bool(meta.get("burst_tp2_hit", False))

            if not tp1_hit and price_change_pct >= tp1_pct:
                trade_data = self._apply_sell(
                    signature=sig,
                    sell_fraction=0.5,
                    current_price=current_price,
                    current_fdv=current_fdv,
                    reason="TP1_PARTIAL",
                )
                if trade_data:
                    meta["burst_tp1_hit"] = True
                    closed_this_cycle.append(trade_data)
                if sig not in self.positions:
                    continue
                pos = self.positions[sig]
                meta = pos.setdefault("strategy_meta", meta)
                tp1_hit = True

            if tp1_hit and (not tp2_hit) and sig in self.positions:
                # 60% of remaining leaves ~20% runner after TP1.
                entry_sol_amount_now = float(pos.get("entry_sol_amount", 0.0) or 0.0)
                amount_now = float(pos.get("amount", 0.0) or 0.0)
                avg_entry_price = (entry_sol_amount_now / amount_now) if amount_now > 0 else avg_entry_price
                price_change_pct = (current_price - avg_entry_price) / avg_entry_price if avg_entry_price > 0 else 0.0
                if price_change_pct >= tp2_pct:
                    trade_data = self._apply_sell(
                        signature=sig,
                        sell_fraction=0.6,
                        current_price=current_price,
                        current_fdv=current_fdv,
                        reason="TP2_PARTIAL",
                    )
                    if trade_data:
                        pos = self.positions.get(sig, None)
                        if pos is not None:
                            meta = pos.setdefault("strategy_meta", meta)
                            meta["burst_tp2_hit"] = True
                            meta["burst_runner_peak_price"] = current_price
                        closed_this_cycle.append(trade_data)
                    if sig not in self.positions:
                        continue

            if sig in self.positions:
                pos = self.positions[sig]
                meta = pos.setdefault("strategy_meta", meta)
                if bool(meta.get("burst_tp2_hit", False)):
                    runner_peak = float(meta.get("burst_runner_peak_price", current_price) or current_price)
                    runner_peak = max(runner_peak, current_price)
                    meta["burst_runner_peak_price"] = runner_peak
                    if current_price <= runner_peak * (1.0 - trail_pct):
                        trade_data = self._apply_sell(
                            signature=sig,
                            sell_fraction=1.0,
                            current_price=current_price,
                            current_fdv=current_fdv,
                            reason="TRAILING_STOP",
                        )
                        if trade_data:
                            closed_this_cycle.append(trade_data)

        if closed_this_cycle:
            self.save_state()

        return closed_this_cycle

    async def close_position(self, signature: str, reason: str = "MANUAL_CLOSE"):
        """Manually closes an active position and returns closed trade payload."""
        if signature not in self.positions:
            return None

        pos = self.positions[signature]
        quote = await self.engine.get_token_price(pos["token_mint"])

        current_price = float(quote.get("price_sol", 0.0) or 0.0) if quote else 0.0
        current_fdv = float(quote.get("fdv", pos.get("last_fdv", pos["entry_fdv"])) or 0.0) if quote else float(pos.get("last_fdv", pos["entry_fdv"]) or 0.0)
        if current_price <= 0:
            current_price = float(pos.get("last_price", pos["entry_price"]) or pos["entry_price"])

        amount_now = float(pos.get("amount", 0.0) or 0.0)
        entry_sol_amount = float(pos.get("entry_sol_amount", pos["entry_price"] * amount_now) or 0.0)
        avg_entry = (entry_sol_amount / amount_now) if amount_now > 0 else float(pos["entry_price"])
        pnl_sol = (current_price * amount_now) - entry_sol_amount
        pnl_pct = ((current_price - avg_entry) / avg_entry) * 100 if avg_entry > 0 else 0.0

        trade_data = {
            "token": pos["token_mint"],
            "pnl": pnl_sol,
            "pnl_pct": pnl_pct,
            "entry_sol_amount": entry_sol_amount,
            "reason": reason,
            "duration": time.time() - pos["start_time"],
            "close_time": time.time(),
            "entry_price": pos["entry_price"],
            "sell_price": current_price,
            "sell_fdv": current_fdv,
            "pair_address": pos.get("pair_address", ""),
            "kol_signals": pos.get("kol_signals", []),
            "ath_during_trade": pos.get("ath_during_trade", {}),
            "token_age_seconds": pos.get("token_age_seconds", 0),
            "creator_twitter": pos.get("creator_twitter", {}),
            "token_mentions": pos.get("token_mentions", {}),
            "holder_intel": pos.get("holder_intel", {}),
            "candle_signals": pos.get("candle_signals", {}),
            "sold_amount_tokens": amount_now,
            "sell_fraction": 1.0,
            "is_partial": False,
            "signature": signature,
            "strategy_mode": pos.get("strategy_mode", ""),
            "strategy_meta": dict(pos.get("strategy_meta", {}) or {}),
            "source_type": str((pos.get("strategy_meta", {}) or {}).get("source_type", "") or ""),
            "source_label": str((pos.get("strategy_meta", {}) or {}).get("source_label", "") or ""),
            "source_author": str((pos.get("strategy_meta", {}) or {}).get("source_author", "") or ""),
            "source_author_name": str((pos.get("strategy_meta", {}) or {}).get("source_author_name", "") or ""),
            "source_tweet_id": str((pos.get("strategy_meta", {}) or {}).get("source_tweet_id", "") or ""),
        }
        self.start_post_close_monitoring(trade_data)

        self.closed_positions.append(trade_data)
        self.total_pnl_sol += pnl_sol
        del self.positions[signature]
        self.save_state()
        return trade_data

    def get_stats(self):
        monitored_pending = len(self.get_pending_post_close_monitoring())
        return {
            "total_pnl": self.total_pnl_sol,
            "active_positions": len(self.positions),
            "closed_trades": len(self.closed_positions),
            "post_close_monitoring": monitored_pending,
        }

    def reset_state(self):
        """Resets paper trading runtime and persisted state."""
        before = self.get_stats()
        self.positions = {}
        self.closed_positions = []
        self.total_pnl_sol = 0.0
        self.save_state()
        return before

    def get_positions(self):
        """Returns detailed active position data for dashboard."""
        positions_list = []
        current_time = time.time()

        for sig, pos in self.positions.items():
            current_price = pos.get("last_price", pos["entry_price"])
            current_fdv = pos.get("last_fdv", pos["entry_fdv"])
            amount_now = float(pos.get("amount", 0.0) or 0.0)
            entry_sol_amount = float(pos.get("entry_sol_amount", pos["entry_price"] * amount_now) or 0.0)
            avg_entry = (entry_sol_amount / amount_now) if amount_now > 0 else float(pos.get("entry_price", 0.0) or 0.0)
            pnl_sol = (current_price * amount_now) - entry_sol_amount
            pnl_pct = ((current_price - avg_entry) / avg_entry) * 100 if avg_entry > 0 else 0.0

            duration_sec = current_time - pos["start_time"]
            duration_min = int(duration_sec / 60)
            tp_pct = float(pos.get("take_profit_pct", self.take_profit_pct))
            sl_pct = float(pos.get("stop_loss_pct", self.stop_loss_pct))
            max_hold_sec = int(pos.get("max_hold_sec", self.time_stop_sec))

            if pnl_pct >= tp_pct * 100:
                status = "NEAR_TP"
            elif pnl_pct <= sl_pct * 100:
                status = "NEAR_SL"
            elif duration_sec >= max_hold_sec * 0.8:
                status = "NEAR_TIME"
            else:
                status = "ACTIVE"

            positions_list.append(
                {
                    "signature": sig,
                    "token": pos["token_mint"],
                    "pair_address": pos.get("pair_address", ""),
                    "entry_price": avg_entry,
                    "entry_fdv": pos["entry_fdv"],
                    "entry_sol_amount": entry_sol_amount,
                    "current_price": current_price,
                    "current_fdv": current_fdv,
                    "amount": amount_now,
                    "pnl_sol": pnl_sol,
                    "pnl_pct": pnl_pct,
                    "duration_min": duration_min,
                    "start_time": pos["start_time"],
                    "status": status,
                    "kol_count": len(pos.get("kol_signals", [])),
                }
            )

        return positions_list
