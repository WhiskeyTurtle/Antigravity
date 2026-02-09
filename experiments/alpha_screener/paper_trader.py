import asyncio
import json
import os
import time
from typing import Dict, List, Optional

from realy_engine import RealyEngine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "paper_trade_state.json")
POST_CLOSE_MONITOR_WINDOW_SEC = 72 * 3600


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

    def _normalize_closed_trade(self, trade: dict) -> dict:
        trade.setdefault("entry_price", 0.0)
        trade.setdefault("sell_price", trade.get("exit_price", 0.0))
        trade.setdefault("entry_sol_amount", 0.0)
        trade.setdefault("pair_address", "")
        trade.setdefault("close_time", time.time())
        trade.setdefault("reason", trade.get("close_reason", "UNKNOWN"))
        trade.setdefault("pnl", 0.0)
        trade.setdefault("pnl_pct", 0.0)
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
                self.positions = data.get("positions", {})
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
    ):
        """Opens a new paper position."""
        if signature in self.positions:
            return

        # Check duplicate active position by mint.
        for existing_sig, pos in self.positions.items():
            if pos.get("token_mint") == token_mint:
                if kol_data:
                    self.add_kol_signal(
                        signature=existing_sig,
                        kol_wallet=kol_data.get("wallet", ""),
                        kol_label=kol_data.get("label", ""),
                    )
                return

        # Cooldown: prevent re-buying a recently closed token.
        cooldown_sec = 900
        current_time = time.time()
        for trade in self.closed_positions:
            if trade.get("token") == token_mint:
                close_time = float(trade.get("close_time", 0))
                if current_time - close_time < cooldown_sec:
                    return

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

        self.positions[signature] = {
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
        }
        self.save_state()

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

    async def update_positions(self):
        """Checks all active positions for TP/SL using parallel requests."""
        if not self.positions:
            return []

        closed_this_cycle = []
        to_close = []

        sigs = list(self.positions.keys())
        tasks = [self.engine.get_token_price(self.positions[sig]["token_mint"]) for sig in sigs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, sig in enumerate(sigs):
            pos = self.positions[sig]
            quote = results[i]

            if isinstance(quote, Exception) or not quote:
                continue

            current_price = float(quote.get("price_sol", 0.0) or 0.0)
            current_fdv = float(quote.get("fdv", 0.0) or 0.0)
            if current_price == 0:
                continue

            pos["last_price"] = current_price
            pos["last_fdv"] = current_fdv
            self.update_ath(sig, current_price, current_fdv)

            price_change_pct = (current_price - pos["entry_price"]) / pos["entry_price"]

            reason = None
            if price_change_pct >= self.take_profit_pct:
                reason = "TAKE_PROFIT"
            elif price_change_pct <= self.stop_loss_pct:
                reason = "STOP_LOSS"

            if reason:
                pnl_sol = (current_price - pos["entry_price"]) * pos["amount"]
                pnl_pct = price_change_pct * 100
                entry_sol_amount = pos["entry_price"] * pos["amount"]

                print(f"PAPER SELL ({reason}): {pos['token_mint'][:8]} PNL: {pnl_sol:.4f} SOL")

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
                }
                self.start_post_close_monitoring(trade_data)

                self.closed_positions.append(trade_data)
                closed_this_cycle.append(trade_data)
                self.total_pnl_sol += pnl_sol
                to_close.append(sig)

        for sig in to_close:
            del self.positions[sig]

        if closed_this_cycle:
            self.save_state()

        return closed_this_cycle

    def get_stats(self):
        monitored_pending = len(self.get_pending_post_close_monitoring())
        return {
            "total_pnl": self.total_pnl_sol,
            "active_positions": len(self.positions),
            "closed_trades": len(self.closed_positions),
            "post_close_monitoring": monitored_pending,
        }

    def get_positions(self):
        """Returns detailed active position data for dashboard."""
        positions_list = []
        current_time = time.time()

        for sig, pos in self.positions.items():
            current_price = pos.get("last_price", pos["entry_price"])
            current_fdv = pos.get("last_fdv", pos["entry_fdv"])
            pnl_sol = (current_price - pos["entry_price"]) * pos["amount"]
            pnl_pct = ((current_price - pos["entry_price"]) / pos["entry_price"]) * 100

            duration_sec = current_time - pos["start_time"]
            duration_min = int(duration_sec / 60)

            if pnl_pct >= self.take_profit_pct * 100:
                status = "NEAR_TP"
            elif pnl_pct <= self.stop_loss_pct * 100:
                status = "NEAR_SL"
            elif duration_sec >= self.time_stop_sec * 0.8:
                status = "NEAR_TIME"
            else:
                status = "ACTIVE"

            positions_list.append(
                {
                    "signature": sig,
                    "token": pos["token_mint"],
                    "pair_address": pos.get("pair_address", ""),
                    "entry_price": pos["entry_price"],
                    "entry_fdv": pos["entry_fdv"],
                    "entry_sol_amount": pos["entry_price"] * pos["amount"],
                    "current_price": current_price,
                    "current_fdv": current_fdv,
                    "amount": pos["amount"],
                    "pnl_sol": pnl_sol,
                    "pnl_pct": pnl_pct,
                    "duration_min": duration_min,
                    "start_time": pos["start_time"],
                    "status": status,
                    "kol_count": len(pos.get("kol_signals", [])),
                }
            )

        return positions_list
