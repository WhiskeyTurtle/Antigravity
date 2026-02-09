import json
import os
import time
from typing import Dict, List

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LIVE_STATE_FILE = os.path.join(BASE_DIR, "live_trade_state.json")


class LiveTradeTracker:
    def __init__(self):
        self.open_positions: Dict[str, dict] = {}
        self.closed_positions: List[dict] = []
        self.total_pnl_sol = 0.0
        self.load_state()

    def load_state(self):
        if not os.path.exists(LIVE_STATE_FILE):
            return
        try:
            with open(LIVE_STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.open_positions = data.get("open_positions", {})
            self.closed_positions = data.get("closed_positions", [])
            self.total_pnl_sol = float(data.get("total_pnl_sol", 0.0) or 0.0)
        except Exception:
            self.open_positions = {}
            self.closed_positions = []
            self.total_pnl_sol = 0.0

    def save_state(self):
        state = {
            "open_positions": self.open_positions,
            "closed_positions": self.closed_positions,
            "total_pnl_sol": self.total_pnl_sol,
        }
        with open(LIVE_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

    def record_buy(self, token: str, token_amount: float, sol_spent: float, tx_sig: str = ""):
        if token_amount <= 0 or sol_spent <= 0:
            return

        pos = self.open_positions.get(token)
        now = time.time()
        if not pos:
            self.open_positions[token] = {
                "token": token,
                "token_amount": token_amount,
                "sol_spent": sol_spent,
                "avg_entry_price_sol": sol_spent / token_amount,
                "opened_at": now,
                "last_buy_tx": tx_sig,
            }
        else:
            new_token_amt = float(pos.get("token_amount", 0.0)) + token_amount
            new_sol_spent = float(pos.get("sol_spent", 0.0)) + sol_spent
            pos["token_amount"] = new_token_amt
            pos["sol_spent"] = new_sol_spent
            pos["avg_entry_price_sol"] = (new_sol_spent / new_token_amt) if new_token_amt > 0 else 0.0
            pos["last_buy_tx"] = tx_sig

        self.save_state()

    def record_sell(self, token: str, token_amount_sold: float, sol_received: float, reason: str = "", tx_sig: str = ""):
        if token_amount_sold <= 0 or sol_received <= 0:
            return None

        pos = self.open_positions.get(token)
        if not pos:
            return None

        open_token_amt = float(pos.get("token_amount", 0.0))
        open_sol_spent = float(pos.get("sol_spent", 0.0))
        if open_token_amt <= 0:
            return None

        sold_ratio = min(1.0, token_amount_sold / open_token_amt)
        cost_basis_sol = open_sol_spent * sold_ratio
        pnl_sol = sol_received - cost_basis_sol
        pnl_pct = (pnl_sol / cost_basis_sol * 100.0) if cost_basis_sol > 0 else 0.0

        closed = {
            "token": token,
            "token_amount_sold": token_amount_sold,
            "sol_received": sol_received,
            "cost_basis_sol": cost_basis_sol,
            "pnl_sol": pnl_sol,
            "pnl_pct": pnl_pct,
            "reason": reason,
            "tx_sig": tx_sig,
            "closed_at": time.time(),
        }
        self.closed_positions.append(closed)
        self.total_pnl_sol += pnl_sol

        remaining_token = max(0.0, open_token_amt - token_amount_sold)
        remaining_sol = max(0.0, open_sol_spent - cost_basis_sol)
        if remaining_token <= 0:
            self.open_positions.pop(token, None)
        else:
            pos["token_amount"] = remaining_token
            pos["sol_spent"] = remaining_sol
            pos["avg_entry_price_sol"] = (remaining_sol / remaining_token) if remaining_token > 0 else 0.0

        self.save_state()
        return closed

    def get_stats(self):
        return {
            "total_pnl_sol": self.total_pnl_sol,
            "open_positions": len(self.open_positions),
            "closed_trades": len(self.closed_positions),
        }

    def get_open_positions(self):
        return list(self.open_positions.values())

    def get_history(self):
        return self.closed_positions
