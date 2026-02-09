
import asyncio
import time
import json
import os
from realy_engine import RealyEngine

STATE_FILE = "paper_trade_state.json"

class PaperTrader:
    def __init__(self):
        self.engine = RealyEngine()
        self.positions = {} # signature -> {entry_price, amount, token_mint, start_time}
        self.closed_positions = []
        self.total_pnl_sol = 0.0
        
        # Risk Settings
        self.take_profit_pct = 0.50 # +50%
        self.stop_loss_pct = -0.20 # -20%
        self.time_stop_sec = 1800 # 30 mins

        self.load_state()

    def load_state(self):
        """Loads trading state from disk"""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    data = json.load(f)
                    self.positions = data.get("positions", {})
                    # Filter out old test tokens "token_0", "token_1" etc if any
                    self.closed_positions = [
                        t for t in data.get("closed_positions", [])
                        if not t["token"].startswith("token_")
                    ]
                    self.total_pnl_sol = sum(t["pnl"] for t in self.closed_positions)
                    print(f"✅ Loaded state: {len(self.positions)} active, {len(self.closed_positions)} closed")
            except Exception as e:
                print(f"⚠️ Failed to load state: {e}")

    def save_state(self):
        """Saves trading state to disk"""
        try:
            state = {
                "positions": self.positions,
                "closed_positions": self.closed_positions,
                "total_pnl_sol": self.total_pnl_sol
            }
            with open(STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"⚠️ Failed to save state: {e}")

    async def open_position(self, signature: str, token_mint: str, entry_price: float, amount_tokens: float, entry_fdv: float = 0, pair_address: str = "", kol_data: dict = None):
        """Opens a new paper position"""
        if signature in self.positions:
            return
            
        # 1. Check for duplicate active position by mint
        for pos in self.positions.values():
            if pos["token_mint"] == token_mint:
                # If it's a new KOL signaling the same token, add to attribution
                if kol_data:
                    self.add_kol_signal(pos, kol_data)
                return

        # 2. Check cooldown (Prevent re-buying a token we just sold for 15 minutes)
        COOLDOWN_SEC = 900 # 15 minutes
        current_time = time.time()
        for trade in self.closed_positions:
            if trade["token"] == token_mint:
                close_time = trade.get("close_time", 0)
                if current_time - close_time < COOLDOWN_SEC:
                    return

        print(f"📝 PAPER BUY: {token_mint[:8]} @ {entry_price:.9f} SOL (MCap: ${entry_fdv:,.0f})")
        
        # Initialize KOL signals list
        kol_signals = []
        if kol_data:
            kol_signals.append({
                "wallet": kol_data.get("wallet", ""),
                "label": kol_data.get("label", ""),
                "timestamp": current_time
            })
            
        self.positions[signature] = {
            "token_mint": token_mint,
            "entry_price": entry_price,
            "entry_fdv": entry_fdv,
            "amount": amount_tokens,
            "start_time": current_time,
            "last_price": entry_price,
            "last_fdv": entry_fdv,
            "pair_address": pair_address,
            
            # Analytics Fields
            "kol_signals": kol_signals,
            "ath_during_trade": {
                "price_sol": entry_price,
                "fdv": entry_fdv,
                "timestamp": current_time,
                "pnl_pct_at_ath": 0.0
            },
            "token_age_seconds": 0, # Will be updated async
            "creator_twitter": {},  # Will be updated async
            "token_mentions": {}    # Will be updated async
        }
        self.save_state()

    def add_kol_signal(self, position, kol_data):
        """Adds a KOL signal to an existing position"""
        # Check if already recorded
        for signal in position.get("kol_signals", []):
            if signal["wallet"] == kol_data["wallet"]:
                return

        position.setdefault("kol_signals", []).append({
            "wallet": kol_data.get("wallet", ""),
            "label": kol_data.get("label", ""),
            "timestamp": time.time()
        })
        self.save_state()
        print(f"➕ Added KOL signal from {kol_data.get('label')} to existing position {position['token_mint'][:8]}")

    def update_analytics_data(self, signature, token_age=None, twitter_data=None, mentions=None):
        """Updates async analytics data for a position"""
        if signature in self.positions:
            pos = self.positions[signature]
            if token_age is not None:
                pos["token_age_seconds"] = token_age
            if twitter_data:
                pos["creator_twitter"] = twitter_data
            if mentions:
                pos["token_mentions"] = mentions
            self.save_state()

    async def update_positions(self):
        """Checks all active positions for TP/SL using parallel requests"""
        if not self.positions:
            return []

        closed_this_cycle = []
        to_close = []
        
        # Prepare tasks for parallel fetching
        sigs = list(self.positions.keys())
        tasks = [self.engine.get_token_price(self.positions[sig]["token_mint"]) for sig in sigs]
        
        # Determine strict concurrency limit to avoid opening too many sockets mostly if many positions
        # But for <10 positions, gather(*tasks) is fine.
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, sig in enumerate(sigs):
            pos = self.positions[sig]
            quote = results[i]

            # Handle failures/Errors
            if isinstance(quote, Exception) or not quote:
                # print(f"⚠️ Price fetch failed for {pos['token_mint']}: {quote}")
                continue # Skip update this cycle

            current_price = quote["price_sol"]
            current_fdv = quote.get("fdv", 0)
            
            if current_price == 0:
                continue # Skip if pricing fails
                
            pos["last_price"] = current_price
            pos["last_fdv"] = current_fdv
            
            # 2. Calculate PNL
            price_change_pct = (current_price - pos["entry_price"]) / pos["entry_price"]
            
            reason = None
            if price_change_pct >= self.take_profit_pct:
                reason = "TAKE_PROFIT"
            elif price_change_pct <= self.stop_loss_pct:
                reason = "STOP_LOSS"
            # TIME_STOP removed - positions can now be held indefinitely
                
            if reason:
                pnl_sol = (current_price - pos["entry_price"]) * pos["amount"]
                pnl_pct = price_change_pct * 100
                entry_sol_amount = pos["entry_price"] * pos["amount"]
                
                print(f"📝 PAPER SELL ({reason}): {pos['token_mint'][:8]} PNL: {pnl_sol:.4f} SOL")
                
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
                    "pair_address": pos.get("pair_address", "")
                }
                self.closed_positions.append(trade_data)
                closed_this_cycle.append(trade_data)
                self.total_pnl_sol += pnl_sol
                to_close.append(sig)
                
        # Remove closed
        for sig in to_close:
            del self.positions[sig]
            
        if closed_this_cycle:
            self.save_state()
            
        return closed_this_cycle # Return list of trades closed this cycle

    def get_stats(self):
        """Returns stats for the dashboard"""
        return {
            "total_pnl": self.total_pnl_sol,
            "active_positions": len(self.positions),
            "closed_trades": len(self.closed_positions)
        }

    def get_positions(self):
        """Returns detailed active position data for dashboard"""
        positions_list = []
        current_time = time.time()
        
        for sig, pos in self.positions.items():
            # Calculate current PnL
            current_price = pos.get("last_price", pos["entry_price"])
            current_fdv = pos.get("last_fdv", pos["entry_fdv"])
            pnl_sol = (current_price - pos["entry_price"]) * pos["amount"]
            pnl_pct = ((current_price - pos["entry_price"]) / pos["entry_price"]) * 100
            
            # Calculate time held
            duration_sec = current_time - pos["start_time"]
            duration_min = int(duration_sec / 60)
            
            # Determine status
            if pnl_pct >= self.take_profit_pct * 100:
                status = "NEAR_TP"
            elif pnl_pct <= self.stop_loss_pct * 100:
                status = "NEAR_SL"
            elif duration_sec >= self.time_stop_sec * 0.8:
                status = "NEAR_TIME"
            else:
                status = "ACTIVE"
            
            
            positions_list.append({
                "signature": sig,
                "token": pos["token_mint"],
                "pair_address": pos.get("pair_address", ""),
                "entry_price": pos["entry_price"],
                "entry_fdv": pos["entry_fdv"],
                "entry_sol_amount": pos["entry_price"] * pos["amount"],  # SOL used to purchase
                "current_price": current_price,
                "current_fdv": current_fdv,
                "amount": pos["amount"],
                "pnl_sol": pnl_sol,
                "pnl_pct": pnl_pct,
                "duration_min": duration_min,
                "start_time": pos["start_time"],
                "status": status
            })
        
        return positions_list
