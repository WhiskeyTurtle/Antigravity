
import asyncio
import time
from realy_engine import RealyEngine

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

    async def open_position(self, signature: str, token_mint: str, entry_price: float, amount_tokens: float, entry_fdv: float = 0, pair_address: str = ""):
        """Opens a new paper position"""
        if signature in self.positions:
            return
            
        print(f"📝 PAPER BUY: {token_mint[:8]} @ {entry_price:.9f} SOL (MCap: ${entry_fdv:,.0f})")
        self.positions[signature] = {
            "token_mint": token_mint,
            "entry_price": entry_price,
            "entry_fdv": entry_fdv,
            "amount": amount_tokens,
            "start_time": time.time(),
            "last_price": entry_price,
            "last_fdv": entry_fdv,
            "pair_address": pair_address
        }

    async def update_positions(self):
        """Checks all active positions for TP/SL"""
        closed_this_cycle = []
        to_close = []
        
        for sig, pos in self.positions.items():
            # 1. Get Current Price
            quote = await self.engine.get_token_price(pos["token_mint"])
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
            elif (time.time() - pos["start_time"]) > self.time_stop_sec:
                reason = "TIME_STOP"
                
            if reason:
                pnl_sol = (current_price - pos["entry_price"]) * pos["amount"]
                print(f"📝 PAPER SELL ({reason}): {pos['token_mint'][:8]} PNL: {pnl_sol:.4f} SOL")
                
                trade_data = {
                    "token": pos["token_mint"],
                    "pnl": pnl_sol,
                    "reason": reason,
                    "duration": time.time() - pos["start_time"],
                    "sell_price": current_price,
                    "sell_fdv": current_fdv,
                    "pair_address": pos["pair_address"]
                }
                self.closed_positions.append(trade_data)
                closed_this_cycle.append(trade_data)
                self.total_pnl_sol += pnl_sol
                to_close.append(sig)
                
        # Remove closed
        for sig in to_close:
            del self.positions[sig]
            
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
                "pair_address": pos["pair_address"],
                "entry_price": pos["entry_price"],
                "entry_fdv": pos["entry_fdv"],
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
