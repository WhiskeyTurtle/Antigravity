import json
import os
import math

STATE_FILE = "paper_trade_state.json"

def analyze():
    if not os.path.exists(STATE_FILE):
        print("No state file found.")
        return

    with open(STATE_FILE, 'r') as f:
        data = json.load(f)

    trades = data.get("closed_positions", [])
    if not trades:
        print("No closed trades to analyze.")
        return

    total_trades = len(trades)
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    win_rate = len(wins) / total_trades * 100
    total_pnl = sum(t['pnl'] for t in trades)
    
    durations = [t['duration'] for t in trades]
    avg_duration = sum(durations) / len(durations) / 60 if durations else 0

    print(f"--- 📊 TRADING PERFORMANCE REPORT ---")
    print(f"Total Trades: {total_trades}")
    print(f"Win Rate: {win_rate:.2f}% ({len(wins)} W / {len(losses)} L)")
    print(f"Total PnL: {total_pnl:.4f} SOL")
    print(f"Avg Duration: {avg_duration:.1f} min")
    
    # Detailed Loss Analysis
    print("\n--- 📉 LOSS ANALYSIS ---")
    
    # Filter for losses that exceeded the expected Stop Loss significantly (assuming -20% target)
    # If pnl_pct < -25%, it's a "Bad Slippage" loss
    bad_losses = [t for t in losses if t.get('pnl_pct', 0) < -25]
    print(f"Number of 'Catastrophic' Losses (<-25%): {len(bad_losses)}")
    
    if bad_losses:
        print("Sample Catastrophic Losses:")
        for t in bad_losses[:5]:
            print(f"Token: {t.get('token')[:8]}... | PnL: {t.get('pnl_pct'):.2f}% | Exit FDV: ${t.get('sell_fdv'):,.0f} | Time: {t.get('duration'):.1f}s")

    # Fast Losses
    fast_losses = [t for t in losses if t['duration'] < 60]
    print(f"\nFast Losses (<1 min): {len(fast_losses)}")
    
    # Liquidity Correlation (Check exit FDV of losses vs wins)
    avg_loss_fdv = sum(t.get('sell_fdv', 0) for t in losses) / len(losses) if losses else 0
    avg_win_fdv = sum(t.get('sell_fdv', 0) for t in wins) / len(wins) if wins else 0
    
    print(f"\nAvg Exit FDV (Wins): ${avg_win_fdv:,.0f}")
    print(f"Avg Exit FDV (Losses): ${avg_loss_fdv:,.0f}")

    print("\n--- RECOMMENDATIONS ---")
    if len(bad_losses) > total_trades * 0.2:
        print("⚠️ High Slippage/Rug Rate detected. Suggest increasing Minimum Liquidity Threshold.")
        
    if len(fast_losses) > total_trades * 0.2:
        print("⚠️ Many instant losses. Suggest increasing Trend Confirmation time.")
        
    if avg_loss_fdv < 10000:
       print("⚠️ Losses are occuring in very low cap tokens. Suggest raising Min FDV.")

if __name__ == "__main__":
    analyze()
