# Experiment Alpha: Solana Momentum Screener - Report

## 📅 Date: 2026-02-05
**Status**: ✅ SUCCESS (MVE Operational)

## 🎯 Objective
Verify if we can programmatically detect token velocity (tx/min) using public RPCs to identify "high momentum" assets without manual chart watching.

## 📊 Results (Simulation Run)
The monitor scanned 3 target tokens using `solana-py` connected to Mainnet Beta.

| Token | Address Prefix | Velocity (Tx/min) | Score | Verdict |
|-------|----------------|-------------------|-------|---------|
| **BONK** | `DezX...` | **34** | 3.4 | ⚠️ WATCH |
| **WIF** | `EKpQ...` | **8** | 0.8 | ⚠️ WATCH |
| **PENGU** | `HeLp...` | **0** | 0.0 | ⚠️ WATCH |

*Note: Velocity is based on a 1-minute sample of the last 100 transactions.*

## 💡 Findings
1.  **Feasibility**: Can successfully query `getSignaturesForAddress` on public RPC without immediate rate limits for low volume.
2.  **Latency**: Response time is acceptable for a "Scanner" (< 2s).
3.  **Limitations**: Public RPC has strict limits (cannot scan 1000s of tokens). Need a paid node (Helius/QuickNode) for production scaling.
4.  **Next Step**: Automate the "Discovery" phase (finding new pairs) rather than checking a static list.

## 🏁 Recommendation
**SCALE**. The mechanism works. We should move to integrating a "New Pools" data source (e.g., Raydium V4 logs) and building a real-time dashboard.
