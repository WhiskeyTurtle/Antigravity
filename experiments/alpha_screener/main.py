
import asyncio
import sys
import os
import html
import time
import shutil
from collections import deque
from datetime import datetime, timezone

# Add current directory to path so imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import json
import aiohttp
from typing import Optional, List, Dict, Any
from solana.rpc.async_api import AsyncClient
import solders.signature
from solana.rpc.commitment import Confirmed
from contextlib import asynccontextmanager

from listener import Listener
from risk_engine import RiskEngine
from analyzer import Analyzer
from sniper import Sniper
from paper_trader import PaperTrader
from kol_monitor import KOLMonitor
from social_monitor import SocialMonitor
from trade_analytics import TradeAnalyticsStore
from holder_intel import HolderIntel
from candle_analyzer import CandleAnalyzer
from live_trade_tracker import LiveTradeTracker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archives")
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERHfYxL2wL4fYB9H9n1tFoZT3zh7TBTtJU"
BIRDEYE_BASE_URL = "https://public-api.birdeye.so"
CHART_WATCH_EXCLUDED_MINTS = {SOL_MINT, USDC_MINT, USDT_MINT}
JUPITER_QUOTE_API = "https://lite-api.jup.ag/swap/v1/quote"
JUPITER_SWAP_API = "https://lite-api.jup.ag/swap/v1/swap"
TRADING_MODE_KOL_MOMENTUM = "kol_momentum_scalper"
TRADING_MODE_EARLY_20 = "early_20_scalper"
TRADING_MODE_LAUNCH_BURST = "kol_launch_burst_scalper"
TRADING_MODE_CHART_PRO = "chart_pro_breakout_scalper"
ALLOWED_TRADING_MODES = {TRADING_MODE_KOL_MOMENTUM, TRADING_MODE_EARLY_20, TRADING_MODE_LAUNCH_BURST, TRADING_MODE_CHART_PRO}


def _normalize_trading_modes_from_settings(settings_obj: dict) -> List[str]:
    modes_raw = settings_obj.get("trading_modes", None)
    normalized: List[str] = []
    if isinstance(modes_raw, list):
        for m in modes_raw:
            mode = str(m or "").strip().lower()
            if mode in ALLOWED_TRADING_MODES and mode not in normalized:
                normalized.append(mode)
    if not normalized:
        legacy = str(settings_obj.get("trading_mode", TRADING_MODE_KOL_MOMENTUM) or TRADING_MODE_KOL_MOMENTUM).strip().lower()
        if legacy in ALLOWED_TRADING_MODES:
            normalized = [legacy]
        else:
            normalized = [TRADING_MODE_KOL_MOMENTUM]
    return normalized

# Global State
class FeedService:
    def __init__(self):
        self.active_connections = []
        self.queue = asyncio.Queue()
        self.running = False
        self.kol_seen_by_token = {}
        self.first_kol_buy_ts_by_token = {}
        self.kol_flow_by_token = {}
        self.market_ticks_by_token = {}
        self.market_trend_seen_by_token = {}
        self.candle_signal_cache_by_token = {}
        self.candle_signal_cache_ttl_sec = 20.0
        self.chart_watchlist_by_token = {}
        self.chart_watch_suppressed_by_token = {}
        self.chart_watch_inflight_tokens = set()
        self.chart_watch_last_broadcast_ts = 0.0
        self.chart_watch_broadcast_min_interval_sec = 1.0
        self.chart_watch_eval_count_window = deque(maxlen=300)
        self.chart_watch_eval_latency_ms_window = deque(maxlen=500)
        self.chart_watch_token_age_cache = {}
        self.chart_watch_safety_cache = {}
        self.chart_watch_holder_cache = {}
        self.chart_watch_source_mix = {}
        self.chart_watch_rejected_age_count = 0
        self.chart_watch_rejected_band_count = 0
        self.chart_watch_market_data_timeout_count = 0
        self.chart_watch_candle_rate_limited_count = 0
        self.chart_watch_candidate_pool_size_raw = 0
        self.chart_watch_candidate_pool_size_post_gate = 0
        self.chart_watch_startup_burst_active = False
        self.chart_watch_startup_burst_admitted = 0
        self.chart_watch_startup_burst_done = False
        self.chart_watch_birdeye_warned_missing = False
        self.chart_watch_last_birdeye_empty_warn_ts = 0.0
        self.autopilot_state = {
            "last_run_ts": 0.0,
            "last_action": "idle",
            "last_reason": "",
            "last_window_trades": 0,
            "last_window_pnl_sol": 0.0,
            "relax_level": 0,
            "dominant_reason_code": "",
            "dominant_reason_streak": 0,
            "live_entry_pause_until_ts": 0.0,
            "live_entry_pause_ref_trade_ts": 0.0,
            "history": deque(maxlen=120),
            "last_good_ts": 0.0,
            "last_good_settings": {},
            "baseline_settings": {},
        }
        self.rpc_url = "https://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff"
        self.settings = self.load_settings()
        self.autopilot_state["baseline_settings"] = self._autopilot_tunable_snapshot()
        
    def load_settings(self):
        """Load settings from disk or use defaults"""
        defaults = {
            "trading_mode": TRADING_MODE_KOL_MOMENTUM,
            "trading_modes": [TRADING_MODE_KOL_MOMENTUM],
            "sol_amount_min": 0.1,
            "sol_amount_max": 0.5,
            "max_live_open_positions": 2,
            # Keep false: live engine must be source-of-truth for live exits.
            "live_follow_paper_exits": False,
            # Safety rails (can be tuned in settings.json manually)
            "min_liquidity_usd": 15000,
            "max_bundlers_holding_pct": 8.0,
            "max_snipers_holding_pct": 20.0,
            "wallet_age_cluster_check_enabled": True,
            "wallet_age_cluster_min_sample": 6,
            "wallet_age_cluster_recent_window_hours": 48.0,
            "wallet_age_cluster_min_recent_share_pct": 70.0,
            "wallet_age_cluster_min_same_hour_pct": 55.0,
            # 0 disables hard veto on rapid 5m pumps.
            "max_price_change_m5_pct": 0.0,
            "max_atr_pct": 16.0,
            # Rapid pump playbook (buy fast / exit fast).
            "rapid_pump_threshold_pct": 120.0,
            "rapid_pump_size_factor": 0.5,
            "fast_exit_take_profit_pct": 0.25,
            "fast_exit_stop_loss_pct": -0.12,
            "fast_exit_max_hold_sec": 300,
            # Cooldown before the same token can be re-entered after close.
            "token_cooldown_sec": 900,
            # Maximum token age allowed for new entries (default: 3 days).
            "max_token_age_sec": 259200,
            # KOL Momentum scalper entry floor (0 disables min-MC gate).
            "kol_min_mcap_usd": 0.0,
            # Early 20 scalper mode (quick in/out, more frequent entries).
            "early_20_min_mcap_usd": 7000.0,
            "early_20_min_liquidity_usd": 7000.0,
            "early_20_min_score": 35.0,
            "early_20_kol_min_sol": 0.10,
            "early_20_take_profit_pct": 0.20,
            "early_20_stop_loss_pct": -0.12,
            "early_20_max_hold_sec": 300,
            "live_early_20_take_profit_1_pct": 0.12,
            "live_early_20_take_profit_2_pct": 0.20,
            "live_early_20_runner_trail_pct": 0.08,
            "live_early_20_stop_loss_pct": -0.12,
            "live_early_20_max_hold_sec": 240,
            "live_early_20_strong_red_pct": 0.02,
            # KOL Launch Burst Scalper mode.
            "launch_burst_min_mcap_usd": 7000.0,
            "launch_burst_max_mcap_usd": 40000.0,
            "launch_burst_min_liquidity_usd": 6000.0,
            "launch_burst_kol_min_sol": 0.10,
            "launch_burst_take_profit_1_pct": 0.12,
            "launch_burst_take_profit_2_pct": 0.20,
            "launch_burst_stop_loss_pct": -0.10,
            "launch_burst_runner_trail_pct": 0.08,
            "launch_burst_max_hold_sec": 90,
            "launch_burst_size_factor": 0.60,
            "launch_burst_cooldown_sec": 600,
            "launch_burst_window_sec": 120,
            "launch_burst_min_score": 35.0,
            "launch_burst_strong_red_pct": 0.02,
            "launch_burst_market_retry_attempts": 5,
            "launch_burst_market_retry_delay_sec": 2.0,
            # Professional chart-driven breakout mode.
            "chart_pro_enabled": True,
            "chart_pro_entry_tf": "1m",
            "chart_pro_bias_tf": "5m",
            "chart_pro_breakout_lookback_bars": 20,
            "chart_pro_min_volume_multiple": 1.8,
            "chart_pro_min_score": 34.0,
            "chart_pro_min_mcap_usd": 20000.0,
            "chart_pro_min_liquidity_usd": 6000.0,
            "chart_pro_require_retest": False,
            "chart_pro_stop_atr_mult": 1.2,
            "chart_pro_tp1_r": 1.0,
            "chart_pro_tp2_r": 2.0,
            "chart_pro_runner_trail_atr_mult": 1.5,
            "chart_pro_max_hold_sec": 360,
            "chart_pro_cooldown_sec": 900,
            "chart_watch_max_tokens": 24,
            "chart_watch_focus_profile": "quality_first",
            "chart_watch_admission_min_volume_m5_usd": 3000.0,
            "chart_watch_admission_min_mcap_usd": 15000.0,
            "chart_watch_admission_max_mcap_usd": 100000000.0,
            "chart_watch_admission_min_liquidity_usd": 2800.0,
            "chart_watch_admission_max_age_sec": 86400,
            "chart_watch_strict_age_sec": 86400,
            "chart_watch_trend_min_volume_m5_usd": 5000.0,
            "chart_watch_trend_min_liquidity_usd": 8000.0,
            "chart_watch_trend_min_mcap_usd": 20000.0,
            "chart_watch_fail_backoff_base_sec": 20,
            "chart_watch_fail_backoff_max_sec": 600,
            "chart_watch_eval_budget_per_cycle": 16,
            "chart_watch_eval_concurrency": 3,
            "chart_watch_market_data_timeout_sec": 2.5,
            "chart_watch_candle_cache_ttl_sec": 30,
            "chart_watch_candle_timeout_sec": 4.0,
            "chart_watch_eval_timeout_sec": 8.0,
            "chart_watch_prewarm_candles": True,
            "chart_watch_startup_burst_enabled": True,
            "chart_watch_startup_burst_target": 12,
            "chart_watch_startup_burst_timeout_sec": 30,
            "chart_watch_source_mode": "birdeye_primary",
            "chart_watch_table_mode": "admitted_only",
            "chart_watch_hide_filtered_in_ui": True,
            # Autonomous chart optimizer (safe bounded tuning).
            "autopilot_enabled": True,
            "autopilot_interval_sec": 120,
            "autopilot_window_sec": 3600,
            "autopilot_no_trade_window_sec": 600,
            "autopilot_max_daily_drawdown_sol": 0.40,
            "autopilot_max_loss_streak": 4,
            "autopilot_target_trades_per_hour": 2.0,
            "autopilot_reason_weighted_tuning_enabled": True,
            "autopilot_reason_low_score_threshold_pct": 0.55,
            "autopilot_reason_rate_limited_threshold_pct": 0.10,
            "autopilot_step_chart_score": 2.0,
            "autopilot_step_volume_gate_pct": 0.12,
            "autopilot_min_chart_score_floor": 30.0,
            "autopilot_min_volume_gate_floor_usd": 2500.0,
            "autopilot_min_liquidity_gate_floor_usd": 2500.0,
            "autopilot_min_mcap_gate_floor_usd": 10000.0,
            # Optional market scanner source (independent of KOL/Twitter).
            "market_trend_source_enabled": True,
            "market_trend_scan_interval_sec": 8,
            "market_trend_requeue_cooldown_sec": 900,
            "market_trend_max_new_per_scan": 12,
            "live_kol_take_profit_1_pct": 0.12,
            "live_kol_take_profit_2_pct": 0.25,
            "live_kol_runner_trail_pct": 0.10,
            "live_kol_stop_loss_pct": -0.12,
            "live_kol_max_hold_sec": 300,
            "live_kol_strong_red_pct": 0.025,
            # Telegram live-trade notifications.
            "telegram_notify_live_trades": True,
            "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
            "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
            "telegram_silent": False,
        }
        try:
            if os.path.exists(SETTINGS_FILE):
                with open(SETTINGS_FILE, 'r') as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    defaults.update(loaded)
                defaults["trading_modes"] = _normalize_trading_modes_from_settings(defaults)
                defaults["trading_mode"] = defaults["trading_modes"][0]
                return defaults
        except Exception as e:
            print(f"Failed to load settings: {e}")
        
        defaults["trading_modes"] = _normalize_trading_modes_from_settings(defaults)
        defaults["trading_mode"] = defaults["trading_modes"][0]
        return defaults

    def _autopilot_tunable_snapshot(self) -> dict:
        settings = dict(self.settings or {})
        return {
            "market_trend_scan_interval_sec": int(settings.get("market_trend_scan_interval_sec", 8) or 8),
            "market_trend_max_new_per_scan": int(settings.get("market_trend_max_new_per_scan", 12) or 12),
            "chart_watch_max_tokens": int(settings.get("chart_watch_max_tokens", 24) or 24),
            "chart_watch_eval_budget_per_cycle": int(settings.get("chart_watch_eval_budget_per_cycle", 16) or 16),
            "chart_watch_eval_concurrency": int(settings.get("chart_watch_eval_concurrency", 3) or 3),
            "chart_watch_market_data_timeout_sec": float(settings.get("chart_watch_market_data_timeout_sec", 2.5) or 2.5),
            "chart_pro_min_score": float(settings.get("chart_pro_min_score", 34.0) or 34.0),
            "chart_watch_admission_min_volume_m5_usd": float(settings.get("chart_watch_admission_min_volume_m5_usd", 3000.0) or 3000.0),
            "chart_watch_admission_min_liquidity_usd": float(settings.get("chart_watch_admission_min_liquidity_usd", 2800.0) or 2800.0),
            "chart_watch_admission_min_mcap_usd": float(settings.get("chart_watch_admission_min_mcap_usd", 15000.0) or 15000.0),
            "chart_watch_candle_cache_ttl_sec": int(settings.get("chart_watch_candle_cache_ttl_sec", 30) or 30),
        }
    
    def save_settings(self):
        """Save settings to disk"""
        try:
            with open(SETTINGS_FILE, 'w') as f:
                json.dump(self.settings, f, indent=2)
            print(f"Settings saved: {self.settings}")
        except Exception as e:
            print(f"Failed to save settings: {e}")
        
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

service = FeedService()

# "Smart Money" / KOL Wallets to Track
# (Using Raydium Protocol addresses as high-volume tests for now)
KOL_WALLETS = {
    "8MaVa9kdt3NW4Q5HyNAm1X5LbR8PQRVDc1W8NMVK88D5": "Daumen",
    "Cum5exPbTUGJgCUFYvQbBpJB1hibVWcdZrS7Sm9jRP2h": "Gains low rate",
    "4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9": "Decu",
    "bwamJzztZsepfkteWRChggmXuiiCQvpLqPietdNfSXa": "55k Devs",
    "7NAd2EpYGGeFofpyvgehSXhH5vg6Ry6VRMW2Y6jiqCu1": "Cupsy Public",
    "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f": "Cupsy Bundle",
    "Di75xbVUg3u1qcmZci3NcZ8rjFMj7tsnYEoFdEMjS4ow": "N'o",
    "9FNz4MjPUmnJqTf6yEDbL1D4SsHVh7uA8zRHhR5K138r": "Danny",
    "GXJ4Up4KjR4UhEPbXN7daRZtobkJzGtpnAgibECzpFRn": "Some Bundler",
    "DNfuF1L62WWyW3pNakVkyGGFzVVhj4Yr52jSmdTyeBHm": "Patrick",
    "6nU2L7MQVUWjtdKHVpuZA9aind73nd3rXC4YFo8KQCy4": "Some big winner",
    "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o": "Cented",
    "BCagckXeMChUKrHEd6fKFA1uiWDtcmCXMsqaheLiUPJd": "DVcs",
    "GUDP1dP2C4S3M6dYrSXMFPumWg1Gt1s1N1FqAThsiXWZ": "HonestDevAlt1",
    "G4MQZpoLvfRZDTdSbtbMDXf4J8GQ9VGMyzKysLhyfotL": "HoenstDevAlt",
    "FM1YCKED2KaqB8Uat8aB1nsffR1vezr7s6FAEieXJgke": "HonestDV",
    "3LUfv2u5yzsDtUzPdsSJ7ygPBuqwfycMkjpNreRR2Yww": "Domy",
    "CS7SmQzJvbWJ5UtdJodDPa3nY4pjoCDs2JsUY2CyNPx7": "Barnabas",
    "6S8GezkxYUfZy9JPtYnanbcZTMB87Wjt1qx3c6ELajKC": "High Gains",
    "G6fUXjMKPJzCY1rveAE6Qm7wy5U3vZgKDJmN1VPAdiZC": "Clukz",
    "4ZdCpHJrSn4E9GmfP8jjfsAExHGja2TEn4JmXfEeNtyT": "Robo"
}

# Global instances for modules
risk_engine: RiskEngine = None
analyzer: Analyzer = None
sniper: Sniper = None
paper_trader: PaperTrader = None
kol_monitor: KOLMonitor = None
kol_monitor_task: Optional[asyncio.Task] = None
social_monitor: SocialMonitor = None
async_client: AsyncClient = None # Global async client
trade_analytics_store: TradeAnalyticsStore = TradeAnalyticsStore()
holder_intel: HolderIntel = HolderIntel(service.rpc_url)
candle_analyzer: CandleAnalyzer = CandleAnalyzer()
live_trade_tracker: LiveTradeTracker = LiveTradeTracker()


def _requires_kol_tracking_from_settings(settings_obj: dict) -> bool:
    modes = _normalize_trading_modes_from_settings(settings_obj or {})
    if not modes:
        return False
    kol_modes = {TRADING_MODE_KOL_MOMENTUM, TRADING_MODE_EARLY_20, TRADING_MODE_LAUNCH_BURST}
    return any(m in kol_modes for m in modes)


def _requires_kol_tracking() -> bool:
    return _requires_kol_tracking_from_settings(service.settings)


async def _on_kol_activity(signature, wallet_address, wallet_name, source_type):
    import time
    if not _requires_kol_tracking():
        return
    await service.queue.put({
        "type": "NEW_POOL",
        "signature": signature,
        "source": f"Tracked: {wallet_name}",
        "kol_wallet": wallet_address,
        "kol_label": wallet_name,
        "time": time.time()
    })


async def _sync_kol_monitor_state():
    global kol_monitor, kol_monitor_task
    want_enabled = bool(service.running and _requires_kol_tracking())

    if want_enabled:
        if kol_monitor is None:
            kol_monitor = KOLMonitor(
                service.rpc_url,
                "wss://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff",
                KOL_WALLETS,
                _on_kol_activity,
            )
            kol_monitor_task = asyncio.create_task(kol_monitor.start())
            try:
                await service.broadcast({"type": "log", "msg": "KOL monitor enabled for active trading modes"})
            except Exception:
                pass
        return

    if kol_monitor:
        try:
            kol_monitor.stop()
        except Exception:
            pass
        kol_monitor = None
        try:
            await service.broadcast({"type": "log", "msg": "KOL monitor disabled (Chart Pro only mode)"})
        except Exception:
            pass
    if kol_monitor_task and not kol_monitor_task.done():
        kol_monitor_task.cancel()
    kol_monitor_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting Alpha Feed Service...")
    
    global risk_engine, analyzer, sniper, paper_trader, kol_monitor, kol_monitor_task, social_monitor, async_client
    
    # Initialize Clients (Helius RPC)
    async_client = AsyncClient(service.rpc_url)
    
    # Start the Queue Processing Task
    asyncio.create_task(process_feed())
    asyncio.create_task(monitor_positions_task())
    asyncio.create_task(monitor_live_positions_task())
    asyncio.create_task(monitor_closed_positions_ath())
    asyncio.create_task(reconcile_live_wallet_task())
    asyncio.create_task(monitor_market_trends_task())
    asyncio.create_task(monitor_chart_watchlist_task())
    asyncio.create_task(monitor_chart_autopilot_task())
    
    # Initialize implementation modules
    risk_engine = RiskEngine(async_client)
    analyzer = Analyzer()
    sniper = Sniper(async_client, service.rpc_url)
    paper_trader = PaperTrader()
    try:
        trade_analytics_store.record_trade_analytics_many(paper_trader.closed_positions)
    except Exception:
        pass
    
    # MONITOR CALLBACKS
    async def on_social_signal(event):
        """Handles events from SocialMonitor (Twitter/Telegram)"""
        await service.queue.put({
            "type": "SOCIAL_SIGNAL",
            "data": event 
        })

    service.running = True
    await _sync_kol_monitor_state()

    # Start Social Monitor
    # User provided tokens for Jack Diamond account
    auth_token = "ec35e4dc84ec7b2cd5a9cefd07157f4a345c2090"
    ct0 = "9f1398a6d5f1071609ef4e7fb22d5868de481a65e2c3e8b0d3396a2ca4d1d9c1692cebfc42bf44407eedf076ad5c69dc730ced1d5971963fa78cd28210ac7f320a34f3dc7fd851cfda6954bbf7d8810b"
    social_monitor = SocialMonitor(on_social_signal, auth_token=auth_token, ct0=ct0)
    asyncio.create_task(social_monitor.start())
    
    yield
    
    # Shutdown
    print("Shutting down...")
    service.running = False
    if kol_monitor_task and not kol_monitor_task.done():
        kol_monitor_task.cancel()
    if kol_monitor:
        kol_monitor.stop()
        kol_monitor = None
    if async_client:
        await async_client.close()


from fastapi import FastAPI, WebSocket, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

class WalletItem(BaseModel):
    address: str
    name: str

@app.post("/api/wallets")
async def add_wallet(item: WalletItem):
    """Add a new KOL wallet to track"""
    KOL_WALLETS[item.address] = item.name
    if kol_monitor:
        await kol_monitor.add_wallet(item.address, item.name)
        return {"status": "ok", "msg": f"Added {item.name}"}
    return {"status": "ok", "msg": f"Added {item.name} (KOL monitor inactive in current mode)"}

@app.get("/api/wallets")
async def get_wallets():
    """Get list of tracked wallets"""
    if kol_monitor:
        return kol_monitor.kols
    return KOL_WALLETS

class SettingsUpdate(BaseModel):
    sol_amount_min: float
    sol_amount_max: float
    trading_mode: Optional[str] = None
    trading_modes: Optional[List[str]] = None
    max_live_open_positions: Optional[int] = None
    token_cooldown_sec: Optional[int] = None
    max_token_age_sec: Optional[int] = None
    kol_min_mcap_usd: Optional[float] = None
    early_20_min_mcap_usd: Optional[float] = None
    launch_burst_min_mcap_usd: Optional[float] = None
    chart_pro_min_mcap_usd: Optional[float] = None
    chart_pro_min_liquidity_usd: Optional[float] = None
    chart_pro_enabled: Optional[bool] = None
    chart_pro_entry_tf: Optional[str] = None
    chart_pro_bias_tf: Optional[str] = None
    chart_pro_breakout_lookback_bars: Optional[int] = None
    chart_pro_min_volume_multiple: Optional[float] = None
    chart_pro_min_score: Optional[float] = None
    chart_pro_require_retest: Optional[bool] = None
    chart_pro_stop_atr_mult: Optional[float] = None
    chart_pro_tp1_r: Optional[float] = None
    chart_pro_tp2_r: Optional[float] = None
    chart_pro_runner_trail_atr_mult: Optional[float] = None
    chart_pro_max_hold_sec: Optional[int] = None
    chart_pro_cooldown_sec: Optional[int] = None
    chart_watch_max_tokens: Optional[int] = None
    chart_watch_focus_profile: Optional[str] = None
    chart_watch_admission_min_volume_m5_usd: Optional[float] = None
    chart_watch_admission_min_mcap_usd: Optional[float] = None
    chart_watch_admission_max_mcap_usd: Optional[float] = None
    chart_watch_admission_min_liquidity_usd: Optional[float] = None
    chart_watch_admission_max_age_sec: Optional[int] = None
    chart_watch_strict_age_sec: Optional[int] = None
    chart_watch_trend_min_volume_m5_usd: Optional[float] = None
    chart_watch_trend_min_liquidity_usd: Optional[float] = None
    chart_watch_trend_min_mcap_usd: Optional[float] = None
    chart_watch_fail_backoff_base_sec: Optional[int] = None
    chart_watch_fail_backoff_max_sec: Optional[int] = None
    chart_watch_eval_budget_per_cycle: Optional[int] = None
    chart_watch_eval_concurrency: Optional[int] = None
    chart_watch_market_data_timeout_sec: Optional[float] = None
    chart_watch_candle_cache_ttl_sec: Optional[int] = None
    chart_watch_prewarm_candles: Optional[bool] = None
    chart_watch_candle_timeout_sec: Optional[float] = None
    chart_watch_eval_timeout_sec: Optional[float] = None
    chart_watch_startup_burst_enabled: Optional[bool] = None
    chart_watch_startup_burst_target: Optional[int] = None
    chart_watch_startup_burst_timeout_sec: Optional[int] = None
    chart_watch_source_mode: Optional[str] = None
    chart_watch_table_mode: Optional[str] = None
    chart_watch_hide_filtered_in_ui: Optional[bool] = None
    market_trend_source_enabled: Optional[bool] = None
    market_trend_scan_interval_sec: Optional[int] = None
    market_trend_requeue_cooldown_sec: Optional[int] = None
    market_trend_max_new_per_scan: Optional[int] = None
    autopilot_enabled: Optional[bool] = None
    autopilot_interval_sec: Optional[int] = None
    autopilot_window_sec: Optional[int] = None
    autopilot_no_trade_window_sec: Optional[int] = None
    autopilot_max_daily_drawdown_sol: Optional[float] = None
    autopilot_max_loss_streak: Optional[int] = None
    autopilot_target_trades_per_hour: Optional[float] = None
    autopilot_reason_weighted_tuning_enabled: Optional[bool] = None
    autopilot_reason_low_score_threshold_pct: Optional[float] = None
    autopilot_reason_rate_limited_threshold_pct: Optional[float] = None
    autopilot_step_chart_score: Optional[float] = None
    autopilot_step_volume_gate_pct: Optional[float] = None
    autopilot_min_chart_score_floor: Optional[float] = None
    autopilot_min_volume_gate_floor_usd: Optional[float] = None
    autopilot_min_liquidity_gate_floor_usd: Optional[float] = None
    autopilot_min_mcap_gate_floor_usd: Optional[float] = None
    wallet_age_cluster_check_enabled: Optional[bool] = None
    wallet_age_cluster_min_sample: Optional[int] = None
    wallet_age_cluster_recent_window_hours: Optional[float] = None
    wallet_age_cluster_min_recent_share_pct: Optional[float] = None
    wallet_age_cluster_min_same_hour_pct: Optional[float] = None


class SwapQuoteRequest(BaseModel):
    input_mint: str
    output_mint: str
    amount: int
    slippage_bps: int = 100


class SwapBuildRequest(BaseModel):
    quote_response: dict
    user_public_key: str


class LiveBuyRecordRequest(BaseModel):
    token: str
    token_amount: float
    sol_spent: float
    tx_sig: str = ""
    entry_fdv: float = 0.0
    strategy_mode: str = ""
    strategy_meta: Optional[dict] = None


class LiveSellRecordRequest(BaseModel):
    token: str
    token_amount_sold: float
    sol_received: float
    reason: str = ""
    tx_sig: str = ""


class KeyImportRequest(BaseModel):
    private_key: str
    enable_trading: bool = True


class ClosePositionRequest(BaseModel):
    signature: Optional[str] = None
    token: str = ""
    reason: str = "MANUAL_CLOSE"


class LiveReconcileRequest(BaseModel):
    owner: str = ""


class LiveCloseRequest(BaseModel):
    token: str
    reason: str = "MANUAL_CLOSE"


class LiveArchiveResetRequest(BaseModel):
    confirm_text: str
    force: bool = False
    reset_live_analytics: bool = True
    archive_tag: str = ""


class PaperArchiveResetRequest(BaseModel):
    confirm_text: str
    force: bool = False
    reset_paper_analytics: bool = True
    archive_tag: str = ""

@app.get("/api/settings")
async def get_settings():
    """Get current settings"""
    return service.settings


@app.get("/api/chart-watch")
async def get_chart_watch():
    """Returns current chart watchlist snapshot for dashboard rendering."""
    return _chart_watch_snapshot()


@app.get("/api/autopilot")
async def get_autopilot():
    """Returns optimizer runtime state and recent actions."""
    state = _autopilot_current_state()
    history = []
    for row in list(state.get("history", deque()) or []):
        history.append(dict(row or {}))
    return {
        "enabled": _autopilot_enabled(),
        "interval_sec": _autopilot_interval_sec(),
        "window_sec": _autopilot_window_sec(),
        "no_trade_window_sec": _autopilot_no_trade_window_sec(),
        "last_run_ts": _safe_float(state.get("last_run_ts", 0.0), 0.0),
        "last_action": str(state.get("last_action", "idle") or "idle"),
        "last_reason": str(state.get("last_reason", "") or ""),
        "last_window_trades": int(state.get("last_window_trades", 0) or 0),
        "last_window_pnl_sol": float(state.get("last_window_pnl_sol", 0.0) or 0.0),
        "relax_level": int(state.get("relax_level", 0) or 0),
        "last_good_ts": _safe_float(state.get("last_good_ts", 0.0), 0.0),
        "last_good_settings": dict(state.get("last_good_settings", {}) or {}),
        "current_tunables": _autopilot_tunable_snapshot(),
        "history": history[-40:],
    }

@app.post("/api/settings")
async def update_settings(settings: SettingsUpdate):
    """Update SOL amount settings"""
    if settings.sol_amount_min <= 0 or settings.sol_amount_max <= 0:
        return JSONResponse(status_code=400, content={"error": "SOL amounts must be positive"})
    
    if settings.sol_amount_min > settings.sol_amount_max:
        return JSONResponse(status_code=400, content={"error": "Min must be less than or equal to max"})
    
    service.settings["sol_amount_min"] = settings.sol_amount_min
    service.settings["sol_amount_max"] = settings.sol_amount_max
    should_reset_startup_burst = False
    if settings.max_live_open_positions is not None:
        max_live = int(settings.max_live_open_positions)
        if max_live < 0:
            return JSONResponse(status_code=400, content={"error": "max_live_open_positions must be >= 0"})
        service.settings["max_live_open_positions"] = max_live
    if settings.token_cooldown_sec is not None:
        cooldown = int(settings.token_cooldown_sec)
        if cooldown < 0:
            return JSONResponse(status_code=400, content={"error": "token_cooldown_sec must be >= 0"})
        service.settings["token_cooldown_sec"] = cooldown
    if settings.max_token_age_sec is not None:
        max_age = int(settings.max_token_age_sec)
        if max_age < 0:
            return JSONResponse(status_code=400, content={"error": "max_token_age_sec must be >= 0"})
        service.settings["max_token_age_sec"] = max_age

    min_mcap_updates = {
        "kol_min_mcap_usd": settings.kol_min_mcap_usd,
        "early_20_min_mcap_usd": settings.early_20_min_mcap_usd,
        "launch_burst_min_mcap_usd": settings.launch_burst_min_mcap_usd,
        "chart_pro_min_mcap_usd": settings.chart_pro_min_mcap_usd,
    }
    for key, value in min_mcap_updates.items():
        if value is None:
            continue
        v = float(value)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": f"{key} must be >= 0"})
        service.settings[key] = v
    if settings.chart_pro_min_liquidity_usd is not None:
        v = float(settings.chart_pro_min_liquidity_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_pro_min_liquidity_usd must be >= 0"})
        service.settings["chart_pro_min_liquidity_usd"] = v

    if settings.chart_pro_enabled is not None:
        service.settings["chart_pro_enabled"] = bool(settings.chart_pro_enabled)
    if settings.chart_pro_entry_tf is not None:
        tf = str(settings.chart_pro_entry_tf or "").strip().lower()
        if tf not in ("1m", "1"):
            return JSONResponse(status_code=400, content={"error": "chart_pro_entry_tf must be '1m'"})
        service.settings["chart_pro_entry_tf"] = "1m"
    if settings.chart_pro_bias_tf is not None:
        tf = str(settings.chart_pro_bias_tf or "").strip().lower()
        if tf not in ("5m", "5"):
            return JSONResponse(status_code=400, content={"error": "chart_pro_bias_tf must be '5m'"})
        service.settings["chart_pro_bias_tf"] = "5m"
    if settings.chart_pro_breakout_lookback_bars is not None:
        v = int(settings.chart_pro_breakout_lookback_bars)
        if v < 5:
            return JSONResponse(status_code=400, content={"error": "chart_pro_breakout_lookback_bars must be >= 5"})
        service.settings["chart_pro_breakout_lookback_bars"] = v
    if settings.chart_pro_min_volume_multiple is not None:
        v = float(settings.chart_pro_min_volume_multiple)
        if v <= 0:
            return JSONResponse(status_code=400, content={"error": "chart_pro_min_volume_multiple must be > 0"})
        service.settings["chart_pro_min_volume_multiple"] = v
    if settings.chart_pro_min_score is not None:
        v = float(settings.chart_pro_min_score)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_pro_min_score must be >= 0"})
        service.settings["chart_pro_min_score"] = v
    if settings.chart_pro_require_retest is not None:
        service.settings["chart_pro_require_retest"] = bool(settings.chart_pro_require_retest)
    if settings.chart_pro_stop_atr_mult is not None:
        v = float(settings.chart_pro_stop_atr_mult)
        if v <= 0:
            return JSONResponse(status_code=400, content={"error": "chart_pro_stop_atr_mult must be > 0"})
        service.settings["chart_pro_stop_atr_mult"] = v
    if settings.chart_pro_tp1_r is not None:
        v = float(settings.chart_pro_tp1_r)
        if v <= 0:
            return JSONResponse(status_code=400, content={"error": "chart_pro_tp1_r must be > 0"})
        service.settings["chart_pro_tp1_r"] = v
    if settings.chart_pro_tp2_r is not None:
        v = float(settings.chart_pro_tp2_r)
        if v <= 0:
            return JSONResponse(status_code=400, content={"error": "chart_pro_tp2_r must be > 0"})
        service.settings["chart_pro_tp2_r"] = v
    if settings.chart_pro_runner_trail_atr_mult is not None:
        v = float(settings.chart_pro_runner_trail_atr_mult)
        if v <= 0:
            return JSONResponse(status_code=400, content={"error": "chart_pro_runner_trail_atr_mult must be > 0"})
        service.settings["chart_pro_runner_trail_atr_mult"] = v
    if settings.chart_pro_max_hold_sec is not None:
        v = int(settings.chart_pro_max_hold_sec)
        if v < 1:
            return JSONResponse(status_code=400, content={"error": "chart_pro_max_hold_sec must be >= 1"})
        service.settings["chart_pro_max_hold_sec"] = v
    if settings.chart_pro_cooldown_sec is not None:
        v = int(settings.chart_pro_cooldown_sec)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_pro_cooldown_sec must be >= 0"})
        service.settings["chart_pro_cooldown_sec"] = v
    if settings.chart_watch_max_tokens is not None:
        v = int(settings.chart_watch_max_tokens)
        if v < 1 or v > 250:
            return JSONResponse(status_code=400, content={"error": "chart_watch_max_tokens must be between 1 and 250"})
        service.settings["chart_watch_max_tokens"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_focus_profile is not None:
        profile = str(settings.chart_watch_focus_profile or "").strip().lower()
        if profile not in ("quality_first", "balanced", "high_frequency", "volatility_first", "momentum_first"):
            return JSONResponse(
                status_code=400,
                content={
                    "error": (
                        "chart_watch_focus_profile must be "
                        "quality_first, balanced, high_frequency, volatility_first, or momentum_first"
                    )
                },
            )
        service.settings["chart_watch_focus_profile"] = profile
        should_reset_startup_burst = True
    if settings.chart_watch_admission_min_volume_m5_usd is not None:
        v = float(settings.chart_watch_admission_min_volume_m5_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_admission_min_volume_m5_usd must be >= 0"})
        service.settings["chart_watch_admission_min_volume_m5_usd"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_admission_min_mcap_usd is not None:
        v = float(settings.chart_watch_admission_min_mcap_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_admission_min_mcap_usd must be >= 0"})
        service.settings["chart_watch_admission_min_mcap_usd"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_admission_max_mcap_usd is not None:
        v = float(settings.chart_watch_admission_max_mcap_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_admission_max_mcap_usd must be >= 0"})
        service.settings["chart_watch_admission_max_mcap_usd"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_admission_min_liquidity_usd is not None:
        v = float(settings.chart_watch_admission_min_liquidity_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_admission_min_liquidity_usd must be >= 0"})
        service.settings["chart_watch_admission_min_liquidity_usd"] = v
    if settings.chart_watch_trend_min_volume_m5_usd is not None:
        v = float(settings.chart_watch_trend_min_volume_m5_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_trend_min_volume_m5_usd must be >= 0"})
        service.settings["chart_watch_trend_min_volume_m5_usd"] = v
    if settings.chart_watch_trend_min_liquidity_usd is not None:
        v = float(settings.chart_watch_trend_min_liquidity_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_trend_min_liquidity_usd must be >= 0"})
        service.settings["chart_watch_trend_min_liquidity_usd"] = v
    if settings.chart_watch_trend_min_mcap_usd is not None:
        v = float(settings.chart_watch_trend_min_mcap_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_trend_min_mcap_usd must be >= 0"})
        service.settings["chart_watch_trend_min_mcap_usd"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_admission_max_age_sec is not None:
        v = int(settings.chart_watch_admission_max_age_sec)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_admission_max_age_sec must be >= 0"})
        service.settings["chart_watch_admission_max_age_sec"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_strict_age_sec is not None:
        v = int(settings.chart_watch_strict_age_sec)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_strict_age_sec must be >= 0"})
        service.settings["chart_watch_strict_age_sec"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_fail_backoff_base_sec is not None:
        v = int(settings.chart_watch_fail_backoff_base_sec)
        if v < 1:
            return JSONResponse(status_code=400, content={"error": "chart_watch_fail_backoff_base_sec must be >= 1"})
        service.settings["chart_watch_fail_backoff_base_sec"] = v
    if settings.chart_watch_fail_backoff_max_sec is not None:
        v = int(settings.chart_watch_fail_backoff_max_sec)
        if v < 1:
            return JSONResponse(status_code=400, content={"error": "chart_watch_fail_backoff_max_sec must be >= 1"})
        service.settings["chart_watch_fail_backoff_max_sec"] = v
    if int(service.settings.get("chart_watch_fail_backoff_max_sec", 600) or 600) < int(service.settings.get("chart_watch_fail_backoff_base_sec", 20) or 20):
        return JSONResponse(status_code=400, content={"error": "chart_watch_fail_backoff_max_sec must be >= chart_watch_fail_backoff_base_sec"})
    if settings.chart_watch_eval_budget_per_cycle is not None:
        v = int(settings.chart_watch_eval_budget_per_cycle)
        if v < 1 or v > 20:
            return JSONResponse(status_code=400, content={"error": "chart_watch_eval_budget_per_cycle must be between 1 and 20"})
        service.settings["chart_watch_eval_budget_per_cycle"] = v
    if settings.chart_watch_eval_concurrency is not None:
        v = int(settings.chart_watch_eval_concurrency)
        if v < 1 or v > 8:
            return JSONResponse(status_code=400, content={"error": "chart_watch_eval_concurrency must be between 1 and 8"})
        service.settings["chart_watch_eval_concurrency"] = v
    if settings.chart_watch_market_data_timeout_sec is not None:
        v = float(settings.chart_watch_market_data_timeout_sec)
        if v < 0.4 or v > 10.0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_market_data_timeout_sec must be between 0.4 and 10.0"})
        service.settings["chart_watch_market_data_timeout_sec"] = v
    if settings.chart_watch_candle_cache_ttl_sec is not None:
        v = int(settings.chart_watch_candle_cache_ttl_sec)
        if v < 5 or v > 300:
            return JSONResponse(status_code=400, content={"error": "chart_watch_candle_cache_ttl_sec must be between 5 and 300"})
        service.settings["chart_watch_candle_cache_ttl_sec"] = v
    if settings.chart_watch_prewarm_candles is not None:
        service.settings["chart_watch_prewarm_candles"] = bool(settings.chart_watch_prewarm_candles)
    if settings.chart_watch_candle_timeout_sec is not None:
        v = float(settings.chart_watch_candle_timeout_sec)
        if v < 1.0 or v > 20.0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_candle_timeout_sec must be between 1.0 and 20.0"})
        service.settings["chart_watch_candle_timeout_sec"] = v
    if settings.chart_watch_eval_timeout_sec is not None:
        v = float(settings.chart_watch_eval_timeout_sec)
        if v < 2.0 or v > 30.0:
            return JSONResponse(status_code=400, content={"error": "chart_watch_eval_timeout_sec must be between 2.0 and 30.0"})
        service.settings["chart_watch_eval_timeout_sec"] = v
    if settings.chart_watch_startup_burst_enabled is not None:
        service.settings["chart_watch_startup_burst_enabled"] = bool(settings.chart_watch_startup_burst_enabled)
        should_reset_startup_burst = True
    if settings.chart_watch_startup_burst_target is not None:
        v = int(settings.chart_watch_startup_burst_target)
        if v < 1 or v > 100:
            return JSONResponse(status_code=400, content={"error": "chart_watch_startup_burst_target must be between 1 and 100"})
        service.settings["chart_watch_startup_burst_target"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_startup_burst_timeout_sec is not None:
        v = int(settings.chart_watch_startup_burst_timeout_sec)
        if v < 5 or v > 300:
            return JSONResponse(status_code=400, content={"error": "chart_watch_startup_burst_timeout_sec must be between 5 and 300"})
        service.settings["chart_watch_startup_burst_timeout_sec"] = v
        should_reset_startup_burst = True
    if settings.chart_watch_source_mode is not None:
        mode = str(settings.chart_watch_source_mode or "").strip().lower()
        if mode not in ("birdeye_primary", "public_fallback"):
            return JSONResponse(status_code=400, content={"error": "chart_watch_source_mode must be birdeye_primary or public_fallback"})
        service.settings["chart_watch_source_mode"] = mode
        should_reset_startup_burst = True
    if settings.chart_watch_table_mode is not None:
        mode = str(settings.chart_watch_table_mode or "").strip().lower()
        if mode not in ("admitted_only", "all"):
            return JSONResponse(status_code=400, content={"error": "chart_watch_table_mode must be admitted_only or all"})
        service.settings["chart_watch_table_mode"] = mode
        if mode == "admitted_only":
            service.settings["chart_watch_hide_filtered_in_ui"] = True
        elif mode == "all":
            service.settings["chart_watch_hide_filtered_in_ui"] = False
    if settings.chart_watch_hide_filtered_in_ui is not None:
        service.settings["chart_watch_hide_filtered_in_ui"] = bool(settings.chart_watch_hide_filtered_in_ui)
    min_mcap = _safe_float(service.settings.get("chart_watch_admission_min_mcap_usd", 20000.0), 20000.0)
    max_mcap = _safe_float(service.settings.get("chart_watch_admission_max_mcap_usd", 100000000.0), 100000000.0)
    if max_mcap > 0 and min_mcap > max_mcap:
        return JSONResponse(status_code=400, content={"error": "chart_watch_admission_max_mcap_usd must be >= chart_watch_admission_min_mcap_usd"})
    if settings.market_trend_source_enabled is not None:
        service.settings["market_trend_source_enabled"] = bool(settings.market_trend_source_enabled)
        should_reset_startup_burst = True
    if settings.market_trend_scan_interval_sec is not None:
        v = int(settings.market_trend_scan_interval_sec)
        if v < 5:
            return JSONResponse(status_code=400, content={"error": "market_trend_scan_interval_sec must be >= 5"})
        service.settings["market_trend_scan_interval_sec"] = v
        should_reset_startup_burst = True
    if settings.market_trend_requeue_cooldown_sec is not None:
        v = int(settings.market_trend_requeue_cooldown_sec)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "market_trend_requeue_cooldown_sec must be >= 0"})
        service.settings["market_trend_requeue_cooldown_sec"] = v
    if settings.market_trend_max_new_per_scan is not None:
        v = int(settings.market_trend_max_new_per_scan)
        if v < 1 or v > 25:
            return JSONResponse(status_code=400, content={"error": "market_trend_max_new_per_scan must be between 1 and 25"})
        service.settings["market_trend_max_new_per_scan"] = v
        should_reset_startup_burst = True
    if settings.autopilot_enabled is not None:
        service.settings["autopilot_enabled"] = bool(settings.autopilot_enabled)
    if settings.autopilot_interval_sec is not None:
        v = int(settings.autopilot_interval_sec)
        if v < 30 or v > 3600:
            return JSONResponse(status_code=400, content={"error": "autopilot_interval_sec must be between 30 and 3600"})
        service.settings["autopilot_interval_sec"] = v
    if settings.autopilot_window_sec is not None:
        v = int(settings.autopilot_window_sec)
        if v < 300 or v > 21600:
            return JSONResponse(status_code=400, content={"error": "autopilot_window_sec must be between 300 and 21600"})
        service.settings["autopilot_window_sec"] = v
    if settings.autopilot_no_trade_window_sec is not None:
        v = int(settings.autopilot_no_trade_window_sec)
        if v < 120 or v > 10800:
            return JSONResponse(status_code=400, content={"error": "autopilot_no_trade_window_sec must be between 120 and 10800"})
        service.settings["autopilot_no_trade_window_sec"] = v
    if int(service.settings.get("autopilot_no_trade_window_sec", 1200) or 1200) > int(service.settings.get("autopilot_window_sec", 3600) or 3600):
        return JSONResponse(status_code=400, content={"error": "autopilot_no_trade_window_sec must be <= autopilot_window_sec"})
    if settings.autopilot_max_daily_drawdown_sol is not None:
        v = float(settings.autopilot_max_daily_drawdown_sol)
        if v <= 0:
            return JSONResponse(status_code=400, content={"error": "autopilot_max_daily_drawdown_sol must be > 0"})
        service.settings["autopilot_max_daily_drawdown_sol"] = v
    if settings.autopilot_max_loss_streak is not None:
        v = int(settings.autopilot_max_loss_streak)
        if v < 1 or v > 20:
            return JSONResponse(status_code=400, content={"error": "autopilot_max_loss_streak must be between 1 and 20"})
        service.settings["autopilot_max_loss_streak"] = v
    if settings.autopilot_target_trades_per_hour is not None:
        v = float(settings.autopilot_target_trades_per_hour)
        if v < 0 or v > 60:
            return JSONResponse(status_code=400, content={"error": "autopilot_target_trades_per_hour must be between 0 and 60"})
        service.settings["autopilot_target_trades_per_hour"] = v
    if settings.autopilot_reason_weighted_tuning_enabled is not None:
        service.settings["autopilot_reason_weighted_tuning_enabled"] = bool(settings.autopilot_reason_weighted_tuning_enabled)
    if settings.autopilot_reason_low_score_threshold_pct is not None:
        v = float(settings.autopilot_reason_low_score_threshold_pct)
        if v < 0 or v > 1:
            return JSONResponse(status_code=400, content={"error": "autopilot_reason_low_score_threshold_pct must be between 0 and 1"})
        service.settings["autopilot_reason_low_score_threshold_pct"] = v
    if settings.autopilot_reason_rate_limited_threshold_pct is not None:
        v = float(settings.autopilot_reason_rate_limited_threshold_pct)
        if v < 0 or v > 1:
            return JSONResponse(status_code=400, content={"error": "autopilot_reason_rate_limited_threshold_pct must be between 0 and 1"})
        service.settings["autopilot_reason_rate_limited_threshold_pct"] = v
    if settings.autopilot_step_chart_score is not None:
        v = float(settings.autopilot_step_chart_score)
        if v <= 0 or v > 20:
            return JSONResponse(status_code=400, content={"error": "autopilot_step_chart_score must be between 0 and 20"})
        service.settings["autopilot_step_chart_score"] = v
    if settings.autopilot_step_volume_gate_pct is not None:
        v = float(settings.autopilot_step_volume_gate_pct)
        if v <= 0 or v >= 1:
            return JSONResponse(status_code=400, content={"error": "autopilot_step_volume_gate_pct must be between 0 and 1"})
        service.settings["autopilot_step_volume_gate_pct"] = v
    if settings.autopilot_min_chart_score_floor is not None:
        v = float(settings.autopilot_min_chart_score_floor)
        if v < 0 or v > 100:
            return JSONResponse(status_code=400, content={"error": "autopilot_min_chart_score_floor must be between 0 and 100"})
        service.settings["autopilot_min_chart_score_floor"] = v
    if settings.autopilot_min_volume_gate_floor_usd is not None:
        v = float(settings.autopilot_min_volume_gate_floor_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "autopilot_min_volume_gate_floor_usd must be >= 0"})
        service.settings["autopilot_min_volume_gate_floor_usd"] = v
    if settings.autopilot_min_liquidity_gate_floor_usd is not None:
        v = float(settings.autopilot_min_liquidity_gate_floor_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "autopilot_min_liquidity_gate_floor_usd must be >= 0"})
        service.settings["autopilot_min_liquidity_gate_floor_usd"] = v
    if settings.autopilot_min_mcap_gate_floor_usd is not None:
        v = float(settings.autopilot_min_mcap_gate_floor_usd)
        if v < 0:
            return JSONResponse(status_code=400, content={"error": "autopilot_min_mcap_gate_floor_usd must be >= 0"})
        service.settings["autopilot_min_mcap_gate_floor_usd"] = v
    if settings.wallet_age_cluster_check_enabled is not None:
        service.settings["wallet_age_cluster_check_enabled"] = bool(settings.wallet_age_cluster_check_enabled)
    if settings.wallet_age_cluster_min_sample is not None:
        v = int(settings.wallet_age_cluster_min_sample)
        if v < 2 or v > 30:
            return JSONResponse(status_code=400, content={"error": "wallet_age_cluster_min_sample must be between 2 and 30"})
        service.settings["wallet_age_cluster_min_sample"] = v
    if settings.wallet_age_cluster_recent_window_hours is not None:
        v = float(settings.wallet_age_cluster_recent_window_hours)
        if v <= 0 or v > 720:
            return JSONResponse(status_code=400, content={"error": "wallet_age_cluster_recent_window_hours must be between 0 and 720"})
        service.settings["wallet_age_cluster_recent_window_hours"] = v
    if settings.wallet_age_cluster_min_recent_share_pct is not None:
        v = float(settings.wallet_age_cluster_min_recent_share_pct)
        if v < 0 or v > 100:
            return JSONResponse(status_code=400, content={"error": "wallet_age_cluster_min_recent_share_pct must be between 0 and 100"})
        service.settings["wallet_age_cluster_min_recent_share_pct"] = v
    if settings.wallet_age_cluster_min_same_hour_pct is not None:
        v = float(settings.wallet_age_cluster_min_same_hour_pct)
        if v < 0 or v > 100:
            return JSONResponse(status_code=400, content={"error": "wallet_age_cluster_min_same_hour_pct must be between 0 and 100"})
        service.settings["wallet_age_cluster_min_same_hour_pct"] = v

    if settings.trading_modes is not None:
        modes = []
        for m in settings.trading_modes:
            mode = str(m or "").strip().lower()
            if mode in ALLOWED_TRADING_MODES and mode not in modes:
                modes.append(mode)
        if not modes:
            return JSONResponse(status_code=400, content={"error": "At least one valid trading mode is required"})
        service.settings["trading_modes"] = modes
        service.settings["trading_mode"] = modes[0]
        should_reset_startup_burst = True
    elif settings.trading_mode is not None:
        mode = str(settings.trading_mode).strip().lower()
        if mode not in ALLOWED_TRADING_MODES:
            return JSONResponse(status_code=400, content={"error": f"Invalid trading_mode: {mode}"})
        service.settings["trading_mode"] = mode
        service.settings["trading_modes"] = [mode]
        should_reset_startup_burst = True
    else:
        modes = _normalize_trading_modes_from_settings(service.settings)
        service.settings["trading_modes"] = modes
        service.settings["trading_mode"] = modes[0]
    if settings.chart_pro_enabled is not None:
        should_reset_startup_burst = True
    if should_reset_startup_burst:
        service.chart_watch_startup_burst_done = False
        service.chart_watch_startup_burst_active = False
        service.chart_watch_startup_burst_admitted = 0
    autopilot_state = _autopilot_current_state()
    autopilot_state["baseline_settings"] = _autopilot_tunable_snapshot()
    await _sync_kol_monitor_state()
    service.save_settings()
    await _broadcast_chart_watch_snapshot(force=True)
    
    return {"status": "ok", "settings": service.settings}


async def _rpc_json(method: str, params: list):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(service.rpc_url, json=payload, timeout=20) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data.get("result")
    except Exception:
        return None


def _parse_token_balance_result(result: dict):
    if not result:
        return {"raw_amount": "0", "ui_amount": 0.0, "decimals": 0}

    raw_total = 0
    ui_total = 0.0
    decimals = 0
    for item in result.get("value", []):
        try:
            amt = item.get("account", {}).get("data", {}).get("parsed", {}).get("info", {}).get("tokenAmount", {})
            raw_total += int(amt.get("amount", "0") or "0")
            ui_total += float(amt.get("uiAmount", 0) or 0)
            decimals = int(amt.get("decimals", decimals))
        except Exception:
            continue
    return {"raw_amount": str(raw_total), "ui_amount": ui_total, "decimals": decimals}


def _parse_ui_token_amount(balance_entry: dict) -> float:
    try:
        amt = (balance_entry or {}).get("uiTokenAmount", {}) or {}
        ui = amt.get("uiAmount", None)
        if ui is not None:
            return float(ui)
        s = str(amt.get("uiAmountString", "0") or "0").strip()
        return float(s) if s else 0.0
    except Exception:
        return 0.0


def _sum_owner_mint_token_balance(rows: list, owner_wallet: str, mint: str) -> float:
    if not rows:
        return 0.0
    total = 0.0
    for r in rows:
        try:
            owner = str((r or {}).get("owner", "") or "")
            m = str((r or {}).get("mint", "") or "")
            if owner != owner_wallet or m != mint:
                continue
            total += _parse_ui_token_amount(r or {})
        except Exception:
            continue
    return total


async def _resolve_kol_sol_amount(signature: str, wallet_addr: str) -> tuple[float, bool]:
    """
    Resolves KOL trade SOL size from parsed transaction:
    1) native lamport delta on the tracked wallet account
    2) wrapped SOL (WSOL) token-balance delta fallback
    Returns (amount_sol, known).
    """
    sig = str(signature or "").strip()
    owner = str(wallet_addr or "").strip()
    if not sig or not owner:
        return 0.0, False

    tx = None
    for i in range(4):
        tx = await _rpc_json(
            "getTransaction",
            [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}],
        )
        if tx:
            break
        if i < 3:
            await asyncio.sleep(0.6 + (i * 0.4))
    if not tx:
        return 0.0, False

    try:
        message = ((tx or {}).get("transaction", {}) or {}).get("message", {}) or {}
        keys_raw = message.get("accountKeys", []) or []
        pre_bal = ((tx or {}).get("meta", {}) or {}).get("preBalances", []) or []
        post_bal = ((tx or {}).get("meta", {}) or {}).get("postBalances", []) or []

        keys = []
        for k in keys_raw:
            if isinstance(k, dict):
                keys.append(str(k.get("pubkey", "") or ""))
            else:
                keys.append(str(k or ""))

        if owner in keys:
            idx = keys.index(owner)
            if idx < len(pre_bal) and idx < len(post_bal):
                pre = float(pre_bal[idx] or 0.0)
                post = float(post_bal[idx] or 0.0)
                lamport_delta = abs(post - pre) / 1_000_000_000.0
                if lamport_delta > 0:
                    return lamport_delta, True
    except Exception:
        pass

    try:
        meta = (tx or {}).get("meta", {}) or {}
        pre_tb = meta.get("preTokenBalances", []) or []
        post_tb = meta.get("postTokenBalances", []) or []
        pre_wsol = _sum_owner_mint_token_balance(pre_tb, owner, SOL_MINT)
        post_wsol = _sum_owner_mint_token_balance(post_tb, owner, SOL_MINT)
        wsol_delta = abs(post_wsol - pre_wsol)
        if wsol_delta > 0:
            return wsol_delta, True
    except Exception:
        pass

    return 0.0, False


async def _send_telegram_message(text: str) -> bool:
    if not bool(service.settings.get("telegram_notify_live_trades", True)):
        return False

    bot_token = str(service.settings.get("telegram_bot_token", "") or "").strip()
    chat_id = str(service.settings.get("telegram_chat_id", "") or "").strip()
    if not bot_token:
        bot_token = str(os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    if not chat_id:
        chat_id = str(os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
    if not bot_token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "disable_notification": bool(service.settings.get("telegram_silent", False)),
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=12) as resp:
                return resp.status == 200
    except Exception:
        return False


async def _notify_live_trade(
    side: str,
    token: str,
    sol_amount: float = 0.0,
    tx_sig: str = "",
    pnl_sol: Optional[float] = None,
    reason: str = "",
):
    side_u = str(side or "").upper()
    icon = "🟢" if side_u == "BUY" else "🔴"
    token_short = html.escape((token or "")[:8] + "...")
    body = [
        f"{icon} <b>LIVE {html.escape(side_u)}</b>",
        f"<b>Token:</b> <code>{token_short}</code>",
    ]
    if sol_amount and sol_amount > 0:
        body.append(f"<b>SOL:</b> {sol_amount:.4f}")
    if pnl_sol is not None:
        sign = "+" if pnl_sol >= 0 else ""
        body.append(f"<b>PNL:</b> {sign}{pnl_sol:.4f} SOL")
    if reason:
        body.append(f"<b>Reason:</b> {html.escape(reason)}")
    if tx_sig:
        tx_sig_safe = html.escape(tx_sig)
        body.append(f"<b>Tx:</b> <a href=\"https://solscan.io/tx/{tx_sig_safe}\">solscan</a>")

    msg = "\n".join(body)
    await _send_telegram_message(msg)


def _get_enabled_trading_modes() -> List[str]:
    modes = _normalize_trading_modes_from_settings(service.settings)
    service.settings["trading_modes"] = list(modes)
    service.settings["trading_mode"] = modes[0]
    return modes


def _get_trading_mode() -> str:
    # Backward-compatible helper for code paths that still expect one mode.
    return _get_enabled_trading_modes()[0]


def _trading_mode_label(mode: str) -> str:
    if mode == TRADING_MODE_EARLY_20:
        return "Early 20 Scalper"
    if mode == TRADING_MODE_LAUNCH_BURST:
        return "KOL Launch Burst Scalper"
    if mode == TRADING_MODE_CHART_PRO:
        return "Chart Pro Breakout Scalper"
    return "KOL Momentum Scalper"


def _mode_kol_min_sol(mode: str) -> float:
    if mode == TRADING_MODE_EARLY_20:
        return float(service.settings.get("early_20_kol_min_sol", 0.10) or 0.10)
    if mode == TRADING_MODE_LAUNCH_BURST:
        return float(service.settings.get("launch_burst_kol_min_sol", 0.10) or 0.10)
    if mode == TRADING_MODE_CHART_PRO:
        return float(service.settings.get("early_20_kol_min_sol", 0.10) or 0.10)
    return 0.23


def _format_mode_labels(modes: List[str]) -> str:
    labels = [_trading_mode_label(m) for m in (modes or [])]
    return ", ".join(labels) if labels else _trading_mode_label(TRADING_MODE_KOL_MOMENTUM)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _safe_archive_tag(tag: str) -> str:
    raw = str(tag or "").strip()
    if not raw:
        return ""
    cleaned = "".join(ch for ch in raw if ch.isalnum() or ch in ("-", "_"))
    return cleaned[:40]


def _chart_watch_enabled() -> bool:
    enabled_modes = _normalize_trading_modes_from_settings(service.settings)
    return TRADING_MODE_CHART_PRO in enabled_modes and bool(service.settings.get("chart_pro_enabled", True))


def _chart_watch_max_tokens() -> int:
    return max(1, min(250, _safe_int(service.settings.get("chart_watch_max_tokens", 16), 16)))


def _chart_watch_eval_interval_sec() -> float:
    profile = str(service.settings.get("chart_watch_focus_profile", "quality_first") or "quality_first").strip().lower()
    if profile == "high_frequency":
        return 2.0
    if profile == "balanced":
        return 3.0
    return 4.0


def _chart_watch_priority_age_sec() -> int:
    # Prioritize newer launches (<24h old).
    return 24 * 3600


def _chart_watch_fade_volume_m5_usd() -> float:
    # Stop watching after exit once volume has faded.
    return max(0.0, _safe_float(service.settings.get("chart_watch_admission_min_volume_m5_usd", 12000.0), 12000.0) * 0.35)


def _chart_watch_stale_sec() -> int:
    # Drop idle watches that no longer update.
    profile = str(service.settings.get("chart_watch_focus_profile", "quality_first") or "quality_first").strip().lower()
    if profile == "high_frequency":
        return 45 * 60
    if profile == "balanced":
        return 75 * 60
    return 2 * 3600


def _chart_watch_eval_budget_per_cycle() -> int:
    return max(1, min(20, _safe_int(service.settings.get("chart_watch_eval_budget_per_cycle", 8), 8)))


def _chart_watch_eval_concurrency() -> int:
    return max(1, min(8, _safe_int(service.settings.get("chart_watch_eval_concurrency", 3), 3)))


def _chart_watch_market_data_timeout_sec() -> float:
    return max(0.4, min(10.0, _safe_float(service.settings.get("chart_watch_market_data_timeout_sec", 2.5), 2.5)))


def _chart_watch_candle_cache_ttl_sec() -> int:
    return max(5, min(300, _safe_int(service.settings.get("chart_watch_candle_cache_ttl_sec", 30), 30)))


def _chart_watch_prewarm_candles_enabled() -> bool:
    return bool(service.settings.get("chart_watch_prewarm_candles", True))


def _chart_watch_startup_burst_enabled() -> bool:
    return bool(service.settings.get("chart_watch_startup_burst_enabled", True))


def _chart_watch_startup_burst_target() -> int:
    return max(1, min(100, _safe_int(service.settings.get("chart_watch_startup_burst_target", 12), 12)))


def _chart_watch_startup_burst_timeout_sec() -> int:
    return max(5, min(300, _safe_int(service.settings.get("chart_watch_startup_burst_timeout_sec", 30), 30)))


def _chart_watch_backoff_bounds() -> tuple[int, int]:
    base = max(1, _safe_int(service.settings.get("chart_watch_fail_backoff_base_sec", 20), 20))
    cap = max(base, _safe_int(service.settings.get("chart_watch_fail_backoff_max_sec", 600), 600))
    return base, cap


def _chart_watch_admission_max_age_sec() -> int:
    return max(0, _safe_int(service.settings.get("chart_watch_admission_max_age_sec", 86400), 86400))


def _chart_watch_strict_age_sec() -> int:
    strict_v = _safe_int(service.settings.get("chart_watch_strict_age_sec", -1), -1)
    admission_v = _chart_watch_admission_max_age_sec()
    if strict_v >= 0:
        strict_v = max(0, strict_v)
        if admission_v > 0:
            return min(strict_v, admission_v)
        return strict_v
    return admission_v


def _chart_watch_admission_mcap_band() -> tuple[float, float]:
    min_mcap = max(0.0, _safe_float(service.settings.get("chart_watch_admission_min_mcap_usd", 20000.0), 20000.0))
    max_mcap = max(0.0, _safe_float(service.settings.get("chart_watch_admission_max_mcap_usd", 100000000.0), 100000000.0))
    if max_mcap > 0 and max_mcap < min_mcap:
        max_mcap = min_mcap
    return min_mcap, max_mcap


def _chart_watch_source_mode() -> str:
    mode = str(service.settings.get("chart_watch_source_mode", "birdeye_primary") or "birdeye_primary").strip().lower()
    if mode not in ("birdeye_primary", "public_fallback"):
        return "birdeye_primary"
    return mode


def _chart_watch_table_mode() -> str:
    mode = str(service.settings.get("chart_watch_table_mode", "admitted_only") or "admitted_only").strip().lower()
    if mode not in ("admitted_only", "all"):
        return "admitted_only"
    return mode


def _chart_watch_focus_profile() -> str:
    profile = str(service.settings.get("chart_watch_focus_profile", "quality_first") or "quality_first").strip().lower()
    if profile not in ("quality_first", "balanced", "high_frequency", "volatility_first", "momentum_first"):
        return "quality_first"
    return profile


AUTOPILOT_TUNABLE_KEYS = (
    "market_trend_scan_interval_sec",
    "market_trend_max_new_per_scan",
    "chart_watch_max_tokens",
    "chart_watch_eval_budget_per_cycle",
    "chart_watch_eval_concurrency",
    "chart_watch_market_data_timeout_sec",
    "chart_pro_min_score",
    "chart_watch_admission_min_volume_m5_usd",
    "chart_watch_admission_min_liquidity_usd",
    "chart_watch_admission_min_mcap_usd",
    "chart_watch_candle_cache_ttl_sec",
)


def _autopilot_enabled() -> bool:
    return bool(service.settings.get("autopilot_enabled", True))


def _autopilot_interval_sec() -> int:
    return max(30, min(3600, _safe_int(service.settings.get("autopilot_interval_sec", 300), 300)))


def _autopilot_window_sec() -> int:
    return max(300, min(21600, _safe_int(service.settings.get("autopilot_window_sec", 3600), 3600)))


def _autopilot_no_trade_window_sec() -> int:
    v = max(120, min(10800, _safe_int(service.settings.get("autopilot_no_trade_window_sec", 1200), 1200)))
    return min(v, _autopilot_window_sec())


def _autopilot_drawdown_limit_sol() -> float:
    return max(0.01, _safe_float(service.settings.get("autopilot_max_daily_drawdown_sol", 0.40), 0.40))


def _autopilot_max_loss_streak() -> int:
    return max(1, min(20, _safe_int(service.settings.get("autopilot_max_loss_streak", 4), 4)))


def _autopilot_target_trades_per_hour() -> float:
    return max(0.0, min(60.0, _safe_float(service.settings.get("autopilot_target_trades_per_hour", 2.0), 2.0)))


def _autopilot_reason_weighted_enabled() -> bool:
    return bool(service.settings.get("autopilot_reason_weighted_tuning_enabled", True))


def _autopilot_low_score_threshold_pct() -> float:
    return max(0.0, min(1.0, _safe_float(service.settings.get("autopilot_reason_low_score_threshold_pct", 0.55), 0.55)))


def _autopilot_rate_limited_threshold_pct() -> float:
    return max(0.0, min(1.0, _safe_float(service.settings.get("autopilot_reason_rate_limited_threshold_pct", 0.10), 0.10)))


def _autopilot_step_chart_score() -> float:
    return max(0.1, min(20.0, _safe_float(service.settings.get("autopilot_step_chart_score", 2.0), 2.0)))


def _autopilot_step_volume_gate_pct() -> float:
    return max(0.01, min(0.95, _safe_float(service.settings.get("autopilot_step_volume_gate_pct", 0.12), 0.12)))


def _autopilot_tunable_snapshot() -> dict:
    return {
        "market_trend_scan_interval_sec": max(5, _safe_int(service.settings.get("market_trend_scan_interval_sec", 8), 8)),
        "market_trend_max_new_per_scan": max(1, _safe_int(service.settings.get("market_trend_max_new_per_scan", 12), 12)),
        "chart_watch_max_tokens": max(1, _safe_int(service.settings.get("chart_watch_max_tokens", 24), 24)),
        "chart_watch_eval_budget_per_cycle": max(1, _safe_int(service.settings.get("chart_watch_eval_budget_per_cycle", 16), 16)),
        "chart_watch_eval_concurrency": max(1, _safe_int(service.settings.get("chart_watch_eval_concurrency", 3), 3)),
        "chart_watch_market_data_timeout_sec": max(0.4, min(10.0, _safe_float(service.settings.get("chart_watch_market_data_timeout_sec", 2.5), 2.5))),
        "chart_pro_min_score": max(0.0, _safe_float(service.settings.get("chart_pro_min_score", 34.0), 34.0)),
        "chart_watch_admission_min_volume_m5_usd": max(0.0, _safe_float(service.settings.get("chart_watch_admission_min_volume_m5_usd", 3000.0), 3000.0)),
        "chart_watch_admission_min_liquidity_usd": max(0.0, _safe_float(service.settings.get("chart_watch_admission_min_liquidity_usd", 2800.0), 2800.0)),
        "chart_watch_admission_min_mcap_usd": max(0.0, _safe_float(service.settings.get("chart_watch_admission_min_mcap_usd", 15000.0), 15000.0)),
        "chart_watch_candle_cache_ttl_sec": max(5, _safe_int(service.settings.get("chart_watch_candle_cache_ttl_sec", 30), 30)),
    }


def _autopilot_collect_closed_chart_trades() -> List[dict]:
    rows: List[dict] = []
    try:
        if paper_trader:
            for t in list(getattr(paper_trader, "closed_positions", []) or []):
                if str(t.get("strategy_mode", "") or "").strip().lower() != TRADING_MODE_CHART_PRO:
                    continue
                rows.append(
                    {
                        "ts": _safe_float(t.get("close_time", 0.0), 0.0),
                        "pnl_sol": _safe_float(t.get("pnl", 0.0), 0.0),
                        "source": "paper",
                    }
                )
    except Exception:
        pass
    try:
        for t in list(getattr(live_trade_tracker, "closed_positions", []) or []):
            if str(t.get("strategy_mode", "") or "").strip().lower() != TRADING_MODE_CHART_PRO:
                continue
            rows.append(
                {
                    "ts": _safe_float(t.get("closed_at", t.get("close_time", 0.0)), 0.0),
                    "pnl_sol": _safe_float(t.get("pnl_sol", t.get("pnl", 0.0)), 0.0),
                    "source": "live",
                }
            )
    except Exception:
        pass
    rows = [r for r in rows if _safe_float(r.get("ts", 0.0), 0.0) > 0]
    rows.sort(key=lambda r: _safe_float(r.get("ts", 0.0), 0.0))
    return rows


def _autopilot_open_chart_positions_count() -> int:
    total = 0
    try:
        if paper_trader:
            for pos in (paper_trader.positions or {}).values():
                if str(pos.get("strategy_mode", "") or "").strip().lower() == TRADING_MODE_CHART_PRO:
                    total += 1
    except Exception:
        pass
    try:
        for pos in live_trade_tracker.get_open_positions():
            if str(pos.get("strategy_mode", "") or "").strip().lower() == TRADING_MODE_CHART_PRO:
                total += 1
    except Exception:
        pass
    return total


def _autopilot_loss_streak(rows_sorted: List[dict]) -> int:
    streak = 0
    for row in reversed(rows_sorted):
        pnl = _safe_float(row.get("pnl_sol", 0.0), 0.0)
        if pnl < 0:
            streak += 1
        else:
            break
    return streak


def _autopilot_current_state() -> dict:
    state = dict(getattr(service, "autopilot_state", {}) or {})
    state.setdefault("history", deque(maxlen=120))
    state.setdefault("last_run_ts", 0.0)
    state.setdefault("last_action", "idle")
    state.setdefault("last_reason", "")
    state.setdefault("last_window_trades", 0)
    state.setdefault("last_window_pnl_sol", 0.0)
    state.setdefault("relax_level", 0)
    state.setdefault("dominant_reason_code", "")
    state.setdefault("dominant_reason_streak", 0)
    state.setdefault("live_entry_pause_until_ts", 0.0)
    state.setdefault("live_entry_pause_ref_trade_ts", 0.0)
    state.setdefault("last_good_ts", 0.0)
    state.setdefault("last_good_settings", {})
    if not state.get("baseline_settings"):
        state["baseline_settings"] = _autopilot_tunable_snapshot()
    service.autopilot_state = state
    return state


def _autopilot_apply_updates(updates: Dict[str, Any], action: str, reason: str, metrics: Optional[dict] = None) -> bool:
    clean: Dict[str, Any] = {}
    for key, value in dict(updates or {}).items():
        if key not in AUTOPILOT_TUNABLE_KEYS:
            continue
        current = service.settings.get(key)
        if isinstance(value, float):
            value = float(value)
            if isinstance(current, (int, float)) and abs(float(current) - value) < 1e-9:
                continue
        else:
            value = int(value)
            if int(current or 0) == value:
                continue
        clean[key] = value

    state = _autopilot_current_state()
    now = time.time()
    state["last_run_ts"] = now
    state["last_action"] = str(action or "hold")
    state["last_reason"] = str(reason or "")
    if metrics:
        state["last_window_trades"] = int(metrics.get("window_trades", 0) or 0)
        state["last_window_pnl_sol"] = float(metrics.get("window_pnl_sol", 0.0) or 0.0)
    if not clean:
        state["history"].append(
            {
                "ts": now,
                "action": state["last_action"],
                "reason": state["last_reason"],
                "changes": {},
                "window_trades": int(state.get("last_window_trades", 0) or 0),
                "window_pnl_sol": float(state.get("last_window_pnl_sol", 0.0) or 0.0),
            }
        )
        return False

    before = {}
    after = {}
    for key, value in clean.items():
        before[key] = service.settings.get(key)
        service.settings[key] = value
        after[key] = value
    service.save_settings()
    state["history"].append(
        {
            "ts": now,
            "action": state["last_action"],
            "reason": state["last_reason"],
            "changes": {k: {"from": before.get(k), "to": after.get(k)} for k in sorted(after.keys())},
            "window_trades": int(state.get("last_window_trades", 0) or 0),
            "window_pnl_sol": float(state.get("last_window_pnl_sol", 0.0) or 0.0),
        }
    )
    return True


def _autopilot_maybe_mark_last_good(metrics: dict):
    if int(metrics.get("window_trades", 0) or 0) < 2:
        return
    if float(metrics.get("window_pnl_sol", 0.0) or 0.0) <= 0:
        return
    if float(metrics.get("window_win_rate", 0.0) or 0.0) < 0.5:
        return
    state = _autopilot_current_state()
    state["last_good_settings"] = _autopilot_tunable_snapshot()
    state["last_good_ts"] = time.time()


def _autopilot_restore_last_good(metrics: dict, reason: str) -> bool:
    state = _autopilot_current_state()
    target = dict(state.get("last_good_settings", {}) or {})
    if not target:
        target = dict(state.get("baseline_settings", {}) or {})
    if not target:
        return False
    changed = _autopilot_apply_updates(target, action="rollback_risk", reason=reason, metrics=metrics)
    if changed:
        state["relax_level"] = 0
    return changed


def _autopilot_collect_metrics(snapshot: Optional[dict] = None) -> dict:
    now = time.time()
    snap = dict(snapshot or _chart_watch_snapshot() or {})
    rows = list(_autopilot_collect_closed_chart_trades())
    window_sec = _autopilot_window_sec()
    no_trade_window_sec = _autopilot_no_trade_window_sec()
    window_cutoff = now - float(window_sec)
    no_trade_cutoff = now - float(no_trade_window_sec)
    day_cutoff = now - 86400.0

    recent = [r for r in rows if _safe_float(r.get("ts", 0.0), 0.0) >= window_cutoff]
    recent_short = [r for r in rows if _safe_float(r.get("ts", 0.0), 0.0) >= no_trade_cutoff]
    day_rows = [r for r in rows if _safe_float(r.get("ts", 0.0), 0.0) >= day_cutoff]
    wins = sum(1 for r in recent if _safe_float(r.get("pnl_sol", 0.0), 0.0) > 0)
    window_trades = len(recent)
    window_pnl = sum(_safe_float(r.get("pnl_sol", 0.0), 0.0) for r in recent)
    day_pnl = sum(_safe_float(r.get("pnl_sol", 0.0), 0.0) for r in day_rows)

    reason_counts: Dict[str, int] = {}
    for row in list(snap.get("tokens", []) or []):
        code = str(row.get("reason_code", "") or "OTHER")
        reason_counts[code] = int(reason_counts.get(code, 0) or 0) + 1
    reason_total = max(1, int(sum(reason_counts.values()) or 0))
    reason_shares = {k: (float(v) / float(reason_total)) for k, v in reason_counts.items()}
    suppressed_count = int(snap.get("suppressed_count", 0) or 0)
    visible_count = int(snap.get("visible_count", 0) or 0)

    return {
        "window_trades": int(window_trades),
        "window_pnl_sol": float(window_pnl),
        "window_win_rate": (float(wins) / float(window_trades)) if window_trades > 0 else 0.0,
        "no_trade_window_trades": int(len(recent_short)),
        "day_pnl_sol": float(day_pnl),
        "loss_streak": int(_autopilot_loss_streak(rows)),
        "open_chart_positions": int(_autopilot_open_chart_positions_count()),
        "admitted_count": int(snap.get("admitted_count", 0) or 0),
        "suppressed_count": suppressed_count,
        "visible_count": visible_count,
        "suppressed_ratio": (float(suppressed_count) / float(max(1, visible_count))),
        "entry_near_count": int(snap.get("entry_near_count", 0) or 0),
        "candidate_pool_size_raw": int(snap.get("candidate_pool_size_raw", 0) or 0),
        "candidate_pool_size_post_gate": int(snap.get("candidate_pool_size_post_gate", 0) or 0),
        "evals_per_min": int(snap.get("evals_per_min", 0) or 0),
        "queue_lag_ms_avg": int(snap.get("queue_lag_ms_avg", 0) or 0),
        "reason_counts": reason_counts,
        "reason_shares": reason_shares,
    }


def _dominant_reason(reason_counts: Dict[str, int]) -> tuple[str, float]:
    counts = dict(reason_counts or {})
    if not counts:
        return "", 0.0
    code, count = max(counts.items(), key=lambda kv: int(kv[1] or 0))
    total = max(1, int(sum(int(v or 0) for v in counts.values()) or 0))
    return str(code or ""), float(count) / float(total)


def _recent_chart_trades_last_n(n: int = 10) -> List[dict]:
    rows = _autopilot_collect_closed_chart_trades()
    if not rows:
        return []
    return rows[-max(1, int(n)) :]


def _chart_live_entry_pause_status() -> tuple[bool, str]:
    """
    Pause NEW live entries for 15 min when last 10 chart trades are <= -0.25 SOL.
    Paper entries are unaffected.
    """
    state = _autopilot_current_state()
    now = time.time()
    pause_until = _safe_float(state.get("live_entry_pause_until_ts", 0.0), 0.0)
    if pause_until > now:
        rem = max(1, int(pause_until - now))
        return True, f"live guard active ({rem}s remaining)"

    rows10 = _recent_chart_trades_last_n(10)
    if len(rows10) < 10:
        state["live_entry_pause_until_ts"] = 0.0
        return False, ""

    pnl_sum = sum(_safe_float(r.get("pnl_sol", 0.0), 0.0) for r in rows10)
    latest_trade_ts = _safe_float(rows10[-1].get("ts", 0.0), 0.0)
    last_ref_ts = _safe_float(state.get("live_entry_pause_ref_trade_ts", 0.0), 0.0)
    if pnl_sum <= -0.25 and latest_trade_ts > last_ref_ts:
        state["live_entry_pause_ref_trade_ts"] = latest_trade_ts
        state["live_entry_pause_until_ts"] = now + 900.0
        return True, f"live guard triggered (last10 pnl {pnl_sum:.4f} SOL)"

    state["live_entry_pause_until_ts"] = 0.0
    return False, ""


def _autopilot_next_relax_updates(metrics: dict) -> tuple[dict, str]:
    state = _autopilot_current_state()
    current = _autopilot_tunable_snapshot()
    relax_level = max(0, _safe_int(state.get("relax_level", 0), 0))
    floor_score = max(0.0, _safe_float(service.settings.get("autopilot_min_chart_score_floor", 30.0), 30.0))
    floor_vol = max(0.0, _safe_float(service.settings.get("autopilot_min_volume_gate_floor_usd", 2500.0), 2500.0))
    floor_liq = max(0.0, _safe_float(service.settings.get("autopilot_min_liquidity_gate_floor_usd", 2500.0), 2500.0))
    floor_mcap = max(0.0, _safe_float(service.settings.get("autopilot_min_mcap_gate_floor_usd", 10000.0), 10000.0))
    score_step = _autopilot_step_chart_score()
    gate_step = _autopilot_step_volume_gate_pct()

    steps = [
        (
            {"market_trend_scan_interval_sec": max(5, current["market_trend_scan_interval_sec"] - 1)}
            if current["market_trend_scan_interval_sec"] > 8
            else {},
            "increase trend scan frequency",
        ),
        (
            {"market_trend_max_new_per_scan": min(18, current["market_trend_max_new_per_scan"] + 2)}
            if current["market_trend_max_new_per_scan"] < 12
            else {},
            "increase new candidates per scan",
        ),
        (
            {"chart_watch_eval_budget_per_cycle": min(20, current["chart_watch_eval_budget_per_cycle"] + 2)}
            if current["chart_watch_eval_budget_per_cycle"] < 16
            else {},
            "increase watch eval budget",
        ),
        (
            {"chart_watch_eval_concurrency": min(6, current["chart_watch_eval_concurrency"] + 1)}
            if current["chart_watch_eval_concurrency"] < 3
            else {},
            "increase eval concurrency",
        ),
        (
            {"chart_watch_max_tokens": min(30, current["chart_watch_max_tokens"] + 4)}
            if current["chart_watch_max_tokens"] < 24
            else {},
            "increase simultaneous watches",
        ),
        (
            {"chart_pro_min_score": round(max(floor_score, current["chart_pro_min_score"] - score_step), 2)}
            if current["chart_pro_min_score"] > floor_score
            else {},
            "lower chart score threshold",
        ),
        (
            {"chart_watch_admission_min_volume_m5_usd": round(max(floor_vol, current["chart_watch_admission_min_volume_m5_usd"] * (1.0 - gate_step)), 2)}
            if current["chart_watch_admission_min_volume_m5_usd"] > floor_vol
            else {},
            "lower volume admission gate",
        ),
        (
            {"chart_watch_admission_min_liquidity_usd": round(max(floor_liq, current["chart_watch_admission_min_liquidity_usd"] * (1.0 - gate_step)), 2)}
            if current["chart_watch_admission_min_liquidity_usd"] > floor_liq
            else {},
            "lower liquidity admission gate",
        ),
        (
            {"chart_watch_admission_min_mcap_usd": round(max(floor_mcap, current["chart_watch_admission_min_mcap_usd"] * (1.0 - gate_step)), 2)}
            if current["chart_watch_admission_min_mcap_usd"] > floor_mcap
            else {},
            "lower min market-cap gate",
        ),
    ]

    for idx in range(relax_level, len(steps)):
        updates, reason = steps[idx]
        if updates:
            state["relax_level"] = idx + 1
            return updates, reason
    return {}, "all relax steps exhausted"


def _autopilot_tighten_updates() -> tuple[dict, str]:
    state = _autopilot_current_state()
    current = _autopilot_tunable_snapshot()
    baseline = dict(state.get("baseline_settings", {}) or {})
    updates: Dict[str, Any] = {}

    for key in AUTOPILOT_TUNABLE_KEYS:
        if key not in baseline:
            continue
        cur = current.get(key)
        base = baseline.get(key)
        if cur is None or base is None or cur == base:
            continue
        if isinstance(base, float):
            delta = abs(float(base) - float(cur))
            step = max(0.5, delta * 0.35)
            if float(cur) < float(base):
                updates[key] = round(min(float(base), float(cur) + step), 2)
            else:
                updates[key] = round(max(float(base), float(cur) - step), 2)
        else:
            delta_i = abs(int(base) - int(cur))
            step_i = max(1, int(round(delta_i * 0.35)))
            if int(cur) < int(base):
                updates[key] = min(int(base), int(cur) + step_i)
            else:
                updates[key] = max(int(base), int(cur) - step_i)

    return updates, "tighten toward baseline"


async def _autopilot_review_and_tune():
    if not _autopilot_enabled():
        return
    if not _chart_watch_enabled():
        state = _autopilot_current_state()
        state["last_run_ts"] = time.time()
        state["last_action"] = "hold"
        state["last_reason"] = "chart watch disabled"
        return

    snapshot = _chart_watch_snapshot()
    metrics = _autopilot_collect_metrics(snapshot)
    _autopilot_maybe_mark_last_good(metrics)
    state = _autopilot_current_state()

    dominant_code, dominant_pct = _dominant_reason(dict(metrics.get("reason_counts", {}) or {}))
    if dominant_code and str(state.get("dominant_reason_code", "")) == dominant_code:
        state["dominant_reason_streak"] = int(state.get("dominant_reason_streak", 0) or 0) + 1
    else:
        state["dominant_reason_code"] = dominant_code
        state["dominant_reason_streak"] = 1 if dominant_code else 0
    dominant_streak = int(state.get("dominant_reason_streak", 0) or 0)

    risk_reason = ""
    if float(metrics.get("day_pnl_sol", 0.0) or 0.0) <= -_autopilot_drawdown_limit_sol():
        risk_reason = f"daily drawdown limit hit ({metrics.get('day_pnl_sol', 0.0):.4f} SOL)"
    elif int(metrics.get("loss_streak", 0) or 0) >= _autopilot_max_loss_streak():
        risk_reason = f"loss streak {metrics.get('loss_streak', 0)} >= {_autopilot_max_loss_streak()}"

    if risk_reason:
        if _autopilot_restore_last_good(metrics, reason=risk_reason):
            await service.broadcast({"type": "log", "msg": f"Autopilot rollback: {risk_reason}"})
        else:
            updates, why = _autopilot_tighten_updates()
            changed = _autopilot_apply_updates(
                updates,
                action="rollback_risk",
                reason=f"{risk_reason}; {why}",
                metrics=metrics,
            )
            if changed:
                await service.broadcast({"type": "log", "msg": f"Autopilot risk rollback: {risk_reason}"})
        await _broadcast_chart_watch_snapshot(force=True)
        return

    target_tph = _autopilot_target_trades_per_hour()
    recent_trades = int(metrics.get("no_trade_window_trades", 0) or 0)
    open_positions = int(metrics.get("open_chart_positions", 0) or 0)
    market_active = (
        int(metrics.get("candidate_pool_size_raw", 0) or 0) >= 10
        or int(metrics.get("admitted_count", 0) or 0) >= 4
        or int(metrics.get("suppressed_count", 0) or 0) >= 8
    )

    if _autopilot_reason_weighted_enabled() and market_active:
        # If market-data fetches dominate failures, increase timeout and reduce evaluator pressure.
        if dominant_code == "NO_MARKET_DATA" and dominant_pct >= 0.30:
            current = _autopilot_tunable_snapshot()
            updates = {
                "chart_watch_market_data_timeout_sec": round(
                    min(4.0, float(current.get("chart_watch_market_data_timeout_sec", 2.5) or 2.5) + 0.5),
                    2,
                ),
                "chart_watch_eval_budget_per_cycle": max(8, int(current.get("chart_watch_eval_budget_per_cycle", 16) or 16) - 2),
                "chart_watch_eval_concurrency": max(1, int(current.get("chart_watch_eval_concurrency", 3) or 3) - 1),
            }
            changed = _autopilot_apply_updates(
                updates,
                action="throttle_market_data",
                reason=f"NO_MARKET_DATA dominant ({dominant_pct:.0%}); increasing timeout and reducing eval pressure",
                metrics=metrics,
            )
            if changed:
                await service.broadcast({"type": "log", "msg": "Autopilot throttle_market_data applied"})
                await _broadcast_chart_watch_snapshot(force=True)
                return

        # If rate limits dominate, back off evaluator pressure and extend cache.
        if dominant_code == "RATE_LIMITED" and dominant_pct >= _autopilot_rate_limited_threshold_pct():
            current = _autopilot_tunable_snapshot()
            updates = {
                "chart_watch_candle_cache_ttl_sec": min(120, int(current.get("chart_watch_candle_cache_ttl_sec", 30) or 30) + 10),
                "chart_watch_eval_budget_per_cycle": max(6, int(current.get("chart_watch_eval_budget_per_cycle", 16) or 16) - 2),
                "chart_watch_eval_concurrency": max(1, int(current.get("chart_watch_eval_concurrency", 3) or 3) - 1),
                "market_trend_scan_interval_sec": min(20, int(current.get("market_trend_scan_interval_sec", 8) or 8) + 1),
            }
            changed = _autopilot_apply_updates(
                updates,
                action="throttle_rate_limit",
                reason=f"RATE_LIMITED dominant ({dominant_pct:.0%}); throttling evaluator pressure",
                metrics=metrics,
            )
            if changed:
                await service.broadcast({"type": "log", "msg": "Autopilot throttle_rate_limit applied"})
                await _broadcast_chart_watch_snapshot(force=True)
                return

        # If low score dominates for 2+ cycles, lower score and ease volume gate slightly.
        if dominant_code == "LOW_SCORE" and dominant_pct >= _autopilot_low_score_threshold_pct() and dominant_streak >= 2:
            current = _autopilot_tunable_snapshot()
            floor_score = max(0.0, _safe_float(service.settings.get("autopilot_min_chart_score_floor", 30.0), 30.0))
            floor_vol = max(0.0, _safe_float(service.settings.get("autopilot_min_volume_gate_floor_usd", 2500.0), 2500.0))
            step_score = _autopilot_step_chart_score()
            step_gate = _autopilot_step_volume_gate_pct() * 0.5
            updates = {
                "chart_pro_min_score": round(max(floor_score, float(current.get("chart_pro_min_score", 34.0) or 34.0) - step_score), 2),
                "chart_watch_admission_min_volume_m5_usd": round(
                    max(
                        floor_vol,
                        float(current.get("chart_watch_admission_min_volume_m5_usd", 3000.0) or 3000.0) * (1.0 - step_gate),
                    ),
                    2,
                ),
            }
            changed = _autopilot_apply_updates(
                updates,
                action="relax_low_score",
                reason=f"LOW_SCORE dominant ({dominant_pct:.0%}) for {dominant_streak} cycles",
                metrics=metrics,
            )
            if changed:
                await service.broadcast({"type": "log", "msg": "Autopilot relax_low_score applied"})
                await _broadcast_chart_watch_snapshot(force=True)
                return

        # If trigger quality is close but not firing, increase cadence only.
        if dominant_code == "NO_TRIGGER" and dominant_pct >= 0.35 and int(metrics.get("admitted_count", 0) or 0) >= 4:
            current = _autopilot_tunable_snapshot()
            updates = {
                "market_trend_scan_interval_sec": max(5, int(current.get("market_trend_scan_interval_sec", 8) or 8) - 1),
            }
            changed = _autopilot_apply_updates(
                updates,
                action="relax_admission",
                reason=f"NO_TRIGGER dominant ({dominant_pct:.0%}); increasing refresh cadence only",
                metrics=metrics,
            )
            if changed:
                await service.broadcast({"type": "log", "msg": "Autopilot cadence-only refresh applied"})
                await _broadcast_chart_watch_snapshot(force=True)
                return

    if target_tph > 0 and open_positions <= 0 and recent_trades <= 0 and market_active:
        updates, why = _autopilot_next_relax_updates(metrics)
        changed = _autopilot_apply_updates(
            updates,
            action="relax_admission",
            reason=f"no chart trades in last {_autopilot_no_trade_window_sec()}s; {why}",
            metrics=metrics,
        )
        if changed:
            await service.broadcast(
                {
                    "type": "log",
                    "msg": (
                        f"Autopilot relax_admission: {why} | trades={metrics.get('window_trades', 0)} "
                        f"entry_near={metrics.get('entry_near_count', 0)} admitted={metrics.get('admitted_count', 0)}"
                    ),
                }
            )
            await _broadcast_chart_watch_snapshot(force=True)
            return

    state["last_run_ts"] = time.time()
    state["last_action"] = "hold"
    state["last_reason"] = (
        f"stable: trades={metrics.get('window_trades', 0)}, "
        f"pnl={metrics.get('window_pnl_sol', 0.0):.4f} SOL, open={metrics.get('open_chart_positions', 0)}"
    )
    state["last_window_trades"] = int(metrics.get("window_trades", 0) or 0)
    state["last_window_pnl_sol"] = float(metrics.get("window_pnl_sol", 0.0) or 0.0)
    state["history"].append(
        {
            "ts": state["last_run_ts"],
            "action": state["last_action"],
            "reason": state["last_reason"],
            "changes": {},
            "window_trades": state["last_window_trades"],
            "window_pnl_sol": state["last_window_pnl_sol"],
        }
    )


def _normalize_candidate_mint(raw: str) -> str:
    token = str(raw or "").strip()
    if token.startswith("solana_"):
        token = token.split("solana_", 1)[1]
    return str(token or "").strip()


def _is_chart_watch_candidate_mint(token: str) -> bool:
    mint = _normalize_candidate_mint(token)
    if len(mint) < 32:
        return False
    return mint not in CHART_WATCH_EXCLUDED_MINTS


def _pick_pool_candidate_mint(base_mint: str, quote_mint: str) -> str:
    base = _normalize_candidate_mint(base_mint)
    quote = _normalize_candidate_mint(quote_mint)
    base_ok = _is_chart_watch_candidate_mint(base)
    quote_ok = _is_chart_watch_candidate_mint(quote)
    if base_ok and quote_ok:
        if base in CHART_WATCH_EXCLUDED_MINTS:
            return quote
        if quote in CHART_WATCH_EXCLUDED_MINTS:
            return base
        return base
    if base_ok:
        return base
    if quote_ok:
        return quote
    return ""


def _is_trend_source_event(source: str, event: Optional[dict] = None) -> bool:
    src = str(source or "").strip().lower()
    ev = dict(event or {})
    if bool(ev.get("market_trend_data")):
        return True
    if "markettrend" in src or "dexboost" in src or "geckotrending" in src:
        return True
    return False


def _chart_watch_is_open(token: str) -> bool:
    token_s = str(token or "")
    if not token_s:
        return False
    try:
        if paper_trader:
            for pos in paper_trader.positions.values():
                if str(pos.get("token_mint", "") or "") == token_s:
                    return True
    except Exception:
        pass
    try:
        for pos in live_trade_tracker.get_open_positions():
            if str(pos.get("token", "") or "") == token_s:
                basis_amt = float(pos.get("basis_token_amount", pos.get("token_amount", 0.0)) or 0.0)
                if basis_amt > 0:
                    return True
    except Exception:
        pass
    return False


def _chart_watch_priority(item: dict):
    age_sec = _safe_int(item.get("token_age_sec", 0), 0)
    is_new = 1 if age_sec <= 0 or age_sec <= _chart_watch_priority_age_sec() else 0
    in_position = 1 if bool(item.get("in_position", False)) else 0
    suppressed = 0 if bool(item.get("suppressed", False)) else 1
    opp = _safe_float(item.get("opportunity_score", 0.0), 0.0)
    vol = _safe_float(item.get("last_volume_m5", 0.0), 0.0)
    seen = _safe_float(item.get("last_seen_ts", 0.0), 0.0)
    score = _safe_float(item.get("last_score", 0.0), 0.0)
    return (in_position, suppressed, is_new, opp, vol, score, seen)


def _chart_watch_drop(token: str, reason: str = "") -> None:
    token_s = str(token or "")
    if not token_s:
        return
    item = service.chart_watchlist_by_token.get(token_s)
    if not item:
        return
    item["state"] = "DROPPED"
    item["last_reason"] = reason or str(item.get("last_reason", "") or "dropped")
    item["last_seen_ts"] = time.time()
    service.chart_watchlist_by_token.pop(token_s, None)
    service.chart_watch_inflight_tokens.discard(token_s)


def _chart_watch_reason_code(reason: str) -> str:
    text = str(reason or "").lower()
    if "candles_rate_limited" in text or "rate limit" in text:
        return "RATE_LIMITED"
    if "insufficient_candles" in text or "insufficient candles" in text:
        return "INSUFFICIENT_CANDLES"
    if "score below threshold" in text or "low_score" in text:
        return "LOW_SCORE"
    if "no chart trigger" in text or "entryready=false" in text:
        return "NO_TRIGGER"
    if "below" in text and "mc" in text:
        return "MCAP_GATE"
    if ("outside" in text or "above" in text) and "mc" in text:
        return "MCAP_BAND_GATE"
    if "below" in text and "liq" in text:
        return "LIQ_GATE"
    if "age" in text and "exceeds" in text:
        return "AGE_GATE"
    if ("volume" in text or "vol " in text or "vol m" in text) and "below" in text:
        return "VOL_GATE"
    if "trend source required" in text:
        return "SOURCE_GATE"
    if "market data unavailable" in text:
        return "NO_MARKET_DATA"
    return "OTHER"


def _chart_watch_record_suppressed(token: str, source: str, reason: str, reason_code: str = "OTHER", market_data: Optional[dict] = None, token_age_sec: int = 0):
    token_s = str(token or "").strip()
    if len(token_s) < 32:
        return
    md = dict(market_data or {})
    now = time.time()
    rec = {
        "token": token_s,
        "source": str(source or "ChartWatch"),
        "state": "FILTERED",
        "status": "SUPPRESSED",
        "reason": str(reason or "suppressed"),
        "reason_code": str(reason_code or _chart_watch_reason_code(reason)),
        "score": 0.0,
        "opportunity_score": 0.0,
        "failure_streak": 1,
        "next_eval_ts": 0.0,
        "next_eval_in_sec": 0,
        "suppressed": True,
        "admission_reason": str(reason or "suppressed"),
        "volume_m5": _safe_float(md.get("volume_m5", 0.0), 0.0),
        "liquidity_usd": _safe_float(md.get("liquidity_usd", 0.0), 0.0),
        "mcap_usd": _safe_float(md.get("mcap_usd", md.get("fdv", 0.0)), 0.0),
        "pair_address": str(md.get("pair_address", "") or ""),
        "token_age_sec": max(0, int(token_age_sec or 0)),
        "newer_24h": bool(max(0, int(token_age_sec or 0)) <= _chart_watch_priority_age_sec()),
        "in_position": False,
        "last_seen_ts": now,
        "last_eval_ts": now,
        "queue_lag_ms": 0,
    }
    service.chart_watch_suppressed_by_token[token_s] = rec


def _chart_watch_backoff_sec(item: dict, reason_code: str) -> int:
    base, cap = _chart_watch_backoff_bounds()
    streak = max(1, _safe_int(item.get("failure_streak", 1), 1))
    hard = reason_code in ("MCAP_GATE", "MCAP_BAND_GATE", "LIQ_GATE", "AGE_GATE", "VOL_GATE", "SOURCE_GATE")
    rate_limited = reason_code in ("RATE_LIMITED", "INSUFFICIENT_CANDLES")
    if hard:
        min_hard = max(base, 300)
        return min(cap, max(min_hard, min_hard * max(1, streak // 2)))
    if rate_limited:
        return min(cap, max(base * 3, base * (2 ** min(4, streak - 1))))
    if reason_code in ("LOW_SCORE", "NO_TRIGGER"):
        return min(cap, max(base, base * (2 ** min(3, streak - 1))))
    return min(cap, max(base, base * (2 ** min(4, streak - 1))))


def _chart_watch_opportunity_score(item: dict, market_data: dict, candle_data: Optional[dict] = None) -> float:
    md = dict(market_data or {})
    cd = dict(candle_data or {})
    vol = _safe_float(md.get("volume_m5", 0.0), 0.0)
    liq = _safe_float(md.get("liquidity_usd", 0.0), 0.0)
    mcap = _safe_float(md.get("mcap_usd", md.get("fdv", 0.0)), 0.0)
    chg_m5 = _safe_float(md.get("price_change_m5", 0.0), 0.0)
    chg_h1 = _safe_float(md.get("price_change_h1", 0.0), 0.0)
    age_sec = max(0, _safe_int(item.get("token_age_sec", 0), 0))
    quality = _safe_float(((cd.get("quality", {}) or {}).get("score", 0.0)), 0.0)
    trend = cd.get("signals", {}) or {}
    atr_pct = _safe_float((trend or {}).get("atr_pct", 0.0), 0.0)
    profile = _chart_watch_focus_profile()
    trend_score = 0.0
    if bool(trend.get("trend_1m", False)):
        trend_score += 45.0
    if bool(trend.get("trend_5m", False)):
        trend_score += 45.0
    if bool(trend.get("volume_spike", False)):
        trend_score += 10.0
    trend_score = max(0.0, min(100.0, trend_score))

    min_vol = max(1.0, _safe_float(service.settings.get("chart_watch_admission_min_volume_m5_usd", 12000.0), 12000.0))
    min_liq = max(1.0, _safe_float(service.settings.get("chart_watch_admission_min_liquidity_usd", 6000.0), 6000.0))
    min_mcap = max(1.0, _safe_float(service.settings.get("chart_watch_admission_min_mcap_usd", 20000.0), 20000.0))
    vol_score = max(0.0, min(100.0, (vol / min_vol) * 100.0))
    liq_score = max(0.0, min(100.0, (liq / min_liq) * 100.0))
    cap_score = max(0.0, min(100.0, (mcap / min_mcap) * 100.0))
    liq_quality = (liq_score * 0.65) + (cap_score * 0.35)
    fresh_score = 100.0 if age_sec <= _chart_watch_priority_age_sec() else 30.0
    momentum_score = max(0.0, min(100.0, (max(0.0, chg_m5) * 2.2) + (max(0.0, chg_h1) * 0.9)))
    volatility_score = max(0.0, min(100.0, (abs(chg_m5) * 2.0) + (abs(chg_h1) * 0.7) + (atr_pct * 3.0)))
    source_conf = 35.0
    source = str(item.get("source", "") or "").lower()
    if "dexboost" in source or "markettrend" in source:
        source_conf = 75.0
    elif "tracked:" in source:
        source_conf = 55.0
    elif "social" in source:
        source_conf = 50.0
    boost = _safe_float(item.get("source_priority", 0.0), 0.0)
    if boost > 0:
        source_conf = max(source_conf, min(100.0, 35.0 + (boost * 8.0)))
    if profile == "volatility_first":
        score = (
            (0.30 * volatility_score)
            + (0.20 * trend_score)
            + (0.15 * quality)
            + (0.15 * vol_score)
            + (0.10 * liq_quality)
            + (0.05 * fresh_score)
            + (0.05 * source_conf)
        )
    elif profile == "momentum_first":
        score = (
            (0.30 * momentum_score)
            + (0.25 * trend_score)
            + (0.15 * quality)
            + (0.12 * vol_score)
            + (0.10 * liq_quality)
            + (0.05 * fresh_score)
            + (0.03 * source_conf)
        )
    elif profile == "quality_first":
        score = (
            (0.18 * trend_score)
            + (0.36 * quality)
            + (0.20 * liq_quality)
            + (0.08 * vol_score)
            + (0.10 * fresh_score)
            + (0.08 * source_conf)
        )
    elif profile == "high_frequency":
        score = (
            (0.28 * ((trend_score * 0.55) + (vol_score * 0.45)))
            + (0.16 * quality)
            + (0.16 * liq_quality)
            + (0.16 * volatility_score)
            + (0.10 * fresh_score)
            + (0.14 * source_conf)
        )
    else:
        score = (
            (0.35 * ((trend_score * 0.55) + (vol_score * 0.45)))
            + (0.25 * quality)
            + (0.20 * liq_quality)
            + (0.10 * fresh_score)
            + (0.10 * source_conf)
        )
    return max(0.0, min(100.0, score))


def _chart_watch_admission_check(token: str, market_data: dict, token_age_sec: int, source_meta: Optional[dict] = None) -> dict:
    token_s = str(token or "").strip()
    md = dict(market_data or {})
    source_meta = dict(source_meta or {})
    if len(token_s) < 32:
        return {"accepted": False, "reason": "invalid token", "reason_code": "INVALID_TOKEN"}

    max_age = _chart_watch_strict_age_sec()
    min_vol = _safe_float(service.settings.get("chart_watch_admission_min_volume_m5_usd", 12000.0), 12000.0)
    min_mcap, max_mcap = _chart_watch_admission_mcap_band()
    min_liq = _safe_float(service.settings.get("chart_watch_admission_min_liquidity_usd", 6000.0), 6000.0)
    vol = _safe_float(md.get("volume_m5", 0.0), 0.0)
    mcap = _safe_float(md.get("mcap_usd", md.get("fdv", 0.0)), 0.0)
    liq = _safe_float(md.get("liquidity_usd", 0.0), 0.0)
    px = _safe_float(md.get("price_sol", 0.0), 0.0)
    md_source = str(md.get("market_data_source", "") or "").strip()
    source = str(source_meta.get("source", "") or "")
    profile = _chart_watch_focus_profile()
    source_l = source.lower()
    has_trend_signal = bool(source_meta.get("has_trend_signal", False)) or ("markettrend" in source_l) or ("dexboost" in source_l)

    soft_gates = (profile == "high_frequency")
    near_mcap = (min_mcap > 0 and mcap >= (min_mcap * 0.85))
    near_liq = (min_liq > 0 and liq >= (min_liq * 0.80))
    near_vol = (min_vol > 0 and vol >= (min_vol * 0.75))

    if px <= 0:
        return {"accepted": False, "hard_reject": not soft_gates, "reason": "market data unavailable (price=0)", "reason_code": "NO_MARKET_DATA"}
    if not md_source:
        return {"accepted": False, "hard_reject": not soft_gates, "reason": "market data unavailable (source missing)", "reason_code": "NO_MARKET_DATA"}
    if profile == "quality_first" and not has_trend_signal:
        return {"accepted": False, "hard_reject": True, "reason": "trend source required in quality_first profile", "reason_code": "SOURCE_GATE"}
    if max_age > 0 and int(token_age_sec or 0) > max_age:
        return {"accepted": False, "hard_reject": True, "reason": f"token age {int(token_age_sec)}s exceeds max {max_age}s", "reason_code": "AGE_GATE"}
    if mcap < min_mcap:
        hard = (not soft_gates) or (not near_mcap)
        return {"accepted": False, "hard_reject": hard, "reason": f"MC ${mcap:,.0f} below ${min_mcap:,.0f}", "reason_code": "MCAP_GATE"}
    if max_mcap > 0 and mcap > max_mcap:
        return {"accepted": False, "hard_reject": True, "reason": f"MC ${mcap:,.0f} outside ${min_mcap:,.0f}-${max_mcap:,.0f}", "reason_code": "MCAP_BAND_GATE"}
    if liq < min_liq:
        hard = (not soft_gates) or (not near_liq)
        return {"accepted": False, "hard_reject": hard, "reason": f"Liq ${liq:,.0f} below ${min_liq:,.0f}", "reason_code": "LIQ_GATE"}
    if vol < min_vol:
        hard = (not soft_gates) or (not near_vol)
        return {"accepted": False, "hard_reject": hard, "reason": f"Vol m5 ${vol:,.0f} below ${min_vol:,.0f}", "reason_code": "VOL_GATE"}
    return {"accepted": True, "hard_reject": False, "reason": f"admitted ({source or 'ChartWatch'})", "reason_code": "ADMITTED"}


def _chart_watch_effective_trend_thresholds() -> tuple[float, float, float]:
    trend_min_vol = _safe_float(service.settings.get("chart_watch_trend_min_volume_m5_usd", 5000.0), 5000.0)
    trend_min_liq = _safe_float(service.settings.get("chart_watch_trend_min_liquidity_usd", 8000.0), 8000.0)
    trend_min_mcap = _safe_float(service.settings.get("chart_watch_trend_min_mcap_usd", 20000.0), 20000.0)
    admission_min_vol = _safe_float(service.settings.get("chart_watch_admission_min_volume_m5_usd", 3000.0), 3000.0)
    admission_min_liq = _safe_float(service.settings.get("chart_watch_admission_min_liquidity_usd", 2800.0), 2800.0)
    admission_min_mcap = _safe_float(service.settings.get("chart_watch_admission_min_mcap_usd", 15000.0), 15000.0)
    profile = _chart_watch_focus_profile()

    # Systemic profile multipliers let the hourly optimizer shift candidate policy
    # without rewriting strategy code.
    if profile == "high_frequency":
        trend_min_vol *= 0.80
        trend_min_liq *= 0.80
        trend_min_mcap *= 0.80
    elif profile == "volatility_first":
        trend_min_vol *= 1.30
        trend_min_liq *= 0.85
        trend_min_mcap *= 0.80
    elif profile == "momentum_first":
        trend_min_vol *= 1.10
        trend_min_liq *= 0.95
        trend_min_mcap *= 0.90
    elif profile == "quality_first":
        trend_min_vol *= 1.00
        trend_min_liq *= 1.00
        trend_min_mcap *= 1.00

    admitted_now = 0
    for item in service.chart_watchlist_by_token.values():
        if not bool((item or {}).get("suppressed", False)):
            admitted_now += 1
    target = max(2, min(8, _chart_watch_max_tokens() // 3))
    if admitted_now <= 0:
        relax_factor = 0.25
    elif admitted_now < target:
        relax_factor = 0.45
    else:
        relax_factor = 1.00
    if profile == "volatility_first":
        relax_factor = max(0.55, relax_factor)
    elif profile == "momentum_first":
        relax_factor = max(0.50, relax_factor)

    eff_vol = max(admission_min_vol, trend_min_vol * relax_factor)
    eff_liq = max(admission_min_liq, trend_min_liq * relax_factor)
    eff_mcap = max(admission_min_mcap, trend_min_mcap * relax_factor)
    return eff_vol, eff_liq, eff_mcap


def _chart_pro_fast_entry_signal(candle_data: Optional[dict], market_data: Optional[dict]) -> tuple[bool, str]:
    cd = dict(candle_data or {})
    md = dict(market_data or {})
    trigger = dict((cd.get("trigger", {}) or {}))
    quality = dict((cd.get("quality", {}) or {}))
    signals = dict((cd.get("signals", {}) or {}))
    score = _safe_float(quality.get("score", 0.0), 0.0)
    min_score = _safe_float(service.settings.get("chart_pro_min_score", 42.0), 42.0)
    vol_m5 = _safe_float(md.get("volume_m5", 0.0), 0.0)
    liq_usd = _safe_float(md.get("liquidity_usd", 0.0), 0.0)
    price_change_m5 = _safe_float(md.get("price_change_m5", 0.0), 0.0)
    atr_pct = _safe_float(signals.get("atr_pct", 0.0), 0.0)
    trend_min_vol, trend_min_liq, _trend_min_mcap = _chart_watch_effective_trend_thresholds()

    if bool(trigger.get("entry_ready", False)) and score >= min_score:
        return True, "trigger_ready"

    breakout = bool(signals.get("breakout", False))
    volume_spike = bool(signals.get("volume_spike", False))
    aggressive_breakout = bool(breakout and volume_spike and score >= max(0.0, min_score - 4.0))
    if aggressive_breakout:
        return True, "aggressive_breakout"

    trend_aligned = bool(signals.get("trend_1m", False) and signals.get("trend_5m", False))
    min_vol_soft = max(350.0, trend_min_vol * 0.45)
    min_liq_soft = max(900.0, trend_min_liq * 0.45)
    momentum_window_ok = (price_change_m5 >= 0.35 and price_change_m5 <= 55.0)
    atr_ok = (atr_pct <= 28.0) if atr_pct > 0 else True
    score_ok = score >= max(0.0, min_score - 14.0)
    fast_momentum = bool(
        trend_aligned
        and score_ok
        and vol_m5 >= min_vol_soft
        and liq_usd >= min_liq_soft
        and momentum_window_ok
        and atr_ok
    )
    if fast_momentum:
        return True, "fast_momentum"
    return False, ""


def _chart_watch_apply_warming(item: dict, reason: str, reason_code: str, market_data: Optional[dict] = None):
    now = time.time()
    item["last_status"] = "WATCH"
    item["state"] = "WARMING"
    item["last_reason"] = str(reason or "")
    item["last_reason_code"] = str(reason_code or _chart_watch_reason_code(reason))
    item["admission_reason"] = str(reason or "")
    item["suppressed"] = True
    item["failure_streak"] = max(0, int(item.get("failure_streak", 0) or 0))
    item["last_eval_ts"] = now
    item["last_seen_ts"] = now
    # Recheck soon; this is a soft gate, not a terminal reject.
    base = max(2, int(_chart_watch_eval_interval_sec() / 2))
    item["next_eval_ts"] = now + float(min(20, base))
    md = dict(market_data or {})
    if md:
        item["last_volume_m5"] = _safe_float(md.get("volume_m5", item.get("last_volume_m5", 0.0)), 0.0)
        item["last_liquidity_usd"] = _safe_float(md.get("liquidity_usd", item.get("last_liquidity_usd", 0.0)), 0.0)
        item["last_mcap_usd"] = _safe_float(md.get("mcap_usd", md.get("fdv", item.get("last_mcap_usd", 0.0))), 0.0)
        item["last_pair_address"] = str(md.get("pair_address", item.get("last_pair_address", "")) or "")
        if md.get("market_data_source"):
            item["last_market_data_source"] = str(md.get("market_data_source", "") or "")


def _chart_watch_upsert(token: str, event: Optional[dict] = None, source: str = "", signature: str = "") -> dict:
    token_s = str(token or "").strip()
    if len(token_s) < 32:
        return {}
    now = time.time()
    event = dict(event or {})
    item = service.chart_watchlist_by_token.get(token_s)
    if not item:
        item = {
            "token": token_s,
            "source": str(source or event.get("source", "ChartWatch") or "ChartWatch"),
            "state": "WATCHING",
            "last_status": "WATCH",
            "last_reason": "waiting for chart trigger",
            "last_score": 0.0,
            "opportunity_score": 0.0,
            "admission_reason": "pending",
            "suppressed": False,
            "failure_streak": 0,
            "last_fail_reason": "",
            "last_seen_ts": now,
            "first_seen_ts": now,
            "last_eval_ts": 0.0,
            "last_queue_ts": 0.0,
            "last_eval_started_ts": 0.0,
            "next_eval_ts": 0.0,
            "last_volume_m5": 0.0,
            "last_liquidity_usd": 0.0,
            "last_mcap_usd": 0.0,
            "last_pair_address": "",
            "token_age_sec": 0,
            "last_age_check_ts": 0.0,
            "display_signature": str(signature or token_s),
            "in_position": False,
            "was_open": False,
            "exit_seen_ts": 0.0,
            "post_exit_checks": 0,
            "market_trend_data": {},
            "social_data": {},
            "source_priority": 0.0,
        }
        service.chart_watchlist_by_token[token_s] = item
    item["last_seen_ts"] = now
    if source:
        item["source"] = str(source)
    sig = str(signature or event.get("signature", "") or "")
    if sig:
        item["display_signature"] = sig
    if event.get("market_trend_data"):
        item["market_trend_data"] = dict(event.get("market_trend_data", {}) or {})
        boost = _safe_float(item["market_trend_data"].get("boost_total", 0.0), 0.0)
        item["source_priority"] = max(_safe_float(item.get("source_priority", 0.0), 0.0), boost)
    if event.get("social_data"):
        item["social_data"] = dict(event.get("social_data", {}) or {})
    item.setdefault("opportunity_score", 0.0)
    item.setdefault("admission_reason", "pending")
    item.setdefault("suppressed", False)
    item.setdefault("failure_streak", 0)
    item.setdefault("last_fail_reason", "")
    item.setdefault("last_eval_started_ts", 0.0)
    item["in_position"] = _chart_watch_is_open(token_s)
    if item["in_position"]:
        item["was_open"] = True
        item["state"] = "IN_POSITION"
        item["last_status"] = "ACTIVE"
        item["last_reason"] = "position currently open"
    return item


def _chart_watch_mark_result(
    token: str,
    status: str = "",
    reason: str = "",
    market_data: Optional[dict] = None,
    score: float = 0.0,
    source: str = "",
):
    token_s = str(token or "").strip()
    if not token_s:
        return
    if token_s not in service.chart_watchlist_by_token:
        return
    item = _chart_watch_upsert(token_s, source=source, signature="")
    if not item:
        return
    now = time.time()
    md = dict(market_data or {})
    if md:
        item["last_volume_m5"] = _safe_float(md.get("volume_m5", item.get("last_volume_m5", 0.0)), 0.0)
        item["last_liquidity_usd"] = _safe_float(md.get("liquidity_usd", item.get("last_liquidity_usd", 0.0)), 0.0)
        item["last_mcap_usd"] = _safe_float(md.get("mcap_usd", md.get("fdv", item.get("last_mcap_usd", 0.0))), 0.0)
        item["last_price_sol"] = _safe_float(md.get("price_sol", item.get("last_price_sol", 0.0)), 0.0)
        item["last_pair_address"] = str(md.get("pair_address", item.get("last_pair_address", "")) or "")
        if md.get("market_data_source"):
            item["last_market_data_source"] = str(md.get("market_data_source", "") or "")
    item["last_score"] = _safe_float(score, _safe_float(item.get("last_score", 0.0), 0.0))
    item["last_eval_ts"] = now
    item["last_seen_ts"] = now
    if status:
        item["last_status"] = str(status)
    if reason:
        item["last_reason"] = str(reason)
    item["last_reason_code"] = _chart_watch_reason_code(str(item.get("last_reason", "") or ""))
    item["suppressed"] = bool(item.get("suppressed", False))
    item["admission_reason"] = str(item.get("admission_reason", item.get("last_reason", "")) or "")

    item["in_position"] = _chart_watch_is_open(token_s)
    if item["in_position"]:
        item["was_open"] = True
        item["state"] = "IN_POSITION"
    else:
        if item.get("was_open", False) and _safe_float(item.get("exit_seen_ts", 0.0), 0.0) <= 0:
            item["exit_seen_ts"] = now
            item["state"] = "POST_EXIT_WATCH"
            item["last_reason"] = "position closed; waiting for volume fade"
        elif str(status or "").upper() in ("BUY_SIGNAL", "WATCH"):
            item["state"] = "WATCHING"
        elif str(status or "").upper() in ("SKIPPED", "HIGH_RISK"):
            item["state"] = "FILTERED"
        elif str(status or "").upper() == "PAPER_OPENED":
            item["state"] = "IN_POSITION"
    if _safe_float(item.get("exit_seen_ts", 0.0), 0.0) > 0 and not item["in_position"]:
        item["post_exit_checks"] = int(item.get("post_exit_checks", 0) or 0) + 1


def _chart_watch_snapshot() -> dict:
    now = time.time()
    autopilot_state = _autopilot_current_state()
    rows = []
    reason_mix: Dict[str, int] = {}
    admitted_count = 0
    suppressed_count = 0
    rejected_age_count = 0
    rejected_band_count = 0
    rate_limited_count = 0
    entry_near_count = 0
    queue_lag_total_ms = 0
    queue_lag_samples = 0
    for token, item in service.chart_watchlist_by_token.items():
        age_sec = _safe_int(item.get("token_age_sec", 0), 0)
        if age_sec <= 0 and _safe_float(item.get("first_seen_ts", 0.0), 0.0) > 0:
            age_sec = max(0, int(now - _safe_float(item.get("first_seen_ts", now), now)))
        status = str(item.get("last_status", "WATCH") or "WATCH")
        state = str(item.get("state", "WATCHING") or "WATCHING")
        suppressed = bool(item.get("suppressed", False))
        reason = str(item.get("last_reason", "") or "")
        reason_code = str(item.get("last_reason_code", "") or _chart_watch_reason_code(reason))
        reason_mix[reason_code] = int(reason_mix.get(reason_code, 0) or 0) + 1
        queue_lag_ms = 0
        last_queue_ts = _safe_float(item.get("last_queue_ts", 0.0), 0.0)
        if last_queue_ts > 0 and _safe_float(item.get("last_eval_ts", 0.0), 0.0) >= last_queue_ts:
            queue_lag_ms = max(0, int((_safe_float(item.get("last_eval_ts", 0.0), 0.0) - last_queue_ts) * 1000.0))
        if queue_lag_ms > 0:
            queue_lag_total_ms += queue_lag_ms
            queue_lag_samples += 1
        if not suppressed:
            admitted_count += 1
        else:
            suppressed_count += 1
            if reason_code == "AGE_GATE":
                rejected_age_count += 1
            if reason_code in ("MCAP_GATE", "MCAP_BAND_GATE"):
                rejected_band_count += 1
        if reason_code == "RATE_LIMITED":
            rate_limited_count += 1
        if state == "ENTRY_NEAR" or status == "BUY_SIGNAL":
            entry_near_count += 1
        rows.append(
            {
                "token": token,
                "source": str(item.get("source", "ChartWatch") or "ChartWatch"),
                "state": state,
                "status": status,
                "reason": reason,
                "reason_code": reason_code,
                "score": _safe_float(item.get("last_score", 0.0), 0.0),
                "opportunity_score": _safe_float(item.get("opportunity_score", 0.0), 0.0),
                "failure_streak": _safe_int(item.get("failure_streak", 0), 0),
                "next_eval_in_sec": max(0, int(_safe_float(item.get("next_eval_ts", 0.0), 0.0) - now)),
                "admission_reason": str(item.get("admission_reason", "") or ""),
                "suppressed": suppressed,
                "volume_m5": _safe_float(item.get("last_volume_m5", 0.0), 0.0),
                "liquidity_usd": _safe_float(item.get("last_liquidity_usd", 0.0), 0.0),
                "mcap_usd": _safe_float(item.get("last_mcap_usd", 0.0), 0.0),
                "pair_address": str(item.get("last_pair_address", "") or ""),
                "token_age_sec": age_sec,
                "newer_24h": bool(age_sec <= 0 or age_sec <= _chart_watch_priority_age_sec()),
                "in_position": bool(item.get("in_position", False)),
                "last_seen_ts": _safe_float(item.get("last_seen_ts", 0.0), 0.0),
                "last_eval_ts": _safe_float(item.get("last_eval_ts", 0.0), 0.0),
                "next_eval_ts": _safe_float(item.get("next_eval_ts", 0.0), 0.0),
                "market_data_source": str(item.get("last_market_data_source", "") or ""),
                "queue_lag_ms": queue_lag_ms,
            }
        )
    # Include a bounded window of recent suppressed candidates for operator debugging.
    suppress_ttl = 300.0
    suppressed_limit = max(16, _chart_watch_max_tokens())
    suppressed_added = 0
    suppressed_items = sorted(
        list(service.chart_watch_suppressed_by_token.items()),
        key=lambda kv: _safe_float((kv[1] or {}).get("last_seen_ts", 0.0), 0.0),
        reverse=True,
    )
    for token, rec in suppressed_items:
        last_seen = _safe_float((rec or {}).get("last_seen_ts", 0.0), 0.0)
        if last_seen <= 0 or (now - last_seen) > suppress_ttl:
            service.chart_watch_suppressed_by_token.pop(token, None)
            continue
        if suppressed_added >= suppressed_limit:
            break
        row = dict(rec or {})
        row.setdefault("token", token)
        row.setdefault("source", "ChartWatch")
        row.setdefault("state", "FILTERED")
        row.setdefault("status", "SUPPRESSED")
        row.setdefault("reason", "suppressed")
        row.setdefault("reason_code", _chart_watch_reason_code(str(row.get("reason", "") or "")))
        row.setdefault("score", 0.0)
        row.setdefault("opportunity_score", 0.0)
        row.setdefault("failure_streak", 1)
        row.setdefault("next_eval_in_sec", 0)
        row.setdefault("admission_reason", str(row.get("reason", "suppressed") or "suppressed"))
        row.setdefault("suppressed", True)
        row.setdefault("volume_m5", 0.0)
        row.setdefault("liquidity_usd", 0.0)
        row.setdefault("mcap_usd", 0.0)
        row.setdefault("pair_address", "")
        row.setdefault("token_age_sec", 0)
        row.setdefault("newer_24h", True)
        row.setdefault("in_position", False)
        row.setdefault("last_seen_ts", last_seen)
        row.setdefault("last_eval_ts", last_seen)
        row.setdefault("next_eval_ts", 0.0)
        row.setdefault("market_data_source", "")
        row.setdefault("queue_lag_ms", 0)
        rows.append(row)
        suppressed_added += 1
        reason_code = str(row.get("reason_code", "OTHER") or "OTHER")
        reason_mix[reason_code] = int(reason_mix.get(reason_code, 0) or 0) + 1
        suppressed_count += 1
        if row.get("reason_code") == "AGE_GATE":
            rejected_age_count += 1
        if row.get("reason_code") in ("MCAP_GATE", "MCAP_BAND_GATE"):
            rejected_band_count += 1
        if row.get("reason_code") == "RATE_LIMITED":
            rate_limited_count += 1
    rows.sort(
        key=lambda r: (
            1 if r.get("in_position", False) else 0,
            1 if str(r.get("state", "") or "") == "ENTRY_NEAR" else 0,
            0 if bool(r.get("suppressed", False)) else 1,
            1 if r.get("newer_24h", False) else 0,
            _safe_float(r.get("opportunity_score", 0.0), 0.0),
            _safe_float(r.get("volume_m5", 0.0), 0.0),
            _safe_float(r.get("score", 0.0), 0.0),
            _safe_float(r.get("last_seen_ts", 0.0), 0.0),
        ),
        reverse=True,
    )
    max_tokens = _chart_watch_max_tokens()
    # Eval throughput (per minute).
    cutoff = now - 60.0
    evals_per_min = sum(1 for ts in list(service.chart_watch_eval_count_window) if ts >= cutoff)
    queue_lag_ms_avg = int(queue_lag_total_ms / queue_lag_samples) if queue_lag_samples > 0 else 0
    suppressed_ratio = float(suppressed_count) / float(max(1, len(rows)))
    latency_samples = sorted(int(v or 0) for v in list(service.chart_watch_eval_latency_ms_window) if int(v or 0) >= 0)
    if latency_samples:
        p50_idx = min(len(latency_samples) - 1, int(round((len(latency_samples) - 1) * 0.50)))
        p95_idx = min(len(latency_samples) - 1, int(round((len(latency_samples) - 1) * 0.95)))
        eval_latency_p50_ms = int(latency_samples[p50_idx])
        eval_latency_p95_ms = int(latency_samples[p95_idx])
    else:
        eval_latency_p50_ms = 0
        eval_latency_p95_ms = 0
    service.chart_watch_rejected_age_count = rejected_age_count
    service.chart_watch_rejected_band_count = rejected_band_count
    table_mode = _chart_watch_table_mode()
    hide_filtered_default = bool(service.settings.get("chart_watch_hide_filtered_in_ui", True))
    if table_mode == "admitted_only":
        hide_filtered_default = True
    elif table_mode == "all":
        hide_filtered_default = False
    return {
        "tokens": rows,
        "max_tokens": max_tokens,
        "active_count": len(service.chart_watchlist_by_token),
        "visible_count": len(rows),
        "admitted_count": admitted_count,
        "suppressed_count": suppressed_count,
        "suppressed_ratio": suppressed_ratio,
        "rejected_age_count": rejected_age_count,
        "rejected_band_count": rejected_band_count,
        "rate_limited_count": rate_limited_count,
        "reason_mix": {k: int(v) for k, v in sorted(reason_mix.items())},
        "entry_near_count": entry_near_count,
        "evals_per_min": int(evals_per_min),
        "queue_lag_ms_avg": queue_lag_ms_avg,
        "eval_latency_p50_ms": eval_latency_p50_ms,
        "eval_latency_p95_ms": eval_latency_p95_ms,
        "market_data_timeout_count": int(service.chart_watch_market_data_timeout_count or 0),
        "candle_rate_limited_count": int(service.chart_watch_candle_rate_limited_count or 0),
        "hide_filtered_default": hide_filtered_default,
        "table_mode": table_mode,
        "source_mix": dict(service.chart_watch_source_mix or {}),
        "source_mix_seen": dict((service.chart_watch_source_mix or {}).get("seen", {}) or {}),
        "source_mix_admitted": dict((service.chart_watch_source_mix or {}).get("admitted", {}) or {}),
        "startup_burst_active": bool(service.chart_watch_startup_burst_active),
        "startup_burst_admitted": int(service.chart_watch_startup_burst_admitted),
        "candidate_pool_size_raw": int(service.chart_watch_candidate_pool_size_raw or 0),
        "candidate_pool_size_post_gate": int(service.chart_watch_candidate_pool_size_post_gate or 0),
        "autopilot_enabled": _autopilot_enabled(),
        "autopilot_last_run_ts": _safe_float(autopilot_state.get("last_run_ts", 0.0), 0.0),
        "autopilot_last_action": str(autopilot_state.get("last_action", "idle") or "idle"),
        "autopilot_last_reason": str(autopilot_state.get("last_reason", "") or ""),
        "autopilot_window_trades": int(autopilot_state.get("last_window_trades", 0) or 0),
        "autopilot_window_pnl_sol": float(autopilot_state.get("last_window_pnl_sol", 0.0) or 0.0),
        "autopilot_relax_level": int(autopilot_state.get("relax_level", 0) or 0),
        "queue_depth": int(service.queue.qsize()),
        "chart_watch_enabled": _chart_watch_enabled(),
        "updated_at": now,
    }


async def _broadcast_chart_watch_snapshot(force: bool = False):
    now = time.time()
    min_interval = max(0.2, _safe_float(service.chart_watch_broadcast_min_interval_sec, 1.0))
    if not force and (now - _safe_float(service.chart_watch_last_broadcast_ts, 0.0)) < min_interval:
        return
    service.chart_watch_last_broadcast_ts = now
    await service.broadcast({"type": "chart_watch_update", "data": _chart_watch_snapshot()})


async def _chart_watch_get_token_age_cached(token: str) -> int:
    token_s = str(token or "").strip()
    if not token_s or not paper_trader or not paper_trader.engine:
        return 0
    now = time.time()
    cached = service.chart_watch_token_age_cache.get(token_s, {})
    if cached and (now - _safe_float(cached.get("ts", 0.0), 0.0)) <= 300.0:
        return max(0, _safe_int(cached.get("age", 0), 0))
    age = await paper_trader.engine.get_token_age(token_s)
    age_i = max(0, int(age or 0))
    service.chart_watch_token_age_cache[token_s] = {"age": age_i, "ts": now}
    return age_i


async def _chart_watch_get_safety_cached(token: str) -> tuple[bool, str]:
    token_s = str(token or "").strip()
    if len(token_s) != 44 or not risk_engine:
        return False, "invalid_token"
    now = time.time()
    cached = service.chart_watch_safety_cache.get(token_s, {})
    if cached and (now - _safe_float(cached.get("ts", 0.0), 0.0)) <= 600.0:
        return bool(cached.get("ok", False)), str(cached.get("msg", "") or "")
    ok, msg = await risk_engine.check_token_safety(token_s)
    service.chart_watch_safety_cache[token_s] = {"ok": bool(ok), "msg": str(msg or ""), "ts": now}
    return bool(ok), str(msg or "")


async def _chart_watch_get_holder_cached(token: str, pair_address: str) -> dict:
    token_s = str(token or "").strip()
    key = f"{token_s}:{str(pair_address or '')}"
    now = time.time()
    cached = service.chart_watch_holder_cache.get(key, {})
    if cached and (now - _safe_float(cached.get("ts", 0.0), 0.0)) <= 90.0:
        return dict(cached.get("data", {}) or {})
    if not holder_intel or not token_s:
        return {}
    try:
        data = await holder_intel.get_holder_intel(token_s, pair_address=pair_address or "")
    except Exception:
        data = {}
    service.chart_watch_holder_cache[key] = {"data": dict(data or {}), "ts": now}
    return dict(data or {})


def _chart_watch_apply_failure(item: dict, reason: str, reason_code: str, market_data: Optional[dict] = None, score: float = 0.0):
    now = time.time()
    streak = max(1, int(item.get("failure_streak", 0) or 0) + 1)
    item["failure_streak"] = streak
    item["last_fail_reason"] = str(reason or "")
    item["last_reason"] = str(reason or "")
    item["last_reason_code"] = str(reason_code or _chart_watch_reason_code(reason))
    item["last_status"] = "WATCH"
    item["state"] = "FILTERED"
    item["suppressed"] = True
    item["admission_reason"] = str(reason or "")
    item["last_score"] = _safe_float(score, _safe_float(item.get("last_score", 0.0), 0.0))
    backoff_sec = _chart_watch_backoff_sec(item, reason_code)
    item["next_eval_ts"] = now + float(backoff_sec)
    md = dict(market_data or {})
    if md:
        item["last_volume_m5"] = _safe_float(md.get("volume_m5", item.get("last_volume_m5", 0.0)), 0.0)
        item["last_liquidity_usd"] = _safe_float(md.get("liquidity_usd", item.get("last_liquidity_usd", 0.0)), 0.0)
        item["last_mcap_usd"] = _safe_float(md.get("mcap_usd", md.get("fdv", item.get("last_mcap_usd", 0.0))), 0.0)
        item["last_pair_address"] = str(md.get("pair_address", item.get("last_pair_address", "")) or "")
        if md.get("market_data_source"):
            item["last_market_data_source"] = str(md.get("market_data_source", "") or "")
    item["last_eval_ts"] = now
    item["last_seen_ts"] = now


async def _chart_watch_evaluate_token(token: str):
    token_s = str(token or "").strip()
    if len(token_s) < 32:
        return
    item = service.chart_watchlist_by_token.get(token_s)
    if not item:
        return
    if _chart_watch_is_open(token_s):
        item["state"] = "IN_POSITION"
        item["last_status"] = "ACTIVE"
        item["last_reason"] = "position currently open"
        item["failure_streak"] = 0
        item["suppressed"] = False
        item["last_eval_ts"] = time.time()
        return
    if not paper_trader or not paper_trader.engine:
        _chart_watch_apply_failure(item, "engine unavailable", "OTHER")
        return

    now = time.time()
    item["last_eval_started_ts"] = now
    service.chart_watch_eval_count_window.append(now)
    service.candle_signal_cache_ttl_sec = float(_chart_watch_candle_cache_ttl_sec())
    was_entry_near = str(item.get("state", "") or "").upper() == "ENTRY_NEAR"

    market_data = await _fetch_market_data_with_retries(
        token_s,
        [TRADING_MODE_CHART_PRO],
        for_chart_watch=True,
        entry_near_retry=was_entry_near,
    )
    if not market_data:
        _chart_watch_apply_failure(item, "market data unavailable", "NO_MARKET_DATA")
        return

    market_data["token"] = token_s
    token_age = await _chart_watch_get_token_age_cached(token_s)
    item["token_age_sec"] = int(token_age)

    admission = _chart_watch_admission_check(
        token_s,
        market_data,
        token_age,
        source_meta={
            "source": item.get("source", ""),
            "has_trend_signal": bool((item or {}).get("market_trend_data")),
        },
    )
    if not bool(admission.get("accepted", False)):
        reason = str(admission.get("reason", "suppressed") or "suppressed")
        code = str(admission.get("reason_code", _chart_watch_reason_code(reason)) or "OTHER")
        hard = bool(admission.get("hard_reject", False))
        if hard:
            _chart_watch_record_suppressed(
                token_s,
                source=str(item.get("source", "") or ""),
                reason=reason,
                reason_code=code,
                market_data=market_data,
                token_age_sec=token_age,
            )
            _chart_watch_drop(token_s, reason=reason)
        else:
            _chart_watch_apply_warming(item, reason=reason, reason_code=code, market_data=market_data)
        return

    service.chart_watch_suppressed_by_token.pop(token_s, None)
    item["suppressed"] = False
    item["admission_reason"] = str(admission.get("reason", "admitted") or "admitted")
    item["state"] = "WATCHING"
    item["last_status"] = "WATCH"
    item["last_reason"] = "admitted"
    item["last_volume_m5"] = _safe_float(market_data.get("volume_m5", item.get("last_volume_m5", 0.0)), 0.0)
    item["last_liquidity_usd"] = _safe_float(market_data.get("liquidity_usd", item.get("last_liquidity_usd", 0.0)), 0.0)
    item["last_mcap_usd"] = _safe_float(market_data.get("mcap_usd", market_data.get("fdv", item.get("last_mcap_usd", 0.0))), 0.0)
    item["last_pair_address"] = str(market_data.get("pair_address", item.get("last_pair_address", "")) or "")
    item["last_market_data_source"] = str(market_data.get("market_data_source", item.get("last_market_data_source", "")) or "")

    # Drop candidates that drift far below trend-quality floors to keep watchlist signal quality high.
    trend_min_vol, trend_min_liq, trend_min_mcap = _chart_watch_effective_trend_thresholds()
    trend_meta = dict(item.get("market_trend_data", {}) or {})
    hint_vol = _safe_float(trend_meta.get("volume_m5", 0.0), 0.0)
    hint_liq = _safe_float(trend_meta.get("liq_usd", 0.0), 0.0)
    hint_mcap = _safe_float(trend_meta.get("mcap_usd", 0.0), 0.0)
    md_vol = max(_safe_float(market_data.get("volume_m5", 0.0), 0.0), hint_vol)
    md_liq = max(_safe_float(market_data.get("liquidity_usd", 0.0), 0.0), hint_liq)
    md_mcap = max(_safe_float(market_data.get("mcap_usd", market_data.get("fdv", 0.0)), 0.0), hint_mcap)
    if trend_min_vol > 0 and md_vol < (trend_min_vol * 0.45):
        reason = f"trend vol m5 ${md_vol:,.0f} below ${trend_min_vol:,.0f}"
        _chart_watch_record_suppressed(token_s, source=str(item.get("source", "") or ""), reason=reason, reason_code="VOL_GATE", market_data=market_data, token_age_sec=token_age)
        _chart_watch_drop(token_s, reason=reason)
        return
    if trend_min_liq > 0 and md_liq < (trend_min_liq * 0.50):
        reason = f"trend liq ${md_liq:,.0f} below ${trend_min_liq:,.0f}"
        _chart_watch_record_suppressed(token_s, source=str(item.get("source", "") or ""), reason=reason, reason_code="LIQ_GATE", market_data=market_data, token_age_sec=token_age)
        _chart_watch_drop(token_s, reason=reason)
        return
    if trend_min_mcap > 0 and md_mcap < (trend_min_mcap * 0.50):
        reason = f"trend MC ${md_mcap:,.0f} below ${trend_min_mcap:,.0f}"
        _chart_watch_record_suppressed(token_s, source=str(item.get("source", "") or ""), reason=reason, reason_code="MCAP_GATE", market_data=market_data, token_age_sec=token_age)
        _chart_watch_drop(token_s, reason=reason)
        return

    candle_data = {}
    try:
        cached = service.candle_signal_cache_by_token.get(token_s, {}) or {}
        cache_age = time.time() - _safe_float(cached.get("ts", 0.0), 0.0)
        if cache_age <= float(service.candle_signal_cache_ttl_sec or 20.0):
            candle_data = dict(cached.get("data", {}) or {})
        else:
            pair_addr_for_candles = market_data.get("pair_address", "")
            candle_cfg = {
                "token_mint": token_s,
                "breakout_lookback_bars": int(service.settings.get("chart_pro_breakout_lookback_bars", 20) or 20),
                "min_volume_multiple": float(service.settings.get("chart_pro_min_volume_multiple", 1.8) or 1.8),
                "require_retest": bool(service.settings.get("chart_pro_require_retest", False)),
                "stop_atr_mult": float(service.settings.get("chart_pro_stop_atr_mult", 1.2) or 1.2),
                "tp1_r": float(service.settings.get("chart_pro_tp1_r", 1.0) or 1.0),
                "tp2_r": float(service.settings.get("chart_pro_tp2_r", 2.0) or 2.0),
                "runner_trail_atr_mult": float(service.settings.get("chart_pro_runner_trail_atr_mult", 1.5) or 1.5),
            }
            candle_timeout = max(1.0, min(10.0, float(service.settings.get("chart_watch_candle_timeout_sec", 4.0) or 4.0)))
            try:
                candle_data = await asyncio.wait_for(
                    candle_analyzer.analyze(pair_addr_for_candles, config=candle_cfg),
                    timeout=candle_timeout,
                )
            except asyncio.TimeoutError:
                candle_data = {"reason": "candles_timeout", "reason_code": "RATE_LIMITED"}
            service.candle_signal_cache_by_token[token_s] = {"ts": time.time(), "data": dict(candle_data or {})}
    except Exception:
        candle_data = {}

    quality = dict((candle_data or {}).get("quality", {}) or {})
    trigger = dict((candle_data or {}).get("trigger", {}) or {})
    chart_score = _safe_float(quality.get("score", 0.0), 0.0)
    min_chart_score = _safe_float(service.settings.get("chart_pro_min_score", 42.0), 42.0)
    fast_entry_ready, fast_entry_reason = _chart_pro_fast_entry_signal(candle_data, market_data)
    item["last_score"] = chart_score
    item["opportunity_score"] = _chart_watch_opportunity_score(item, market_data, candle_data)

    candle_reason = str((candle_data or {}).get("reason", "") or "")
    if candle_reason in ("candles_rate_limited", "insufficient_candles", "no_pool"):
        code = str((candle_data or {}).get("reason_code", "") or _chart_watch_reason_code(candle_reason))
        if code == "RATE_LIMITED":
            service.chart_watch_candle_rate_limited_count = int(service.chart_watch_candle_rate_limited_count or 0) + 1
        _chart_watch_apply_failure(item, f"chart unavailable ({candle_reason})", code, market_data=market_data, score=chart_score)
        return
    if chart_score < max(0.0, min_chart_score - 14.0) and not fast_entry_ready:
        _chart_watch_apply_failure(item, f"chart score below threshold ({chart_score:.1f}/{min_chart_score:.1f})", "LOW_SCORE", market_data=market_data, score=chart_score)
        return
    if not bool(trigger.get("entry_ready", False)) and not fast_entry_ready:
        _chart_watch_apply_failure(item, "no chart trigger", "NO_TRIGGER", market_data=market_data, score=chart_score)
        return

    # Stage C (expensive) only for entry-near tokens.
    is_safe, safety_msg = await _chart_watch_get_safety_cached(token_s)
    if not is_safe:
        _chart_watch_apply_failure(item, f"risk veto: {safety_msg or 'unsafe token'}", "OTHER", market_data=market_data, score=chart_score)
        return
    holder_data = await _chart_watch_get_holder_cached(token_s, str(market_data.get("pair_address", "") or ""))
    pretrade_veto = _get_pretrade_veto_reason(market_data, holder_data, candle_data, TRADING_MODE_CHART_PRO)
    if pretrade_veto:
        _chart_watch_apply_failure(item, pretrade_veto, "OTHER", market_data=market_data, score=chart_score)
        return

    # Progression success.
    item["failure_streak"] = 0
    item["last_fail_reason"] = ""
    item["suppressed"] = False
    item["state"] = "ENTRY_NEAR"
    item["last_status"] = "BUY_SIGNAL"
    if fast_entry_ready and not bool(trigger.get("entry_ready", False)):
        item["last_reason"] = f"entry ready ({fast_entry_reason}, score={chart_score:.1f})"
    else:
        item["last_reason"] = f"entry ready (score={chart_score:.1f})"
    item["admission_reason"] = "admitted"
    item["last_eval_ts"] = time.time()
    item["last_seen_ts"] = item["last_eval_ts"]
    item["next_eval_ts"] = item["last_eval_ts"] + max(5.0, _chart_watch_eval_interval_sec())

    last_trigger = _safe_float(item.get("last_entry_trigger_ts", 0.0), 0.0)
    if (time.time() - last_trigger) < 25.0:
        return
    item["last_entry_trigger_ts"] = time.time()
    await service.queue.put(
        {
            "type": "NEW_POOL",
            "signature": token_s,
            "source": str(item.get("source", "ChartWatch") or "ChartWatch"),
            "time": time.time(),
            "chart_watch_entry": True,
            "market_trend_data": dict(item.get("market_trend_data", {}) or {}),
            "social_data": dict(item.get("social_data", {}) or {}),
        }
    )


async def _fetch_market_trend_candidates(limit: int = 20) -> List[dict]:
    """
    Pulls token candidates from independent market-trend feeds.
    Returns [{"token": <mint>, "source": "...", "boost_amount": float, "boost_total": float}, ...]
    """
    timeout = aiohttp.ClientTimeout(total=10.0)
    merged = {}
    now_ts = time.time()
    strict_age_sec = max(0, _chart_watch_strict_age_sec())
    source_mode = _chart_watch_source_mode()
    birdeye_key = os.getenv("BIRDEYE_API_KEY", "").strip()

    def _source_short_name(source_label: str) -> str:
        return str(source_label or "MarketTrend").split(":", 1)[-1].strip() or "MarketTrend"

    def _parse_ts_sec(value) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 2_000_000_000_000:
                ts /= 1000.0
            return ts if ts > 1_000_000_000 else 0.0
        text = str(value or "").strip()
        if not text:
            return 0.0
        try:
            num = float(text)
            if num > 2_000_000_000_000:
                num /= 1000.0
            return num if num > 1_000_000_000 else 0.0
        except Exception:
            pass
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return datetime.fromisoformat(text).timestamp()
        except Exception:
            return 0.0

    def _token_age_from_row(row: dict) -> int:
        created_ts = 0.0
        for key in ("raw_created_at", "created_at", "createdAt", "pool_created_at", "created_time", "createTime", "created_time_ms"):
            created_ts = max(created_ts, _parse_ts_sec((row or {}).get(key)))
        age = int(max(0.0, now_ts - created_ts)) if created_ts > 0 else 0
        return age

    def _extract_token_from_any(item: dict) -> str:
        d = dict(item or {})
        for key in ("token", "address", "mint", "tokenAddress", "token_address", "token_mint"):
            v = d.get(key)
            if isinstance(v, dict):
                for sk in ("address", "mint", "tokenAddress", "token_address"):
                    sv = v.get(sk)
                    if _is_chart_watch_candidate_mint(sv):
                        return _normalize_candidate_mint(sv)
            if _is_chart_watch_candidate_mint(v):
                return _normalize_candidate_mint(v)
        return ""

    def _merge_candidate(row: dict):
        token = _normalize_candidate_mint((row or {}).get("token"))
        if not _is_chart_watch_candidate_mint(token):
            return
        merged_row = dict(row or {})
        merged_row["token"] = token
        source_label = str(merged_row.get("source", "MarketTrend") or "MarketTrend")
        merged_row["source"] = source_label
        merged_row["source_short"] = _source_short_name(source_label)
        if not merged_row.get("sources"):
            merged_row["sources"] = merged_row["source_short"]
        if _safe_int(merged_row.get("raw_age_sec", 0), 0) <= 0:
            merged_row["raw_age_sec"] = _token_age_from_row(merged_row)
        rank = _safe_float(merged_row.get("rank_score", 0.0), 0.0)
        prev = merged.get(token)
        if not prev:
            merged[token] = merged_row
            return
        prev_rank = _safe_float(prev.get("rank_score", 0.0), 0.0)
        if rank > prev_rank:
            prev.update(merged_row)
            prev["token"] = token
        prev_sources = set(str(prev.get("sources", "") or "").split("|")) if prev.get("sources") else set()
        row_sources = set(str(merged_row.get("sources", "") or "").split("|")) if merged_row.get("sources") else set()
        sources = {s for s in (prev_sources | row_sources) if s}
        if sources:
            prev["sources"] = "|".join(sorted(sources))
            prev["rank_score"] = _safe_float(prev.get("rank_score", 0.0), 0.0) + (max(0, len(sources) - 1) * 150.0)
        for key in ("liq_usd", "volume_m5", "volume_h1", "vol_24h", "mcap_usd", "txns_h1", "txns_24h", "source_priority"):
            prev[key] = max(_safe_float(prev.get(key, 0.0), 0.0), _safe_float(merged_row.get(key, 0.0), 0.0))
        prev_age = _safe_int(prev.get("raw_age_sec", 0), 0)
        row_age = _safe_int(merged_row.get("raw_age_sec", 0), 0)
        if prev_age <= 0 or (row_age > 0 and row_age < prev_age):
            prev["raw_age_sec"] = row_age

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Source 1 (primary): Birdeye token trending (requires API key).
        birdeye_rows = 0
        if source_mode == "birdeye_primary" and birdeye_key:
            headers = {"X-API-KEY": birdeye_key, "x-chain": "solana", "accept": "application/json"}
            for sort_by in ("rank", "liquidity", "volume24hUSD"):
                params = {
                    "sort_by": sort_by,
                    "sort_type": "asc" if sort_by == "rank" else "desc",
                    "offset": 0,
                    "limit": max(25, min(100, int(limit or 20) * 4)),
                }
                payload = {}
                for path in ("/defi/token_trending", "/defi/v3/token/trending"):
                    try:
                        async with session.get(f"{BIRDEYE_BASE_URL}{path}", headers=headers, params=params) as resp:
                            if resp.status == 200:
                                payload = await resp.json()
                                break
                    except Exception:
                        continue
                rows = []
                if isinstance(payload, dict):
                    data = payload.get("data", payload.get("result", payload.get("items", payload.get("tokens", []))))
                    if isinstance(data, dict):
                        rows = (
                            data.get("items")
                            or data.get("tokens")
                            or data.get("list")
                            or data.get("data")
                            or []
                        )
                    elif isinstance(data, list):
                        rows = data
                elif isinstance(payload, list):
                    rows = payload

                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    token = _extract_token_from_any(item)
                    if not token:
                        continue
                    vol_24h = _safe_float(item.get("volume24hUSD", item.get("v24hUSD", item.get("volume24h", 0.0))), 0.0)
                    liq = _safe_float(item.get("liquidity", item.get("liquidityUSD", item.get("liquidity_usd", 0.0))), 0.0)
                    mcap = _safe_float(item.get("marketCap", item.get("market_cap", item.get("market_cap_usd", item.get("fdv", 0.0)))), 0.0)
                    txns_24h = _safe_float(item.get("txns24h", item.get("tx24h", item.get("trade24h", 0.0))), 0.0)
                    rank = _safe_float(item.get("rank", item.get("position", 0.0)), 0.0)
                    created_ts = 0.0
                    for key in ("createdAt", "created_at", "createTime", "created_time", "pairCreatedAt", "launchTime"):
                        created_ts = max(created_ts, _parse_ts_sec(item.get(key)))
                    age_sec = int(max(0.0, now_ts - created_ts)) if created_ts > 0 else 0
                    chg_m5 = abs(_safe_float(item.get("priceChange5m", item.get("price_change_m5", 0.0)), 0.0))
                    chg_h1 = abs(_safe_float(item.get("priceChange1h", item.get("price_change_h1", 0.0)), 0.0))
                    freshness_bonus = 0.0
                    stale_penalty = 0.0
                    if strict_age_sec > 0 and age_sec > 0:
                        if age_sec > strict_age_sec:
                            continue
                        freshness_bonus = 125000.0
                    rank_component = max(0.0, 300.0 - rank) * 100.0 if rank > 0 else 0.0
                    rank_score = (
                        rank_component
                        + (vol_24h * 0.045)
                        + (liq * 0.34)
                        + (txns_24h * 12.0)
                        + ((chg_m5 * 1.6 + chg_h1 * 0.8) * 40.0)
                        + freshness_bonus
                        - stale_penalty
                    )
                    _merge_candidate(
                        {
                            "token": token,
                            "source": "MarketTrend: BirdeyeTrending",
                            "rank_score": rank_score,
                            "sources": "BirdeyeTrending",
                            "source_priority": 100.0,
                            "vol_24h": vol_24h,
                            "liq_usd": liq,
                            "mcap_usd": mcap,
                            "txns_24h": txns_24h,
                            "raw_age_sec": age_sec,
                            "raw_created_at": created_ts,
                        }
                    )
                    birdeye_rows += 1

        # Source 2: DexScreener boosts (paid momentum).
        for url in (
            "https://api.dexscreener.com/token-boosts/latest/v1",
            "https://api.dexscreener.com/token-boosts/top/v1",
        ):
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        continue
                    payload = await resp.json()
            except Exception:
                continue

            if not isinstance(payload, list):
                continue
            for item in payload:
                if not isinstance(item, dict):
                    continue
                chain = str(item.get("chainId", "") or "").lower()
                token = str(item.get("tokenAddress", "") or "").strip()
                if chain != "solana" or not _is_chart_watch_candidate_mint(token):
                    continue
                boost_amount = _safe_float(item.get("amount", 0.0), 0.0)
                boost_total = _safe_float(item.get("totalAmount", 0.0), 0.0)
                rank_score = (boost_total * 100.0) + boost_amount + 450.0
                _merge_candidate(
                    {
                        "token": token,
                        "source": "MarketTrend: DexBoost",
                        "boost_amount": boost_amount,
                        "boost_total": boost_total,
                        "rank_score": rank_score,
                        "sources": "DexBoost",
                        "source_priority": 65.0,
                    }
                )

        # Source 3: DexScreener search snapshots (high-volume discovery parity).
        for query in ("solana pump", "solana meme", "pump fun"):
            try:
                async with session.get("https://api.dexscreener.com/latest/dex/search", params={"q": query}) as resp:
                    if resp.status != 200:
                        continue
                    payload = await resp.json()
            except Exception:
                continue

            if not isinstance(payload, dict):
                continue
            for pair in (payload.get("pairs", []) or []):
                if not isinstance(pair, dict):
                    continue
                chain = str(pair.get("chainId", "") or "").lower()
                if chain != "solana":
                    continue
                base = dict(pair.get("baseToken", {}) or {})
                quote = dict(pair.get("quoteToken", {}) or {})
                token = _pick_pool_candidate_mint(
                    str(base.get("address", "") or ""),
                    str(quote.get("address", "") or ""),
                )
                if not token:
                    continue
                volume = dict(pair.get("volume", {}) or {})
                txns = dict(pair.get("txns", {}) or {})
                tx_h1 = dict(txns.get("h1", {}) or {})
                txns_h1 = _safe_float(tx_h1.get("buys", 0.0), 0.0) + _safe_float(tx_h1.get("sells", 0.0), 0.0)
                vol_m5 = _safe_float(volume.get("m5", 0.0), 0.0)
                vol_h1 = _safe_float(volume.get("h1", 0.0), 0.0)
                liq = _safe_float((pair.get("liquidity", {}) or {}).get("usd", 0.0), 0.0)
                mcap = _safe_float(pair.get("marketCap", pair.get("fdv", 0.0)), 0.0)
                if (vol_m5 <= 0 and txns_h1 < 20.0) or liq < 3000.0:
                    continue
                if mcap > 100000000.0:
                    continue
                created_ts = _parse_ts_sec(pair.get("pairCreatedAt"))
                age_sec = int(max(0.0, now_ts - created_ts)) if created_ts > 0 else 0
                price_change = dict(pair.get("priceChange", {}) or {})
                chg_m5 = abs(_safe_float(price_change.get("m5", 0.0), 0.0))
                chg_h1 = abs(_safe_float(price_change.get("h1", 0.0), 0.0))
                freshness_mult = 1.0
                freshness_bonus = 0.0
                if strict_age_sec > 0 and age_sec > 0:
                    if age_sec <= strict_age_sec:
                        freshness_mult = 1.30
                        freshness_bonus = 80000.0
                    else:
                        freshness_mult = 0.25
                activity = (vol_m5 * 4.0) + (vol_h1 * 0.20) + (txns_h1 * 95.0)
                momentum = ((chg_m5 * 2.0) + (chg_h1 * 0.8)) * 55.0
                quality = (min(liq, 250000.0) * 0.10) + (min(mcap, 20000000.0) * 0.004)
                rank_score = ((activity + momentum + quality) * freshness_mult) + freshness_bonus
                _merge_candidate(
                    {
                        "token": token,
                        "source": "MarketTrend: DexSearch",
                        "rank_score": rank_score,
                        "sources": "DexSearch",
                        "volume_m5": vol_m5,
                        "volume_h1": vol_h1,
                        "liq_usd": liq,
                        "mcap_usd": mcap,
                        "txns_h1": txns_h1,
                        "raw_age_sec": age_sec,
                        "raw_created_at": created_ts,
                        "source_priority": 70.0,
                    }
                )

        # Source 4: GeckoTerminal trending pools (organic trend momentum).
        for page in (1, 2):
            gecko_url = f"https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page={page}"
            try:
                async with session.get(gecko_url) as resp:
                    if resp.status != 200:
                        continue
                    payload = await resp.json()
            except Exception:
                continue

            if not isinstance(payload, dict):
                continue
            for pool in (payload.get("data", []) or []):
                if not isinstance(pool, dict):
                    continue
                attrs = dict(pool.get("attributes", {}) or {})
                rel = dict(pool.get("relationships", {}) or {})
                base_data = dict((rel.get("base_token", {}) or {}).get("data", {}) or {})
                quote_data = dict((rel.get("quote_token", {}) or {}).get("data", {}) or {})
                token = _pick_pool_candidate_mint(
                    str(base_data.get("id", attrs.get("base_token_address", "")) or ""),
                    str(quote_data.get("id", attrs.get("quote_token_address", "")) or ""),
                )
                if not token:
                    continue

                vol_m5 = _safe_float((attrs.get("volume_usd", {}) or {}).get("m5", 0.0), 0.0)
                vol_h1 = _safe_float((attrs.get("volume_usd", {}) or {}).get("h1", 0.0), 0.0)
                liq = _safe_float(attrs.get("reserve_in_usd", 0.0), 0.0)
                mcap = _safe_float(attrs.get("market_cap_usd", attrs.get("fdv_usd", 0.0)), 0.0)
                tx_h1 = dict((attrs.get("transactions", {}) or {}).get("h1", {}) or {})
                buys_h1 = _safe_float(tx_h1.get("buys", 0.0), 0.0)
                sells_h1 = _safe_float(tx_h1.get("sells", 0.0), 0.0)
                txns_h1 = max(0.0, buys_h1 + sells_h1)
                created_ts = 0.0
                for key in ("pool_created_at", "created_at", "poolCreatedAt"):
                    created_ts = max(created_ts, _parse_ts_sec(attrs.get(key)))
                age_sec = int(max(0.0, now_ts - created_ts)) if created_ts > 0 else 0
                if strict_age_sec > 0 and age_sec > 0 and age_sec > strict_age_sec:
                    continue
                freshness_mult = 1.0
                freshness_bonus = 0.0
                if strict_age_sec > 0 and age_sec > 0 and age_sec <= strict_age_sec:
                    freshness_mult = 1.35
                    freshness_bonus = 90000.0
                rank_score = (
                    ((vol_m5 * 2.8) + (vol_h1 * 0.30) + (txns_h1 * 60.0) + (liq * 0.24) + (mcap * 0.015))
                    * freshness_mult
                ) + freshness_bonus
                _merge_candidate(
                    {
                        "token": token,
                        "source": "MarketTrend: GeckoTrending",
                        "boost_amount": vol_m5,
                        "boost_total": max(1.0, txns_h1),
                        "rank_score": rank_score,
                        "sources": "GeckoTrending",
                        "volume_m5": vol_m5,
                        "volume_h1": vol_h1,
                        "liq_usd": liq,
                        "mcap_usd": mcap,
                        "txns_h1": txns_h1,
                        "raw_age_sec": age_sec,
                        "raw_created_at": created_ts,
                        "source_priority": 60.0,
                    }
                )

        # Source 5: DexScreener latest token profiles (new-token discovery).
        if source_mode != "public_fallback":
            payload = []
        else:
            try:
                async with session.get("https://api.dexscreener.com/token-profiles/latest/v1") as resp:
                    payload = await resp.json() if resp.status == 200 else []
            except Exception:
                payload = []
        if isinstance(payload, list):
            for idx, item in enumerate(payload[:250]):
                if not isinstance(item, dict):
                    continue
                chain = str(item.get("chainId", "") or "").lower()
                token = str(item.get("tokenAddress", "") or "").strip()
                if chain != "solana" or not _is_chart_watch_candidate_mint(token):
                    continue
                # Recency score (higher for earlier rows) with modest weight.
                recency_score = max(1.0, 260.0 - float(idx))
                _merge_candidate(
                    {
                        "token": token,
                        "source": "MarketTrend: DexProfiles",
                        "boost_amount": recency_score,
                        "boost_total": recency_score,
                        "rank_score": recency_score * 4.0,
                        "sources": "DexProfiles",
                        "source_priority": 45.0,
                    }
                )

    if source_mode == "birdeye_primary" and not birdeye_key:
        if not bool(service.chart_watch_birdeye_warned_missing):
            await service.broadcast({"type": "log", "msg": "Chart watch trend source mode is birdeye_primary but BIRDEYE_API_KEY is missing; using public fallback feeds."})
            service.chart_watch_birdeye_warned_missing = True
    else:
        service.chart_watch_birdeye_warned_missing = False
    if source_mode == "birdeye_primary" and birdeye_key and birdeye_rows <= 0:
        now_warn = time.time()
        if (now_warn - _safe_float(service.chart_watch_last_birdeye_empty_warn_ts, 0.0)) >= 300.0:
            await service.broadcast({"type": "log", "msg": "Birdeye trending returned no rows; using Gecko/Dex fallback feeds this cycle."})
            service.chart_watch_last_birdeye_empty_warn_ts = now_warn

    rows = sorted(merged.values(), key=lambda x: float(x.get("rank_score", 0.0) or 0.0), reverse=True)
    return rows[: max(1, int(limit or 1))]


async def _chart_watch_admit_trend_candidates(
    candidates: List[dict],
    max_emit: int,
    cooldown_sec: int,
    bypass_cooldown: bool = False,
) -> dict:
    emitted = 0
    emitted_by_source = {}
    seen_by_source = {}
    post_gate_count = 0
    now = time.time()
    strict_age = _chart_watch_strict_age_sec()
    min_mcap, max_mcap = _chart_watch_admission_mcap_band()

    for row in candidates or []:
        token = str((row or {}).get("token", "") or "")
        if not token:
            continue
        source_label = str((row or {}).get("source", "MarketTrend") or "MarketTrend")
        source_key = str(source_label or "MarketTrend").split(":", 1)[-1].strip() or "MarketTrend"
        seen_by_source[source_key] = int(seen_by_source.get(source_key, 0) or 0) + 1
        last_seen = float(service.market_trend_seen_by_token.get(token, 0.0) or 0.0)
        if (not bypass_cooldown) and cooldown_sec > 0 and (now - last_seen) < cooldown_sec:
            continue

        hint_age = max(0, _safe_int((row or {}).get("raw_age_sec", 0), 0))
        if strict_age > 0 and hint_age > 0 and hint_age > strict_age:
            _chart_watch_record_suppressed(
                token,
                source=source_label,
                reason=f"token age {hint_age}s exceeds max {strict_age}s",
                reason_code="AGE_GATE",
                market_data={},
                token_age_sec=hint_age,
            )
            continue

        hint_mcap = _safe_float((row or {}).get("mcap_usd", 0.0), 0.0)
        if hint_mcap > 0 and ((hint_mcap < min_mcap) or (max_mcap > 0 and hint_mcap > max_mcap)):
            profile = _chart_watch_focus_profile()
            # In high-frequency mode, ignore soft hint-band mismatches and only reject obvious mega-caps.
            if profile != "high_frequency":
                _chart_watch_record_suppressed(
                    token,
                    source=source_label,
                    reason=f"MC ${hint_mcap:,.0f} outside ${min_mcap:,.0f}-${max_mcap:,.0f}",
                    reason_code="MCAP_BAND_GATE",
                    market_data={},
                    token_age_sec=hint_age,
                )
                continue
            if max_mcap > 0 and hint_mcap > (max_mcap * 3.0):
                _chart_watch_record_suppressed(
                    token,
                    source=source_label,
                    reason=f"MC ${hint_mcap:,.0f} outside ${min_mcap:,.0f}-${max_mcap:,.0f}",
                    reason_code="MCAP_BAND_GATE",
                    market_data={},
                    token_age_sec=hint_age,
                )
                continue

        post_gate_count += 1
        market_data = {}
        token_age = 0
        hint_vol = _safe_float((row or {}).get("volume_m5", 0.0), 0.0)
        hint_liq = _safe_float((row or {}).get("liq_usd", 0.0), 0.0)
        hint_mcap_live = _safe_float((row or {}).get("mcap_usd", 0.0), 0.0)
        if paper_trader and paper_trader.engine:
            market_data = await paper_trader.engine.get_token_price(token) or {}
            token_age = await _chart_watch_get_token_age_cached(token)
        if hint_vol > 0 and _safe_float(market_data.get("volume_m5", 0.0), 0.0) <= 0:
            market_data["volume_m5"] = hint_vol
        if hint_liq > 0 and _safe_float(market_data.get("liquidity_usd", 0.0), 0.0) <= 0:
            market_data["liquidity_usd"] = hint_liq
        if hint_mcap_live > 0 and _safe_float(market_data.get("mcap_usd", market_data.get("fdv", 0.0)), 0.0) <= 0:
            market_data["mcap_usd"] = hint_mcap_live
        # Trend-quality hard gates (match external trending boards).
        trend_min_vol, trend_min_liq, trend_min_mcap = _chart_watch_effective_trend_thresholds()
        md_vol = _safe_float(market_data.get("volume_m5", hint_vol), hint_vol)
        md_liq = _safe_float(market_data.get("liquidity_usd", hint_liq), hint_liq)
        md_mcap = _safe_float(market_data.get("mcap_usd", market_data.get("fdv", hint_mcap_live)), hint_mcap_live)
        md_change_m5 = _safe_float(market_data.get("price_change_m5", 0.0), 0.0)
        profile = _chart_watch_focus_profile()
        hf_profile = profile in ("high_frequency", "volatility_first", "momentum_first")
        vol_soft_ratio = 0.50
        liq_soft_ratio = 0.50
        mcap_soft_ratio = 0.60
        if profile == "volatility_first":
            vol_soft_ratio = 0.70
            liq_soft_ratio = 0.45
            mcap_soft_ratio = 0.45
        elif profile == "momentum_first":
            vol_soft_ratio = 0.65
            liq_soft_ratio = 0.60
            mcap_soft_ratio = 0.60
            # Strong short-term momentum can justify warming slightly earlier.
            if md_change_m5 >= 4.0:
                vol_soft_ratio = max(0.45, vol_soft_ratio - 0.12)
                liq_soft_ratio = max(0.45, liq_soft_ratio - 0.08)
                mcap_soft_ratio = max(0.50, mcap_soft_ratio - 0.06)

        def _maybe_soft_warm(reason: str, code: str, pass_ratio: float) -> bool:
            if not hf_profile:
                return False
            gate_floor = max(1.0, float(pass_ratio))
            current_value = 0.0
            required_value = 0.0
            if code == "VOL_GATE":
                current_value = md_vol
                required_value = trend_min_vol
            elif code == "LIQ_GATE":
                current_value = md_liq
                required_value = trend_min_liq
            elif code == "MCAP_GATE":
                current_value = md_mcap
                required_value = trend_min_mcap
            if required_value <= 0:
                return False
            if current_value < (required_value * gate_floor):
                return False
            item = _chart_watch_upsert(
                token,
                event={"signature": token, "market_trend_data": dict(row or {})},
                source=source_label,
                signature=token,
            )
            if item:
                _chart_watch_apply_warming(item, reason=reason, reason_code=code, market_data=market_data)
                service.market_trend_seen_by_token[token] = now
            return True

        if trend_min_vol > 0 and md_vol < trend_min_vol:
            reason = f"Trend vol m5 ${md_vol:,.0f} below ${trend_min_vol:,.0f}"
            if _maybe_soft_warm(reason, "VOL_GATE", vol_soft_ratio):
                continue
            _chart_watch_record_suppressed(
                token,
                source=source_label,
                reason=reason,
                reason_code="VOL_GATE",
                market_data=market_data,
                token_age_sec=token_age,
            )
            continue
        if trend_min_liq > 0 and md_liq < trend_min_liq:
            reason = f"Trend liq ${md_liq:,.0f} below ${trend_min_liq:,.0f}"
            if _maybe_soft_warm(reason, "LIQ_GATE", liq_soft_ratio):
                continue
            _chart_watch_record_suppressed(
                token,
                source=source_label,
                reason=reason,
                reason_code="LIQ_GATE",
                market_data=market_data,
                token_age_sec=token_age,
            )
            continue
        if trend_min_mcap > 0 and md_mcap < trend_min_mcap:
            reason = f"Trend MC ${md_mcap:,.0f} below ${trend_min_mcap:,.0f}"
            if _maybe_soft_warm(reason, "MCAP_GATE", mcap_soft_ratio):
                continue
            _chart_watch_record_suppressed(
                token,
                source=source_label,
                reason=reason,
                reason_code="MCAP_GATE",
                market_data=market_data,
                token_age_sec=token_age,
            )
            continue
        admission = _chart_watch_admission_check(
            token,
            market_data,
            token_age,
            source_meta={"source": source_label, "has_trend_signal": True},
        )
        if not bool(admission.get("accepted", False)):
            hard = bool(admission.get("hard_reject", False))
            if hard:
                _chart_watch_record_suppressed(
                    token,
                    source=source_label,
                    reason=str(admission.get("reason", "suppressed") or "suppressed"),
                    reason_code=str(admission.get("reason_code", "OTHER") or "OTHER"),
                    market_data=market_data,
                    token_age_sec=token_age,
                )
                continue
            # Soft gate: still insert token into watchlist in WARMING state.
            item = _chart_watch_upsert(
                token,
                event={"signature": token, "market_trend_data": dict(row or {})},
                source=source_label,
                signature=token,
            )
            if item:
                _chart_watch_apply_warming(
                    item,
                    reason=str(admission.get("reason", "warming") or "warming"),
                    reason_code=str(admission.get("reason_code", "OTHER") or "OTHER"),
                    market_data=market_data,
                )
            continue

        service.market_trend_seen_by_token[token] = now
        item = _chart_watch_upsert(
            token,
            event={"signature": token, "market_trend_data": dict(row or {})},
            source=source_label,
            signature=token,
        )
        item["suppressed"] = False
        item["admission_reason"] = str(admission.get("reason", "admitted") or "admitted")
        item["token_age_sec"] = int(token_age)
        item["last_volume_m5"] = _safe_float((market_data or {}).get("volume_m5", item.get("last_volume_m5", 0.0)), 0.0)
        item["last_liquidity_usd"] = _safe_float((market_data or {}).get("liquidity_usd", item.get("last_liquidity_usd", 0.0)), 0.0)
        item["last_mcap_usd"] = _safe_float((market_data or {}).get("mcap_usd", (market_data or {}).get("fdv", item.get("last_mcap_usd", 0.0))), 0.0)
        item["next_eval_ts"] = 0.0
        emitted += 1
        emitted_by_source[source_key] = int(emitted_by_source.get(source_key, 0) or 0) + 1
        if emitted >= max_emit:
            break

    return {
        "emitted": int(emitted),
        "emitted_by_source": {k: int(v) for k, v in sorted(emitted_by_source.items())},
        "seen_by_source": {k: int(v) for k, v in sorted(seen_by_source.items())},
        "post_gate_count": int(post_gate_count),
    }


async def _chart_watch_prewarm_candles(tokens: List[str], limit: int = 6):
    if not _chart_watch_prewarm_candles_enabled():
        return
    if not tokens:
        return
    max_tokens = max(1, min(20, int(limit or 6)))
    ttl_sec = float(_chart_watch_candle_cache_ttl_sec())
    service.candle_signal_cache_ttl_sec = ttl_sec
    selected = []
    now = time.time()
    for token in tokens:
        token_s = str(token or "").strip()
        if len(token_s) < 32:
            continue
        cached = service.candle_signal_cache_by_token.get(token_s, {}) or {}
        age = now - _safe_float(cached.get("ts", 0.0), 0.0)
        if age <= ttl_sec:
            continue
        selected.append(token_s)
        if len(selected) >= max_tokens:
            break
    if not selected:
        return

    sem = asyncio.Semaphore(_chart_watch_eval_concurrency())

    async def _one(token_s: str):
        async with sem:
            try:
                md = await _fetch_market_data_with_retries(
                    token_s,
                    [TRADING_MODE_CHART_PRO],
                    for_chart_watch=True,
                    entry_near_retry=False,
                )
                pair_addr = str((md or {}).get("pair_address", "") or "")
                if not pair_addr:
                    return
                candle_cfg = {
                    "token_mint": token_s,
                    "breakout_lookback_bars": int(service.settings.get("chart_pro_breakout_lookback_bars", 20) or 20),
                    "min_volume_multiple": float(service.settings.get("chart_pro_min_volume_multiple", 1.8) or 1.8),
                    "require_retest": bool(service.settings.get("chart_pro_require_retest", False)),
                    "stop_atr_mult": float(service.settings.get("chart_pro_stop_atr_mult", 1.2) or 1.2),
                    "tp1_r": float(service.settings.get("chart_pro_tp1_r", 1.0) or 1.0),
                    "tp2_r": float(service.settings.get("chart_pro_tp2_r", 2.0) or 2.0),
                    "runner_trail_atr_mult": float(service.settings.get("chart_pro_runner_trail_atr_mult", 1.5) or 1.5),
                }
                candle_timeout = max(1.0, min(10.0, float(service.settings.get("chart_watch_candle_timeout_sec", 4.0) or 4.0)))
                try:
                    candle_data = await asyncio.wait_for(
                        candle_analyzer.analyze(pair_addr, config=candle_cfg),
                        timeout=candle_timeout,
                    )
                except asyncio.TimeoutError:
                    candle_data = {"reason": "candles_timeout", "reason_code": "RATE_LIMITED"}
                service.candle_signal_cache_by_token[token_s] = {"ts": time.time(), "data": dict(candle_data or {})}
            except Exception:
                return

    await asyncio.gather(*[_one(t) for t in selected], return_exceptions=True)


async def _run_chart_watch_startup_burst() -> int:
    if not _chart_watch_startup_burst_enabled():
        return 0
    if not _chart_watch_enabled():
        return 0
    target = _chart_watch_startup_burst_target()
    timeout_sec = _chart_watch_startup_burst_timeout_sec()
    deadline = time.time() + float(timeout_sec)
    cooldown_sec = max(0, int(service.settings.get("market_trend_requeue_cooldown_sec", 900) or 900))
    default_emit = max(1, min(25, int(service.settings.get("market_trend_max_new_per_scan", 12) or 12)))
    admitted_total = 0
    seen_accum = {}
    admitted_accum = {}
    service.chart_watch_startup_burst_active = True
    service.chart_watch_startup_burst_admitted = 0
    try:
        while service.running and _chart_watch_enabled() and time.time() < deadline and admitted_total < target:
            remaining = max(1, target - admitted_total)
            fetch_limit = max(40, min(250, remaining * 20))
            candidates = await _fetch_market_trend_candidates(limit=fetch_limit)
            service.chart_watch_candidate_pool_size_raw = len(candidates or [])
            if not candidates:
                await _broadcast_chart_watch_snapshot(force=True)
                await asyncio.sleep(0.6)
                continue
            result = await _chart_watch_admit_trend_candidates(
                candidates=candidates,
                max_emit=max(default_emit, remaining),
                cooldown_sec=cooldown_sec,
                bypass_cooldown=True,
            )
            service.chart_watch_candidate_pool_size_post_gate = int(result.get("post_gate_count", 0) or 0)
            admitted_total += int(result.get("emitted", 0) or 0)
            service.chart_watch_startup_burst_admitted = admitted_total
            for k, v in (result.get("seen_by_source", {}) or {}).items():
                seen_accum[k] = int(seen_accum.get(k, 0) or 0) + int(v or 0)
            for k, v in (result.get("emitted_by_source", {}) or {}).items():
                admitted_accum[k] = int(admitted_accum.get(k, 0) or 0) + int(v or 0)
            service.chart_watch_source_mix = {
                "seen": {k: int(v) for k, v in sorted(seen_accum.items())},
                "admitted": {k: int(v) for k, v in sorted(admitted_accum.items())},
            }
            if _chart_watch_prewarm_candles_enabled():
                ranked_admitted = sorted(
                    (
                        token
                        for token, item in service.chart_watchlist_by_token.items()
                        if not bool((item or {}).get("suppressed", False))
                    ),
                    key=lambda t: _chart_watch_priority(service.chart_watchlist_by_token.get(t, {})),
                    reverse=True,
                )
                prewarm_limit = max(2, min(20, _chart_watch_eval_budget_per_cycle()))
                await _chart_watch_prewarm_candles(ranked_admitted[:prewarm_limit], limit=prewarm_limit)
            await _broadcast_chart_watch_snapshot(force=True)
            if int(result.get("emitted", 0) or 0) <= 0:
                await asyncio.sleep(0.8)
            else:
                await asyncio.sleep(0.2)
    finally:
        service.chart_watch_startup_burst_active = False
    return int(admitted_total)


def _live_exit_profile_for_mode(mode: str) -> dict:
    mode_key = str(mode or "").strip().lower()
    if mode_key == TRADING_MODE_CHART_PRO:
        return {
            "staged": True,
            "tp1_pct": _safe_float(service.settings.get("live_early_20_take_profit_1_pct", 0.12), 0.12),
            "tp2_pct": _safe_float(service.settings.get("live_early_20_take_profit_2_pct", 0.20), 0.20),
            "runner_trail_pct": _safe_float(service.settings.get("live_early_20_runner_trail_pct", 0.08), 0.08),
            "stop_loss_pct": _safe_float(service.settings.get("live_early_20_stop_loss_pct", -0.12), -0.12),
            "max_hold_sec": _safe_int(service.settings.get("chart_pro_max_hold_sec", 360), 360),
            "strong_red_pct": _safe_float(service.settings.get("live_early_20_strong_red_pct", 0.02), 0.02),
        }
    if mode_key == TRADING_MODE_LAUNCH_BURST:
        return {
            "staged": True,
            "tp1_pct": _safe_float(service.settings.get("launch_burst_take_profit_1_pct", 0.12), 0.12),
            "tp2_pct": _safe_float(service.settings.get("launch_burst_take_profit_2_pct", 0.20), 0.20),
            "runner_trail_pct": _safe_float(service.settings.get("launch_burst_runner_trail_pct", 0.08), 0.08),
            "stop_loss_pct": _safe_float(service.settings.get("launch_burst_stop_loss_pct", -0.10), -0.10),
            "max_hold_sec": _safe_int(service.settings.get("launch_burst_max_hold_sec", 90), 90),
            "strong_red_pct": _safe_float(service.settings.get("launch_burst_strong_red_pct", 0.02), 0.02),
        }
    if mode_key == TRADING_MODE_EARLY_20:
        return {
            "staged": True,
            "tp1_pct": _safe_float(service.settings.get("live_early_20_take_profit_1_pct", 0.12), 0.12),
            "tp2_pct": _safe_float(service.settings.get("live_early_20_take_profit_2_pct", 0.20), 0.20),
            "runner_trail_pct": _safe_float(service.settings.get("live_early_20_runner_trail_pct", 0.08), 0.08),
            "stop_loss_pct": _safe_float(service.settings.get("live_early_20_stop_loss_pct", service.settings.get("early_20_stop_loss_pct", -0.12)), -0.12),
            "max_hold_sec": _safe_int(service.settings.get("live_early_20_max_hold_sec", service.settings.get("early_20_max_hold_sec", 240)), 240),
            "strong_red_pct": _safe_float(service.settings.get("live_early_20_strong_red_pct", 0.02), 0.02),
        }
    return {
        "staged": True,
        "tp1_pct": _safe_float(service.settings.get("live_kol_take_profit_1_pct", 0.12), 0.12),
        "tp2_pct": _safe_float(service.settings.get("live_kol_take_profit_2_pct", service.settings.get("fast_exit_take_profit_pct", 0.25)), 0.25),
        "runner_trail_pct": _safe_float(service.settings.get("live_kol_runner_trail_pct", 0.10), 0.10),
        "stop_loss_pct": _safe_float(service.settings.get("live_kol_stop_loss_pct", service.settings.get("fast_exit_stop_loss_pct", -0.12)), -0.12),
        "max_hold_sec": _safe_int(service.settings.get("live_kol_max_hold_sec", service.settings.get("fast_exit_max_hold_sec", 300)), 300),
        "strong_red_pct": _safe_float(service.settings.get("live_kol_strong_red_pct", 0.025), 0.025),
    }


def _max_live_open_positions() -> int:
    # <=0 means unlimited.
    v = _safe_int(service.settings.get("max_live_open_positions", 2), 2)
    return max(0, v)


def _strategy_performance_summary(rows: list, pnl_key: str = "pnl", mode_key: str = "strategy_mode") -> dict:
    summary = {}
    for row in rows or []:
        mode = str((row or {}).get(mode_key, "") or TRADING_MODE_KOL_MOMENTUM)
        pnl = _safe_float((row or {}).get(pnl_key, 0.0), 0.0)
        item = summary.setdefault(mode, {"trades": 0, "wins": 0, "losses": 0, "total_pnl_sol": 0.0})
        item["trades"] += 1
        if pnl >= 0:
            item["wins"] += 1
        else:
            item["losses"] += 1
        item["total_pnl_sol"] += pnl
    for mode, item in summary.items():
        trades = int(item.get("trades", 0) or 0)
        item["mode_label"] = _trading_mode_label(mode)
        item["win_rate"] = (float(item.get("wins", 0) or 0) / trades * 100.0) if trades > 0 else 0.0
    return summary


def _live_closed_to_analytics_trade(closed: dict) -> dict:
    c = dict(closed or {})
    return {
        "trade_type": "LIVE",
        "strategy_mode": str(c.get("strategy_mode", "") or TRADING_MODE_KOL_MOMENTUM),
        "token": str(c.get("token", "") or ""),
        "entry_price": 0.0,
        "sell_price": 0.0,
        "pnl": float(c.get("pnl_sol", 0.0) or 0.0),
        "pnl_pct": float(c.get("pnl_pct", 0.0) or 0.0),
        "reason": str(c.get("reason", "LIVE_SELL") or "LIVE_SELL"),
        "close_time": float(c.get("closed_at", time.time()) or time.time()),
        "duration": 0.0,
        "entry_sol_amount": float(c.get("cost_basis_sol", 0.0) or 0.0),
        "pair_address": "",
        "kol_signals": [],
        "ath_during_trade": {},
        "token_age_seconds": 0,
        "creator_twitter": {},
        "token_mentions": {},
        "holder_intel": {},
        "candle_signals": {},
        "ath_post_close_72h": {},
    }


def _mode_entry_gate_reason(
    mode: str,
    token_mint: str,
    kol_side: str,
    fdv: float,
    liq_now: float,
    mcap_known: bool,
    liq_known: bool,
    candle_data: Optional[dict] = None,
    market_data: Optional[dict] = None,
):
    mode_name = _trading_mode_label(mode)

    if mode == TRADING_MODE_KOL_MOMENTUM:
        min_mcap = float(service.settings.get("kol_min_mcap_usd", 0.0) or 0.0)
        if min_mcap > 0:
            if not mcap_known:
                return f"Mode: {mode_name} | MC unresolved after retries"
            if fdv < min_mcap:
                return f"Mode: {mode_name} | MC ${fdv:,.0f} below ${min_mcap:,.0f}"
        return ""

    if mode == TRADING_MODE_EARLY_20:
        min_mcap = float(service.settings.get("early_20_min_mcap_usd", 7000.0) or 7000.0)
        if fdv < min_mcap:
            return f"Mode: {mode_name} | MC ${fdv:,.0f} below ${min_mcap:,.0f}"
        return ""

    if mode == TRADING_MODE_LAUNCH_BURST:
        first_buy_ts = float(service.first_kol_buy_ts_by_token.get(token_mint, 0.0) or 0.0)
        if first_buy_ts <= 0 and kol_side == "BUY":
            first_buy_ts = time.time()
            service.first_kol_buy_ts_by_token[token_mint] = first_buy_ts
        burst_window = int(service.settings.get("launch_burst_window_sec", 120) or 120)
        age_since_first_buy = time.time() - first_buy_ts if first_buy_ts > 0 else 999999.0
        min_mcap = float(service.settings.get("launch_burst_min_mcap_usd", 7000.0) or 7000.0)
        max_mcap = float(service.settings.get("launch_burst_max_mcap_usd", 40000.0) or 40000.0)
        min_liq = float(service.settings.get("launch_burst_min_liquidity_usd", 6000.0) or 6000.0)

        if kol_side != "BUY":
            return f"Mode: {mode_name} | KOL side must be BUY"
        if age_since_first_buy > burst_window:
            return (
                f"Mode: {mode_name} | Window expired "
                f"({age_since_first_buy:.0f}s > {burst_window}s)"
            )
        if not mcap_known:
            return f"Mode: {mode_name} | MC unresolved after retries"
        if fdv < min_mcap or fdv > max_mcap:
            return (
                f"Mode: {mode_name} | MC ${fdv:,.0f} outside "
                f"${min_mcap:,.0f}-${max_mcap:,.0f}"
            )
        if not liq_known:
            return f"Mode: {mode_name} | Liq unresolved after retries"
        if liq_now < min_liq:
            return f"Mode: {mode_name} | Liq ${liq_now:,.0f} below ${min_liq:,.0f}"
        return ""

    if mode == TRADING_MODE_CHART_PRO:
        if not bool(service.settings.get("chart_pro_enabled", True)):
            return f"Mode: {mode_name} | disabled in settings"
        min_mcap = float(service.settings.get("chart_pro_min_mcap_usd", 20000.0) or 20000.0)
        min_liq = float(service.settings.get("chart_pro_min_liquidity_usd", 6000.0) or 6000.0)
        if not mcap_known:
            return f"Mode: {mode_name} | MC unresolved after retries"
        if fdv < min_mcap:
            return f"Mode: {mode_name} | MC ${fdv:,.0f} below ${min_mcap:,.0f}"
        if not liq_known:
            return f"Mode: {mode_name} | Liq unresolved after retries"
        if liq_now < min_liq:
            return f"Mode: {mode_name} | Liq ${liq_now:,.0f} below ${min_liq:,.0f}"
        if not candle_data:
            return f"Mode: {mode_name} | chart data unavailable"
        candle_reason = str((candle_data or {}).get("reason", "") or "")
        if candle_reason in ("candles_rate_limited", "insufficient_candles", "no_pool"):
            return f"Mode: {mode_name} | chart unavailable ({candle_reason})"
        trigger = dict((candle_data or {}).get("trigger", {}) or {})
        quality = dict((candle_data or {}).get("quality", {}) or {})
        min_score = float(service.settings.get("chart_pro_min_score", 42.0) or 42.0)
        chart_score = float(quality.get("score", 0.0) or 0.0)
        fast_entry_ready, _ = _chart_pro_fast_entry_signal(candle_data, market_data or {"liquidity_usd": liq_now})
        if chart_score < max(0.0, min_score - 14.0) and not fast_entry_ready:
            return f"Mode: {mode_name} | chart score below threshold"
        if not bool(trigger.get("entry_ready", False)) and not fast_entry_ready:
            return f"Mode: {mode_name} | no chart trigger"
        return ""

    return ""


def _track_kol_flow(token: str, side: str, sol_amount: float):
    if not token:
        return
    now = time.time()
    side_u = str(side or "").upper()
    amount = float(sol_amount or 0.0)
    flow = service.kol_flow_by_token.setdefault(token, deque(maxlen=256))
    flow.append((now, side_u, amount))

    # Keep a 2-minute sliding window.
    cutoff = now - 120.0
    while flow and flow[0][0] < cutoff:
        flow.popleft()


def _track_market_tick(token: str, price_sol: float, volume_m5: float):
    if not token:
        return
    now = time.time()
    ticks = service.market_ticks_by_token.setdefault(token, deque(maxlen=300))
    ticks.append((now, float(price_sol or 0.0), float(volume_m5 or 0.0)))

    cutoff = now - 180.0
    while ticks and ticks[0][0] < cutoff:
        ticks.popleft()


def _burst_momentum_confirmation(token: str, market_data: dict, candle_data: dict):
    now = time.time()
    orderflow_ok = False
    breakout_ok = False

    flow = service.kol_flow_by_token.get(token)
    if flow:
        cutoff = now - 20.0
        buy_sol = 0.0
        sell_sol = 0.0
        for ts, side, amt in flow:
            if ts < cutoff:
                continue
            if side == "BUY":
                buy_sol += amt
            elif side == "SELL":
                sell_sol += amt
        orderflow_ok = buy_sol > sell_sol and buy_sol > 0.0

    current_price = float((market_data or {}).get("price_sol", 0.0) or 0.0)
    current_vol = float((market_data or {}).get("volume_m5", 0.0) or 0.0)
    ticks = service.market_ticks_by_token.get(token, deque())
    if current_price > 0 and len(ticks) >= 4:
        prior = [t for t in ticks if t[0] < now - 2.0]
        if len(prior) >= 3:
            prior_high = max(float(p[1] or 0.0) for p in prior)
            prior_vols = [float(p[2] or 0.0) for p in prior]
            avg_vol = sum(prior_vols) / len(prior_vols) if prior_vols else 0.0
            breakout_ok = current_price > prior_high and (avg_vol <= 0 or current_vol >= avg_vol * 1.05)

    candle_signals = ((candle_data or {}).get("signals", {}) or {})
    candle_breakout = bool(candle_signals.get("breakout", False))
    candle_volume = bool(candle_signals.get("volume_spike", False))

    confirmed = bool(orderflow_ok or breakout_ok or (candle_breakout and candle_volume))
    reasons = []
    if orderflow_ok:
        reasons.append("orderflow")
    if breakout_ok:
        reasons.append("breakout")
    if candle_breakout and candle_volume:
        reasons.append("candles")
    return confirmed, ",".join(reasons) if reasons else "none"


async def _fetch_market_data_with_retries(
    token_mint: str,
    enabled_modes: List[str],
    *,
    for_chart_watch: bool = False,
    entry_near_retry: bool = False,
):
    if not paper_trader or not paper_trader.engine:
        return {}

    attempts = 1
    delay_sec = 0.0
    timeout_sec = 0.0
    burst_enabled = TRADING_MODE_LAUNCH_BURST in set(enabled_modes or [])
    if for_chart_watch:
        timeout_sec = _chart_watch_market_data_timeout_sec()
        attempts = max(attempts, 2)
        delay_sec = max(delay_sec, 0.12)
    if burst_enabled:
        attempts = max(1, int(service.settings.get("launch_burst_market_retry_attempts", 5) or 5))
        delay_sec = max(0.0, float(service.settings.get("launch_burst_market_retry_delay_sec", 2.0) or 2.0))

    latest = {}
    for i in range(attempts):
        try:
            coro = paper_trader.engine.get_token_price(token_mint)
            if timeout_sec > 0:
                latest = await asyncio.wait_for(coro, timeout=timeout_sec)
            else:
                latest = await coro
        except asyncio.TimeoutError:
            latest = {}
            if for_chart_watch:
                service.chart_watch_market_data_timeout_count = int(service.chart_watch_market_data_timeout_count or 0) + 1
        except Exception:
            latest = {}
        if not latest:
            latest = {}
        if not burst_enabled:
            if for_chart_watch and not latest and i < attempts - 1:
                if delay_sec > 0:
                    await asyncio.sleep(delay_sec)
                continue
            break

        liq_known = bool(latest.get("liquidity_known", False))
        mcap_known = bool(latest.get("mcap_known", False))
        if liq_known and mcap_known:
            break
        if i < attempts - 1 and delay_sec > 0:
            await asyncio.sleep(delay_sec)

    # One quick retry for entry-near chart-watch tokens under strict timeout.
    if for_chart_watch and entry_near_retry and not latest:
        try:
            latest = await asyncio.wait_for(
                paper_trader.engine.get_token_price(token_mint),
                timeout=max(0.6, timeout_sec * 1.25),
            )
        except asyncio.TimeoutError:
            latest = {}
            service.chart_watch_market_data_timeout_count = int(service.chart_watch_market_data_timeout_count or 0) + 1
        except Exception:
            latest = {}

    return latest


def _get_pretrade_veto_reason(market_data: dict, holder_data: dict, candle_data: dict, trading_mode: str):
    liq = float((market_data or {}).get("liquidity_usd", 0) or 0)
    change_m5 = float((market_data or {}).get("price_change_m5", 0) or 0)

    if trading_mode == TRADING_MODE_EARLY_20:
        min_liq = float(service.settings.get("early_20_min_liquidity_usd", 7000.0) or 7000.0)
    elif trading_mode == TRADING_MODE_LAUNCH_BURST:
        min_liq = float(service.settings.get("launch_burst_min_liquidity_usd", 6000.0) or 6000.0)
    elif trading_mode == TRADING_MODE_CHART_PRO:
        min_liq = float(service.settings.get("chart_pro_min_liquidity_usd", 6000.0) or 6000.0)
    else:
        min_liq = float(service.settings.get("min_liquidity_usd", 15000) or 15000)
    max_change_m5 = float(service.settings.get("max_price_change_m5_pct", 0.0) or 0.0)
    max_bundlers = float(service.settings.get("max_bundlers_holding_pct", 8.0) or 8.0)
    max_snipers = float(service.settings.get("max_snipers_holding_pct", 20.0) or 20.0)
    max_atr = float(service.settings.get("max_atr_pct", 16.0) or 16.0)

    reasons = []
    if liq < min_liq:
        reasons.append(f"liq ${liq:,.0f} < ${min_liq:,.0f}")
    if max_change_m5 > 0 and change_m5 > max_change_m5:
        reasons.append(f"5m pump {change_m5:.1f}% > {max_change_m5:.1f}%")

    if holder_data:
        bundlers = float(holder_data.get("bundlers_holding_pct", 0) or 0)
        snipers = float(holder_data.get("snipers_holding_pct", 0) or 0)
        if bundlers > max_bundlers:
            reasons.append(f"bundlers {bundlers:.2f}% > {max_bundlers:.2f}%")
        if snipers > max_snipers:
            reasons.append(f"snipers {snipers:.2f}% > {max_snipers:.2f}%")

        if bool(service.settings.get("wallet_age_cluster_check_enabled", True)):
            min_sample = int(service.settings.get("wallet_age_cluster_min_sample", 6) or 6)
            recent_window_h = float(service.settings.get("wallet_age_cluster_recent_window_hours", 48.0) or 48.0)
            recent_share_min = float(service.settings.get("wallet_age_cluster_min_recent_share_pct", 70.0) or 70.0)
            same_hour_min = float(service.settings.get("wallet_age_cluster_min_same_hour_pct", 55.0) or 55.0)
            age_hours = holder_data.get("wallet_age_hours_sample", []) or []
            if isinstance(age_hours, list):
                ages = []
                for v in age_hours:
                    try:
                        fv = float(v)
                    except Exception:
                        continue
                    if fv >= 0:
                        ages.append(fv)
                if len(ages) >= max(2, min_sample):
                    recent_share = (sum(1 for h in ages if h <= recent_window_h) / len(ages)) * 100.0
                    same_hour_cluster = float(holder_data.get("wallet_age_same_hour_cluster_pct", 0.0) or 0.0)
                    if recent_share >= recent_share_min and same_hour_cluster >= same_hour_min:
                        reasons.append(
                            f"holder-age cluster recent={recent_share:.1f}%/{recent_window_h:.0f}h same-hour={same_hour_cluster:.1f}%"
                        )

    atr_pct = float(((candle_data or {}).get("signals", {}) or {}).get("atr_pct", 0) or 0)
    if atr_pct > max_atr:
        reasons.append(f"ATR {atr_pct:.2f}% > {max_atr:.2f}%")

    if not reasons:
        return ""
    return "Hard veto: " + " | ".join(reasons)


def _is_confirmed_rapid_pump(market_data: dict, holder_data: dict, candle_data: dict):
    change_m5 = float((market_data or {}).get("price_change_m5", 0) or 0)
    liq = float((market_data or {}).get("liquidity_usd", 0) or 0)

    rapid_thr = float(service.settings.get("rapid_pump_threshold_pct", 120.0) or 120.0)
    if change_m5 < rapid_thr:
        return False

    signals = ((candle_data or {}).get("signals", {}) or {})
    trend_1m = bool(signals.get("trend_1m", False))
    trend_5m = bool(signals.get("trend_5m", False))
    volume_spike = bool(signals.get("volume_spike", False))
    atr_pct = float(signals.get("atr_pct", 0) or 0)

    min_liq = float(service.settings.get("min_liquidity_usd", 15000) or 15000)
    max_bundlers = float(service.settings.get("max_bundlers_holding_pct", 8.0) or 8.0)
    max_snipers = float(service.settings.get("max_snipers_holding_pct", 20.0) or 20.0)
    max_atr = float(service.settings.get("max_atr_pct", 16.0) or 16.0)

    bundlers = float((holder_data or {}).get("bundlers_holding_pct", 0) or 0)
    snipers = float((holder_data or {}).get("snipers_holding_pct", 0) or 0)

    # Require confirmation to chase rapid pumps.
    if not (trend_1m and trend_5m and volume_spike):
        return False
    if liq < min_liq * 1.2:
        return False
    if bundlers > max_bundlers or snipers > max_snipers:
        return False
    if atr_pct > max_atr:
        return False
    return True


async def _reconcile_live_positions_for_owner(owner: str):
    owner = (owner or "").strip()
    if not owner:
        return []

    positions = live_trade_tracker.get_open_positions()
    changes = []

    for pos in positions:
        token = str(pos.get("token", "") or "")
        if not token:
            continue

        result = await _rpc_json(
            "getTokenAccountsByOwner",
            [owner, {"mint": token}, {"encoding": "jsonParsed"}],
        )
        bal = _parse_token_balance_result(result)
        onchain_amount = float(bal.get("ui_amount", 0.0) or 0.0)
        tracked_amount = float(pos.get("token_amount", 0.0) or 0.0)

        if abs(onchain_amount - tracked_amount) <= 1e-9:
            continue

        mark_price = 0.0
        try:
            if paper_trader and paper_trader.engine:
                quote = await paper_trader.engine.get_token_price(token)
                mark_price = float((quote or {}).get("price_sol", 0.0) or 0.0)
        except Exception:
            mark_price = 0.0

        change = live_trade_tracker.reconcile_token_balance(
            token=token,
            onchain_token_amount=onchain_amount,
            mark_price_sol=mark_price,
            reason="EXTERNAL_WALLET_SYNC",
        )
        if change and change.get("action") != "noop":
            changes.append(change)

    return changes


async def _sync_untracked_live_tokens_for_owner(owner: str):
    owner = (owner or "").strip()
    if not owner:
        return []

    result = await _rpc_json(
        "getTokenAccountsByOwner",
        [
            owner,
            {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
            {"encoding": "jsonParsed"},
        ],
    )
    if not result:
        return []

    mint_totals = {}
    for item in result.get("value", []):
        try:
            amt = item.get("account", {}).get("data", {}).get("parsed", {}).get("info", {}).get("tokenAmount", {})
            mint = str(item.get("account", {}).get("data", {}).get("parsed", {}).get("info", {}).get("mint", "") or "")
            ui_amount = float(amt.get("uiAmount", 0.0) or 0.0)
            if not mint or mint == SOL_MINT or ui_amount <= 0:
                continue
            mint_totals[mint] = mint_totals.get(mint, 0.0) + ui_amount
        except Exception:
            continue

    if not mint_totals:
        return []

    changes = []
    tracked_tokens = {str(p.get("token", "") or "") for p in live_trade_tracker.get_open_positions()}
    known_tokens = set(tracked_tokens)
    known_tokens.update(str(t.get("token", "") or "") for t in live_trade_tracker.get_history())
    if paper_trader:
        known_tokens.update(str(p.get("token_mint", "") or "") for p in paper_trader.positions.values())
        known_tokens.update(str(t.get("token", "") or "") for t in paper_trader.closed_positions)
    for token, amount in mint_totals.items():
        if token in tracked_tokens:
            continue
        quote = None
        try:
            if paper_trader and paper_trader.engine:
                quote = await paper_trader.engine.get_token_price(token)
        except Exception:
            quote = None
        price_sol = float((quote or {}).get("price_sol", 0.0) or 0.0)
        fdv = float((quote or {}).get("fdv", 0.0) or 0.0)
        if price_sol > 0:
            # Ignore tiny dust balances when price is known.
            if amount * price_sol < 0.002:
                continue
        else:
            # If price is unavailable, import only if this is a known bot-traded token.
            if token not in known_tokens:
                continue
            price_sol = 0.0
            fdv = 0.0

        change = live_trade_tracker.import_external_position(
            token=token,
            token_amount=amount,
            mark_price_sol=price_sol,
            entry_fdv=fdv,
        )
        if change:
            if price_sol <= 0:
                change["note"] = "imported_without_quote"
            changes.append(change)

    return changes


async def _jupiter_get_quote(input_mint: str, output_mint: str, amount: int, slippage_bps: int = 100):
    urls = [
        (
            f"{JUPITER_QUOTE_API}?inputMint={input_mint}"
            f"&outputMint={output_mint}"
            f"&amount={amount}"
            f"&slippageBps={slippage_bps}"
        ),
        (
            f"https://quote-api.jup.ag/v6/quote?inputMint={input_mint}"
            f"&outputMint={output_mint}"
            f"&amount={amount}"
            f"&slippageBps={slippage_bps}"
        ),
    ]
    import socket
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    timeout = aiohttp.ClientTimeout(total=15)
    for url in urls:
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    if data and data.get("outAmount"):
                        return data
        except Exception:
            continue
    return None


async def _attempt_live_exit(token: str, sell_fraction: float, reason: str, mark_price_sol: float = 0.0):
    token = str(token or "").strip()
    if not token or not (sniper and sniper.is_custodial_enabled()):
        return {"status": "skipped", "reason": "live_not_enabled"}

    try:
        frac = max(0.0, min(1.0, float(sell_fraction or 1.0)))
        if frac < 0.999:
            result = await sniper.execute_sell_fraction(token, frac)
        else:
            result = await sniper.execute_sell_all(token)
    except Exception as e:
        await service.broadcast({"type": "log", "msg": f"LIVE SELL exception: {e}"})
        return {"status": "failed", "reason": str(e)}

    status = str(result.get("status", "") or "")
    if status == "executed":
        closed = live_trade_tracker.record_sell(
            token=token,
            token_amount_sold=float(result.get("token_amount_sold_ui", 0.0) or 0.0),
            sol_received=float(result.get("sol_received", 0.0) or 0.0),
            reason=reason,
            tx_sig=str(result.get("tx_sig", "") or ""),
        )
        if closed:
            trade_analytics_store.record_trade_analytics(_live_closed_to_analytics_trade(closed))
        await service.broadcast({
            "type": "snipe",
            "msg": (
                f"LIVE SELL{' PARTIAL' if frac < 0.999 else ''}: {token[:8]}... "
                f"{float(result.get('sol_received', 0.0) or 0.0):.4f} SOL "
                f"{'(tx ' + str(result.get('tx_sig', ''))[:10] + '...)' if result.get('tx_sig') else '(tx pending/unknown)'}"
            ),
        })
        asyncio.create_task(
            _notify_live_trade(
                side="SELL",
                token=token,
                sol_amount=float(result.get("sol_received", 0.0) or 0.0),
                tx_sig=str(result.get("tx_sig", "") or ""),
                pnl_sol=float((closed or {}).get("pnl_sol", 0.0) or 0.0),
                reason=reason,
            )
        )
        await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
        await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
        return {"status": "executed", "closed": closed, "result": result}

    try:
        bal = await sniper.get_wallet_token_balance(token)
        onchain_amt = float(bal.get("ui_amount", 0.0) or 0.0)
    except Exception:
        onchain_amt = 0.0

    recon = live_trade_tracker.reconcile_token_balance(
        token=token,
        onchain_token_amount=onchain_amt,
        mark_price_sol=float(mark_price_sol or 0.0),
        reason=reason,
    )
    if recon and recon.get("action") != "noop":
        await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
        await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
    if status not in ("no_balance", "skipped"):
        await service.broadcast({
            "type": "log",
            "msg": f"LIVE SELL failed: {result.get('reason', 'unknown')}",
        })
    return {"status": status or "failed", "result": result, "reconciled": recon}


async def monitor_positions_task():
    """Dedicated high-frequency loop for tracking active positions"""
    print("Starting High-Frequency Position Monitor...")
    import time
    while service.running:
        try:
            if not paper_trader:
                await asyncio.sleep(1)
                continue
                
            start_time = time.time()
            
            # 1. Update Positions (Parallel Fetch)
            closed_trades = await paper_trader.update_positions()
            
            # 2. Broadcast Closed Trades
            for trade in closed_trades:
                is_partial = bool(trade.get("is_partial", False))
                if not is_partial:
                    trade_analytics_store.record_trade_analytics(trade)
                pnl_sign = "+" if trade["pnl"] >= 0 else ""
                await service.broadcast({
                    "type": "snipe",
                    "msg": (
                        f"PAPER SELL ({trade['reason']}{' | PARTIAL' if is_partial else ''}): "
                        f"{trade['token'][:8]}... PNL: {pnl_sign}{trade['pnl']:.4f} SOL "
                        f"(MCap: ${trade['sell_fdv']:,.0f})"
                    )
                })
                await service.broadcast({
                    "type": "paper_sell_signal",
                    "data": {
                        "token": trade.get("token", ""),
                        "reason": trade.get("reason", ""),
                        "sell_price": trade.get("sell_price", 0),
                        "pnl": trade.get("pnl", 0),
                        "sell_fraction": float(trade.get("sell_fraction", 1.0) or 1.0),
                        "is_partial": is_partial,
                    }
                })
                if (
                    bool(service.settings.get("live_follow_paper_exits", False))
                    and sniper
                    and sniper.is_custodial_enabled()
                ):
                    try:
                        sell_fraction = float(trade.get("sell_fraction", 1.0) or 1.0)
                        if sell_fraction < 0.999:
                            sell_result = await sniper.execute_sell_fraction(
                                trade.get("token", ""),
                                sell_fraction,
                            )
                        else:
                            sell_result = await sniper.execute_sell_all(trade.get("token", ""))
                        if sell_result.get("status") == "executed":
                            closed = live_trade_tracker.record_sell(
                                token=trade.get("token", ""),
                                token_amount_sold=float(sell_result.get("token_amount_sold_ui", 0.0)),
                                sol_received=float(sell_result.get("sol_received", 0.0)),
                                reason=trade.get("reason", "AUTO_SELL"),
                                tx_sig=sell_result.get("tx_sig", ""),
                            )
                            await service.broadcast({
                                "type": "snipe",
                                "msg": (
                                    f"LIVE SELL{' PARTIAL' if sell_fraction < 0.999 else ''}: {trade.get('token', '')[:8]}... "
                                    f"{sell_result.get('sol_received', 0):.4f} SOL "
                                    f"(tx {str(sell_result.get('tx_sig', ''))[:10]}...)"
                                ),
                            })
                            asyncio.create_task(
                                _notify_live_trade(
                                    side="SELL",
                                    token=trade.get("token", ""),
                                    sol_amount=float(sell_result.get("sol_received", 0.0) or 0.0),
                                    tx_sig=str(sell_result.get("tx_sig", "") or ""),
                                    pnl_sol=float((closed or {}).get("pnl_sol", 0.0) or 0.0),
                                    reason=trade.get("reason", "AUTO_SELL"),
                                )
                            )
                            await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                            await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
                        else:
                            status = str(sell_result.get("status", "") or "")
                            if status not in ("no_balance", "skipped"):
                                await service.broadcast({
                                    "type": "log",
                                    "msg": f"LIVE SELL failed: {sell_result.get('reason', 'unknown')}",
                                })
                            # Always reconcile live tracker against actual wallet balance after a failed/empty sell.
                            try:
                                bal = await sniper.get_wallet_token_balance(trade.get("token", ""))
                                onchain_amt = float(bal.get("ui_amount", 0.0) or 0.0)
                                mark_price = 0.0
                                try:
                                    if paper_trader and paper_trader.engine:
                                        q = await paper_trader.engine.get_token_price(trade.get("token", ""))
                                        mark_price = float((q or {}).get("price_sol", 0.0) or 0.0)
                                except Exception:
                                    mark_price = 0.0
                                recon = live_trade_tracker.reconcile_token_balance(
                                    token=trade.get("token", ""),
                                    onchain_token_amount=onchain_amt,
                                    mark_price_sol=mark_price,
                                    reason="AUTO_SELL_SYNC",
                                )
                                if recon and recon.get("action") != "noop":
                                    await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                                    await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
                            except Exception:
                                pass
                    except Exception as e:
                        await service.broadcast({"type": "log", "msg": f"LIVE SELL exception: {e}"})

            # 3. Broadcast Updated Positions (Real-time updates)
            active_count = len(paper_trader.positions)
            if active_count > 0:
                positions = paper_trader.get_positions()
                await service.broadcast({
                    "type": "positions_update",
                    "data": positions
                })

            # 4. Adaptive Rate Limiting
            duration = time.time() - start_time
            
            if active_count == 0:
                sleep_time = 5.0 # Sleep longer if nothing to track
            elif active_count < 3:
                sleep_time = max(0.5 - duration, 0.5) # Aim for 0.5s interval (near real-time)
            elif active_count < 10:
                sleep_time = max(0.5 - duration, 0.5) # Aim for 0.5s interval
            else:
                sleep_time = max(1.0 - duration, 1.0) # Aim for 1s interval for many positions
                
            await asyncio.sleep(sleep_time)
            
        except Exception as e:
            print(f"Monitor Loop Error: {e}")
            await asyncio.sleep(5)

async def monitor_live_positions_task():
    """Authoritative live exit loop driven by live position state."""
    print("Starting live position monitor...")
    while service.running:
        try:
            if not (sniper and sniper.is_custodial_enabled()):
                await asyncio.sleep(1.0)
                continue

            positions = live_trade_tracker.get_open_positions()
            if not positions:
                await asyncio.sleep(1.0)
                continue

            loop_started = time.time()
            for pos in positions:
                token = str(pos.get("token", "") or "").strip()
                if not token:
                    continue
                if not (paper_trader and paper_trader.engine):
                    continue

                quote = await paper_trader.engine.get_token_price(token)
                if not quote:
                    continue

                current_price = float(quote.get("price_sol", 0.0) or 0.0)
                current_fdv = float(quote.get("fdv", 0.0) or 0.0)
                if current_price <= 0:
                    continue

                token_amount = float(pos.get("token_amount", 0.0) or 0.0)
                basis_amount = float(pos.get("basis_token_amount", token_amount) or token_amount)
                sol_spent = float(pos.get("sol_spent", 0.0) or 0.0)
                if token_amount <= 0 or basis_amount <= 0 or sol_spent <= 0:
                    continue

                avg_entry_price = (sol_spent / basis_amount) if basis_amount > 0 else 0.0
                if avg_entry_price <= 0:
                    continue

                mode = str(pos.get("strategy_mode", "") or "").strip().lower() or TRADING_MODE_KOL_MOMENTUM
                profile = _live_exit_profile_for_mode(mode)
                tp1_pct = float(profile.get("tp1_pct", 0.12) or 0.12)
                tp2_pct = float(profile.get("tp2_pct", 0.20) or 0.20)
                trail_pct = float(profile.get("runner_trail_pct", 0.08) or 0.08)
                stop_loss_pct = float(profile.get("stop_loss_pct", -0.12) or -0.12)
                max_hold_sec = int(profile.get("max_hold_sec", 300) or 300)
                strong_red_pct = float(profile.get("strong_red_pct", 0.02) or 0.02)

                pnl_pct = (current_price - avg_entry_price) / avg_entry_price
                opened_at = float(pos.get("opened_at", time.time()) or time.time())
                held_sec = max(0.0, time.time() - opened_at)

                prev_price = float(pos.get("last_mark_price", avg_entry_price) or avg_entry_price)
                peak_price = max(float(pos.get("live_peak_price", avg_entry_price) or avg_entry_price), current_price)
                runner_peak = max(float(pos.get("live_runner_peak_price", current_price) or current_price), current_price)
                red_streak = int(pos.get("live_red_streak", 0) or 0)
                tp1_hit = bool(pos.get("live_tp1_hit", False))
                tp2_hit = bool(pos.get("live_tp2_hit", False))

                if peak_price >= avg_entry_price * 1.08:
                    if current_price <= prev_price * (1.0 - strong_red_pct):
                        red_streak += 1
                    elif current_price >= prev_price:
                        red_streak = 0
                else:
                    red_streak = 0

                live_trade_tracker.update_position_runtime(
                    token,
                    {
                        "last_mark_price": current_price,
                        "last_mark_fdv": current_fdv,
                        "last_mark_at": time.time(),
                        "live_peak_price": peak_price,
                        "live_runner_peak_price": runner_peak,
                        "live_red_streak": red_streak,
                    },
                    persist=False,
                )

                if mode == TRADING_MODE_CHART_PRO:
                    meta = dict(pos.get("strategy_meta", {}) or {})
                    chart_stop = float(meta.get("chart_stop_price", 0.0) or 0.0)
                    tp1_price = float(meta.get("chart_tp1_price", 0.0) or 0.0)
                    tp2_price = float(meta.get("chart_tp2_price", 0.0) or 0.0)
                    tp1_r_chart = max(0.1, float(meta.get("chart_tp1_r", 1.0) or 1.0))
                    tp2_r_chart = max(tp1_r_chart, float(meta.get("chart_tp2_r", 2.0) or 2.0))
                    tp1_hit_chart = bool(meta.get("chart_tp1_hit", False))
                    tp2_hit_chart = bool(meta.get("chart_tp2_hit", False))
                    runner_peak = float(meta.get("chart_runner_peak", current_price) or current_price)
                    runner_peak = max(runner_peak, current_price)
                    meta["chart_runner_peak"] = runner_peak
                    atr_abs = float(meta.get("chart_atr_abs", 0.0) or 0.0)
                    trail_mult = float(meta.get("chart_runner_trail_atr_mult", service.settings.get("chart_pro_runner_trail_atr_mult", 1.5)) or 1.5)
                    trail_abs = atr_abs * trail_mult if atr_abs > 0 else max(0.0, current_price * 0.03)
                    max_hold_chart = int(service.settings.get("chart_pro_max_hold_sec", 360) or 360)
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
                        tp1_price = avg_entry_price * (1.0 + (chart_risk_pct * tp1_r_chart))
                        meta["chart_tp1_price"] = tp1_price
                    if tp2_price <= tp1_price or tp2_price > (avg_entry_price * 4.0):
                        tp2_price = avg_entry_price * (1.0 + (chart_risk_pct * tp2_r_chart))
                        meta["chart_tp2_price"] = tp2_price

                    if chart_stop > 0 and current_price <= chart_stop:
                        await _attempt_live_exit(token, 1.0, "CHART_STOP_LOSS", mark_price_sol=current_price)
                        continue
                    if held_sec >= max_hold_chart:
                        await _attempt_live_exit(token, 1.0, "CHART_TIME_EXIT", mark_price_sol=current_price)
                        continue
                    if (not tp1_hit_chart) and tp1_price > 0 and current_price >= tp1_price:
                        result = await _attempt_live_exit(token, 0.5, "CHART_TP1_PARTIAL", mark_price_sol=current_price)
                        if str(result.get("status", "")) == "executed":
                            meta["chart_tp1_hit"] = True
                            live_trade_tracker.update_position_runtime(token, {"strategy_meta": meta}, persist=True)
                        continue
                    if bool(meta.get("chart_tp1_hit", False)) and (not tp2_hit_chart) and tp2_price > 0 and current_price >= tp2_price:
                        result = await _attempt_live_exit(token, 0.5, "CHART_TP2_PARTIAL", mark_price_sol=current_price)
                        if str(result.get("status", "")) == "executed":
                            meta["chart_tp2_hit"] = True
                            meta["chart_runner_peak"] = current_price
                            live_trade_tracker.update_position_runtime(token, {"strategy_meta": meta}, persist=True)
                        continue
                    if bool(meta.get("chart_tp2_hit", False)) and trail_abs > 0 and current_price <= max(0.0, runner_peak - trail_abs):
                        await _attempt_live_exit(token, 1.0, "CHART_TRAILING_STOP", mark_price_sol=current_price)
                        continue

                    live_trade_tracker.update_position_runtime(token, {"strategy_meta": meta}, persist=False)
                    continue

                if pnl_pct <= stop_loss_pct:
                    await _attempt_live_exit(token, 1.0, "STOP_LOSS", mark_price_sol=current_price)
                    continue
                if held_sec >= max_hold_sec:
                    await _attempt_live_exit(token, 1.0, "TIME_EXIT", mark_price_sol=current_price)
                    continue
                if red_streak >= 2:
                    await _attempt_live_exit(token, 1.0, "EARLY_FAILURE", mark_price_sol=current_price)
                    continue

                if bool(profile.get("staged", True)):
                    if not tp1_hit and pnl_pct >= tp1_pct:
                        result = await _attempt_live_exit(token, 0.5, "TP1_PARTIAL", mark_price_sol=current_price)
                        if str(result.get("status", "")) == "executed":
                            live_trade_tracker.update_position_runtime(token, {"live_tp1_hit": True}, persist=True)
                        continue

                    if tp1_hit and (not tp2_hit) and pnl_pct >= tp2_pct:
                        result = await _attempt_live_exit(token, 0.6, "TP2_PARTIAL", mark_price_sol=current_price)
                        if str(result.get("status", "")) == "executed":
                            live_trade_tracker.update_position_runtime(
                                token,
                                {"live_tp2_hit": True, "live_runner_peak_price": current_price},
                                persist=True,
                            )
                        continue

                    if tp2_hit and current_price <= runner_peak * (1.0 - trail_pct):
                        await _attempt_live_exit(token, 1.0, "TRAILING_STOP", mark_price_sol=current_price)
                        continue
                else:
                    if pnl_pct >= tp2_pct:
                        await _attempt_live_exit(token, 1.0, "TAKE_PROFIT", mark_price_sol=current_price)
                        continue

            live_trade_tracker.save_state()
            duration = time.time() - loop_started
            open_count = len(positions)
            target_interval = 0.75 if open_count <= 3 else 1.0
            await asyncio.sleep(max(0.2, target_interval - duration))
        except Exception as e:
            await service.broadcast({"type": "log", "msg": f"Live monitor error: {e}"})
            await asyncio.sleep(1.5)


async def monitor_closed_positions_ath():
    """Monitors closed positions for 72 hours to track missed upside after exit."""
    print("Starting post-close ATH monitor (72h window)...")
    while service.running:
        try:
            if not paper_trader:
                await asyncio.sleep(2)
                continue

            pending = paper_trader.get_pending_post_close_monitoring()
            if not pending:
                await asyncio.sleep(60)
                continue

            tokens = sorted({t.get("token") for t in pending if t.get("token")})
            for token in tokens:
                quote = await paper_trader.engine.get_token_price(token)
                if not quote:
                    continue
                price = float(quote.get("price_sol", 0) or 0)
                fdv = float(quote.get("fdv", 0) or 0)
                if price <= 0:
                    continue
                paper_trader.update_post_close_ath(token, price, fdv)

            analytics_changed = False
            for trade in pending:
                changed = trade_analytics_store.update_trade_post_close_ath(
                    trade.get("token", ""),
                    float(trade.get("close_time", 0)),
                    trade.get("ath_post_close_72h", {}),
                    autosave=False,
                )
                analytics_changed = analytics_changed or bool(changed)
            if analytics_changed:
                trade_analytics_store.save()

            await asyncio.sleep(60)
        except Exception as e:
            print(f"Post-close ATH monitor error: {e}")
            await asyncio.sleep(15)


async def reconcile_live_wallet_task():
    """Server-side reconciliation for custodial wallet external changes."""
    while service.running:
        try:
            if sniper and sniper.is_custodial_enabled():
                owner = (sniper.public_key() or "").strip()
                if owner:
                    changes = []
                    if live_trade_tracker.get_open_positions():
                        changes.extend(await _reconcile_live_positions_for_owner(owner))
                    changes.extend(await _sync_untracked_live_tokens_for_owner(owner))
                    if changes:
                        await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                        await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
                        for change in changes:
                            await service.broadcast({
                                "type": "log",
                                "msg": (
                                    f"LIVE SYNC: {str(change.get('token', ''))[:8]}... "
                                    f"{change.get('action', 'updated')} via server reconciliation"
                                ),
                            })
            await asyncio.sleep(8)
        except Exception:
            await asyncio.sleep(4)


async def monitor_chart_watchlist_task():
    """Direct evaluator loop for Chart Pro watchlist (no recurring queue flood)."""
    print("Starting chart watch monitor...")
    while service.running:
        try:
            interval_sec = _chart_watch_eval_interval_sec()
            if not _chart_watch_enabled():
                await _broadcast_chart_watch_snapshot()
                await asyncio.sleep(max(2.0, interval_sec))
                continue

            now = time.time()
            max_tokens = _chart_watch_max_tokens()
            stale_sec = _chart_watch_stale_sec()
            fade_threshold = _chart_watch_fade_volume_m5_usd()

            # Housekeeping: state updates, staleness, and post-exit fade drops.
            for token, item in list(service.chart_watchlist_by_token.items()):
                in_position = _chart_watch_is_open(token)
                item["in_position"] = in_position
                if in_position:
                    item["was_open"] = True
                    item["state"] = "IN_POSITION"
                    item["last_status"] = "ACTIVE"
                    item["last_reason"] = "position currently open"
                    item["last_seen_ts"] = now
                    item["failure_streak"] = 0
                    item["suppressed"] = False
                    continue

                if bool(item.get("was_open", False)) and _safe_float(item.get("exit_seen_ts", 0.0), 0.0) <= 0:
                    item["exit_seen_ts"] = now
                    item["state"] = "POST_EXIT_WATCH"
                    item["last_reason"] = "position closed; waiting for volume fade"

                age_since_seen = now - _safe_float(item.get("last_seen_ts", now), now)
                if age_since_seen > stale_sec:
                    _chart_watch_drop(token, reason=f"idle > {int(stale_sec)}s")
                    continue

                if _safe_float(item.get("exit_seen_ts", 0.0), 0.0) > 0:
                    post_checks = int(item.get("post_exit_checks", 0) or 0)
                    if post_checks >= 3 and _safe_float(item.get("last_volume_m5", 0.0), 0.0) < fade_threshold:
                        _chart_watch_drop(token, reason=f"volume faded below ${fade_threshold:,.0f}")

            # Capacity clamp.
            tokens_ranked = sorted(
                list(service.chart_watchlist_by_token.keys()),
                key=lambda t: _chart_watch_priority(service.chart_watchlist_by_token.get(t, {})),
                reverse=True,
            )
            if len(tokens_ranked) > max_tokens:
                keep = set(tokens_ranked[:max_tokens])
                for token in list(service.chart_watchlist_by_token.keys()):
                    if token in keep:
                        continue
                    if _chart_watch_is_open(token):
                        continue
                    _chart_watch_drop(token, reason=f"deprioritized (max {max_tokens})")

            # Direct evaluations with cycle budget.
            budget = _chart_watch_eval_budget_per_cycle()
            tokens_ranked = sorted(
                list(service.chart_watchlist_by_token.keys()),
                key=lambda t: _chart_watch_priority(service.chart_watchlist_by_token.get(t, {})),
                reverse=True,
            )
            tokens_to_eval = []
            for token in tokens_ranked:
                if len(tokens_to_eval) >= budget:
                    break
                item = service.chart_watchlist_by_token.get(token, {})
                if not item:
                    continue
                if _chart_watch_is_open(token):
                    continue
                if token in service.chart_watch_inflight_tokens:
                    continue
                next_eval_ts = _safe_float(item.get("next_eval_ts", 0.0), 0.0)
                if now < next_eval_ts:
                    continue
                tokens_to_eval.append(token)

            if tokens_to_eval:
                sem = asyncio.Semaphore(_chart_watch_eval_concurrency())

                async def _run_eval(token: str):
                    start_ts = time.time()
                    started = time.perf_counter()
                    item = service.chart_watchlist_by_token.get(token, {})
                    service.chart_watch_inflight_tokens.add(token)
                    if item:
                        item["state"] = "QUEUED"
                        item["last_queue_ts"] = start_ts
                    try:
                        async with sem:
                            eval_timeout = max(2.0, min(20.0, float(service.settings.get("chart_watch_eval_timeout_sec", 8.0) or 8.0)))
                            try:
                                await asyncio.wait_for(_chart_watch_evaluate_token(token), timeout=eval_timeout)
                            except asyncio.TimeoutError:
                                # Best-effort: keep the token in watch and retry soon.
                                item = service.chart_watchlist_by_token.get(token, {})
                                if item:
                                    _chart_watch_apply_warming(item, reason="eval timeout", reason_code="OTHER", market_data={})
                    finally:
                        service.chart_watch_inflight_tokens.discard(token)
                        elapsed_ms = max(0, int((time.perf_counter() - started) * 1000.0))
                        service.chart_watch_eval_latency_ms_window.append(elapsed_ms)

                await asyncio.gather(*[_run_eval(token) for token in tokens_to_eval], return_exceptions=True)

            await _broadcast_chart_watch_snapshot()
            await asyncio.sleep(interval_sec)
        except Exception as e:
            await service.broadcast({"type": "log", "msg": f"Chart watch monitor error: {e}"})
            await asyncio.sleep(2.0)


async def monitor_market_trends_task():
    """Scans market-trend feeds and pushes synthetic NEW_POOL events."""
    print("Starting market trend scanner...")
    while service.running:
        try:
            enabled = bool(service.settings.get("market_trend_source_enabled", True))
            interval_sec = max(5, int(service.settings.get("market_trend_scan_interval_sec", 8) or 8))
            if not enabled:
                await asyncio.sleep(interval_sec)
                continue

            enabled_modes = _get_enabled_trading_modes()
            if TRADING_MODE_CHART_PRO not in enabled_modes:
                await asyncio.sleep(interval_sec)
                continue

            if _chart_watch_startup_burst_enabled() and not bool(service.chart_watch_startup_burst_done):
                burst_admitted = await _run_chart_watch_startup_burst()
                service.chart_watch_startup_burst_done = True
                await service.broadcast(
                    {
                        "type": "log",
                        "msg": (
                            f"Chart watch startup burst complete: admitted {burst_admitted}/"
                            f"{_chart_watch_startup_burst_target()} candidate(s)"
                        ),
                    }
                )

            max_emit = max(1, min(25, int(service.settings.get("market_trend_max_new_per_scan", 12) or 12)))
            cooldown_sec = max(0, int(service.settings.get("market_trend_requeue_cooldown_sec", 900) or 900))
            candidates = await _fetch_market_trend_candidates(limit=max_emit * 6)
            service.chart_watch_candidate_pool_size_raw = len(candidates or [])
            if not candidates:
                service.chart_watch_source_mix = {}
                service.chart_watch_candidate_pool_size_post_gate = 0
                await asyncio.sleep(interval_sec)
                continue

            result = await _chart_watch_admit_trend_candidates(
                candidates=candidates,
                max_emit=max_emit,
                cooldown_sec=cooldown_sec,
                bypass_cooldown=False,
            )
            emitted = int(result.get("emitted", 0) or 0)
            emitted_by_source = dict(result.get("emitted_by_source", {}) or {})
            seen_by_source = dict(result.get("seen_by_source", {}) or {})
            service.chart_watch_candidate_pool_size_post_gate = int(result.get("post_gate_count", 0) or 0)
            service.chart_watch_source_mix = {
                "seen": {k: int(v) for k, v in sorted(seen_by_source.items())},
                "admitted": {k: int(v) for k, v in sorted(emitted_by_source.items())},
            }
            if emitted > 0:
                parts = [f"{k}={v}" for k, v in sorted(emitted_by_source.items())]
                src_msg = ", ".join(parts) if parts else "unknown"
                await service.broadcast({"type": "log", "msg": f"MarketTrend admitted {emitted} token(s) for chart watch ({src_msg})"})
            if _chart_watch_prewarm_candles_enabled():
                ranked_admitted = sorted(
                    (
                        token
                        for token, item in service.chart_watchlist_by_token.items()
                        if not bool((item or {}).get("suppressed", False))
                    ),
                    key=lambda t: _chart_watch_priority(service.chart_watchlist_by_token.get(t, {})),
                    reverse=True,
                )
                prewarm_limit = max(2, min(20, _chart_watch_eval_budget_per_cycle()))
                await _chart_watch_prewarm_candles(ranked_admitted[:prewarm_limit], limit=prewarm_limit)

            await asyncio.sleep(interval_sec)
        except Exception as e:
            await service.broadcast({"type": "log", "msg": f"MarketTrend scanner error: {e}"})
            await asyncio.sleep(20)


async def monitor_chart_autopilot_task():
    """Autonomously reviews Chart Pro results and tunes bounded settings."""
    print("Starting chart autopilot optimizer...")
    while service.running:
        try:
            await _autopilot_review_and_tune()
            await asyncio.sleep(_autopilot_interval_sec())
        except Exception as e:
            await service.broadcast({"type": "log", "msg": f"Autopilot error: {e}"})
            await asyncio.sleep(30)


async def process_feed():
    """Background loop to process events and update clients."""
    last_heartbeat = 0
    import time

    global risk_engine, analyzer, sniper, paper_trader, social_monitor, async_client

    print("Starting Alpha Feed Service loop...")

    def _balance_amount_for_owner(balances, owner_wallet: str, mint: str) -> float:
        if not balances:
            return 0.0
        for b in balances:
            try:
                b_owner = str(getattr(b, "owner", ""))
                b_mint = str(getattr(b, "mint", ""))
                if b_owner != owner_wallet or b_mint != mint:
                    continue
                ui_amt = getattr(b, "ui_token_amount", None)
                if ui_amt is not None:
                    val = getattr(ui_amt, "ui_amount", None)
                    if val is not None:
                        return float(val)
                    val_str = getattr(ui_amt, "ui_amount_string", None)
                    if val_str is not None:
                        return float(val_str)
            except Exception:
                continue
        return 0.0

    while service.running:
        current_time = time.time()

        if current_time - last_heartbeat > 5:
            await service.broadcast({"type": "log", "msg": "System Pulse: Scanning mempool..."})
            if paper_trader:
                stats = paper_trader.get_stats()
                await service.broadcast({"type": "pnl_update", "data": stats})
                await service.broadcast({"type": "positions_update", "data": paper_trader.get_positions()})
            await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
            await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
            last_heartbeat = current_time

        try:
            event = await asyncio.wait_for(service.queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        try:
            token_mint = ""
            watch_token = ""
            if event.get("type") == "SOCIAL_SIGNAL":
                social_data = event.get("data", {})
                social_token = social_data.get("token") or social_data.get("signature")
                if social_token:
                    event = {
                        "type": "NEW_POOL",
                        "signature": social_token,
                        "source": f"Social: {social_data.get('author_name') or social_data.get('author') or 'Unknown'}",
                        "social_data": social_data,
                    }

            if event.get("type") != "NEW_POOL":
                continue

            signature = str(event.get("signature", ""))
            source = event.get("source", "UNKNOWN")
            is_chart_watch_tick = bool(event.get("chart_watch_tick", False))
            is_chart_watch_entry = bool(event.get("chart_watch_entry", False))
            watch_token = str(event.get("chart_watch_token", "") or "")
            if not signature:
                continue
            enabled_modes = _get_enabled_trading_modes()
            chart_only = (len(enabled_modes) == 1 and TRADING_MODE_CHART_PRO in enabled_modes)
            if chart_only and event.get("kol_wallet"):
                # In Chart-Pro-only mode, skip KOL events before any tx/RPC resolution work.
                continue

            token_mint = None
            sol_amount = 0.0
            kol_sol_amount = 0.0
            kol_amount_known = False
            kol_side = "UNKNOWN"
            resolved_meta = None
            resolved_tx_value = None

            is_probable_sig = len(signature) > 60
            if is_chart_watch_tick and watch_token:
                token_mint = watch_token
                is_probable_sig = False
            elif is_probable_sig:
                try:
                    for _ in range(5):
                        try:
                            tx = await async_client.get_transaction(
                                solders.signature.Signature.from_string(signature),
                                max_supported_transaction_version=0,
                                commitment=Confirmed,
                            )
                            if tx.value:
                                resolved_tx_value = tx.value
                                meta = tx.value.transaction.meta
                                resolved_meta = meta
                                if meta.err:
                                    token_mint = "FAILED_TX"
                                    break

                                if meta.pre_balances and meta.post_balances:
                                    for pre, post in zip(meta.pre_balances, meta.post_balances):
                                        sol_change = abs(pre - post) / 1e9
                                        if sol_change > sol_amount:
                                            sol_amount = sol_change

                                balances = meta.post_token_balances or meta.pre_token_balances
                                if not balances:
                                    token_mint = "SOL_TRANSFER"
                                    break

                                for balance in balances:
                                    mint_addr = str(balance.mint)
                                    if mint_addr != "So11111111111111111111111111111111111111112":
                                        token_mint = mint_addr
                                        break
                            if token_mint:
                                break
                        except Exception as exc:
                            if "429" in str(exc):
                                await asyncio.sleep(2)
                            else:
                                await asyncio.sleep(1)
                except Exception as e:
                    print(f"Failed to resolve mint for {signature}: {e}")
            else:
                token_mint = signature

            if event.get("kol_wallet"):
                wallet_addr = str(event.get("kol_wallet", ""))
                if is_probable_sig:
                    resolved_amt, known = await _resolve_kol_sol_amount(signature, wallet_addr)
                    kol_sol_amount = float(resolved_amt or 0.0)
                    kol_amount_known = bool(known)
                else:
                    kol_sol_amount = float(sol_amount or 0.0)
                    kol_amount_known = kol_sol_amount > 0

                try:
                    if resolved_meta and token_mint and len(str(token_mint)) == 44:
                        pre_amt = _balance_amount_for_owner(
                            getattr(resolved_meta, "pre_token_balances", []),
                            wallet_addr,
                            str(token_mint),
                        )
                        post_amt = _balance_amount_for_owner(
                            getattr(resolved_meta, "post_token_balances", []),
                            wallet_addr,
                            str(token_mint),
                        )
                        delta = post_amt - pre_amt
                        if delta > 0:
                            kol_side = "BUY"
                        elif delta < 0:
                            kol_side = "SELL"
                except Exception:
                    pass

            enabled_mode_labels = _format_mode_labels(enabled_modes)
            kol_min_sol = min(_mode_kol_min_sol(m) for m in enabled_modes) if enabled_modes else 0.23

            if event.get("kol_wallet") and (not kol_amount_known):
                print(
                    f"Skipping KOL trade from {event.get('kol_label', 'Unknown')}: "
                    f"amount unresolved for tx {signature[:12]}..."
                )
                if token_mint:
                    _chart_watch_mark_result(
                        token_mint,
                        status="SKIPPED",
                        reason="kol amount unresolved",
                        source=str(source or ""),
                    )
                continue

            if event.get("kol_wallet") and kol_sol_amount < kol_min_sol:
                print(
                    f"Skipping KOL trade from {event.get('kol_label', 'Unknown')}: "
                    f"{kol_sol_amount:.4f} SOL (below {kol_min_sol:.2f} SOL threshold "
                    f"across modes: {enabled_mode_labels})"
                )
                if token_mint:
                    _chart_watch_mark_result(
                        token_mint,
                        status="SKIPPED",
                        reason=f"kol sol {kol_sol_amount:.4f} below {kol_min_sol:.2f}",
                        source=str(source or ""),
                    )
                continue

            if not token_mint:
                token_mint = signature

            token_mint = str(token_mint)
            chart_watch_candidate_admitted = True
            watch_item = None
            chart_only = (len(enabled_modes) == 1 and TRADING_MODE_CHART_PRO in enabled_modes)
            if (
                chart_only
                and _chart_watch_focus_profile() == "quality_first"
                and (not is_chart_watch_tick)
                and (not is_chart_watch_entry)
                and (not _is_trend_source_event(str(source or ""), event))
            ):
                # In strict quality-first mode, Chart Pro intake is trend-feed only.
                continue
            if _chart_watch_enabled() and len(token_mint) >= 32:
                if is_chart_watch_tick:
                    watch_item = _chart_watch_upsert(token_mint, event=event, source=str(source or ""), signature=signature)
                    if watch_item and not watch_item.get("display_signature"):
                        watch_item["display_signature"] = signature
                elif is_chart_watch_entry:
                    watch_item = _chart_watch_upsert(token_mint, event=event, source=str(source or ""), signature=signature)
                    if watch_item:
                        watch_item["suppressed"] = False
                        watch_item["admission_reason"] = str(watch_item.get("admission_reason", "admitted") or "admitted")
                        if not watch_item.get("display_signature"):
                            watch_item["display_signature"] = signature
                else:
                    market_hint = {}
                    token_age_hint = 0
                    admission = {"accepted": False, "reason": "engine unavailable", "reason_code": "NO_MARKET_DATA"}
                    if paper_trader and paper_trader.engine:
                        market_hint = await paper_trader.engine.get_token_price(token_mint) or {}
                        token_age_hint = await _chart_watch_get_token_age_cached(token_mint)
                        admission = _chart_watch_admission_check(
                            token_mint,
                            market_hint,
                            token_age_hint,
                            source_meta={"source": str(source or ""), "has_trend_signal": _is_trend_source_event(str(source or ""), event)},
                        )
                    if bool(admission.get("accepted", False)):
                        watch_item = _chart_watch_upsert(token_mint, event=event, source=str(source or ""), signature=signature)
                        if watch_item and not watch_item.get("display_signature"):
                            watch_item["display_signature"] = signature
                        if watch_item:
                            watch_item["suppressed"] = False
                            watch_item["admission_reason"] = str(admission.get("reason", "admitted") or "admitted")
                            watch_item["token_age_sec"] = int(token_age_hint)
                            watch_item["last_volume_m5"] = _safe_float((market_hint or {}).get("volume_m5", watch_item.get("last_volume_m5", 0.0)), 0.0)
                            watch_item["last_liquidity_usd"] = _safe_float((market_hint or {}).get("liquidity_usd", watch_item.get("last_liquidity_usd", 0.0)), 0.0)
                            watch_item["last_mcap_usd"] = _safe_float((market_hint or {}).get("mcap_usd", (market_hint or {}).get("fdv", watch_item.get("last_mcap_usd", 0.0))), 0.0)
                    else:
                        chart_watch_candidate_admitted = False
                        _chart_watch_record_suppressed(
                            token_mint,
                            source=str(source or ""),
                            reason=str(admission.get("reason", "suppressed") or "suppressed"),
                            reason_code=str(admission.get("reason_code", "OTHER") or "OTHER"),
                            market_data=market_hint,
                            token_age_sec=token_age_hint,
                        )
                        service.chart_watchlist_by_token.pop(token_mint, None)
            if is_chart_watch_tick and watch_item:
                signature = str(watch_item.get("display_signature", signature) or signature)
            if chart_only and (not is_chart_watch_tick) and (not chart_watch_candidate_admitted):
                await service.broadcast(
                    {
                        "type": "update",
                        "signature": signature,
                        "token": token_mint,
                        "status": "SKIPPED",
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                        "risk_msg": "Chart watch admission rejected (quality-first trend-first)",
                    }
                )
                continue
            if event.get("kol_wallet"):
                _track_kol_flow(token_mint, kol_side, kol_sol_amount)
                if kol_side == "BUY":
                    service.first_kol_buy_ts_by_token.setdefault(token_mint, time.time())

            if (not is_chart_watch_tick) and (not is_chart_watch_entry):
                await service.broadcast(
                    {
                        "type": "new_token",
                        "signature": signature,
                        "token": token_mint,
                        "source": source,
                        "time": time.strftime("%H:%M:%S"),
                        "status": "ANALYZING...",
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                    }
                )

            existing_sig = None
            for sig, pos in paper_trader.positions.items():
                if pos.get("token_mint") == token_mint:
                    existing_sig = sig
                    break

            if existing_sig:
                if event.get("kol_wallet"):
                    paper_trader.add_kol_signal(
                        signature=existing_sig,
                        kol_wallet=event.get("kol_wallet", ""),
                        kol_label=event.get("kol_label", ""),
                    )
                kol_count = paper_trader.count_distinct_kols_for_token(token_mint)
                boost = max(0, (kol_count - 1) * 5)
                addon_msg = ""
                addon_executed = False
                pos = paper_trader.positions.get(existing_sig, {})
                pos_strategy_mode = str(pos.get("strategy_mode", "") or "")
                if (
                    TRADING_MODE_LAUNCH_BURST in enabled_modes
                    and pos_strategy_mode == TRADING_MODE_LAUNCH_BURST
                ):
                    meta = dict(pos.get("strategy_meta", {}) or {})
                    addon_done = bool(meta.get("burst_addon_done", False))
                    pos_age = time.time() - float(pos.get("start_time", time.time()) or time.time())
                    if (not addon_done) and pos_age >= 10 and kol_side == "BUY":
                        addon_quote = await paper_trader.engine.get_token_price(token_mint)
                        if addon_quote:
                            addon_price = float(addon_quote.get("price_sol", 0.0) or 0.0)
                            breakout_px = float(meta.get("burst_breakout_price", pos.get("entry_price", 0.0)) or 0.0)
                            m5_change = float(addon_quote.get("price_change_m5", 0.0) or 0.0)
                            m5_vol = float(addon_quote.get("volume_m5", 0.0) or 0.0)
                            if addon_price > 0 and breakout_px > 0 and addon_price >= breakout_px and m5_change > 0 and m5_vol > 0:
                                import random

                                base_min = float(service.settings.get("sol_amount_min", 0.1) or 0.1)
                                base_max = float(service.settings.get("sol_amount_max", 0.5) or 0.5)
                                size_factor = float(service.settings.get("launch_burst_size_factor", 0.60) or 0.60)
                                addon_sol = random.uniform(base_min, base_max) * size_factor * 0.5
                                addon_tokens = addon_sol / addon_price if addon_price > 0 else 0.0
                                if addon_tokens > 0 and paper_trader.scale_in_position(existing_sig, addon_tokens, addon_price, float(addon_quote.get("fdv", 0.0) or 0.0)):
                                    paper_trader.set_strategy_meta(existing_sig, {"burst_addon_done": True})
                                    addon_executed = True
                                    addon_msg = f" Add-on +{addon_sol:.4f} SOL (hold>10s above breakout)."
                                    if sniper and sniper.is_custodial_enabled():
                                        live_addon = await sniper.execute_buy(token_mint, amount_sol=addon_sol)
                                        if live_addon.get("status") in ("executed", "balance_detected"):
                                            used_lamports = float(live_addon.get("in_amount_lamports", 0) or 0)
                                            used_sol = used_lamports / 1_000_000_000 if used_lamports > 0 else float(addon_sol or 0.0)
                                            live_trade_tracker.record_buy(
                                                token=token_mint,
                                                token_amount=float(live_addon.get("out_amount_ui", 0.0) or 0.0),
                                                sol_spent=used_sol,
                                                tx_sig=str(live_addon.get("tx_sig", "") or ""),
                                                entry_fdv=float(addon_quote.get("fdv", 0.0) or 0.0),
                                                strategy_mode=pos_strategy_mode or TRADING_MODE_KOL_MOMENTUM,
                                                strategy_meta=dict(pos.get("strategy_meta", {}) or {}),
                                            )
                                            await service.broadcast({
                                                "type": "snipe",
                                                "msg": (
                                                    f"LIVE ADD-ON BUY: {token_mint[:8]}... "
                                                    f"{used_sol:.4f} SOL "
                                                    f"{'(tx ' + str(live_addon.get('tx_sig', ''))[:10] + '...)' if live_addon.get('tx_sig') else '(tx pending/unknown)'}"
                                                ),
                                            })
                                            await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                                            await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
                await service.broadcast(
                    {
                        "type": "update",
                        "signature": signature,
                        "token": token_mint,
                        "status": "ADD_ON" if addon_executed else "ALREADY_ACTIVE",
                        "score": boost,
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                        "risk_msg": (
                            f"Token already active. KOL consensus: {kol_count}"
                            f" (score boost +{boost}).{addon_msg}"
                        ),
                    }
                )
                _chart_watch_mark_result(
                    token_mint,
                    status="ADD_ON" if addon_executed else "ALREADY_ACTIVE",
                    reason=f"token already active; kol consensus={kol_count}",
                    score=boost,
                    source=str(source or ""),
                )
                continue

            if len(token_mint) == 44:
                is_safe, risk_msg = await risk_engine.check_token_safety(token_mint)
            else:
                is_safe, risk_msg = False, "Incomplete data (resolution failed)"

            if not is_safe:
                if "Mint Account Not Found" not in str(risk_msg):
                    await service.broadcast(
                        {
                            "type": "update",
                            "signature": signature,
                            "status": "HIGH_RISK",
                            "kol_wallet": event.get("kol_wallet", ""),
                            "kol_label": event.get("kol_label", ""),
                            "kol_sol_amount": kol_sol_amount,
                            "kol_side": kol_side,
                            "risk_msg": f"{risk_msg}",
                        }
                    )
                _chart_watch_mark_result(
                    token_mint,
                    status="HIGH_RISK",
                    reason=str(risk_msg or ""),
                    source=str(source or ""),
                )
                continue

            market_data = await _fetch_market_data_with_retries(token_mint, enabled_modes)
            if not market_data:
                await service.broadcast(
                    {
                        "type": "update",
                        "signature": signature,
                        "token": token_mint,
                        "status": "WATCH",
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                        "risk_msg": "Market data unavailable",
                    }
                )
                _chart_watch_mark_result(
                    token_mint,
                    status="WATCH",
                    reason="market data unavailable",
                    source=str(source or ""),
                )
                continue

            market_data["token"] = token_mint
            liq_known = bool(market_data.get("liquidity_known", False))
            mcap_known = bool(market_data.get("mcap_known", False))
            liq_now = float(market_data.get("liquidity_usd", 0) or 0)
            fdv = float(market_data.get("mcap_usd", market_data.get("fdv", 0)) or 0)
            market_data["fdv"] = fdv
            _track_market_tick(token_mint, float(market_data.get("price_sol", 0.0) or 0.0), float(market_data.get("volume_m5", 0.0) or 0.0))
            _chart_watch_mark_result(
                token_mint,
                status="WATCH",
                reason="market data refreshed",
                market_data=market_data,
                source=str(source or ""),
            )

            candle_data = {}
            candle_score_adj = 0
            try:
                cached = service.candle_signal_cache_by_token.get(token_mint, {}) or {}
                cache_age = time.time() - float(cached.get("ts", 0.0) or 0.0)
                if cache_age <= float(service.candle_signal_cache_ttl_sec or 20.0):
                    candle_data = dict(cached.get("data", {}) or {})
                else:
                    pair_addr_for_candles = market_data.get("pair_address", "")
                    candle_cfg = {
                        "token_mint": token_mint,
                        "breakout_lookback_bars": int(service.settings.get("chart_pro_breakout_lookback_bars", 20) or 20),
                        "min_volume_multiple": float(service.settings.get("chart_pro_min_volume_multiple", 1.8) or 1.8),
                        "require_retest": bool(service.settings.get("chart_pro_require_retest", False)),
                        "stop_atr_mult": float(service.settings.get("chart_pro_stop_atr_mult", 1.2) or 1.2),
                        "tp1_r": float(service.settings.get("chart_pro_tp1_r", 1.0) or 1.0),
                        "tp2_r": float(service.settings.get("chart_pro_tp2_r", 2.0) or 2.0),
                        "runner_trail_atr_mult": float(service.settings.get("chart_pro_runner_trail_atr_mult", 1.5) or 1.5),
                    }
                    candle_data = await candle_analyzer.analyze(pair_addr_for_candles, config=candle_cfg)
                    service.candle_signal_cache_by_token[token_mint] = {
                        "ts": time.time(),
                        "data": dict(candle_data or {}),
                    }
                candle_score_adj = int(candle_data.get("score_adjustment", 0) or 0)
            except Exception as e:
                print(f"Candle analysis unavailable for {token_mint[:8]}: {e}")

            mode_gate_failures = {}
            eligible_modes = []
            for mode in enabled_modes:
                gate_reason = _mode_entry_gate_reason(
                    mode=mode,
                    token_mint=token_mint,
                    kol_side=kol_side,
                    fdv=fdv,
                    liq_now=liq_now,
                    mcap_known=mcap_known,
                    liq_known=liq_known,
                    candle_data=candle_data,
                    market_data=market_data,
                )
                if gate_reason:
                    mode_gate_failures[mode] = gate_reason
                else:
                    eligible_modes.append(mode)

            if not eligible_modes:
                gate_msg = " | ".join(mode_gate_failures.get(m, "") for m in enabled_modes if mode_gate_failures.get(m, ""))
                if market_data.get("market_data_source"):
                    gate_msg = f"{gate_msg} (source: {market_data.get('market_data_source', 'unknown')})"
                await service.broadcast(
                    {
                        "type": "update",
                        "signature": signature,
                        "token": token_mint,
                        "status": "WATCH",
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                        "risk_msg": gate_msg or "No enabled strategy entry gates passed",
                    }
                )
                _chart_watch_mark_result(
                    token_mint,
                    status="WATCH",
                    reason=gate_msg or "no enabled strategy entry gates passed",
                    market_data=market_data,
                    source=str(source or ""),
                )
                continue

            kol_count = 0
            score_boost = 0
            if event.get("kol_wallet"):
                wallet = event.get("kol_wallet")
                seen = service.kol_seen_by_token.setdefault(token_mint, set())
                seen.add(wallet)
                kol_count = len(seen)
                score_boost = max(0, (kol_count - 1) * 5)

            holder_data = {}
            holder_score_adj = 0
            try:
                holder_data = await holder_intel.get_holder_intel(
                    token_mint,
                    pair_address=market_data.get("pair_address", ""),
                )
                bundlers_pct = float(holder_data.get("bundlers_holding_pct", 0) or 0)
                snipers_pct = float(holder_data.get("snipers_holding_pct", 0) or 0)

                # Penalize high concentration by bundlers/snipers.
                if bundlers_pct >= 10:
                    holder_score_adj -= 8
                elif bundlers_pct >= 5:
                    holder_score_adj -= 4

                if snipers_pct >= 15:
                    holder_score_adj -= 5

                cluster_same_hour = float(holder_data.get("wallet_age_same_hour_cluster_pct", 0.0) or 0.0)
                if cluster_same_hour >= 50.0:
                    holder_score_adj -= 6
            except Exception as e:
                print(f"Holder intel unavailable for {token_mint[:8]}: {e}")

            total_score_boost = score_boost + holder_score_adj + candle_score_adj
            analysis = analyzer.score_token(market_data, score_boost=total_score_boost)
            rapid_pump_confirmed = _is_confirmed_rapid_pump(market_data, holder_data, candle_data)

            mode_decisions = []
            for mode in eligible_modes:
                mode_name = _trading_mode_label(mode)
                mode_analysis = {
                    "score": float(analysis.get("score", 0.0) or 0.0),
                    "verdict": str(analysis.get("verdict", "WATCH") or "WATCH"),
                    "risk_msg": str(analysis.get("risk_msg", "") or ""),
                    "metrics": dict(analysis.get("metrics", {}) or {}),
                }
                mode_hard_veto = _get_pretrade_veto_reason(market_data, holder_data, candle_data, mode)
                if mode_hard_veto and rapid_pump_confirmed:
                    # Explicitly allow strong momentum setups even after extreme 5m pumps.
                    mode_hard_veto = ""

                burst_momentum_ok = True
                burst_momentum_reason = "n/a"
                if mode == TRADING_MODE_LAUNCH_BURST:
                    burst_momentum_ok, burst_momentum_reason = _burst_momentum_confirmation(token_mint, market_data, candle_data)
                    if not burst_momentum_ok:
                        mode_analysis["verdict"] = "WATCH"
                        mode_analysis["risk_msg"] = f"{mode_analysis.get('risk_msg', '')} | No burst momentum confirmation"

                if mode == TRADING_MODE_EARLY_20 and mode_analysis.get("verdict") != "BUY":
                    mode_min_score = float(service.settings.get("early_20_min_score", 35.0) or 35.0)
                    mode_min_liq = float(service.settings.get("early_20_min_liquidity_usd", 7000.0) or 7000.0)
                    if float(mode_analysis.get("score", 0) or 0) >= mode_min_score and liq_now >= mode_min_liq:
                        mode_analysis["verdict"] = "BUY"
                        mode_analysis["risk_msg"] = (
                            f"{mode_analysis.get('risk_msg', '')} | Mode override BUY ({mode_name})"
                        )
                elif mode == TRADING_MODE_LAUNCH_BURST and burst_momentum_ok and mode_analysis.get("verdict") != "BUY":
                    mode_min_score = float(service.settings.get("launch_burst_min_score", 35.0) or 35.0)
                    mode_min_liq = float(service.settings.get("launch_burst_min_liquidity_usd", 6000.0) or 6000.0)
                    if float(mode_analysis.get("score", 0) or 0) >= mode_min_score and liq_now >= mode_min_liq:
                        mode_analysis["verdict"] = "BUY"
                        mode_analysis["risk_msg"] = (
                            f"{mode_analysis.get('risk_msg', '')} | Mode override BUY ({mode_name})"
                        )
                elif mode == TRADING_MODE_CHART_PRO:
                    trigger = dict((candle_data or {}).get("trigger", {}) or {})
                    quality = dict((candle_data or {}).get("quality", {}) or {})
                    chart_score = float(quality.get("score", 0.0) or 0.0)
                    min_chart_score = float(service.settings.get("chart_pro_min_score", 42.0) or 42.0)
                    fast_entry_ready, fast_entry_reason = _chart_pro_fast_entry_signal(candle_data, market_data)
                    mode_analysis["score"] = max(float(mode_analysis.get("score", 0.0) or 0.0), chart_score)
                    if bool(trigger.get("entry_ready", False)) or fast_entry_ready:
                        mode_analysis["verdict"] = "BUY"
                        if fast_entry_ready and not bool(trigger.get("entry_ready", False)):
                            mode_analysis["risk_msg"] = (
                                f"ChartPro BUY ({fast_entry_reason}) | score={chart_score:.1f} | "
                                f"Bias={((candle_data or {}).get('regime', {}) or {}).get('trend_bias_5m', 'n/a')}"
                            )
                        else:
                            mode_analysis["risk_msg"] = (
                                f"ChartPro BUY | score={chart_score:.1f} | "
                                f"Bias={((candle_data or {}).get('regime', {}) or {}).get('trend_bias_5m', 'n/a')}"
                            )
                    else:
                        mode_analysis["verdict"] = "WATCH"
                        mode_analysis["risk_msg"] = (
                            f"ChartPro WAIT | score={chart_score:.1f}/{min_chart_score:.1f} | "
                            f"EntryReady={bool(trigger.get('entry_ready', False))}"
                        )

                mode_decisions.append(
                    {
                        "mode": mode,
                        "analysis": mode_analysis,
                        "hard_veto_reason": mode_hard_veto,
                        "burst_momentum_reason": burst_momentum_reason,
                    }
                )

            selected_decision = None
            for decision in mode_decisions:
                if decision["analysis"].get("verdict") == "BUY" and not decision["hard_veto_reason"]:
                    selected_decision = decision
                    break

            display_decision = selected_decision or (mode_decisions[0] if mode_decisions else None)
            trading_mode = str((display_decision or {}).get("mode", TRADING_MODE_KOL_MOMENTUM) or TRADING_MODE_KOL_MOMENTUM)
            trading_mode_name = _trading_mode_label(trading_mode)
            analysis = dict((display_decision or {}).get("analysis", analysis) or analysis)
            hard_veto_reason = str((display_decision or {}).get("hard_veto_reason", "") or "")
            burst_momentum_reason = str((display_decision or {}).get("burst_momentum_reason", "n/a") or "n/a")

            metrics = analysis.get("metrics", {})
            vol_str = f"${metrics.get('vol_m5', 0):,.0f}"
            liq_str = f"${metrics.get('liq', 0):,.0f}"
            boost_note = []
            if score_boost:
                boost_note.append(f"KOL:{score_boost:+d}")
            if candle_score_adj:
                boost_note.append(f"Candles:{candle_score_adj:+d}")
            if holder_score_adj:
                boost_note.append(f"Holders:{holder_score_adj:+d}")
            if rapid_pump_confirmed:
                boost_note.append("RapidPump:CONFIRMED")
            if trading_mode == TRADING_MODE_LAUNCH_BURST:
                boost_note.append(f"Momentum:{burst_momentum_reason}")
            if trading_mode == TRADING_MODE_CHART_PRO:
                regime = dict((candle_data or {}).get("regime", {}) or {})
                setup = dict((candle_data or {}).get("setup", {}) or {})
                trigger = dict((candle_data or {}).get("trigger", {}) or {})
                risk_sig = dict((candle_data or {}).get("risk", {}) or {})
                dbg = dict((candle_data or {}).get("debug", {}) or {})
                boost_note.append(f"Bias:{regime.get('trend_bias_5m', 'n/a')}")
                boost_note.append(f"Breakout:{float(setup.get('breakout_level', 0.0) or 0.0):.8f}")
                boost_note.append(f"VolMult:{float(dbg.get('volume_multiple', 0.0) or 0.0):.2f}")
                boost_note.append(f"ATR:{float(risk_sig.get('atr_pct', 0.0) or 0.0):.2f}%")
                boost_note.append(f"EntryReady:{bool(trigger.get('entry_ready', False))}")
            boost_note.append(f"Enabled:{enabled_mode_labels}")
            if selected_decision:
                boost_note.append(f"Selected:{trading_mode_name}")
            boost_note_str = f" | Adj: {' / '.join(boost_note)}" if boost_note else ""
            holder_note = ""
            if holder_data:
                holder_note = (
                    f" | Top10:{holder_data.get('top10_holding_pct', 0):.2f}%"
                    f" Bundlers:{holder_data.get('bundlers_holding_pct', 0):.2f}%"
                    f" Snipers:{holder_data.get('snipers_holding_pct', 0):.2f}%"
                    f" AgeCluster:{holder_data.get('wallet_age_same_hour_cluster_pct', 0):.1f}%"
                )

            mode_status_detail = []
            for decision in mode_decisions:
                mode_label = _trading_mode_label(decision["mode"])
                mode_verdict = str(decision["analysis"].get("verdict", "WATCH") or "WATCH")
                mode_veto = str(decision.get("hard_veto_reason", "") or "")
                if mode_veto:
                    mode_status_detail.append(f"{mode_label}:{mode_verdict} ({mode_veto})")
                else:
                    mode_status_detail.append(f"{mode_label}:{mode_verdict}")
            mode_status_msg = " | ".join(mode_status_detail)
            status = (
                "BUY_SIGNAL"
                if selected_decision
                else ("HIGH_RISK" if any(str(d.get("hard_veto_reason", "") or "") for d in mode_decisions) else "WATCH")
            )
            risk_prefix = analysis.get("risk_msg", "") if selected_decision else f"No BUY mode selected | {mode_status_msg}"

            await service.broadcast(
                {
                    "type": "update",
                    "signature": signature,
                    "token": token_mint,
                    "status": status,
                    "kol_wallet": event.get("kol_wallet", ""),
                    "kol_label": event.get("kol_label", ""),
                    "kol_sol_amount": kol_sol_amount,
                    "kol_side": kol_side,
                    "score": analysis["score"],
                    "risk_msg": (
                        f"{risk_prefix} | Vol: {vol_str} | Liq: {liq_str}{boost_note_str}{holder_note}"
                        f"{' | ' + hard_veto_reason if hard_veto_reason else ''}"
                    ),
                    "image_url": market_data.get("image_url", ""),
                    "websites": market_data.get("websites", []),
                    "socials": market_data.get("socials", []),
                }
            )
            _chart_watch_mark_result(
                token_mint,
                status=status,
                reason=risk_prefix,
                market_data=market_data,
                score=float(analysis.get("score", 0.0) or 0.0),
                source=str(source or ""),
            )

            if not selected_decision:
                continue

            token_age = await paper_trader.engine.get_token_age(token_mint)
            creator_metrics = await paper_trader.engine.get_creator_twitter_metrics(token_mint)

            if creator_metrics and creator_metrics.get("username") and social_monitor:
                bird_creator = await social_monitor.get_creator_metrics(creator_metrics.get("username"))
                if bird_creator.get("followers", 0) > 0:
                    creator_metrics["followers"] = bird_creator["followers"]
                creator_metrics["scraped_at"] = bird_creator.get("scraped_at", creator_metrics.get("scraped_at"))

            max_token_age_sec = int(service.settings.get("max_token_age_sec", 259200) or 259200)
            if max_token_age_sec > 0 and float(token_age or 0) > float(max_token_age_sec):
                await service.broadcast(
                    {
                        "type": "update",
                        "signature": signature,
                        "token": token_mint,
                        "status": "SKIPPED",
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                        "score": analysis.get("score", 0.0),
                        "risk_msg": (
                            f"Entry skipped: token age {int(token_age)}s exceeds max {max_token_age_sec}s"
                        ),
                    }
                )
                await service.broadcast(
                    {
                        "type": "log",
                        "msg": (
                            f"Skipped entry for {token_mint[:8]}... "
                            f"(age {int(token_age)}s > max {max_token_age_sec}s)"
                        ),
                    }
                )
                _chart_watch_mark_result(
                    token_mint,
                    status="SKIPPED",
                    reason=f"token age {int(token_age)}s exceeds max {max_token_age_sec}s",
                    market_data=market_data,
                    score=float(analysis.get("score", 0.0) or 0.0),
                    source=str(source or ""),
                )
                continue

            mentions = {}
            if social_monitor:
                mentions = await social_monitor.get_token_mention_count(token_mint)

            token_id = token_mint
            price_sol = market_data.get("price_sol", 0) or 0.0001
            fdv = market_data.get("fdv", 0)
            pair_addr = market_data.get("pair_address", "")

            import random

            sol_min = service.settings.get("sol_amount_min", 0.1)
            sol_max = service.settings.get("sol_amount_max", 0.5)
            sol_to_spend = random.uniform(sol_min, sol_max)
            fast_exit_profile = trading_mode in (TRADING_MODE_EARLY_20, TRADING_MODE_LAUNCH_BURST, TRADING_MODE_CHART_PRO)
            if trading_mode == TRADING_MODE_LAUNCH_BURST:
                # Conservative burst sizing: always trade at user's configured minimum.
                sol_to_spend = float(sol_min or 0.0)
            elif rapid_pump_confirmed and trading_mode != TRADING_MODE_EARLY_20:
                size_factor = float(service.settings.get("rapid_pump_size_factor", 0.5) or 0.5)
                size_factor = max(0.1, min(1.0, size_factor))
                sol_to_spend *= size_factor
                fast_exit_profile = True
            amount_tokens = sol_to_spend / price_sol

            try:
                kol_data = None
                if event.get("kol_wallet"):
                    kol_data = {
                        "wallet": event.get("kol_wallet", ""),
                        "label": event.get("kol_label", "Unknown"),
                    }

                cooldown_sec = int(service.settings.get("token_cooldown_sec", 900) or 900)
                strategy_mode = trading_mode
                strategy_meta = {}
                if trading_mode == TRADING_MODE_LAUNCH_BURST:
                    cooldown_sec = int(service.settings.get("launch_burst_cooldown_sec", cooldown_sec) or cooldown_sec)
                    strategy_meta = {
                        "burst_tp1_pct": float(service.settings.get("launch_burst_take_profit_1_pct", 0.12) or 0.12),
                        "burst_tp2_pct": float(service.settings.get("launch_burst_take_profit_2_pct", 0.20) or 0.20),
                        "burst_runner_trail_pct": float(service.settings.get("launch_burst_runner_trail_pct", 0.08) or 0.08),
                        "burst_strong_red_pct": float(service.settings.get("launch_burst_strong_red_pct", 0.02) or 0.02),
                        "burst_breakout_price": float(price_sol or 0.0),
                        "burst_addon_done": False,
                        "burst_tp1_hit": False,
                        "burst_tp2_hit": False,
                        "burst_runner_peak_price": float(price_sol or 0.0),
                        "burst_peak_price": float(price_sol or 0.0),
                        "burst_red_streak": 0,
                    }
                elif trading_mode == TRADING_MODE_CHART_PRO:
                    cooldown_sec = int(service.settings.get("chart_pro_cooldown_sec", cooldown_sec) or cooldown_sec)
                    trigger = dict((candle_data or {}).get("trigger", {}) or {})
                    setup = dict((candle_data or {}).get("setup", {}) or {})
                    risk = dict((candle_data or {}).get("risk", {}) or {})
                    quality = dict((candle_data or {}).get("quality", {}) or {})
                    chart_entry_raw = float(trigger.get("entry_price", 0.0) or 0.0)
                    chart_stop_raw = float(risk.get("stop_price", 0.0) or 0.0)
                    risk_per_unit_raw = float(risk.get("risk_per_unit", 0.0) or 0.0)
                    chart_entry = float(price_sol or chart_entry_raw or 0.0)
                    risk_pct = 0.0
                    if chart_entry_raw > 0:
                        if risk_per_unit_raw > 0:
                            risk_pct = risk_per_unit_raw / chart_entry_raw
                        elif chart_stop_raw > 0 and chart_stop_raw < chart_entry_raw:
                            risk_pct = (chart_entry_raw - chart_stop_raw) / chart_entry_raw
                    if risk_pct <= 0:
                        atr_pct = float(risk.get("atr_pct", 0.0) or 0.0)
                        if atr_pct > 0:
                            risk_pct = atr_pct / 100.0
                    if risk_pct <= 0:
                        risk_pct = 0.02
                    risk_pct = max(0.005, min(0.35, risk_pct))
                    chart_stop = chart_entry * (1.0 - risk_pct) if chart_entry > 0 else 0.0
                    risk_per_unit = (chart_entry - chart_stop) if chart_entry > 0 and chart_stop > 0 else 0.0
                    tp1_r = float(service.settings.get("chart_pro_tp1_r", 1.0) or 1.0)
                    tp2_r = float(service.settings.get("chart_pro_tp2_r", 2.0) or 2.0)
                    tp1_price = chart_entry + (risk_per_unit * tp1_r) if risk_per_unit > 0 else 0.0
                    tp2_price = chart_entry + (risk_per_unit * tp2_r) if risk_per_unit > 0 else 0.0
                    strategy_meta = {
                        "chart_entry_price": chart_entry,
                        "chart_entry_price_raw": chart_entry_raw,
                        "chart_stop_price": chart_stop,
                        "chart_stop_price_raw": chart_stop_raw,
                        "chart_risk_per_unit": risk_per_unit,
                        "chart_risk_per_unit_raw": risk_per_unit_raw,
                        "chart_risk_pct": risk_pct,
                        "chart_tp1_price": tp1_price,
                        "chart_tp2_price": tp2_price,
                        "chart_tp1_r": tp1_r,
                        "chart_tp2_r": tp2_r,
                        "chart_runner_peak": chart_entry,
                        "chart_tp1_hit": False,
                        "chart_tp2_hit": False,
                        "chart_setup_level": float(setup.get("breakout_level", 0.0) or 0.0),
                        "chart_signal_version": "v1",
                        "chart_runner_trail_atr_mult": float(service.settings.get("chart_pro_runner_trail_atr_mult", 1.5) or 1.5),
                        "chart_atr_abs": float(risk.get("atr_abs", 0.0) or 0.0),
                        "chart_quality_score": float(quality.get("score", 0.0) or 0.0),
                    }
                elif trading_mode == TRADING_MODE_EARLY_20:
                    strategy_meta = {
                        "tp_pct": float(service.settings.get("early_20_take_profit_pct", 0.20) or 0.20),
                        "sl_pct": float(service.settings.get("early_20_stop_loss_pct", -0.12) or -0.12),
                        "max_hold_sec": int(service.settings.get("early_20_max_hold_sec", 300) or 300),
                    }
                else:
                    strategy_meta = {
                        "tp_pct": float(service.settings.get("fast_exit_take_profit_pct", 0.25) or 0.25),
                        "sl_pct": float(service.settings.get("fast_exit_stop_loss_pct", -0.12) or -0.12),
                        "max_hold_sec": int(service.settings.get("fast_exit_max_hold_sec", 300) or 300),
                    }

                source_meta = {
                    "source_type": "UNKNOWN",
                    "source_label": str(source or ""),
                }
                social_data = dict(event.get("social_data", {}) or {})
                market_trend_data = dict(event.get("market_trend_data", {}) or {})
                if market_trend_data:
                    source_meta.update(
                        {
                            "source_type": "MARKET_TREND",
                            "source_author": "dexscreener",
                            "source_author_name": "DexScreener Boosts",
                            "source_tweet_id": "",
                            "source_text_excerpt": "",
                            "source_boost_amount": float(market_trend_data.get("boost_amount", 0.0) or 0.0),
                            "source_boost_total": float(market_trend_data.get("boost_total", 0.0) or 0.0),
                        }
                    )
                elif social_data:
                    source_meta.update(
                        {
                            "source_type": "SOCIAL",
                            "source_author": str(social_data.get("author", "") or ""),
                            "source_author_name": str(social_data.get("author_name", "") or ""),
                            "source_tweet_id": str(social_data.get("tweet_id", "") or ""),
                            "source_text_excerpt": str(social_data.get("text", "") or "")[:280],
                        }
                    )
                elif event.get("kol_wallet"):
                    source_meta.update(
                        {
                            "source_type": "KOL",
                            "source_author": str(event.get("kol_wallet", "") or ""),
                            "source_author_name": str(event.get("kol_label", "") or ""),
                            "source_tweet_id": "",
                            "source_text_excerpt": "",
                        }
                    )
                strategy_meta.update(source_meta)

                open_result = await paper_trader.open_position(
                    signature,
                    token_id,
                    price_sol,
                    amount_tokens,
                    fdv,
                    pair_addr,
                    kol_data=kol_data,
                    cooldown_sec=cooldown_sec,
                    strategy_mode=strategy_mode,
                    strategy_meta=strategy_meta,
                )
                if not open_result or not open_result.get("opened"):
                    skip_reason = str((open_result or {}).get("reason", "open_rejected") or "open_rejected")
                    await service.broadcast(
                        {
                            "type": "update",
                            "signature": signature,
                            "token": token_id,
                            "status": "SKIPPED",
                            "kol_wallet": event.get("kol_wallet", ""),
                            "kol_label": event.get("kol_label", ""),
                            "kol_sol_amount": kol_sol_amount,
                            "kol_side": kol_side,
                            "score": analysis.get("score", 0.0),
                            "risk_msg": f"Entry skipped: {skip_reason}",
                        }
                    )
                    await service.broadcast(
                        {
                            "type": "log",
                            "msg": (
                                f"Skipped entry for {token_id[:8]}... "
                                f"({(open_result or {}).get('reason', 'open_rejected')})"
                            ),
                        }
                    )
                    _chart_watch_mark_result(
                        token_mint,
                        status="SKIPPED",
                        reason=f"entry skipped: {skip_reason}",
                        market_data=market_data,
                        score=float(analysis.get("score", 0.0) or 0.0),
                        source=str(source or ""),
                    )
                    continue

                await service.broadcast(
                    {
                        "type": "update",
                        "signature": signature,
                        "token": token_id,
                        "status": "PAPER_OPENED",
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                        "score": analysis.get("score", 0.0),
                        "risk_msg": (
                            f"Paper position opened | {sol_to_spend:.4f} SOL"
                            f" | MCap: ${fdv:,.0f}"
                        ),
                    }
                )
                _chart_watch_mark_result(
                    token_mint,
                    status="PAPER_OPENED",
                    reason=f"paper position opened ({sol_to_spend:.4f} SOL)",
                    market_data=market_data,
                    score=float(analysis.get("score", 0.0) or 0.0),
                    source=str(source or ""),
                )

                paper_trader.update_analytics_data(
                    open_result.get("signature", signature),
                    token_age=token_age,
                    twitter_data=creator_metrics,
                    mentions=mentions,
                    holder_intel=holder_data,
                    candle_signals=candle_data,
                )

                if fast_exit_profile:
                    if trading_mode == TRADING_MODE_EARLY_20:
                        take_profit_pct = float(service.settings.get("early_20_take_profit_pct", 0.20) or 0.20)
                        stop_loss_pct = float(service.settings.get("early_20_stop_loss_pct", -0.12) or -0.12)
                        max_hold_sec = int(service.settings.get("early_20_max_hold_sec", 300) or 300)
                    elif trading_mode == TRADING_MODE_CHART_PRO:
                        tp1_r = float(service.settings.get("chart_pro_tp1_r", 1.0) or 1.0)
                        stop_loss_pct = float(service.settings.get("live_early_20_stop_loss_pct", -0.12) or -0.12)
                        # Keep compatibility with generic fields while chart mode relies on strategy_meta prices.
                        take_profit_pct = max(0.05, min(5.0, tp1_r * 0.10))
                        max_hold_sec = int(service.settings.get("chart_pro_max_hold_sec", 360) or 360)
                    elif trading_mode == TRADING_MODE_LAUNCH_BURST:
                        # Primary TP/runner logic is handled in staged burst exits.
                        take_profit_pct = float(service.settings.get("launch_burst_take_profit_2_pct", 0.20) or 0.20)
                        stop_loss_pct = float(service.settings.get("launch_burst_stop_loss_pct", -0.10) or -0.10)
                        max_hold_sec = int(service.settings.get("launch_burst_max_hold_sec", 90) or 90)
                    else:
                        take_profit_pct = float(service.settings.get("fast_exit_take_profit_pct", 0.25) or 0.25)
                        stop_loss_pct = float(service.settings.get("fast_exit_stop_loss_pct", -0.12) or -0.12)
                        max_hold_sec = int(service.settings.get("fast_exit_max_hold_sec", 300) or 300)
                    paper_trader.set_exit_profile(
                        open_result.get("signature", signature),
                        take_profit_pct=take_profit_pct,
                        stop_loss_pct=stop_loss_pct,
                        max_hold_sec=max_hold_sec,
                    )
                    await service.broadcast({
                        "type": "log",
                        "msg": (
                            f"Fast-exit profile enabled for {token_id[:8]}... "
                            f"({trading_mode_name})"
                        ),
                    })

                if sniper and sniper.is_custodial_enabled():
                    existing_live = [
                        p for p in live_trade_tracker.get_open_positions()
                        if p.get("token") == token_id
                    ]
                    if trading_mode == TRADING_MODE_CHART_PRO:
                        pause_live, pause_reason = _chart_live_entry_pause_status()
                        if pause_live:
                            await service.broadcast({
                                "type": "log",
                                "msg": (
                                    f"LIVE BUY paused for {token_id[:8]}... "
                                    f"({pause_reason})"
                                ),
                            })
                            continue
                    if existing_live:
                        await service.broadcast({
                            "type": "log",
                            "msg": f"LIVE BUY skipped for {token_id[:8]}... (already open in live tracker)",
                        })
                        continue
                    max_live_positions = _max_live_open_positions()
                    current_live_open = sum(
                        1
                        for p in live_trade_tracker.get_open_positions()
                        if float(p.get("basis_token_amount", p.get("token_amount", 0.0)) or 0.0) > 0
                        and float(p.get("sol_spent", 0.0) or 0.0) > 0
                    )
                    if max_live_positions > 0 and current_live_open >= max_live_positions:
                        await service.broadcast({
                            "type": "log",
                            "msg": (
                                f"LIVE BUY skipped for {token_id[:8]}... "
                                f"(max_live_open_positions={max_live_positions} reached)"
                            ),
                        })
                        continue
                    buy_result = await sniper.execute_buy(token_id, amount_sol=sol_to_spend)
                    if buy_result.get("status") in ("executed", "balance_detected"):
                        used_lamports = float(buy_result.get("in_amount_lamports", 0) or 0)
                        used_sol = used_lamports / 1_000_000_000 if used_lamports > 0 else float(sol_to_spend or 0.0)
                        live_trade_tracker.record_buy(
                            token=token_id,
                            token_amount=float(buy_result.get("out_amount_ui", 0.0)),
                            sol_spent=used_sol,
                            tx_sig=buy_result.get("tx_sig", ""),
                            entry_fdv=float(fdv or 0.0),
                            strategy_mode=trading_mode,
                            strategy_meta=dict(strategy_meta or {}),
                        )
                        if buy_result.get("status") == "balance_detected":
                            await service.broadcast({
                                "type": "log",
                                "msg": (
                                    f"LIVE BUY balance detected for {token_id[:8]}... "
                                    f"after RPC/preflight error; position recorded."
                                ),
                            })
                        await service.broadcast({
                            "type": "snipe",
                            "msg": (
                                f"LIVE BUY: {token_id[:8]}... "
                                f"{used_sol:.4f} SOL "
                                f"{'(tx ' + str(buy_result.get('tx_sig', ''))[:10] + '...)' if buy_result.get('tx_sig') else '(tx pending/unknown)'}"
                            ),
                        })
                        asyncio.create_task(
                            _notify_live_trade(
                                side="BUY",
                                token=token_id,
                                sol_amount=used_sol,
                                tx_sig=str(buy_result.get("tx_sig", "") or ""),
                            )
                        )
                        await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                        await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
                    elif buy_result.get("status") == "failed":
                        tx_hint = str(buy_result.get("tx_sig", "") or "")
                        await service.broadcast({
                            "type": "log",
                            "msg": (
                                f"LIVE BUY failed: {buy_result.get('reason', 'unknown')}"
                                f"{' (tx ' + tx_hint[:10] + '...)' if tx_hint else ''}"
                            ),
                        })

                await service.broadcast(
                    {
                        "type": "snipe",
                        "msg": (
                            f"PAPER BUY: {token_id[:8]}... @ {price_sol:.6f} SOL "
                            f"({sol_to_spend:.4f} SOL / MCap: ${fdv:,.0f})"
                        ),
                    }
                )
            except Exception as e:
                print(f"Trade execution failed: {e}")
                await service.broadcast(
                    {
                        "type": "update",
                        "signature": signature,
                        "token": token_id,
                        "status": "EXECUTION_ERROR",
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                        "score": analysis.get("score", 0.0) if isinstance(analysis, dict) else 0.0,
                        "risk_msg": f"Execution error: {e}",
                    }
                )
                _chart_watch_mark_result(
                    token_mint,
                    status="EXECUTION_ERROR",
                    reason=str(e),
                    market_data=(market_data if ('market_data' in locals() and isinstance(market_data, dict)) else {}),
                    score=float(analysis.get("score", 0.0) or 0.0) if ('analysis' in locals() and isinstance(analysis, dict)) else 0.0,
                    source=str(source or ""),
                )

        finally:
            if token_mint:
                service.chart_watch_inflight_tokens.discard(str(token_mint))
            if watch_token:
                service.chart_watch_inflight_tokens.discard(str(watch_token))
            service.queue.task_done()

@app.get("/")
async def get():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "static/index.html")
    with open(file_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/history")
async def get_history_page():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "static/history.html")
    with open(file_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/history")
async def get_history_api():
    """Returns normalized trade history for both paper and live trading."""
    def _to_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return float(default)

    def _to_epoch_seconds(*values):
        for raw in values:
            if raw in (None, ""):
                continue
            ts = _to_float(raw, default=0.0)
            if ts <= 0:
                continue
            # Normalize ms timestamps into seconds when needed.
            if ts > 1e12:
                ts = ts / 1000.0
            return ts
        return 0.0

    paper_rows = []
    live_rows = []

    if paper_trader:
        for t in paper_trader.closed_positions:
            pnl = _to_float(t.get("pnl", 0.0), default=0.0)
            close_time = _to_epoch_seconds(t.get("close_time"), t.get("closed_at"))
            duration = _to_float(t.get("duration", 0.0), default=0.0)
            entry_time = _to_epoch_seconds(
                t.get("entry_time"),
                t.get("start_time"),
                t.get("opened_at"),
                t.get("timestamp"),
            )
            if entry_time <= 0 and close_time > 0:
                entry_time = max(0.0, close_time - duration) if duration > 0 else close_time
            paper_rows.append(
                {
                    "trade_type": "PAPER",
                    "token": str(t.get("token", "") or ""),
                    "result": "WIN" if pnl >= 0 else "LOSS",
                    "pnl": pnl,
                    "pnl_pct": _to_float(t.get("pnl_pct", 0.0), default=0.0),
                    "entry_sol_amount": _to_float(t.get("entry_sol_amount", 0.0), default=0.0),
                    "entry_price": _to_float(t.get("entry_price", 0.0), default=0.0),
                    "exit_price": _to_float(t.get("sell_price", 0.0), default=0.0),
                    "duration": duration,
                    "reason": str(t.get("reason", "UNKNOWN") or "UNKNOWN"),
                    "pair_address": str(t.get("pair_address", "") or ""),
                    "entry_time": entry_time,
                    "close_time": close_time,
                    "strategy_mode": str(t.get("strategy_mode", "") or TRADING_MODE_KOL_MOMENTUM),
                    "source_type": str(t.get("source_type", "") or ""),
                    "source_label": str(t.get("source_label", "") or ""),
                    "source_author": str(t.get("source_author", "") or ""),
                    "source_author_name": str(t.get("source_author_name", "") or ""),
                    "source_tweet_id": str(t.get("source_tweet_id", "") or ""),
                }
            )

    for t in live_trade_tracker.get_history():
        pnl = _to_float(t.get("pnl_sol", 0.0), default=0.0)
        close_time = _to_epoch_seconds(t.get("close_time"), t.get("closed_at"))
        duration = _to_float(t.get("duration", 0.0), default=0.0)
        entry_time = _to_epoch_seconds(
            t.get("entry_time"),
            t.get("opened_at"),
            t.get("start_time"),
            t.get("timestamp"),
        )
        if entry_time <= 0 and close_time > 0:
            entry_time = max(0.0, close_time - duration) if duration > 0 else close_time
        live_rows.append(
            {
                "trade_type": "LIVE",
                "token": str(t.get("token", "") or ""),
                "result": "WIN" if pnl >= 0 else "LOSS",
                "pnl": pnl,
                "pnl_pct": _to_float(t.get("pnl_pct", 0.0), default=0.0),
                "entry_sol_amount": _to_float(t.get("cost_basis_sol", 0.0), default=0.0),
                "entry_price": 0.0,
                "exit_price": 0.0,
                "duration": duration,
                "reason": str(t.get("reason", "UNKNOWN") or "UNKNOWN"),
                "pair_address": "",
                "entry_time": entry_time,
                "close_time": close_time,
                "tx_sig": str(t.get("tx_sig", "") or ""),
                "strategy_mode": str(t.get("strategy_mode", "") or TRADING_MODE_KOL_MOMENTUM),
                "source_type": str(t.get("source_type", "") or ""),
                "source_label": str(t.get("source_label", "") or ""),
                "source_author": str(t.get("source_author", "") or ""),
                "source_author_name": str(t.get("source_author_name", "") or ""),
                "source_tweet_id": str(t.get("source_tweet_id", "") or ""),
            }
        )

    combined = paper_rows + live_rows
    combined.sort(key=lambda x: float(x.get("close_time", 0.0) or 0.0), reverse=True)
    return combined


@app.get("/api/sol-price")
async def get_sol_price():
    """Returns current SOL price in USD for UI conversions."""
    url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return {"usd": 0.0}
                data = await resp.json()
                return {"usd": float(data.get("solana", {}).get("usd", 0.0) or 0.0)}
    except Exception:
        return {"usd": 0.0}


@app.post("/api/trading/quote")
async def trading_quote(req: SwapQuoteRequest):
    quote = await _jupiter_get_quote(
        req.input_mint.strip(),
        req.output_mint.strip(),
        int(req.amount),
        int(req.slippage_bps),
    )
    if not quote:
        return JSONResponse(status_code=502, content={"error": "Failed to fetch Jupiter quote"})
    return quote


@app.post("/api/trading/build-swap")
async def trading_build_swap(req: SwapBuildRequest):
    payload = {
        "quoteResponse": req.quote_response,
        "userPublicKey": req.user_public_key,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": "auto",
    }
    urls = [JUPITER_SWAP_API, "https://quote-api.jup.ag/v6/swap"]
    import socket
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    timeout = aiohttp.ClientTimeout(total=25)
    last_err = "unknown"
    for url in urls:
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        last_err = text
                        continue
                    data = json.loads(text)
                    if data.get("swapTransaction"):
                        return data
                    last_err = "missing swapTransaction"
        except Exception as e:
            last_err = str(e)
            continue
    return JSONResponse(status_code=502, content={"error": f"Swap build request failed: {last_err}"})


@app.get("/api/trading/token-balance")
async def trading_token_balance(owner: str, mint: str):
    result = await _rpc_json(
        "getTokenAccountsByOwner",
        [owner, {"mint": mint}, {"encoding": "jsonParsed"}],
    )
    return _parse_token_balance_result(result)


@app.get("/api/live-trading/stats")
async def live_trading_stats():
    return live_trade_tracker.get_stats()


@app.get("/api/trading/custodial-status")
async def custodial_status():
    return {
        "enabled": bool(sniper and sniper.is_custodial_enabled()),
        "wallet": sniper.public_key() if sniper else "",
    }


@app.post("/api/trading/import-key")
async def import_custodial_key(req: KeyImportRequest):
    if not sniper:
        return JSONResponse(status_code=503, content={"error": "Sniper not initialized"})
    try:
        result = sniper.import_private_key(req.private_key, enable_trading=req.enable_trading)
        return {"status": "ok", **result}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid private key: {e}"})


@app.post("/api/trading/clear-key")
async def clear_custodial_key():
    if not sniper:
        return JSONResponse(status_code=503, content={"error": "Sniper not initialized"})
    return {"status": "ok", **sniper.clear_private_key()}


@app.get("/api/live-trading/history")
async def live_trading_history():
    return live_trade_tracker.get_history()


@app.get("/api/live-trading/open-positions")
async def live_trading_open_positions(owner: str = ""):
    effective_owner = (owner or "").strip()
    if not effective_owner and sniper and sniper.is_custodial_enabled():
        effective_owner = (sniper.public_key() or "").strip()
    if effective_owner:
        if not live_trade_tracker.get_open_positions():
            try:
                changes = await _sync_untracked_live_tokens_for_owner(effective_owner)
                if changes:
                    await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                    await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
            except Exception:
                pass
        rows = []
        for pos in live_trade_tracker.get_open_positions():
            row = dict(pos)
            token = str(row.get("token", "") or "")

            quote = {}
            current_price = 0.0
            current_fdv = 0.0
            pair_address = ""
            try:
                if token and paper_trader and paper_trader.engine:
                    quote = await paper_trader.engine.get_token_price(token)
                    current_price = float((quote or {}).get("price_sol", 0.0) or 0.0)
                    current_fdv = float((quote or {}).get("fdv", 0.0) or 0.0)
                    pair_address = str((quote or {}).get("pair_address", "") or "")
            except Exception:
                pass

            token_amount = float(row.get("token_amount", 0.0) or 0.0)
            basis_amount = float(row.get("basis_token_amount", token_amount) or token_amount)
            sol_spent = float(row.get("sol_spent", 0.0) or 0.0)
            position_value_sol = current_price * basis_amount if current_price > 0 and basis_amount > 0 else 0.0
            pnl_sol = position_value_sol - sol_spent
            pnl_pct = (pnl_sol / sol_spent * 100.0) if sol_spent > 0 else 0.0

            row["current_price_sol"] = current_price
            row["current_fdv"] = current_fdv
            row["pair_address"] = pair_address
            row["pnl_sol"] = pnl_sol
            row["pnl_pct"] = pnl_pct

            avg_fdv = float(row.get("avg_entry_fdv", 0.0) or 0.0)
            if avg_fdv <= 0 and paper_trader and paper_trader.engine:
                try:
                    curr_price = current_price
                    curr_fdv = current_fdv
                    entry_price = float(row.get("avg_entry_price_sol", 0.0) or 0.0)
                    if curr_price > 0 and curr_fdv > 0 and entry_price > 0:
                        row["avg_entry_fdv"] = curr_fdv * (entry_price / curr_price)
                except Exception:
                    pass
            rows.append(row)
        return rows
    # Avoid showing stale global state when no wallet context is available.
    return []


@app.post("/api/live-trading/close")
async def live_trading_close(req: LiveCloseRequest):
    token = str(req.token or "").strip()
    if not token:
        return JSONResponse(status_code=400, content={"error": "token is required"})

    if not (sniper and sniper.is_custodial_enabled()):
        return JSONResponse(
            status_code=400,
            content={"error": "Manual live close endpoint is available only in server custodial mode"},
        )

    try:
        result = await sniper.execute_sell_all(token)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Live close failed: {e}"})

    status = str(result.get("status", ""))
    if status == "executed":
        closed = live_trade_tracker.record_sell(
            token=token,
            token_amount_sold=float(result.get("token_amount_sold_ui", 0.0)),
            sol_received=float(result.get("sol_received", 0.0)),
            reason=req.reason or "MANUAL_CLOSE",
            tx_sig=str(result.get("tx_sig", "") or ""),
        )
        if closed:
            trade_analytics_store.record_trade_analytics(_live_closed_to_analytics_trade(closed))
        asyncio.create_task(
            _notify_live_trade(
                side="SELL",
                token=token,
                sol_amount=float(result.get("sol_received", 0.0) or 0.0),
                tx_sig=str(result.get("tx_sig", "") or ""),
                pnl_sol=float((closed or {}).get("pnl_sol", 0.0) or 0.0),
                reason=req.reason or "MANUAL_CLOSE",
            )
        )
        await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
        await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
        return {"status": "ok", "closed": closed, "tx_sig": result.get("tx_sig", "")}

    if status in ("no_balance", "skipped"):
        mark_price = 0.0
        try:
            if paper_trader and paper_trader.engine:
                quote = await paper_trader.engine.get_token_price(token)
                mark_price = float((quote or {}).get("price_sol", 0.0) or 0.0)
        except Exception:
            mark_price = 0.0
        reconciled = live_trade_tracker.reconcile_token_balance(
            token=token,
            onchain_token_amount=0.0,
            mark_price_sol=mark_price,
            reason=req.reason or "MANUAL_CLOSE",
        )
        await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
        await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
        return {"status": "ok", "reconciled": reconciled}

    return JSONResponse(
        status_code=502,
        content={"error": f"Sell failed: {result.get('reason', 'unknown_error')}"},
    )


@app.post("/api/live-trading/reconcile")
async def live_trading_reconcile(req: LiveReconcileRequest):
    owner = (req.owner or "").strip()
    if not owner and sniper and sniper.is_custodial_enabled():
        owner = (sniper.public_key() or "").strip()
    if not owner:
        return JSONResponse(status_code=400, content={"error": "owner wallet is required"})

    changes = []
    changes.extend(await _reconcile_live_positions_for_owner(owner))
    changes.extend(await _sync_untracked_live_tokens_for_owner(owner))

    if changes:
        await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
        await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
        for change in changes:
            await service.broadcast({
                "type": "log",
                "msg": (
                    f"LIVE SYNC: {str(change.get('token', ''))[:8]}... "
                    f"{change.get('action', 'updated')} via on-chain balance reconciliation"
                ),
            })

    return {
        "status": "ok",
        "owner": owner,
        "changes": changes,
        "stats": live_trade_tracker.get_stats(),
    }


@app.post("/api/live-trading/archive-reset")
async def live_trading_archive_reset(req: LiveArchiveResetRequest):
    required_phrase = "ARCHIVE_RESET_LIVE"
    if str(req.confirm_text or "").strip() != required_phrase:
        return JSONResponse(
            status_code=400,
            content={"error": f"confirm_text must be exactly '{required_phrase}'"},
        )

    open_positions = len(live_trade_tracker.get_open_positions())
    if open_positions > 0 and not bool(req.force):
        return JSONResponse(
            status_code=409,
            content={
                "error": "Live positions are still open. Close them first or retry with force=true.",
                "open_positions": open_positions,
            },
        )

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_tag = _safe_archive_tag(req.archive_tag)
    suffix = f"_{safe_tag}" if safe_tag else ""
    archived_files = []

    live_state_src = getattr(live_trade_tracker, "state_file", os.path.join(BASE_DIR, "live_trade_state.json"))
    live_state_archive = os.path.join(ARCHIVE_DIR, f"live_trade_state_{ts}{suffix}.json")
    if os.path.exists(live_state_src):
        shutil.copy2(live_state_src, live_state_archive)
    else:
        with open(live_state_archive, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "open_positions": live_trade_tracker.get_open_positions(),
                    "closed_positions": live_trade_tracker.get_history(),
                    "total_pnl_sol": live_trade_tracker.get_stats().get("total_pnl_sol", 0.0),
                },
                f,
                indent=2,
            )
    archived_files.append(live_state_archive)

    removed_live_analytics = 0
    analytics_archived = ""
    if bool(req.reset_live_analytics):
        analytics_src = str(getattr(trade_analytics_store, "analytics_file", "") or "")
        if analytics_src:
            analytics_archive = os.path.join(ARCHIVE_DIR, f"trade_analytics_{ts}{suffix}.json")
            if os.path.exists(analytics_src):
                shutil.copy2(analytics_src, analytics_archive)
                analytics_archived = analytics_archive
                archived_files.append(analytics_archive)
            removed_live_analytics = int(trade_analytics_store.purge_trade_type("LIVE") or 0)

    before = live_trade_tracker.reset_state()
    after = live_trade_tracker.get_stats()

    await service.broadcast({"type": "live_pnl_update", "data": after})
    await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
    await service.broadcast({
        "type": "log",
        "msg": (
            "LIVE ARCHIVE/RESET complete | "
            f"archived={len(archived_files)} file(s) | "
            f"reset closed={before.get('closed_trades', 0)} open={before.get('open_positions', 0)}"
        ),
    })

    return {
        "status": "ok",
        "archived_files": archived_files,
        "analytics_archived_file": analytics_archived,
        "removed_live_analytics": removed_live_analytics,
        "before": before,
        "after": after,
        "archive_dir": ARCHIVE_DIR,
    }


@app.post("/api/paper-trading/archive-reset")
async def paper_trading_archive_reset(req: PaperArchiveResetRequest):
    required_phrase = "ARCHIVE_RESET_PAPER"
    if str(req.confirm_text or "").strip() != required_phrase:
        return JSONResponse(
            status_code=400,
            content={"error": f"confirm_text must be exactly '{required_phrase}'"},
        )

    if not paper_trader:
        return JSONResponse(status_code=503, content={"error": "Paper trader not initialized"})

    open_positions = len(paper_trader.positions)
    if open_positions > 0 and not bool(req.force):
        return JSONResponse(
            status_code=409,
            content={
                "error": "Paper positions are still open. Close them first or retry with force=true.",
                "open_positions": open_positions,
            },
        )

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_tag = _safe_archive_tag(req.archive_tag)
    suffix = f"_{safe_tag}" if safe_tag else ""
    archived_files = []

    paper_state_src = os.path.join(BASE_DIR, "paper_trade_state.json")
    paper_state_archive = os.path.join(ARCHIVE_DIR, f"paper_trade_state_{ts}{suffix}.json")
    if os.path.exists(paper_state_src):
        shutil.copy2(paper_state_src, paper_state_archive)
    else:
        with open(paper_state_archive, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "positions": paper_trader.positions,
                    "closed_positions": paper_trader.closed_positions,
                    "total_pnl_sol": paper_trader.total_pnl_sol,
                },
                f,
                indent=2,
            )
    archived_files.append(paper_state_archive)

    removed_paper_analytics = 0
    analytics_archived = ""
    if bool(req.reset_paper_analytics):
        analytics_src = str(getattr(trade_analytics_store, "analytics_file", "") or "")
        if analytics_src:
            analytics_archive = os.path.join(ARCHIVE_DIR, f"trade_analytics_{ts}{suffix}.json")
            if os.path.exists(analytics_src):
                shutil.copy2(analytics_src, analytics_archive)
                analytics_archived = analytics_archive
                archived_files.append(analytics_archive)
            removed_paper_analytics = int(trade_analytics_store.purge_trade_type("PAPER") or 0)

    before = paper_trader.reset_state()
    after = paper_trader.get_stats()

    await service.broadcast({"type": "pnl_update", "data": after})
    await service.broadcast({"type": "positions_update", "data": paper_trader.get_positions()})
    await service.broadcast({
        "type": "log",
        "msg": (
            "PAPER ARCHIVE/RESET complete | "
            f"archived={len(archived_files)} file(s) | "
            f"reset closed={before.get('closed_trades', 0)} open={before.get('active_positions', 0)}"
        ),
    })

    return {
        "status": "ok",
        "archived_files": archived_files,
        "analytics_archived_file": analytics_archived,
        "removed_paper_analytics": removed_paper_analytics,
        "before": before,
        "after": after,
        "archive_dir": ARCHIVE_DIR,
    }


@app.post("/api/live-trading/record-buy")
async def live_record_buy(req: LiveBuyRecordRequest):
    entry_fdv = float(req.entry_fdv or 0.0)
    strategy_mode = str(req.strategy_mode or "").strip().lower()
    strategy_meta = dict(req.strategy_meta or {})
    if (not strategy_mode) and paper_trader:
        for _sig, p in paper_trader.positions.items():
            if str(p.get("token_mint", "") or "") == str(req.token or ""):
                strategy_mode = str(p.get("strategy_mode", "") or "").strip().lower()
                if not strategy_meta:
                    strategy_meta = dict(p.get("strategy_meta", {}) or {})
                break
    if entry_fdv <= 0:
        try:
            if paper_trader and paper_trader.engine:
                quote = await paper_trader.engine.get_token_price(req.token)
                entry_fdv = float((quote or {}).get("fdv", 0.0) or 0.0)
        except Exception:
            entry_fdv = 0.0

    live_trade_tracker.record_buy(
        token=req.token,
        token_amount=float(req.token_amount),
        sol_spent=float(req.sol_spent),
        tx_sig=req.tx_sig,
        entry_fdv=entry_fdv,
        strategy_mode=strategy_mode,
        strategy_meta=strategy_meta,
    )
    asyncio.create_task(
        _notify_live_trade(
            side="BUY",
            token=req.token,
            sol_amount=float(req.sol_spent),
            tx_sig=req.tx_sig,
        )
    )
    stats = live_trade_tracker.get_stats()
    await service.broadcast({"type": "live_pnl_update", "data": stats})
    await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
    return {"status": "ok", "stats": stats}


@app.post("/api/live-trading/record-sell")
async def live_record_sell(req: LiveSellRecordRequest):
    closed = live_trade_tracker.record_sell(
        token=req.token,
        token_amount_sold=float(req.token_amount_sold),
        sol_received=float(req.sol_received),
        reason=req.reason,
        tx_sig=req.tx_sig,
    )
    if closed:
        trade_analytics_store.record_trade_analytics(_live_closed_to_analytics_trade(closed))
    if closed:
        asyncio.create_task(
            _notify_live_trade(
                side="SELL",
                token=req.token,
                sol_amount=float(req.sol_received),
                tx_sig=req.tx_sig,
                pnl_sol=float(closed.get("pnl_sol", 0.0) or 0.0),
                reason=req.reason,
            )
        )
    stats = live_trade_tracker.get_stats()
    await service.broadcast({"type": "live_pnl_update", "data": stats})
    await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
    return {"status": "ok", "closed": closed, "stats": stats}


@app.post("/api/positions/close")
async def manual_close_position(req: ClosePositionRequest):
    if not paper_trader:
        return JSONResponse(status_code=503, content={"error": "Paper trader not initialized"})

    close_sig = (req.signature or "").strip()
    if (not close_sig or close_sig == "undefined") and req.token:
        close_sig = paper_trader._find_signature_by_token(req.token) or ""
    if not close_sig:
        return JSONResponse(
            status_code=400,
            content={"error": "Missing position signature/token for manual close"},
        )

    try:
        trade = await paper_trader.close_position(close_sig, reason=req.reason or "MANUAL_CLOSE")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Manual close failed: {e}"})

    if not trade:
        return JSONResponse(
            status_code=404,
            content={
                "error": "Position not found",
                "signature": close_sig,
                "token": req.token,
                "active_positions": len(paper_trader.positions),
            },
        )

    trade_analytics_store.record_trade_analytics(trade)
    pnl_sign = "+" if trade.get("pnl", 0) >= 0 else ""
    await service.broadcast({
        "type": "snipe",
        "msg": (
            f"PAPER SELL ({trade.get('reason', 'MANUAL_CLOSE')}): "
            f"{str(trade.get('token', ''))[:8]}... "
            f"PNL: {pnl_sign}{float(trade.get('pnl', 0)):.4f} SOL "
            f"(MCap: ${float(trade.get('sell_fdv', 0)):,.0f})"
        ),
    })
    await service.broadcast({
        "type": "paper_sell_signal",
        "data": {
            "token": trade.get("token", ""),
            "reason": trade.get("reason", "MANUAL_CLOSE"),
            "sell_price": trade.get("sell_price", 0),
            "pnl": trade.get("pnl", 0),
        }
    })

    if (
        bool(service.settings.get("live_follow_paper_exits", False))
        and sniper
        and sniper.is_custodial_enabled()
    ):
        try:
            sell_result = await sniper.execute_sell_all(trade.get("token", ""))
            if sell_result.get("status") == "executed":
                closed_live = live_trade_tracker.record_sell(
                    token=trade.get("token", ""),
                    token_amount_sold=float(sell_result.get("token_amount_sold_ui", 0.0)),
                    sol_received=float(sell_result.get("sol_received", 0.0)),
                    reason=trade.get("reason", "MANUAL_CLOSE"),
                    tx_sig=sell_result.get("tx_sig", ""),
                )
                if closed_live:
                    trade_analytics_store.record_trade_analytics(_live_closed_to_analytics_trade(closed_live))
                    asyncio.create_task(
                        _notify_live_trade(
                            side="SELL",
                            token=trade.get("token", ""),
                            sol_amount=float(sell_result.get("sol_received", 0.0) or 0.0),
                            tx_sig=str(sell_result.get("tx_sig", "") or ""),
                            pnl_sol=float(closed_live.get("pnl_sol", 0.0) or 0.0),
                            reason=trade.get("reason", "MANUAL_CLOSE"),
                        )
                    )
                await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                await service.broadcast({"type": "live_positions_update", "data": live_trade_tracker.get_open_positions()})
        except Exception as e:
            await service.broadcast({"type": "log", "msg": f"LIVE SELL exception: {e}"})

    await service.broadcast({"type": "positions_update", "data": paper_trader.get_positions()})
    await service.broadcast({"type": "pnl_update", "data": paper_trader.get_stats()})
    return {"status": "ok", "trade": trade}


@app.get("/api/analytics/kol-summary")
async def get_kol_summary():
    return trade_analytics_store.get_kol_performance_summary()


@app.get("/api/analytics/overview")
async def get_analytics_overview():
    overview = trade_analytics_store.get_overview()
    if paper_trader:
        paper_stats = paper_trader.get_stats()
        overview["paper_runtime"] = {
            "active_positions": paper_stats.get("active_positions", 0),
            "closed_trades_runtime": paper_stats.get("closed_trades", 0),
            "total_pnl_runtime_sol": paper_stats.get("total_pnl", 0.0),
            "post_close_monitoring_pending_runtime": paper_stats.get("post_close_monitoring", 0),
        }
    live_stats = live_trade_tracker.get_stats()
    overview["live_runtime"] = {
        "active_positions": live_stats.get("open_positions", 0),
        "closed_trades_runtime": live_stats.get("closed_trades", 0),
        "total_pnl_runtime_sol": live_stats.get("total_pnl_sol", 0.0),
        "external_pnl_estimate_sol": live_stats.get("external_pnl_estimate_sol", 0.0),
    }
    overview["strategy_summary"] = {
        "paper": _strategy_performance_summary(
            paper_trader.closed_positions if paper_trader else [],
            pnl_key="pnl",
            mode_key="strategy_mode",
        ),
        "live": live_trade_tracker.get_strategy_summary(),
    }
    return overview


@app.get("/api/analytics/strategy-summary")
async def get_strategy_summary():
    return {
        "paper": _strategy_performance_summary(
            paper_trader.closed_positions if paper_trader else [],
            pnl_key="pnl",
            mode_key="strategy_mode",
        ),
        "live": live_trade_tracker.get_strategy_summary(),
    }


@app.get("/api/analytics/missed-opportunities")
async def get_missed_opportunities(min_gap_pct: float = 20.0):
    return trade_analytics_store.get_missed_opportunity_report(min_gap_pct=min_gap_pct)


@app.post("/api/analytics/export-csv")
async def export_analytics_csv():
    path = trade_analytics_store.export_analytics_csv()
    return {"status": "ok", "file": path}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await service.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Handle client commands if any
    except WebSocketDisconnect:
        service.disconnect(websocket)


