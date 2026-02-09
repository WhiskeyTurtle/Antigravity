
import asyncio
import sys
import os

# Add current directory to path so imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import json
import aiohttp
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
SOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_QUOTE_API = "https://lite-api.jup.ag/swap/v1/quote"
JUPITER_SWAP_API = "https://lite-api.jup.ag/swap/v1/swap"

# Global State
class FeedService:
    def __init__(self):
        self.active_connections = []
        self.queue = asyncio.Queue()
        self.running = False
        self.kol_seen_by_token = {}
        self.rpc_url = "https://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff"
        self.settings = self.load_settings()
        
    def load_settings(self):
        """Load settings from disk or use defaults"""
        try:
            if os.path.exists(SETTINGS_FILE):
                with open(SETTINGS_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"⚠️ Failed to load settings: {e}")
        
        # Default settings
        return {
            "sol_amount_min": 0.1,
            "sol_amount_max": 0.5
        }
    
    def save_settings(self):
        """Save settings to disk"""
        try:
            with open(SETTINGS_FILE, 'w') as f:
                json.dump(self.settings, f, indent=2)
            print(f"✅ Settings saved: {self.settings}")
        except Exception as e:
            print(f"⚠️ Failed to save settings: {e}")
        
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
social_monitor: SocialMonitor = None
async_client: AsyncClient = None # Global async client
trade_analytics_store: TradeAnalyticsStore = TradeAnalyticsStore()
holder_intel: HolderIntel = HolderIntel(service.rpc_url)
candle_analyzer: CandleAnalyzer = CandleAnalyzer()
live_trade_tracker: LiveTradeTracker = LiveTradeTracker()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Starting Alpha Feed Service...")
    
    global risk_engine, analyzer, sniper, paper_trader, kol_monitor, social_monitor, async_client
    
    # Initialize Clients (Helius RPC)
    async_client = AsyncClient(service.rpc_url)
    
    # Start the Queue Processing Task
    asyncio.create_task(process_feed())
    asyncio.create_task(monitor_positions_task())
    asyncio.create_task(monitor_closed_positions_ath())
    
    # Initialize implementation modules
    risk_engine = RiskEngine(async_client)
    analyzer = Analyzer()
    sniper = Sniper(async_client, service.rpc_url)
    paper_trader = PaperTrader()
    for trade in paper_trader.closed_positions:
        trade_analytics_store.record_trade_analytics(trade)
    
    # MONITOR CALLBACKS
    async def on_kol_activity(signature, wallet_address, wallet_name, source_type):
        import time 
        await service.queue.put({
            "type": "NEW_POOL", 
            "signature": signature,
            "source": f"Tracked: {wallet_name}",
            "kol_wallet": wallet_address,
            "kol_label": wallet_name,
            "time": time.time()
        })

    async def on_social_signal(event):
        """Handles events from SocialMonitor (Twitter/Telegram)"""
        await service.queue.put({
            "type": "SOCIAL_SIGNAL",
            "data": event 
        })
        
    # Start KOL Monitor
    kol_monitor = KOLMonitor(service.rpc_url, "wss://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff", KOL_WALLETS, on_kol_activity)
    service.running = True
    asyncio.create_task(kol_monitor.start())

    # Start Social Monitor
    # User provided tokens for Jack Diamond account
    auth_token = "ec35e4dc84ec7b2cd5a9cefd07157f4a345c2090"
    ct0 = "9f1398a6d5f1071609ef4e7fb22d5868de481a65e2c3e8b0d3396a2ca4d1d9c1692cebfc42bf44407eedf076ad5c69dc730ced1d5971963fa78cd28210ac7f320a34f3dc7fd851cfda6954bbf7d8810b"
    social_monitor = SocialMonitor(on_social_signal, auth_token=auth_token, ct0=ct0)
    asyncio.create_task(social_monitor.start())
    
    yield
    
    # Shutdown
    print("🛑 Shutting down...")
    service.running = False
    if kol_monitor:
        kol_monitor.stop()
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
    if kol_monitor:
        await kol_monitor.add_wallet(item.address, item.name)
        # Update global list
        KOL_WALLETS[item.address] = item.name
        return {"status": "ok", "msg": f"Added {item.name}"}
    return JSONResponse(status_code=503, content={"error": "Monitor not active"})

@app.get("/api/wallets")
async def get_wallets():
    """Get list of tracked wallets"""
    if kol_monitor:
        return kol_monitor.kols
    return KOL_WALLETS

class SettingsUpdate(BaseModel):
    sol_amount_min: float
    sol_amount_max: float


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


class LiveSellRecordRequest(BaseModel):
    token: str
    token_amount_sold: float
    sol_received: float
    reason: str = ""
    tx_sig: str = ""


class KeyImportRequest(BaseModel):
    private_key: str
    enable_trading: bool = True

@app.get("/api/settings")
async def get_settings():
    """Get current settings"""
    return service.settings

@app.post("/api/settings")
async def update_settings(settings: SettingsUpdate):
    """Update SOL amount settings"""
    if settings.sol_amount_min <= 0 or settings.sol_amount_max <= 0:
        return JSONResponse(status_code=400, content={"error": "SOL amounts must be positive"})
    
    if settings.sol_amount_min > settings.sol_amount_max:
        return JSONResponse(status_code=400, content={"error": "Min must be less than or equal to max"})
    
    service.settings["sol_amount_min"] = settings.sol_amount_min
    service.settings["sol_amount_max"] = settings.sol_amount_max
    service.save_settings()
    
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

async def monitor_positions_task():
    """Dedicated high-frequency loop for tracking active positions"""
    print("⚡ Starting High-Frequency Position Monitor...")
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
                trade_analytics_store.record_trade_analytics(trade)
                pnl_sign = "+" if trade['pnl'] >= 0 else ""
                await service.broadcast({
                    "type": "snipe",
                    "msg": f"💰 PAPER SELL ({trade['reason']}): {trade['token'][:8]}... PNL: {pnl_sign}{trade['pnl']:.4f} SOL (MCap: ${trade['sell_fdv']:,.0f})"
                })
                await service.broadcast({
                    "type": "paper_sell_signal",
                    "data": {
                        "token": trade.get("token", ""),
                        "reason": trade.get("reason", ""),
                        "sell_price": trade.get("sell_price", 0),
                        "pnl": trade.get("pnl", 0),
                    }
                })
                if sniper and sniper.is_custodial_enabled():
                    try:
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
                                    f"LIVE SELL: {trade.get('token', '')[:8]}... "
                                    f"{sell_result.get('sol_received', 0):.4f} SOL "
                                    f"(tx {str(sell_result.get('tx_sig', ''))[:10]}...)"
                                ),
                            })
                            await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                        elif sell_result.get("status") not in ("no_balance", "skipped"):
                            await service.broadcast({
                                "type": "log",
                                "msg": f"LIVE SELL failed: {sell_result.get('reason', 'unknown')}",
                            })
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
            print(f"⚠️ Monitor Loop Error: {e}")
            await asyncio.sleep(5)

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

            for trade in pending:
                trade_analytics_store.update_trade_post_close_ath(
                    trade.get("token", ""),
                    float(trade.get("close_time", 0)),
                    trade.get("ath_post_close_72h", {}),
                )

            await asyncio.sleep(60)
        except Exception as e:
            print(f"Post-close ATH monitor error: {e}")
            await asyncio.sleep(15)


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
            last_heartbeat = current_time

        try:
            event = await asyncio.wait_for(service.queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        try:
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
            if not signature:
                continue

            token_mint = None
            sol_amount = 0.0
            kol_sol_amount = 0.0
            kol_side = "UNKNOWN"
            resolved_meta = None
            resolved_tx_value = None

            is_probable_sig = len(signature) > 60
            if is_probable_sig:
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
                kol_sol_amount = sol_amount
                wallet_addr = str(event.get("kol_wallet", ""))
                try:
                    if resolved_meta and resolved_tx_value:
                        msg = resolved_tx_value.transaction.transaction.message
                        account_keys = [str(k) for k in getattr(msg, "account_keys", [])]
                        if wallet_addr in account_keys:
                            i = account_keys.index(wallet_addr)
                            pre = float(resolved_meta.pre_balances[i]) if i < len(resolved_meta.pre_balances) else 0.0
                            post = float(resolved_meta.post_balances[i]) if i < len(resolved_meta.post_balances) else 0.0
                            kol_sol_amount = abs(pre - post) / 1e9
                except Exception:
                    pass

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

            if event.get("kol_wallet") and kol_sol_amount < 0.23:
                print(
                    f"Skipping KOL trade from {event.get('kol_label', 'Unknown')}: "
                    f"{kol_sol_amount:.4f} SOL (below 0.23 SOL threshold)"
                )
                continue

            if not token_mint:
                token_mint = signature

            token_mint = str(token_mint)

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
                await service.broadcast(
                    {
                        "type": "update",
                        "signature": signature,
                        "token": token_mint,
                        "status": "ALREADY_ACTIVE",
                        "score": boost,
                        "kol_wallet": event.get("kol_wallet", ""),
                        "kol_label": event.get("kol_label", ""),
                        "kol_sol_amount": kol_sol_amount,
                        "kol_side": kol_side,
                        "risk_msg": (
                            f"Token already active. KOL consensus: {kol_count}"
                            f" (score boost +{boost})."
                        ),
                    }
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
                continue

            market_data = await paper_trader.engine.get_token_price(token_mint)
            if not market_data:
                continue

            market_data["token"] = token_mint

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
            except Exception as e:
                print(f"Holder intel unavailable for {token_mint[:8]}: {e}")

            candle_data = {}
            candle_score_adj = 0
            try:
                pair_addr_for_candles = market_data.get("pair_address", "")
                candle_data = await candle_analyzer.analyze(pair_addr_for_candles)
                candle_score_adj = int(candle_data.get("score_adjustment", 0) or 0)
            except Exception as e:
                print(f"Candle analysis unavailable for {token_mint[:8]}: {e}")

            total_score_boost = score_boost + holder_score_adj + candle_score_adj
            analysis = analyzer.score_token(market_data, score_boost=total_score_boost)

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
            boost_note_str = f" | Adj: {' / '.join(boost_note)}" if boost_note else ""
            holder_note = ""
            if holder_data:
                holder_note = (
                    f" | Top10:{holder_data.get('top10_holding_pct', 0):.2f}%"
                    f" Bundlers:{holder_data.get('bundlers_holding_pct', 0):.2f}%"
                    f" Snipers:{holder_data.get('snipers_holding_pct', 0):.2f}%"
                )

            await service.broadcast(
                {
                    "type": "update",
                    "signature": signature,
                    "token": token_mint,
                    "status": "BUY_SIGNAL" if analysis["verdict"] == "BUY" else "WATCH",
                    "kol_wallet": event.get("kol_wallet", ""),
                    "kol_label": event.get("kol_label", ""),
                    "kol_sol_amount": kol_sol_amount,
                    "kol_side": kol_side,
                    "score": analysis["score"],
                    "risk_msg": (
                        f"{analysis.get('risk_msg', '')} | Vol: {vol_str} | Liq: {liq_str}{boost_note_str}{holder_note}"
                    ),
                    "image_url": market_data.get("image_url", ""),
                    "websites": market_data.get("websites", []),
                    "socials": market_data.get("socials", []),
                }
            )

            if analysis["verdict"] != "BUY":
                continue

            token_age = await paper_trader.engine.get_token_age(token_mint)
            creator_metrics = await paper_trader.engine.get_creator_twitter_metrics(token_mint)

            if creator_metrics and creator_metrics.get("username") and social_monitor:
                bird_creator = await social_monitor.get_creator_metrics(creator_metrics.get("username"))
                if bird_creator.get("followers", 0) > 0:
                    creator_metrics["followers"] = bird_creator["followers"]
                creator_metrics["scraped_at"] = bird_creator.get("scraped_at", creator_metrics.get("scraped_at"))

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
            amount_tokens = sol_to_spend / price_sol

            try:
                kol_data = None
                if event.get("kol_wallet"):
                    kol_data = {
                        "wallet": event.get("kol_wallet", ""),
                        "label": event.get("kol_label", "Unknown"),
                    }

                await paper_trader.open_position(
                    signature,
                    token_id,
                    price_sol,
                    amount_tokens,
                    fdv,
                    pair_addr,
                    kol_data=kol_data,
                )

                paper_trader.update_analytics_data(
                    signature,
                    token_age=token_age,
                    twitter_data=creator_metrics,
                    mentions=mentions,
                    holder_intel=holder_data,
                    candle_signals=candle_data,
                )

                if sniper and sniper.is_custodial_enabled():
                    buy_result = await sniper.execute_buy(token_id, amount_sol=sol_to_spend)
                    if buy_result.get("status") == "executed":
                        live_trade_tracker.record_buy(
                            token=token_id,
                            token_amount=float(buy_result.get("out_amount_ui", 0.0)),
                            sol_spent=float(buy_result.get("in_amount_lamports", 0)) / 1_000_000_000,
                            tx_sig=buy_result.get("tx_sig", ""),
                        )
                        await service.broadcast({
                            "type": "snipe",
                            "msg": (
                                f"LIVE BUY: {token_id[:8]}... "
                                f"{float(buy_result.get('in_amount_lamports', 0))/1_000_000_000:.4f} SOL "
                                f"(tx {str(buy_result.get('tx_sig', ''))[:10]}...)"
                            ),
                        })
                        await service.broadcast({"type": "live_pnl_update", "data": live_trade_tracker.get_stats()})
                    elif buy_result.get("status") == "failed":
                        await service.broadcast({
                            "type": "log",
                            "msg": f"LIVE BUY failed: {buy_result.get('reason', 'unknown')}",
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

        finally:
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
    """Returns closed trade history"""
    if paper_trader:
        return paper_trader.closed_positions
    return []


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


@app.post("/api/live-trading/record-buy")
async def live_record_buy(req: LiveBuyRecordRequest):
    live_trade_tracker.record_buy(
        token=req.token,
        token_amount=float(req.token_amount),
        sol_spent=float(req.sol_spent),
        tx_sig=req.tx_sig,
    )
    stats = live_trade_tracker.get_stats()
    await service.broadcast({"type": "live_pnl_update", "data": stats})
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
    stats = live_trade_tracker.get_stats()
    await service.broadcast({"type": "live_pnl_update", "data": stats})
    return {"status": "ok", "closed": closed, "stats": stats}


@app.get("/api/analytics/kol-summary")
async def get_kol_summary():
    return trade_analytics_store.get_kol_performance_summary()


@app.get("/api/analytics/overview")
async def get_analytics_overview():
    overview = trade_analytics_store.get_overview()
    if paper_trader:
        stats = paper_trader.get_stats()
        overview["live"] = {
            "active_positions": stats.get("active_positions", 0),
            "closed_trades_runtime": stats.get("closed_trades", 0),
            "total_pnl_runtime_sol": stats.get("total_pnl", 0.0),
            "post_close_monitoring_pending_runtime": stats.get("post_close_monitoring", 0),
        }
    return overview


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

