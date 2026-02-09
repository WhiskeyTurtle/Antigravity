
import asyncio
import sys
import os

# Add current directory to path so imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import json
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

# Global State
class FeedService:
    def __init__(self):
        self.active_connections = []
        self.queue = asyncio.Queue()
        self.running = False
        self.rpc_url = "https://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff"
        self.settings = self.load_settings()
        
    def load_settings(self):
        """Load settings from disk or use defaults"""
        settings_file = "experiments/alpha_screener/settings.json"
        try:
            if os.path.exists(settings_file):
                with open(settings_file, 'r') as f:
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
        settings_file = "experiments/alpha_screener/settings.json"
        try:
            with open(settings_file, 'w') as f:
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
async_client: AsyncClient = None # Global async client

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Starting Alpha Feed Service...")
    
    global risk_engine, analyzer, sniper, paper_trader, kol_monitor, async_client
    
    # Initialize Clients (Helius RPC)
    async_client = AsyncClient(service.rpc_url)
    
    # Start the Queue Processing Task
    asyncio.create_task(process_feed())
    asyncio.create_task(monitor_positions_task())
    
    # Initialize implementation modules
    risk_engine = RiskEngine(async_client)
    analyzer = Analyzer()
    sniper = Sniper(async_client)
    paper_trader = PaperTrader()
    
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
app.mount("/static", StaticFiles(directory="experiments/alpha_screener/static"), name="static")

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
                pnl_sign = "+" if trade['pnl'] >= 0 else ""
                await service.broadcast({
                    "type": "snipe",
                    "msg": f"💰 PAPER SELL ({trade['reason']}): {trade['token'][:8]}... PNL: {pnl_sign}{trade['pnl']:.4f} SOL (MCap: ${trade['sell_fdv']:,.0f})"
                })

            # 3. Broadcast Updated Positions (Real-time updates)
            if active_count > 0:
                positions = paper_trader.get_positions()
                await service.broadcast({
                    "type": "positions_update",
                    "data": positions
                })

            # 4. Adaptive Rate Limiting
            active_count = len(paper_trader.positions)
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

async def process_feed():
    """Background loop to process events and update clients"""
    last_heartbeat = 0
    import time
    
    # Import globals
    global risk_engine, analyzer, sniper, paper_trader, async_client
    
    print("🚀 Starting Alpha Feed Service...")
    
    while service.running:

        current_time = time.time()
        
        # Heartbeat every 5s
        if current_time - last_heartbeat > 5:
            await service.broadcast({"type": "log", "msg": "❤️ System Pulse: Scanning Mempool..."})
            
            # Broadcast PNL Stats
            if paper_trader:
                stats = paper_trader.get_stats()
                await service.broadcast({
                    "type": "pnl_update",
                    "data": stats
                })
                
                # Broadcast Active Positions
                positions = paper_trader.get_positions()
                if positions:
                    print(f"📡 Broadcasting {len(positions)} active positions")
                await service.broadcast({
                    "type": "positions_update",
                    "data": positions
                })
            
            last_heartbeat = current_time

        try:
            # Wait for event with timeout so we can heartbeat
            event = await asyncio.wait_for(service.queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
            
        if event["type"] == "NEW_POOL":
            signature = event["signature"]
            source = event.get("source", "UNKNOWN")
            
            # RESOLVE MINT FROM SIGNATURE
            token_mint = None
            sol_amount = 0  # Track SOL amount for KOL filtering
            try:
                # 5 Retries for RPC (Public RPCs are flaky)
                for _ in range(5):
                    try:
                        tx = await async_client.get_transaction(
                            solders.signature.Signature.from_string(signature),
                            max_supported_transaction_version=0,
                            commitment=Confirmed
                        )
                        if tx.value:
                            meta = tx.value.transaction.meta
                            if meta.err:
                                print(f"⚠️ Transaction failed on-chain: {signature}")
                                token_mint = "FAILED_TX" 
                                break

                            # Extract SOL amount from pre/post balances
                            if meta.pre_balances and meta.post_balances:
                                # Calculate SOL change (absolute value)
                                for i, (pre, post) in enumerate(zip(meta.pre_balances, meta.post_balances)):
                                    sol_change = abs(pre - post) / 1e9  # Convert lamports to SOL
                                    if sol_change > sol_amount:
                                        sol_amount = sol_change

                            # Look for the first mint that is NOT SOL (So111...)
                            balances = meta.post_token_balances
                            if not balances:
                                # Fallback to pre_token_balances
                                balances = meta.pre_token_balances
                            
                            if not balances:
                                print(f"ℹ️ No token balances (Likely SOL transfer): {signature}")
                                token_mint = "SOL_TRANSFER"
                                break

                            for balance in balances:
                                mint_addr = str(balance.mint)
                                if mint_addr != "So11111111111111111111111111111111111111112":
                                    token_mint = mint_addr
                                    print(f"✅ Resolved Mint: {token_mint}")
                                    break
                        else:
                            print(f"⚠️ get_transaction returned None value for {signature}")

                        if token_mint:
                            break
                    except Exception as exc:
                        if "429" in str(exc):
                            print(f"⚠️ RPC 429 (Rate Limit) fetching {signature}")
                            await asyncio.sleep(2) # Backoff
                        else:
                            print(f"⚠️ RPC Error resolving mint: {exc}")
                        await asyncio.sleep(1)
            except Exception as e:
                print(f"⚠️ Failed to resolve mint for {signature}: {e}")

            # Filter KOL trades under 0.23 SOL (likely top-buying manipulation)
            if event.get("kol_wallet") and sol_amount < 0.23:
                print(f"⏩ Skipping KOL trade from {event.get('kol_label', 'Unknown')}: {sol_amount:.4f} SOL (below 0.23 SOL threshold)")
                continue

            if not token_mint:
                token_mint = signature
                print(f"⚠️ Could not resolve mint. Using Sig: {token_mint}")
            else:
                print(f"🔍 Resolved Mint: {token_mint} (from {signature[:8]})")

            # 1. Update Dashboard
            await service.broadcast({
                "type": "new_token",
                "signature": signature,
                "token": str(token_mint),
                "source": source,
                "time": time.strftime("%H:%M:%S"),
                "status": "ANALYZING..." 
            })

            # Check if we already have a position for this token (Optimization)
            if paper_trader:
                for pos in paper_trader.positions.values():
                    if pos["token_mint"] == str(token_mint):
                        print(f"⏩ Skipping analysis for {token_mint[:8]} (Already in positions)")
                        await service.broadcast({
                            "type": "update",
                            "signature": signature,
                            "token": str(token_mint),
                            "status": "ALREADY_ACTIVE",
                            "score": 0,
                            "risk_msg": "Token already in active positions"
                        })
                        continue

            # 2. Risk Check (SKIP if we only have the signature)
            if len(str(token_mint)) == 44:
                is_safe, risk_msg = await risk_engine.check_token_safety(token_mint)
            else:
                is_safe, risk_msg = False, "Incomplete data (Resolution failed)"
            
            if is_safe:
                print(f"✅ Risk Check Passed: {token_mint[:8]}")
                # 3. Analyze (Fetch Market Data First)
                market_data = await paper_trader.engine.get_token_price(token_mint)
                
                if not market_data:
                    print(f"⚠️ Failed to fetch market data for {token_mint}. Skipping analysis.")
                    continue
                
                print(f"📉 Market Data Fetched: {token_mint[:8]} | Liq: {market_data.get('liquidity_usd', 'N/A')}")
                market_data["token"] = token_mint
                
                analysis = analyzer.score_token(market_data)
                
                # Format metrics for UI
                metrics = analysis.get("metrics", {})
                vol_str = f"${metrics.get('vol_m5', 0):,.0f}"
                liq_str = f"${metrics.get('liq', 0):,.0f}"
                
                await service.broadcast({
                    "type": "update",
                    "signature": signature,
                    "token": str(token_mint),
                    "status": "BUY_SIGNAL" if analysis["verdict"] == "BUY" else "WATCH",
                    "score": analysis["score"],
                    "risk_msg": f"{analysis.get('risk_msg', '')} | Vol: {vol_str} | Liq: {liq_str}",
                    "image_url": market_data.get("image_url", ""),
                    "websites": market_data.get("websites", []),
                    "socials": market_data.get("socials", [])
                })
                
                if analysis["verdict"] == "BUY":
                    # Auto-Snipe Signal
                    token_id = str(token_mint)
                    price_sol = market_data.get("price_sol", 0) or 0.0001
                    fdv = market_data.get("fdv", 0)
                    pair_addr = market_data.get("pair_address", "")
                    
                    # Calculate buy amount based on settings
                    import random
                    sol_min = service.settings.get("sol_amount_min", 0.1)
                    sol_max = service.settings.get("sol_amount_max", 0.5)
                    sol_to_spend = random.uniform(sol_min, sol_max)
                    amount_tokens = sol_to_spend / price_sol  # Calculate token amount
                    
                    try:
                        # 1. Execute Snipe (Simulated/Simultaneous)
                        try:
                            await sniper.execute_swap(token_id) 
                        except Exception as e:
                            print(f"⚠️ Sniper Net Error (Ignored for Paper Trade): {e}")

                        # 2. Open Paper Position
                        await paper_trader.open_position(signature, token_id, price_sol, amount_tokens, fdv, pair_addr)
                        
                        await service.broadcast({
                            "type": "snipe",
                            "msg": f"🔫 PAPER BUY: {token_id[:8]}... @ {price_sol:.6f} SOL ({sol_to_spend:.4f} SOL / MCap: ${fdv:,.0f})"
                        })
                    except Exception as e:
                        print(f"⚠️ Trade Execution Failed: {e}")
            else:
                # Don't broadcast "Mint Account Not Found" - it's too noisy and usually just means
                # the token is very new or the transaction doesn't involve a token mint
                if "Mint Account Not Found" not in risk_msg:
                    await service.broadcast({
                        "type": "update",
                        "signature": signature,
                        "status": "HIGH_RISK",
                        "risk_msg": f"⚠️ {risk_msg}"
                    })

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

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await service.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Handle client commands if any
    except WebSocketDisconnect:
        service.disconnect(websocket)
