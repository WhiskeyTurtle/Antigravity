
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
    
    # Initialize implementation modules
    risk_engine = RiskEngine(async_client)
    analyzer = Analyzer()
    sniper = Sniper(async_client)
    paper_trader = PaperTrader()
    
    # MONITOR CALLBACKS
    async def on_kol_activity(signature, wallet_address, wallet_name, source_type):
        import time # Import time locally if not already global
        await service.queue.put({
            "type": "NEW_POOL", # Ensure type is set for process_feed
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
            "data": event # Pass the whole dict
        })
        
    # Start KOL Monitor
    kol_monitor = KOLMonitor(service.rpc_url, "wss://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff", KOL_WALLETS, on_kol_activity)
    service.running = True
    asyncio.create_task(kol_monitor.start())

    # Start Social Monitor
    social_monitor = SocialMonitor(on_social_signal)
    asyncio.create_task(social_monitor.start())
    
    # NOTE: Disabled generic "All New Tokens" listener to focus on KOLs as requested
    # listener = Listener(service.queue)
    # asyncio.create_task(listener.start())
    
    yield
    
    # Shutdown
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

# ... imports ...

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

async def process_feed():
    """Background loop to process events and update clients"""
    last_heartbeat = 0
    last_pnl_check = 0
    import time
    
    # Import globals
    global risk_engine, analyzer, sniper, paper_trader, async_client
    
    print("🚀 Starting Alpha Feed Service...")
    
    # Initialize Clients (Helius RPC)
    
    while service.running:

        current_time = time.time()
        
        # Heartbeat every 5s
        if current_time - last_heartbeat > 5:
            await service.broadcast({"type": "log", "msg": "❤️ System Pulse: Scanning Mempool..."})
            
            # Broadcast PNL Stats
            stats = paper_trader.get_stats()
            await service.broadcast({
                "type": "pnl_update",
                "data": stats
            })
            
            # Broadcast Active Positions
            positions = paper_trader.get_positions()
            await service.broadcast({
                "type": "positions_update",
                "data": positions
            })
            
            last_heartbeat = current_time

        # Check Paper Positions every 10s
        if current_time - last_pnl_check > 10:
            closed_trades = await paper_trader.update_positions()
            for trade in closed_trades:
                pnl_sign = "+" if trade['pnl'] >= 0 else ""
                await service.broadcast({
                    "type": "snipe",
                    "msg": f"💰 PAPER SELL ({trade['reason']}): {trade['token'][:8]}... PNL: {pnl_sign}{trade['pnl']:.4f} SOL (MCap: ${trade['sell_fdv']:,.0f})"
                })
            last_pnl_check = current_time

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
            
            # 2. Risk Check (SKIP if we only have the signature)
            if len(str(token_mint)) == 44:
                is_safe, risk_msg = await risk_engine.check_token_safety(token_mint)
            else:
                is_safe, risk_msg = False, "Incomplete data (Resolution failed)"
            
            if is_safe:
                # 3. Analyze (Fetch Market Data First)
                market_data = await paper_trader.engine.get_token_price(token_mint)
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
                    "risk_msg": f"{analysis.get('risk_msg', '')} | Vol: {vol_str} | Liq: {liq_str}"
                })
                
                if analysis["verdict"] == "BUY":
                    # Auto-Snipe Signal
                    token_id = str(token_mint)
                    price_sol = market_data.get("price_sol", 0) or 0.0001
                    fdv = market_data.get("fdv", 0)
                    pair_addr = market_data.get("pair_address", "")
                    
                    try:
                        # 1. Execute Snipe (Simulated/Simultaneous)
                        try:
                            await sniper.execute_swap(token_id) 
                        except Exception as e:
                            print(f"⚠️ Sniper Net Error (Ignored for Paper Trade): {e}")

                        # 2. Open Paper Position
                        await paper_trader.open_position(signature, token_id, price_sol, 1000, fdv, pair_addr)
                        
                        await service.broadcast({
                            "type": "snipe",
                            "msg": f"🔫 PAPER BUY: {token_id[:8]}... @ {price_sol:.6f} SOL (MCap: ${fdv:,.0f})"
                        })
                    except Exception as e:
                        print(f"⚠️ Trade Execution Failed: {e}")
            else:
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

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await service.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Handle client commands if any
    except WebSocketDisconnect:
        service.disconnect(websocket)
