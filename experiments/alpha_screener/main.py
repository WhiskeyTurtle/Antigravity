
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Starting Alpha Feed Service...")
    client = AsyncClient(service.rpc_url)
    listener = Listener(service.queue)
    risk_engine = RiskEngine(client)
    sniper = Sniper(client)
    analyzer = Analyzer()
    paper_trader = PaperTrader()
    
    service.running = True
    asyncio.create_task(listener.start())
    asyncio.create_task(process_feed(listener, risk_engine, sniper, analyzer, paper_trader))
    
    yield
    
    # Shutdown
    print("🛑 Shutting down...")
    service.running = False
    listener.stop()
    await client.close()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="experiments/alpha_screener/static"), name="static")

async def process_feed(listener, risk_engine, sniper, analyzer, paper_trader):
    """Background loop to process events and update clients"""
    last_heartbeat = 0
    last_pnl_check = 0
    import time
    
    print("🚀 Starting Alpha Feed Service...")
    
    # Initialize Clients (Helius RPC)
    async_client = AsyncClient("https://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff")
    
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
            last_heartbeat = current_time

        # Check Paper Positions every 10s
        if current_time - last_pnl_check > 10:
            if await paper_trader.update_positions():
                await service.broadcast({"type": "log", "msg": "💰 PNL Updated (Trade Closed)"})
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
                            # Look for the first mint that is NOT SOL (So111...)
                            balances = tx.value.transaction.meta.post_token_balances
                            for balance in balances:
                                mint_addr = str(balance.mint)
                                if mint_addr != "So11111111111111111111111111111111111111112":
                                    token_mint = mint_addr
                                    break
                        if token_mint:
                            break
                    except Exception as exc:
                        if "429" in str(exc):
                            await asyncio.sleep(2) # Backoff
                        else:
                            print(f"RPC Error resolving mint: {exc}")
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
                # 3. Analyze
                analysis = analyzer.score_token({"token": str(token_mint)})
                
                await service.broadcast({
                    "type": "update",
                    "signature": signature,
                    "token": str(token_mint),
                    "status": "BUY_SIGNAL" if analysis["verdict"] == "BUY" else "WATCH",
                    "score": analysis["score"],
                    "risk_msg": "✅ Safe to trade"
                })
                
                if analysis["verdict"] == "BUY":
                    # Auto-Snipe Signal
                    try:
                        token_id = str(token_mint)
                        
                        # Check Price (Real)
                        quote = await paper_trader.engine.get_token_price(token_id)
                        entry_price = quote["price_sol"]
                        print(f"💰 Price Check for {token_id}: {entry_price} SOL")
                        
                        # Execute Snipe (Simulated)
                        try:
                            snipe_res = await sniper.execute_swap(token_id) 
                        except Exception as e:
                            print(f"⚠️ Sniper Net Error (Ignored for Paper Trade): {e}")
                            snipe_res = {"status": "failed_network"}

                        if entry_price > 0:
                            # Calculate estimated tokens assuming 0.01 SOL trade
                            estimated_tokens = 0.01 / entry_price
                            await paper_trader.open_position(token_id, token_id, entry_price, estimated_tokens)

                            await service.broadcast({
                                "type": "snipe",
                                "msg": f"🔫 PAPER BUY: {token_id[:8]}... @ {entry_price:.6f} SOL"
                            })
                        else:
                             print("⚠️ Skipping Paper Trade: No Price Data")
                             
                    except Exception as e:
                        print(f"⚠️ Sniper Failed: {e}")
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
