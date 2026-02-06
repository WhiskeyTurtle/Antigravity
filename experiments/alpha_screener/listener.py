
import asyncio
import json
import websockets
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

# Raydium Liquidity Pool V4 Program ID
RAYDIUM_V4_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
# Pump.fun Program ID
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

WSS_URL = "wss://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff"

class Listener:
    def __init__(self, queue):
        self.queue = queue
        self.running = False

    async def start(self):
        self.running = True
        print(f"👂 Connecting to Solana WebSocket: {WSS_URL}")
        
        while self.running:
            try:
                async with websockets.connect(WSS_URL) as websocket:
                    print("   ✅ WS Connected!")
                    # Subscribe to logs for BOTH Raydium and Pump.fun
                    program_ids = [RAYDIUM_V4_PROGRAM_ID, PUMP_FUN_PROGRAM_ID]
                    
                    for pid in program_ids:
                        print(f"   -> Subscribing to: {pid[:8]}...")
                        payload = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "logsSubscribe",
                            "params": [
                                {"mentions": [pid]},
                                {"commitment": "confirmed"}
                            ]
                        }
                        await websocket.send(json.dumps(payload))
                        response = await websocket.recv()
                        print(f"   ✅ Subscribed to {pid[:8]}: {str(response)[:50]}")

                    while self.running:
                        try:
                            message = await websocket.recv()
                            data = json.loads(message)
                            
                            if "method" in data and data["method"] == "logsNotification":
                                logs = data["params"]["result"]["value"]["logs"]
                                signature = data["params"]["result"]["value"]["signature"]
                                
                                # Check Raydium
                                is_raydium = any("initialize2" in log for log in logs)
                                
                                # Check Pump.fun
                                is_pump = any(
                                    PUMP_FUN_PROGRAM_ID in log or "Program log: Instruction: Create" in log 
                                    for log in logs
                                )
                                
                                source = "UNKNOWN"
                                if is_raydium:
                                    source = "RAYDIUM"
                                elif is_pump:
                                    source = "PUMP.FUN"
                                
                                if is_raydium or is_pump:
                                    await self.queue.put({
                                        "type": "NEW_POOL",
                                        "source": source,
                                        "signature": signature,
                                        "logs": logs
                                    })
                                    
                        except websockets.exceptions.ConnectionClosed:
                            print("❌ WebSocket Closed. Reconnecting...")
                            break
                        except Exception as e:
                            print(f"⚠️ Error in listener: {e}")
                            break
            
            except Exception as e:
                print(f"⚠️ WebSocket Connection Error: {e}. Retrying in 10s...")
                await asyncio.sleep(10)

    def stop(self):
        self.running = False
