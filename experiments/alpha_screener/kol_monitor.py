
import asyncio
import logging
from typing import Dict, Callable
from solana.rpc.websocket_api import connect
from solana.rpc.commitment import Confirmed
from solders.rpc.config import RpcTransactionLogsFilterMentions
from solders.pubkey import Pubkey

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KOLMonitor")


import asyncio
import logging
from typing import Dict, Callable
from solana.rpc.websocket_api import connect
from solana.rpc.commitment import Confirmed
from solders.rpc.config import RpcTransactionLogsFilterMentions
from solders.pubkey import Pubkey

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KOLMonitor")

class KOLMonitor:
    def __init__(self, rpc_url: str, ws_url: str, kols: Dict[str, str], callback: Callable):
        """
        :param rpc_url: HTTP RPC URL
        :param ws_url: WebSocket RPC URL
        :param kols: Dictionary of {WalletAddress: Name/Label}
        :param callback: Async function to call with (signature, wallet_name)
        """
        self.rpc_url = rpc_url
        self.ws_url = ws_url
        self.kols = kols # Persistent store of wallets
        self.callback = callback
        self.running = False
        self.cmd_queue = asyncio.Queue() # Queue for new subscription requests
        
    async def add_wallet(self, address: str, name: str):
        """Adds a wallet to tracking dynamically"""
        if address in self.kols:
            logger.info(f"Wallet {name} ({address}) already tracked.")
            return
            
        self.kols[address] = name
        await self.cmd_queue.put((address, name))
        logger.info(f"Queueing subscription for {name} ({address})")

    async def _handle_new_subscriptions(self, websocket, sub_map: Dict[int, str]):
        """Internal task to process new subscriptions from the queue"""
        while self.running:
            try:
                # Wait for new wallet to add
                address, name = await self.cmd_queue.get()
                logger.info(f"➕ Adding KOL: {name} ({address})")
                
                try:
                    self.pending_subscriptions.append((address, name))
                    pubkey = Pubkey.from_string(address)
                    await websocket.logs_subscribe(
                        filter_=RpcTransactionLogsFilterMentions(pubkey),
                        commitment=Confirmed
                    )
                    # Response handled in main loop
                except Exception as e:
                    logger.error(f"Failed to dynamic subscribe {name}: {e}")
                
                self.cmd_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in subscription handler: {e}")

    async def start(self):
        self.running = True
        logger.info(f"🕵️ KOL Monitor started. Tracking {len(self.kols)} initial wallets...")
        
        # Queue for mapping asynchronous subscription responses
        self.pending_subscriptions = [] 
        
        while self.running:
            try:
                # Subscription ID -> Wallet mapping
                sub_map = {}
                
                async with connect(self.ws_url) as websocket:
                    
                    # 1. Initial Subscriptions
                    for wallet_addr, name in self.kols.items():
                        try:
                            # We record the pending subscription first
                            self.pending_subscriptions.append((wallet_addr, name))
                            pubkey = Pubkey.from_string(wallet_addr)
                            
                            # Fire and forget (mostly), since response often leaks to main loop
                            await websocket.logs_subscribe(
                                filter_=RpcTransactionLogsFilterMentions(pubkey),
                                commitment=Confirmed
                            )
                            # We don't rely on the return value here anymore as it often returns None
                            # The response will be caught in the main loop
                            
                        except Exception as e:
                            logger.error(f"Failed to trigger subscribe for {name}: {e}")
                            # Remove from pending if send failed
                            if self.pending_subscriptions:
                                self.pending_subscriptions.pop()
                        
                        # Guard rate limit
                        await asyncio.sleep(0.1)

                    # 2. Start Dynamic Subscription Handler
                    sub_task = asyncio.create_task(self._handle_new_subscriptions(websocket, sub_map))

                    try:
                        # 3. Process messages
                        async for batched_msgs in websocket:
                            if not self.running:
                                break
                          # Websocket returns a list of messages
                            for msg in batched_msgs:
                                try:
                                    # Case A: Log Notification (Has subscription ID)
                                    if hasattr(msg, 'subscription'):
                                        sub_id = msg.subscription
                                        
                                        # Default to Unknown if not found
                                        wallet_info = sub_map.get(sub_id, ("Unknown", "Unknown KOL"))
                                        wallet_addr, wallet_name = wallet_info if isinstance(wallet_info, tuple) else ("Unknown", wallet_info)
                                        
                                        if hasattr(msg, 'result') and not msg.result.value.err:
                                            signature = str(msg.result.value.signature)
                                            # Pass address and name
                                            await self.callback(signature, wallet_addr, wallet_name, "KOL")
                                    
                                    # Case B: Subscription Confirmation (Leaked response)
                                    elif hasattr(msg, 'result') and not hasattr(msg, 'subscription'):
                                        # This is likely a SubscriptionResult
                                        actual_id = msg.result
                                        
                                        if self.pending_subscriptions:
                                            p_addr, p_name = self.pending_subscriptions.pop(0)
                                            sub_map[actual_id] = (p_addr, p_name)
                                            logger.info(f"✅ Confirmed subscription for {p_name} (ID: {actual_id})")
                                        else:
                                            logger.warning(f"Received unprompted subscription ID: {actual_id}")

                                except Exception as e:
                                    logger.error(f"Error parsing KOL log: {e}")
                    finally:
                        sub_task.cancel()
                        try:
                            await sub_task
                        except asyncio.CancelledError:
                            pass
                            
            except Exception as e:
                logger.error(f"KOL Monitor Connection Error: {e}")
                await asyncio.sleep(5)
                
    def stop(self):
        self.running = False

