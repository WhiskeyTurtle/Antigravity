
import asyncio
import aiohttp
import base64
import json
import os
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey

# Jupiter V6 API
JUPITER_QUOTE_API = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_API = "https://quote-api.jup.ag/v6/swap"

# Mock Wallet if none provided
MOCK_PRIVATE_KEY = os.getenv("SOLANA_PRIVATE_KEY") 

class Sniper:
    def __init__(self, rpc_client: AsyncClient):
        self.client = rpc_client
        self.simulation_mode = True # Default to Safety
        if MOCK_PRIVATE_KEY:
            self.wallet = Keypair.from_base58_string(MOCK_PRIVATE_KEY)
            # Default to REAL trading if key is present (but user must explicitly disable sim mode in code)
            # For this Phase, we Force Sim Mode to avoid accidents.
            print("⚠️ Wallet Loaded. SIMULATION MODE is ACTIVE.")
        else:
            self.wallet = None
            print("⚠️ No Wallet. SIMULATION MODE only.")

    async def get_quote(self, input_mint: str, output_mint: str, amount_lamports: int):
        """Fetch a swap quote from Jupiter"""
        input_mint = input_mint.strip()
        output_mint = output_mint.strip()
        
        url = (
            f"{JUPITER_QUOTE_API}?inputMint={input_mint}"
            f"&outputMint={output_mint}"
            f"&amount={amount_lamports}"
            f"&slippageBps=100"
        )
        
        import socket
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False) # Force IPv4, Ignore SSL certs for debug
        
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        text = await response.text()
                        print(f"⚠️ Jupiter API Error: {response.status} - {text}")
                        return None
        except Exception as e:
            print(f"⚠️ Sniper Net Error: {e}")
            return None

    async def execute_swap(self, token_mint: str, amount_sol: float = 0.01):
        """
        Executes a Buy Order (SOL -> Token).
        If Simulation Mode: Logs the quote price.
        """
        sol_mint = "So11111111111111111111111111111111111111112"
        lamports = int(amount_sol * 1_000_000_000)
        
        print(f"🔫 SNIPER: Targeting {token_mint} with {amount_sol} SOL...")
        
        # 1. Get Quote
        quote = await self.get_quote(sol_mint, token_mint, lamports)
        if not quote:
            print("❌ SNIPER: No Quote Found (Liquidity too low?)")
            return {"status": "failed", "reason": "no_quote"}
            
        out_amount = int(quote['outAmount']) / (10 ** 6) # Approx for logging (assuming 6 decimals)
        
        if self.simulation_mode:
            print(f"✅ SNIPER (SIM): Would receive ~{out_amount:.2f} tokens.")
            print(f"   Quote ID: {quote.get('id', 'N/A')}")
            return {
                "status": "simulated", 
                "out_amount": out_amount, 
                "quote": quote
            }
            
        # 2. (Real Trading would go here: Call /swap, sign transaction, send)
        # Not implemented for Phase 5 to prevent real money loss.
        return {"status": "skipped", "reason": "real_trading_disabled"}
