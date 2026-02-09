
import pytest

pytestmark = pytest.mark.skip(reason="Manual integration script; not an automated pytest test.")

import asyncio
from solana.rpc.websocket_api import connect
from solana.rpc.commitment import Confirmed
from solders.rpc.config import RpcTransactionLogsFilterMentions
from solders.pubkey import Pubkey

async def test_monitor():
    ws_url = "wss://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff"
    
    # Raydium AMM Program ID (Guaranteed traffic)
    target_wallet_str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
    target_wallet = Pubkey.from_string(target_wallet_str)
    
    print(f"listening for {target_wallet_str}...")
    
    async with connect(ws_url) as websocket:
        await websocket.logs_subscribe(
            filter_=RpcTransactionLogsFilterMentions(target_wallet),
            commitment=Confirmed
        )
        
        count = 0
        async for msg in websocket:
            print(f"RAW MSG: {msg}")
            count += 1
            if count >= 3:
                break

if __name__ == "__main__":
    asyncio.run(test_monitor())
