
import asyncio
from solana.rpc.async_api import AsyncClient
from risk_engine import RiskEngine

async def test_risk():
    rpc_url = "https://mainnet.helius-rpc.com/?api-key=a16f485e-cca9-47be-a815-f8936034edff"
    client = AsyncClient(rpc_url)
    risk_engine = RiskEngine(client)
    
    # Example Pump.fun mint address
    test_mint = "DzcnScz4mKDYSycR7EUM4U6baqhgJDtoqf9uJFmMpump"
    
    print(f"🧪 Testing Risk for {test_mint}...")
    is_safe, msg = await risk_engine.check_token_safety(test_mint)
    print(f"✅ Result: Safe={is_safe}, Msg={msg}")
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(test_risk())
