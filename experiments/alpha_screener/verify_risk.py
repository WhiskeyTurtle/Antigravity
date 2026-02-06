
import asyncio
from solana.rpc.async_api import AsyncClient
from risk_engine import RiskEngine

async def test_risk_check():
    client = AsyncClient("https://api.mainnet-beta.solana.com")
    risk_engine = RiskEngine(client)
    
    # 1. Check Safe Token (BONK)
    print("Testing Low Risk Token (BONK)...")
    is_safe, reason = await risk_engine.check_token_safety("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263")
    print(f"Result: {is_safe} ({reason})")
    
    # 2. Check High Risk Token (Mock - Random Pubkey usually has no mint info, so returns error/unsafe)
    # Using a random address that isn't a mint should fail 'Mint Account Not Found'
    print("\nTesting Invalid/Risk Token (Random)...")
    is_safe, reason = await risk_engine.check_token_safety("11111111111111111111111111111111")
    print(f"Result: {is_safe} ({reason})")
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(test_risk_check())
