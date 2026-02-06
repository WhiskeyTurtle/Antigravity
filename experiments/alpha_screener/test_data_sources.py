
import aiohttp
import asyncio
import socket

async def test_url(name, url):
    print(f"Testing {name} ({url})...")
    try:
        connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(url) as response:
                print(f"[{response.status}] {name}")
                if response.status == 200:
                    try:
                        data = await response.json()
                        print(f"   Success! Data keys: {list(data.keys())[:3]}")
                        return True
                    except:
                        print("   Text response (not json)")
                else:
                    print(f"   Failed: {await response.text()}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    return False

async def main():
    # 1. Jupiter (Known Bad, checking just in case)
    await test_url("Jupiter Quote", "https://quote-api.jup.ag/v6/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&amount=1000000")
    
    # 2. DexScreener (Alternative for Price)
    # Check BONK
    await test_url("DexScreener", "https://api.dexscreener.com/latest/dex/tokens/DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263")
    
    # 3. Raydium API
    await test_url("Raydium API", "https://api.raydium.io/v2/main/pairs")

if __name__ == "__main__":
    asyncio.run(main())
