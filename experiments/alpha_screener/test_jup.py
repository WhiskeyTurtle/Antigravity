
import pytest

pytestmark = pytest.mark.skip(reason="Manual integration script; not an automated pytest test.")

import asyncio
import aiohttp

async def test_conn():
    url = "https://quote-api.jup.ag/v6/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263&amount=10000000"
    print(f"Testing connection to: {url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                print(f"Status: {response.status}")
                print(await response.text())
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_conn())
