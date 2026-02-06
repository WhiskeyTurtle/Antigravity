
import asyncio
import re
import logging
# pip install telethon
from telethon import TelegramClient, events

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SocialMonitor")

# ---------------------------------------------------------
# 1. SETUP CREDENTIALS
# Go to https://my.telegram.org/apps to get API_ID and API_HASH
# ---------------------------------------------------------
API_ID = 123456  # REPLACE ME
API_HASH = "your_api_hash_here" # REPLACE ME
PHONE_NUMBER = "+1234567890" # REPLACE ME

# Channels to Monitor (Username or ID)
CHANNELS = [
    "solana_calls", 
    "bonkbot_alerts",
    # Add your favorite channels here
]

# Solana Address Regex (Base58, 32-44 chars)
SOL_ADDRESS_REGEX = r'[1-9A-HJ-NP-Za-km-z]{32,44}'

class SocialMonitor:
    def __init__(self, callback):
        self.callback = callback
        self.client = TelegramClient('anon', API_ID, API_HASH)

    async def start(self):
        logger.info("📱 Social Monitor starting...")
        
        @self.client.on(events.NewMessage(chats=CHANNELS))
        async def handler(event):
            try:
                text = event.message.message
                # Find all potential addresses
                matches = re.findall(SOL_ADDRESS_REGEX, text)
                
                for addr in matches:
                    # Basic validation (could use solders.pubkey to verify)
                    if len(addr) >= 32:
                        logger.info(f"📢 Signal found in {event.chat_id}: {addr}")
                        await self.callback(addr, f"Telegram: {event.chat_id}", "SOCIAL")
                        
            except Exception as e:
                logger.error(f"Error processing social message: {e}")

        logger.info("📱 Connecting to Telegram...")
        await self.client.start(phone=PHONE_NUMBER)
        logger.info("✅ Social Monitor Connected!")
        await self.client.run_until_disconnected()

# Standalone test
if __name__ == "__main__":
    async def dump_callback(addr, source, type):
        print(f"Callback: {addr} from {source}")
        
    mon = SocialMonitor(dump_callback)
    asyncio.run(mon.start())
