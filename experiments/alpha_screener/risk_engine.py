
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
from spl.token.constants import TOKEN_PROGRAM_ID
import traceback

class RiskEngine:
    def __init__(self, rpc_client: AsyncClient):
        self.client = rpc_client

    async def check_token_safety(self, token_mint_str: str):
        """
        Checks if a token has Mint Authority or Freeze Authority enabled.
        Returns: IsSafe (bool), Reason (str)
        """
        try:
            mint_pubkey = Pubkey.from_string(token_mint_str)
           
            # 1. Get Mint Account Info
            resp = await self.client.get_account_info(mint_pubkey)
            if not resp.value:
                return False, "Mint Account Not Found"
           
            # Extract data - convert to bytes if needed
            data = resp.value.data
            if not isinstance(data, bytes):
                data = bytes(data)
            
            # Debug log data length and hex dump
            print(f"🔬 Data length: {len(data)} bytes for {token_mint_str[:12]}")
            if len(data) >= 82:
                print(f"🔬 Hex (0-82): {data[:82].hex()}")
            
            # Check Mint Authority (Offset 0, 4 bytes)
            mint_auth_discriminant = int.from_bytes(data[0:4], "little")
            
            # Check Freeze Authority (Offset 46, 4 bytes)
            freeze_auth_discriminant = int.from_bytes(data[46:50], "little")
            
            # Debug: print actual values
            print(f"🔬 {token_mint_str[:12]}: mint_auth={mint_auth_discriminant}, freeze_auth={freeze_auth_discriminant}")
            
            risks = []
            if mint_auth_discriminant == 1:
                risks.append("Mint Auth Enabled")
            
            if freeze_auth_discriminant == 1:
                risks.append("Freeze Auth Enabled")
                
            is_safe = len(risks) == 0
            
            return is_safe, ", ".join(risks) if risks else "Safe"
            
        except Exception as e:
            error_msg = f"{str(e)}"
            print(f"❌ Risk check error for {token_mint_str[:12]}: {error_msg}")
            print(traceback.format_exc())
            return False, f"Error: {error_msg}"
