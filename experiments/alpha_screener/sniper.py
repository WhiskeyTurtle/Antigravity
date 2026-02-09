import base64
import binascii
import json
import os
from typing import Optional

import aiohttp
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts

SOL_MINT = "So11111111111111111111111111111111111111112"
JUP_QUOTE_URLS = [
    "https://lite-api.jup.ag/swap/v1/quote",
    "https://quote-api.jup.ag/v6/quote",
]
JUP_SWAP_URLS = [
    "https://lite-api.jup.ag/swap/v1/swap",
    "https://quote-api.jup.ag/v6/swap",
]


class Sniper:
    def __init__(self, rpc_client: AsyncClient, rpc_url: str):
        self.client = rpc_client
        self.rpc_url = rpc_url
        self.wallet: Optional[Keypair] = None

        priv = os.getenv("CUSTODIAL_PRIVATE_KEY") or os.getenv("SOLANA_PRIVATE_KEY")
        enabled = os.getenv("ENABLE_CUSTODIAL_TRADING", "false").lower() in ("1", "true", "yes", "on")

        if priv:
            try:
                self.wallet = Keypair.from_base58_string(priv)
            except Exception:
                self.wallet = None

        self.simulation_mode = not (enabled and self.wallet)
        if self.simulation_mode:
            print("Sniper in simulation mode (set ENABLE_CUSTODIAL_TRADING=true and valid CUSTODIAL_PRIVATE_KEY to enable real trading).")
        else:
            print(f"Custodial trading enabled with wallet {self.public_key()}")

    def _parse_private_key(self, raw: str) -> Keypair:
        s = (raw or "").strip().strip("'").strip('"')
        if not s:
            raise ValueError("empty input")

        # Detect mnemonic/seed phrase paste early for clearer guidance.
        if " " in s and len(s.split()) >= 12:
            raise ValueError("seed phrase detected; paste private key export instead")

        # 1) Direct base58 keypair string
        try:
            return Keypair.from_base58_string(s)
        except Exception:
            pass

        # 2) Phantom/Solana JSON array formats
        # - Keypair JSON string (from solana-keygen): "[1,2,...]"
        # - Raw list pasted directly.
        try:
            if s.startswith("[") and s.endswith("]"):
                arr = json.loads(s)
                if isinstance(arr, list):
                    b = bytes(int(x) for x in arr)
                    if len(b) == 64:
                        return Keypair.from_bytes(b)
                    if len(b) == 32:
                        return Keypair.from_seed(b)
            # solders supports keypair JSON string directly.
            return Keypair.from_json(s)
        except Exception:
            pass

        # 3) Base64-encoded bytes (32 seed or 64 keypair)
        try:
            b = base64.b64decode(s, validate=True)
            if len(b) == 64:
                return Keypair.from_bytes(b)
            if len(b) == 32:
                return Keypair.from_seed(b)
        except Exception:
            pass

        # 4) Hex string (with/without 0x), 32 or 64 bytes
        try:
            hx = s[2:] if s.lower().startswith("0x") else s
            b = binascii.unhexlify(hx)
            if len(b) == 64:
                return Keypair.from_bytes(b)
            if len(b) == 32:
                return Keypair.from_seed(b)
        except Exception:
            pass

        raise ValueError("unsupported private key format")

    def import_private_key(self, private_key_base58: str, enable_trading: bool = True):
        """Import/replace custodial key at runtime (in-memory only)."""
        kp = self._parse_private_key(private_key_base58)
        self.wallet = kp
        self.simulation_mode = not bool(enable_trading)
        return {"enabled": self.is_custodial_enabled(), "wallet": self.public_key()}

    def clear_private_key(self):
        self.wallet = None
        self.simulation_mode = True
        return {"enabled": False, "wallet": ""}

    def public_key(self) -> str:
        return str(self.wallet.pubkey()) if self.wallet else ""

    def is_custodial_enabled(self) -> bool:
        return not self.simulation_mode and self.wallet is not None

    async def _http_get_json(self, url: str, timeout_sec: int = 15):
        import socket

        connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return None
                    return await resp.json()
        except Exception:
            return None

    async def _http_post_json(self, url: str, payload: dict, timeout_sec: int = 25):
        import socket

        connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        return None
                    return await resp.json()
        except Exception:
            return None

    async def _rpc_json(self, method: str, params: list):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.rpc_url, json=payload, timeout=20) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    return data.get("result")
        except Exception:
            return None

    async def get_quote(self, input_mint: str, output_mint: str, amount_raw: int, slippage_bps: int = 100):
        input_mint = input_mint.strip()
        output_mint = output_mint.strip()
        for base in JUP_QUOTE_URLS:
            url = (
                f"{base}?inputMint={input_mint}"
                f"&outputMint={output_mint}"
                f"&amount={int(amount_raw)}"
                f"&slippageBps={int(slippage_bps)}"
            )
            data = await self._http_get_json(url)
            if data and data.get("outAmount"):
                return data
        return None

    async def _build_swap_tx(self, quote_response: dict):
        if not self.wallet:
            return None
        payload = {
            "quoteResponse": quote_response,
            "userPublicKey": str(self.wallet.pubkey()),
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": "auto",
        }
        for url in JUP_SWAP_URLS:
            data = await self._http_post_json(url, payload)
            if data and data.get("swapTransaction"):
                return data
        return None

    async def _sign_and_send(self, swap_tx_b64: str):
        if not self.wallet:
            return None
        raw_bytes = base64.b64decode(swap_tx_b64)
        raw_tx = VersionedTransaction.from_bytes(raw_bytes)
        signed = VersionedTransaction(raw_tx.message, [self.wallet])
        resp = await self.client.send_raw_transaction(bytes(signed), opts=TxOpts(skip_preflight=False, max_retries=3))
        return str(resp.value)

    async def get_wallet_token_balance(self, token_mint: str):
        if not self.wallet:
            return {"raw_amount": 0, "ui_amount": 0.0, "decimals": 0}
        result = await self._rpc_json(
            "getTokenAccountsByOwner",
            [str(self.wallet.pubkey()), {"mint": token_mint}, {"encoding": "jsonParsed"}],
        )
        if not result:
            return {"raw_amount": 0, "ui_amount": 0.0, "decimals": 0}

        raw_total = 0
        ui_total = 0.0
        decimals = 0
        for item in result.get("value", []):
            try:
                amt = item.get("account", {}).get("data", {}).get("parsed", {}).get("info", {}).get("tokenAmount", {})
                raw_total += int(amt.get("amount", "0") or "0")
                ui_total += float(amt.get("uiAmount", 0) or 0)
                decimals = int(amt.get("decimals", decimals))
            except Exception:
                continue
        return {"raw_amount": raw_total, "ui_amount": ui_total, "decimals": decimals}

    async def execute_buy(self, token_mint: str, amount_sol: float = 0.01, slippage_tiers=(100, 300, 700)):
        amount_lamports = int(amount_sol * 1_000_000_000)
        if amount_lamports <= 0:
            return {"status": "failed", "reason": "invalid_amount"}

        if self.simulation_mode:
            quote = await self.get_quote(SOL_MINT, token_mint, amount_lamports, slippage_bps=100)
            if not quote:
                return {"status": "failed", "reason": "no_quote"}
            out_ui = float(quote.get("outAmount", 0)) / 1_000_000
            return {"status": "simulated", "out_amount_ui": out_ui, "quote": quote}

        last_reason = "no_quote"
        for slip in slippage_tiers:
            quote = await self.get_quote(SOL_MINT, token_mint, amount_lamports, slippage_bps=slip)
            if not quote:
                continue
            swap = await self._build_swap_tx(quote)
            if not swap:
                last_reason = "swap_build_failed"
                continue
            try:
                tx_sig = await self._sign_and_send(swap["swapTransaction"])
                out_ui = float(quote.get("outAmount", 0)) / 1_000_000
                return {
                    "status": "executed",
                    "tx_sig": tx_sig,
                    "slippage_bps": slip,
                    "in_amount_lamports": int(quote.get("inAmount", amount_lamports)),
                    "out_amount_ui": out_ui,
                    "quote": quote,
                }
            except Exception as e:
                last_reason = str(e)
                continue

        return {"status": "failed", "reason": last_reason}

    async def execute_sell_all(self, token_mint: str, slippage_tiers=(200, 500, 1000)):
        if self.simulation_mode:
            return {"status": "skipped", "reason": "simulation_mode"}
        bal = await self.get_wallet_token_balance(token_mint)
        raw_amount = int(bal.get("raw_amount", 0) or 0)
        ui_amount = float(bal.get("ui_amount", 0) or 0)
        if raw_amount <= 0:
            return {"status": "no_balance", "ui_amount": 0.0}

        last_reason = "no_quote"
        for slip in slippage_tiers:
            quote = await self.get_quote(token_mint, SOL_MINT, raw_amount, slippage_bps=slip)
            if not quote:
                continue
            swap = await self._build_swap_tx(quote)
            if not swap:
                last_reason = "swap_build_failed"
                continue
            try:
                tx_sig = await self._sign_and_send(swap["swapTransaction"])
                sol_out = float(quote.get("outAmount", 0)) / 1_000_000_000
                return {
                    "status": "executed",
                    "tx_sig": tx_sig,
                    "slippage_bps": slip,
                    "token_amount_sold_ui": ui_amount,
                    "sol_received": sol_out,
                    "quote": quote,
                }
            except Exception as e:
                last_reason = str(e)
                continue

        return {"status": "failed", "reason": last_reason, "token_amount_sold_ui": ui_amount}
