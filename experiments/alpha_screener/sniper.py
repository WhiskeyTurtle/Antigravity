import base64
import binascii
import asyncio
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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CUSTODIAL_STATE_FILE = os.path.join(BASE_DIR, "custodial_wallet_state.json")


class Sniper:
    def __init__(self, rpc_client: AsyncClient, rpc_url: str):
        self.client = rpc_client
        self.rpc_url = rpc_url
        self.wallet: Optional[Keypair] = None

        env_priv = os.getenv("CUSTODIAL_PRIVATE_KEY") or os.getenv("SOLANA_PRIVATE_KEY")
        env_enabled = os.getenv("ENABLE_CUSTODIAL_TRADING")
        priv = env_priv
        enabled = (env_enabled or "false").lower() in ("1", "true", "yes", "on")

        # If env vars are not provided, load persisted key/state from disk.
        if not priv:
            persisted = self._load_persisted_state()
            if persisted:
                priv = persisted.get("private_key", "")
                enabled = bool(persisted.get("enabled", False))

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

    def _load_persisted_state(self) -> Optional[dict]:
        if not os.path.exists(CUSTODIAL_STATE_FILE):
            return None
        try:
            with open(CUSTODIAL_STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return None
            pk = str(data.get("private_key", "") or "").strip()
            if not pk:
                return None
            return {"private_key": pk, "enabled": bool(data.get("enabled", False))}
        except Exception:
            return None

    def _save_persisted_state(self, private_key_base58: str, enabled: bool):
        payload = {
            "private_key": private_key_base58,
            "enabled": bool(enabled),
        }
        with open(CUSTODIAL_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    def _clear_persisted_state(self):
        try:
            if os.path.exists(CUSTODIAL_STATE_FILE):
                os.remove(CUSTODIAL_STATE_FILE)
        except Exception:
            pass

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
        """Import/replace custodial key at runtime and persist it on disk."""
        kp = self._parse_private_key(private_key_base58)
        serialized = str(kp)
        self.wallet = kp
        self.simulation_mode = not bool(enable_trading)
        self._save_persisted_state(serialized, not self.simulation_mode)
        return {"enabled": self.is_custodial_enabled(), "wallet": self.public_key()}

    def clear_private_key(self):
        self.wallet = None
        self.simulation_mode = True
        self._clear_persisted_state()
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

    async def _wait_for_sell_balance_change(
        self,
        token_mint: str,
        pre_raw_amount: int,
        pre_ui_amount: float,
        polls: int = 8,
        delay_sec: float = 1.5,
    ):
        """Poll wallet balance after submit to confirm a sell actually reduced holdings."""
        last_raw = int(pre_raw_amount or 0)
        last_ui = float(pre_ui_amount or 0.0)
        for _ in range(max(1, int(polls or 1))):
            bal = await self.get_wallet_token_balance(token_mint)
            last_raw = int(bal.get("raw_amount", 0) or 0)
            last_ui = float(bal.get("ui_amount", 0.0) or 0.0)
            if last_raw < int(pre_raw_amount or 0):
                sold_ui = max(0.0, float(pre_ui_amount or 0.0) - last_ui)
                return {"confirmed": True, "sold_ui": sold_ui, "post_ui": last_ui, "post_raw": last_raw}
            await asyncio.sleep(max(0.1, float(delay_sec or 1.0)))
        return {"confirmed": False, "sold_ui": 0.0, "post_ui": last_ui, "post_raw": last_raw}

    async def _wait_for_buy_balance_change(
        self,
        token_mint: str,
        pre_raw_amount: int,
        pre_ui_amount: float,
        polls: int = 8,
        delay_sec: float = 1.2,
    ):
        """Poll wallet balance after submit to confirm a buy increased holdings."""
        last_raw = int(pre_raw_amount or 0)
        last_ui = float(pre_ui_amount or 0.0)
        for _ in range(max(1, int(polls or 1))):
            bal = await self.get_wallet_token_balance(token_mint)
            last_raw = int(bal.get("raw_amount", 0) or 0)
            last_ui = float(bal.get("ui_amount", 0.0) or 0.0)
            if last_raw > int(pre_raw_amount or 0):
                bought_ui = max(0.0, last_ui - float(pre_ui_amount or 0.0))
                return {"confirmed": True, "bought_ui": bought_ui, "post_ui": last_ui, "post_raw": last_raw}
            await asyncio.sleep(max(0.1, float(delay_sec or 1.0)))
        return {"confirmed": False, "bought_ui": 0.0, "post_ui": last_ui, "post_raw": last_raw}

    async def execute_buy(self, token_mint: str, amount_sol: float = 0.01, slippage_tiers=(100, 300, 700, 1200, 2000)):
        amount_lamports = int(amount_sol * 1_000_000_000)
        if amount_lamports <= 0:
            return {"status": "failed", "reason": "invalid_amount"}

        if self.simulation_mode:
            quote = await self.get_quote(SOL_MINT, token_mint, amount_lamports, slippage_bps=100)
            if not quote:
                return {"status": "failed", "reason": "no_quote"}
            out_ui = float(quote.get("outAmount", 0)) / 1_000_000
            return {"status": "simulated", "out_amount_ui": out_ui, "quote": quote}

        pre_bal = await self.get_wallet_token_balance(token_mint)
        pre_raw = int(pre_bal.get("raw_amount", 0) or 0)
        pre_ui = float(pre_bal.get("ui_amount", 0.0) or 0.0)

        last_reason = "no_quote"
        last_submitted_sig = ""
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
                last_submitted_sig = str(tx_sig or "")
                confirm = await self._wait_for_buy_balance_change(
                    token_mint=token_mint,
                    pre_raw_amount=pre_raw,
                    pre_ui_amount=pre_ui,
                )
                if bool(confirm.get("confirmed", False)):
                    out_ui_observed = float(confirm.get("bought_ui", 0.0) or 0.0)
                    out_ui_quote = float(quote.get("outAmount", 0)) / 1_000_000
                    return {
                        "status": "executed",
                        "tx_sig": tx_sig,
                        "slippage_bps": slip,
                        "in_amount_lamports": int(quote.get("inAmount", amount_lamports)),
                        "out_amount_ui": out_ui_observed if out_ui_observed > 0 else out_ui_quote,
                        "quote": quote,
                    }
                # Stop resubmitting new buy transactions once one has been sent.
                last_reason = "submitted_but_balance_unchanged"
                break
            except Exception as e:
                last_reason = str(e)
                continue

        # Fallback: even if swap call errors, wallet balance may still increase due to
        # race/duplicate send behavior on fast markets. Poll briefly before declaring failure.
        for _ in range(6):
            post_bal = await self.get_wallet_token_balance(token_mint)
            post_ui = float(post_bal.get("ui_amount", 0.0) or 0.0)
            delta_ui = max(0.0, post_ui - pre_ui)
            if delta_ui > 0:
                return {
                    "status": "balance_detected",
                    "reason": last_reason,
                    "out_amount_ui": delta_ui,
                    "in_amount_lamports": amount_lamports,
                    "tx_sig": last_submitted_sig,
                }
            await asyncio.sleep(2)

        return {"status": "failed", "reason": last_reason, "tx_sig": last_submitted_sig}

    async def execute_sell_all(self, token_mint: str, slippage_tiers=(200, 500, 1000, 2000, 3500)):
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
                confirm = await self._wait_for_sell_balance_change(
                    token_mint=token_mint,
                    pre_raw_amount=raw_amount,
                    pre_ui_amount=ui_amount,
                )
                if not bool(confirm.get("confirmed", False)):
                    last_reason = "submitted_but_balance_unchanged"
                    continue
                sold_ui_observed = float(confirm.get("sold_ui", 0.0) or 0.0)
                sold_ratio = sold_ui_observed / ui_amount if ui_amount > 0 else 1.0
                sold_ratio = max(0.0, min(1.0, sold_ratio))
                sol_out = float(quote.get("outAmount", 0)) / 1_000_000_000
                if sold_ratio > 0 and sold_ratio < 0.999:
                    sol_out *= sold_ratio
                return {
                    "status": "executed",
                    "tx_sig": tx_sig,
                    "slippage_bps": slip,
                    "token_amount_sold_ui": sold_ui_observed if sold_ui_observed > 0 else ui_amount,
                    "sol_received": sol_out,
                    "quote": quote,
                }
            except Exception as e:
                last_reason = str(e)
                continue

        return {"status": "failed", "reason": last_reason, "token_amount_sold_ui": ui_amount}

    async def execute_sell_fraction(self, token_mint: str, fraction: float, slippage_tiers=(200, 500, 1000, 2000, 3500)):
        frac = max(0.0, min(1.0, float(fraction or 0.0)))
        if frac <= 0:
            return {"status": "skipped", "reason": "invalid_fraction", "token_amount_sold_ui": 0.0}
        if frac >= 0.999:
            return await self.execute_sell_all(token_mint, slippage_tiers=slippage_tiers)
        if self.simulation_mode:
            return {"status": "skipped", "reason": "simulation_mode"}

        bal = await self.get_wallet_token_balance(token_mint)
        raw_total = int(bal.get("raw_amount", 0) or 0)
        ui_total = float(bal.get("ui_amount", 0) or 0)
        if raw_total <= 0:
            return {"status": "no_balance", "ui_amount": 0.0}

        raw_amount = int(raw_total * frac)
        if raw_amount <= 0:
            raw_amount = 1
        if raw_amount >= raw_total:
            return await self.execute_sell_all(token_mint, slippage_tiers=slippage_tiers)

        ui_amount = ui_total * (raw_amount / raw_total) if raw_total > 0 else 0.0
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
                confirm = await self._wait_for_sell_balance_change(
                    token_mint=token_mint,
                    pre_raw_amount=raw_total,
                    pre_ui_amount=ui_total,
                )
                if not bool(confirm.get("confirmed", False)):
                    last_reason = "submitted_but_balance_unchanged"
                    continue
                sold_ui_observed = float(confirm.get("sold_ui", 0.0) or 0.0)
                sold_ratio = sold_ui_observed / ui_total if ui_total > 0 else 1.0
                sold_ratio = max(0.0, min(1.0, sold_ratio))
                sol_out = float(quote.get("outAmount", 0)) / 1_000_000_000
                if sold_ratio > 0 and sold_ratio < 0.999:
                    sol_out *= sold_ratio
                return {
                    "status": "executed",
                    "tx_sig": tx_sig,
                    "slippage_bps": slip,
                    "token_amount_sold_ui": sold_ui_observed if sold_ui_observed > 0 else ui_amount,
                    "sol_received": sol_out,
                    "quote": quote,
                }
            except Exception as e:
                last_reason = str(e)
                continue

        return {"status": "failed", "reason": last_reason, "token_amount_sold_ui": ui_amount}
