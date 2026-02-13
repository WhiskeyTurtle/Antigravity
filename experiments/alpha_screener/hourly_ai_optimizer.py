import argparse
import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Tuple


ALLOWED_RANGES = {
    "chart_pro_min_score": (10.0, 90.0),
    "chart_watch_admission_min_volume_m5_usd": (0.0, 50000.0),
    "chart_watch_admission_min_liquidity_usd": (0.0, 100000.0),
    "chart_watch_admission_min_mcap_usd": (0.0, 2000000.0),
    "market_trend_scan_interval_sec": (4.0, 120.0),
    "market_trend_max_new_per_scan": (1.0, 40.0),
    "chart_watch_max_tokens": (4.0, 64.0),
    "chart_watch_eval_budget_per_cycle": (2.0, 64.0),
    "chart_watch_eval_concurrency": (1.0, 8.0),
    "chart_watch_market_data_timeout_sec": (0.8, 5.0),
    "chart_watch_candle_cache_ttl_sec": (10.0, 180.0),
}

MAX_STEP_PER_RUN = {
    "chart_pro_min_score": 3.0,
    "chart_watch_admission_min_volume_m5_usd": 400.0,
    "chart_watch_admission_min_liquidity_usd": 600.0,
    "chart_watch_admission_min_mcap_usd": 2000.0,
    "market_trend_scan_interval_sec": 2.0,
    "market_trend_max_new_per_scan": 2.0,
    "chart_watch_max_tokens": 4.0,
    "chart_watch_eval_budget_per_cycle": 3.0,
    "chart_watch_eval_concurrency": 1.0,
    "chart_watch_market_data_timeout_sec": 0.2,
    "chart_watch_candle_cache_ttl_sec": 10.0,
}

INT_KEYS = {
    "market_trend_scan_interval_sec",
    "market_trend_max_new_per_scan",
    "chart_watch_max_tokens",
    "chart_watch_eval_budget_per_cycle",
    "chart_watch_eval_concurrency",
    "chart_watch_candle_cache_ttl_sec",
}

SYSTEMIC_PROFILES = (
    "quality_first",
    "balanced",
    "high_frequency",
    "volatility_first",
    "momentum_first",
)

SYSTEMIC_DWELL_SEC = 3 * 3600

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
STATE_FILE = os.path.join(LOG_DIR, "hourly_ai_optimizer_state.json")
DIAG_LOG_FILE = os.path.join(LOG_DIR, "hourly_ai_optimizer.jsonl")


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _ensure_log_dir() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)


def _load_state() -> Dict[str, Any]:
    _ensure_log_dir()
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    _ensure_log_dir()
    safe_state = dict(state or {})
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(safe_state, f, ensure_ascii=True, indent=2)


def _append_diag(row: Dict[str, Any]) -> None:
    _ensure_log_dir()
    with open(DIAG_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(dict(row or {}), ensure_ascii=True))
        f.write("\n")


def _http_json(method: str, url: str, payload: Dict[str, Any] | None = None, timeout_sec: float = 20.0) -> Dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        data = encoded
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url=url, data=data, method=method.upper(), headers=headers)
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        if not body.strip():
            return {}
        parsed = json.loads(body)
        if isinstance(parsed, dict):
            return parsed
        return {"data": parsed}


def _http_json_openai(url: str, api_key: str, payload: Dict[str, Any], timeout_sec: float = 25.0) -> Dict[str, Any]:
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=encoded,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        parsed = json.loads(body)
        if isinstance(parsed, dict):
            return parsed
        return {"data": parsed}


def _summarize_chart_history(history_rows: list, window_sec: int) -> Dict[str, Any]:
    now = time.time()
    min_ts = now - max(300, int(window_sec))
    recent = []
    for row in history_rows:
        if str((row or {}).get("strategy_mode", "") or "").strip().lower() != "chart_pro_breakout_scalper":
            continue
        ts = _to_float((row or {}).get("close_time", 0.0), 0.0)
        if ts < min_ts:
            continue
        recent.append(row)
    trades = len(recent)
    wins = 0
    pnl_sum = 0.0
    reasons: Dict[str, int] = {}
    for row in recent:
        pnl = _to_float((row or {}).get("pnl", 0.0), 0.0)
        if pnl > 0:
            wins += 1
        pnl_sum += pnl
        reason = str((row or {}).get("reason", "UNKNOWN") or "UNKNOWN")
        reasons[reason] = int(reasons.get(reason, 0)) + 1
    win_rate = (float(wins) / float(trades)) if trades > 0 else 0.0
    return {
        "window_sec": int(window_sec),
        "trades": int(trades),
        "wins": int(wins),
        "win_rate": float(win_rate),
        "pnl_sol": float(pnl_sum),
        "reasons": reasons,
    }


def _extract_llm_json(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        parsed = json.loads(raw[start : end + 1])
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _sanitize_systemic_action(raw: Dict[str, Any], current_profile: str) -> Dict[str, Any]:
    action = dict(raw or {})
    target = str(action.get("target_profile", current_profile) or current_profile).strip().lower()
    if target not in SYSTEMIC_PROFILES:
        target = current_profile
    reason = str(action.get("reason", "") or "").strip()
    change_required = bool(action.get("change_required", False)) or (target != current_profile)
    force = bool(action.get("force", False))
    return {
        "change_required": change_required,
        "target_profile": target,
        "reason": reason,
        "force": force,
    }


def _reason_pct(reason_mix: Dict[str, Any], code: str) -> float:
    total = 0.0
    target = 0.0
    for key, value in (reason_mix or {}).items():
        v = max(0.0, _to_float(value, 0.0))
        total += v
        if str(key or "").strip().upper() == str(code or "").strip().upper():
            target += v
    if total <= 0:
        return 0.0
    return target / total


def _heuristic_systemic_profile(snapshot: Dict[str, Any], current_profile: str) -> tuple[str, str]:
    cw = dict((snapshot or {}).get("chart_watch", {}) or {})
    hw = dict((snapshot or {}).get("history_window", {}) or {})
    reason_mix = dict(cw.get("reason_mix", {}) or {})
    trades = _to_int(hw.get("trades", 0), 0)
    win_rate = _to_float(hw.get("win_rate", 0.0), 0.0)
    pnl_sol = _to_float(hw.get("pnl_sol", 0.0), 0.0)
    suppressed_ratio = _to_float(cw.get("suppressed_ratio", 0.0), 0.0)
    entry_near = _to_int(cw.get("entry_near_count", 0), 0)
    queue_lag = _to_float(cw.get("queue_lag_ms_avg", 0.0), 0.0)
    no_trigger_pct = _reason_pct(reason_mix, "NO_TRIGGER")
    vol_gate_pct = _reason_pct(reason_mix, "VOL_GATE")
    low_score_pct = _reason_pct(reason_mix, "LOW_SCORE")
    timeout_count = _to_int(cw.get("market_data_timeout_count", 0), 0)

    if trades <= 6 and no_trigger_pct >= 0.35 and entry_near == 0:
        return "momentum_first", "NO_TRIGGER dominates with low fills"
    if trades <= 6 and vol_gate_pct >= 0.30 and suppressed_ratio >= 0.70:
        return "volatility_first", "VOL_GATE dominates under low fill regime"
    if trades >= 12 and (pnl_sol < 0.0) and (win_rate <= 0.45):
        return "quality_first", "negative pnl and weak win-rate"
    if (queue_lag >= 2500.0 or timeout_count >= 120) and current_profile == "quality_first":
        return "high_frequency", "latency/timeouts elevated while in strict profile"
    if low_score_pct >= 0.50 and trades <= 8:
        return "momentum_first", "low score density with low fill"
    return current_profile, "stable regime"


def _systemic_preset_for_profile(profile: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    current = dict(settings or {})
    trend_vol = max(500.0, _to_float(current.get("chart_watch_trend_min_volume_m5_usd", 5000.0), 5000.0))
    trend_liq = max(500.0, _to_float(current.get("chart_watch_trend_min_liquidity_usd", 8000.0), 8000.0))
    trend_mcap = max(1000.0, _to_float(current.get("chart_watch_trend_min_mcap_usd", 20000.0), 20000.0))
    score = max(10.0, _to_float(current.get("chart_pro_min_score", 34.0), 34.0))
    interval = max(4.0, _to_float(current.get("market_trend_scan_interval_sec", 8), 8))
    max_new = max(1.0, _to_float(current.get("market_trend_max_new_per_scan", 12), 12))
    eval_budget = max(2.0, _to_float(current.get("chart_watch_eval_budget_per_cycle", 16), 16))
    eval_conc = max(1.0, _to_float(current.get("chart_watch_eval_concurrency", 3), 3))

    if profile == "volatility_first":
        return {
            "chart_watch_focus_profile": "volatility_first",
            "chart_watch_trend_min_volume_m5_usd": round(max(1200.0, trend_vol * 1.20), 2),
            "chart_watch_trend_min_liquidity_usd": round(max(1800.0, trend_liq * 0.82), 2),
            "chart_watch_trend_min_mcap_usd": round(max(6000.0, trend_mcap * 0.80), 2),
            "chart_pro_min_score": round(max(18.0, min(score, 28.0)), 2),
            "market_trend_scan_interval_sec": int(max(5, min(20, round(interval - 1)))),
            "market_trend_max_new_per_scan": int(max(8, min(25, round(max_new + 2)))),
            "chart_watch_eval_budget_per_cycle": int(max(12, min(25, round(eval_budget + 2)))),
            "chart_watch_eval_concurrency": int(max(2, min(6, round(eval_conc)))),
        }
    if profile == "momentum_first":
        return {
            "chart_watch_focus_profile": "momentum_first",
            "chart_watch_trend_min_volume_m5_usd": round(max(1500.0, trend_vol * 1.08), 2),
            "chart_watch_trend_min_liquidity_usd": round(max(2200.0, trend_liq * 0.92), 2),
            "chart_watch_trend_min_mcap_usd": round(max(8000.0, trend_mcap * 0.92), 2),
            "chart_pro_min_score": round(max(20.0, min(score, 30.0)), 2),
            "market_trend_scan_interval_sec": int(max(5, min(20, round(interval)))),
            "market_trend_max_new_per_scan": int(max(8, min(25, round(max_new + 1)))),
            "chart_watch_eval_budget_per_cycle": int(max(12, min(25, round(eval_budget + 1)))),
            "chart_watch_eval_concurrency": int(max(2, min(6, round(eval_conc)))),
        }
    if profile == "quality_first":
        return {
            "chart_watch_focus_profile": "quality_first",
            "chart_watch_trend_min_volume_m5_usd": round(max(2500.0, trend_vol * 1.15), 2),
            "chart_watch_trend_min_liquidity_usd": round(max(3500.0, trend_liq * 1.10), 2),
            "chart_watch_trend_min_mcap_usd": round(max(12000.0, trend_mcap * 1.10), 2),
            "chart_pro_min_score": round(max(score, 30.0), 2),
            "market_trend_scan_interval_sec": int(max(6, min(30, round(interval + 1)))),
            "market_trend_max_new_per_scan": int(max(6, min(18, round(max_new - 1)))),
            "chart_watch_eval_budget_per_cycle": int(max(10, min(20, round(eval_budget)))),
            "chart_watch_eval_concurrency": int(max(2, min(4, round(eval_conc)))),
        }
    if profile == "high_frequency":
        return {
            "chart_watch_focus_profile": "high_frequency",
            "chart_watch_trend_min_volume_m5_usd": round(max(1000.0, trend_vol * 0.85), 2),
            "chart_watch_trend_min_liquidity_usd": round(max(1500.0, trend_liq * 0.80), 2),
            "chart_watch_trend_min_mcap_usd": round(max(5000.0, trend_mcap * 0.78), 2),
            "chart_pro_min_score": round(max(16.0, min(score, 26.0)), 2),
            "market_trend_scan_interval_sec": int(max(4, min(16, round(interval - 1)))),
            "market_trend_max_new_per_scan": int(max(10, min(25, round(max_new + 2)))),
            "chart_watch_eval_budget_per_cycle": int(max(14, min(25, round(eval_budget + 2)))),
            "chart_watch_eval_concurrency": int(max(2, min(6, round(eval_conc + 1)))),
        }
    return {
        "chart_watch_focus_profile": "balanced",
        "chart_pro_min_score": round(max(18.0, min(score, 34.0)), 2),
    }


def _sanitize_updates(proposed: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    safe: Dict[str, Any] = {}
    for key, bounds in ALLOWED_RANGES.items():
        if key not in proposed:
            continue
        lo, hi = bounds
        cur = _to_float(current.get(key, 0.0), 0.0)
        val = _to_float(proposed.get(key), cur)
        step = _to_float(MAX_STEP_PER_RUN.get(key, hi - lo), hi - lo)
        if step > 0:
            val = max(cur - step, min(cur + step, val))
        val = max(lo, min(hi, val))
        if key in INT_KEYS:
            val = int(round(val))
        else:
            val = round(float(val), 4)
        if val != current.get(key):
            safe[key] = val
    return safe


def _call_ai(
    api_key: str,
    model: str,
    snapshot: Dict[str, Any],
    current_tunables: Dict[str, Any],
    current_profile: str,
) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
    system_msg = (
        "You are a trading-system optimizer. "
        "Return strict JSON only with keys: rationale (string), updates (object), systemic_action (object). "
        "Use only allowed setting keys provided by user input. "
        "A systemic_action should be used when regime-level changes are warranted."
    )
    user_payload = {
        "goal": "Increase quality trade attempts without destabilizing risk controls",
        "allowed_keys": list(ALLOWED_RANGES.keys()),
        "allowed_profiles": list(SYSTEMIC_PROFILES),
        "current_profile": current_profile,
        "bounds": ALLOWED_RANGES,
        "max_step_per_run": MAX_STEP_PER_RUN,
        "current_tunables": current_tunables,
        "snapshot": snapshot,
        "output_contract": {
            "rationale": "string",
            "updates": {"setting_key": "number"},
            "systemic_action": {
                "change_required": "boolean",
                "target_profile": "quality_first|balanced|high_frequency|volatility_first|momentum_first",
                "reason": "string",
                "force": "boolean",
            },
        },
    }
    req = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": json.dumps(user_payload, separators=(",", ":"), ensure_ascii=True)},
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }
    resp = _http_json_openai("https://api.openai.com/v1/chat/completions", api_key=api_key, payload=req)
    choices = list(resp.get("choices", []) or [])
    if not choices:
        return {}, "no choices from model", {}
    content = str((((choices[0] or {}).get("message", {}) or {}).get("content", "")) or "")
    parsed = _extract_llm_json(content)
    rationale = str(parsed.get("rationale", "") or "").strip()
    updates = parsed.get("updates", {})
    if not isinstance(updates, dict):
        updates = {}
    systemic_action = parsed.get("systemic_action", {})
    if not isinstance(systemic_action, dict):
        systemic_action = {}
    return updates, rationale, systemic_action


def main() -> int:
    parser = argparse.ArgumentParser(description="Hourly AI optimizer for Alpha Screener.")
    parser.add_argument("--api-base", default="http://127.0.0.1:8000")
    parser.add_argument("--window-sec", type=int, default=3600)
    parser.add_argument("--model", default=os.getenv("OPENAI_MODEL", "gpt-5-mini"))
    args = parser.parse_args()

    api_base = str(args.api_base).rstrip("/")
    window_sec = max(300, int(args.window_sec))

    try:
        settings = _http_json("GET", f"{api_base}/api/settings")
        chart_watch = _http_json("GET", f"{api_base}/api/chart-watch")
        autopilot = _http_json("GET", f"{api_base}/api/autopilot")
    except Exception as exc:
        print(json.dumps({"status": "error", "reason": f"fetch_failed: {exc}"}, ensure_ascii=True))
        return 0

    history: Any = []
    history_fetch_error = ""
    try:
        history = _http_json("GET", f"{api_base}/api/history", timeout_sec=30.0)
    except Exception as exc:
        history_fetch_error = str(exc)
        history = []

    if not isinstance(history, list):
        history = list((history or {}).get("data", []) or [])

    chart_hist = _summarize_chart_history(history, window_sec=window_sec)
    snapshot = {
        "chart_watch": {
            "admitted_count": int(chart_watch.get("admitted_count", 0) or 0),
            "suppressed_count": int(chart_watch.get("suppressed_count", 0) or 0),
            "suppressed_ratio": _to_float(chart_watch.get("suppressed_ratio", 0.0), 0.0),
            "entry_near_count": int(chart_watch.get("entry_near_count", 0) or 0),
            "evals_per_min": int(chart_watch.get("evals_per_min", 0) or 0),
            "queue_lag_ms_avg": _to_float(chart_watch.get("queue_lag_ms_avg", 0.0), 0.0),
            "eval_latency_p95_ms": _to_float(chart_watch.get("eval_latency_p95_ms", 0.0), 0.0),
            "market_data_timeout_count": int(chart_watch.get("market_data_timeout_count", 0) or 0),
            "candle_rate_limited_count": int(chart_watch.get("candle_rate_limited_count", 0) or 0),
            "reason_mix": dict(chart_watch.get("reason_mix", {}) or {}),
            "source_mix_seen": dict(chart_watch.get("source_mix_seen", {}) or {}),
        },
        "autopilot": {
            "last_action": str(autopilot.get("last_action", "") or ""),
            "last_reason": str(autopilot.get("last_reason", "") or ""),
            "last_window_trades": int(autopilot.get("last_window_trades", 0) or 0),
            "last_window_pnl_sol": _to_float(autopilot.get("last_window_pnl_sol", 0.0), 0.0),
        },
        "history_window": chart_hist,
        "history_fetch_error": history_fetch_error,
    }

    state = _load_state()
    now_ts = time.time()
    current_profile = str(settings.get("chart_watch_focus_profile", "quality_first") or "quality_first").strip().lower()
    if current_profile not in SYSTEMIC_PROFILES:
        current_profile = "quality_first"
    heuristic_profile, heuristic_reason = _heuristic_systemic_profile(snapshot, current_profile=current_profile)

    current_tunables = {}
    for key in ALLOWED_RANGES.keys():
        current_tunables[key] = settings.get(key, 0.0)

    api_key = str(os.getenv("OPENAI_API_KEY", "") or "").strip()
    proposed_updates: Dict[str, Any] = {}
    rationale = ""
    systemic_action_raw: Dict[str, Any] = {}
    decision_source = "heuristic"

    if api_key:
        try:
            proposed_updates, rationale, systemic_action_raw = _call_ai(
                api_key=api_key,
                model=str(args.model),
                snapshot=snapshot,
                current_tunables=current_tunables,
                current_profile=current_profile,
            )
            decision_source = "ai"
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = str(exc)
            rationale = f"openai_http_error: {body[:220]}"
            decision_source = "heuristic_fallback"
        except Exception as exc:
            rationale = f"openai_call_failed: {exc}"
            decision_source = "heuristic_fallback"
    else:
        rationale = "OPENAI_API_KEY missing; heuristic fallback"
        decision_source = "heuristic_only"

    safe_updates = _sanitize_updates(proposed_updates, current=current_tunables)
    systemic_ai = _sanitize_systemic_action(systemic_action_raw, current_profile=current_profile)
    if not systemic_ai.get("change_required", False):
        systemic_ai = {
            "change_required": (heuristic_profile != current_profile),
            "target_profile": heuristic_profile,
            "reason": heuristic_reason,
            "force": False,
        }

    systemic_updates: Dict[str, Any] = {}
    systemic_applied = False
    systemic_skipped_reason = ""
    last_switch_ts = _to_float(state.get("last_profile_switch_ts", 0.0), 0.0)
    target_profile = str(systemic_ai.get("target_profile", current_profile) or current_profile).strip().lower()
    if target_profile not in SYSTEMIC_PROFILES:
        target_profile = current_profile
    if target_profile != current_profile:
        dwell_ok = (now_ts - last_switch_ts) >= float(SYSTEMIC_DWELL_SEC)
        if dwell_ok or bool(systemic_ai.get("force", False)):
            systemic_updates = _systemic_preset_for_profile(target_profile, settings=settings)
            systemic_applied = bool(systemic_updates)
            if systemic_applied:
                state["last_profile"] = target_profile
                state["last_profile_switch_ts"] = now_ts
                state["last_profile_reason"] = str(systemic_ai.get("reason", heuristic_reason) or heuristic_reason)
        else:
            systemic_skipped_reason = (
                f"profile switch blocked by dwell ({int(now_ts - last_switch_ts)}s < {SYSTEMIC_DWELL_SEC}s)"
            )
    else:
        state.setdefault("last_profile", current_profile)
        state.setdefault("last_profile_switch_ts", last_switch_ts)
        state.setdefault("last_profile_reason", "unchanged")

    payload = {
        "sol_amount_min": _to_float(settings.get("sol_amount_min", 0.03), 0.03),
        "sol_amount_max": _to_float(settings.get("sol_amount_max", 0.03), 0.03),
    }
    payload.update(safe_updates)
    payload.update(systemic_updates)
    if payload["sol_amount_min"] <= 0:
        payload["sol_amount_min"] = 0.01
    if payload["sol_amount_max"] < payload["sol_amount_min"]:
        payload["sol_amount_max"] = payload["sol_amount_min"]

    if (not safe_updates) and (not systemic_updates):
        output = {
            "status": "hold",
            "source": decision_source,
            "current_profile": current_profile,
            "target_profile": target_profile,
            "systemic_applied": False,
            "systemic_skipped_reason": systemic_skipped_reason,
            "updates": {},
            "systemic_updates": {},
            "rationale": rationale[:300],
            "reason": "no updates required",
        }
        _append_diag(
            {
                "ts": now_ts,
                "status": "hold",
                "source": decision_source,
                "reason": "no updates required",
                "rationale": rationale[:300],
                "current_profile": current_profile,
                "target_profile": target_profile,
                "systemic_applied": False,
                "systemic_skipped_reason": systemic_skipped_reason,
                "safe_updates": {},
                "systemic_updates": {},
                "snapshot_summary": {
                    "suppressed_ratio": snapshot["chart_watch"]["suppressed_ratio"],
                    "entry_near_count": snapshot["chart_watch"]["entry_near_count"],
                    "reason_mix": snapshot["chart_watch"]["reason_mix"],
                    "window_trades": snapshot["history_window"]["trades"],
                    "window_pnl_sol": snapshot["history_window"]["pnl_sol"],
                    "window_win_rate": snapshot["history_window"]["win_rate"],
                },
            }
        )
        print(json.dumps(output, ensure_ascii=True))
        return 0

    status = "hold"
    reason = ""
    try:
        _http_json("POST", f"{api_base}/api/settings", payload=payload)
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = str(exc)
        status = "error"
        reason = f"settings_http_error: {body[:400]}"
        result = {
            "status": status,
            "reason": reason,
            "updates": safe_updates,
            "systemic_updates": systemic_updates,
        }
        _append_diag(
            {
                "ts": now_ts,
                "status": status,
                "source": decision_source,
                "reason": reason,
                "rationale": rationale[:300],
                "current_profile": current_profile,
                "target_profile": target_profile,
                "systemic_applied": systemic_applied,
                "systemic_skipped_reason": systemic_skipped_reason,
                "safe_updates": safe_updates,
                "systemic_updates": systemic_updates,
                "snapshot_summary": {
                    "suppressed_ratio": snapshot["chart_watch"]["suppressed_ratio"],
                    "entry_near_count": snapshot["chart_watch"]["entry_near_count"],
                    "reason_mix": snapshot["chart_watch"]["reason_mix"],
                    "window_trades": snapshot["history_window"]["trades"],
                    "window_pnl_sol": snapshot["history_window"]["pnl_sol"],
                    "window_win_rate": snapshot["history_window"]["win_rate"],
                },
            }
        )
        print(json.dumps(result, ensure_ascii=True))
        return 0
    except Exception as exc:
        status = "error"
        reason = f"settings_apply_failed: {exc}"
        result = {
            "status": status,
            "reason": reason,
            "updates": safe_updates,
            "systemic_updates": systemic_updates,
        }
        _append_diag(
            {
                "ts": now_ts,
                "status": status,
                "source": decision_source,
                "reason": reason,
                "rationale": rationale[:300],
                "current_profile": current_profile,
                "target_profile": target_profile,
                "systemic_applied": systemic_applied,
                "systemic_skipped_reason": systemic_skipped_reason,
                "safe_updates": safe_updates,
                "systemic_updates": systemic_updates,
                "snapshot_summary": {
                    "suppressed_ratio": snapshot["chart_watch"]["suppressed_ratio"],
                    "entry_near_count": snapshot["chart_watch"]["entry_near_count"],
                    "reason_mix": snapshot["chart_watch"]["reason_mix"],
                    "window_trades": snapshot["history_window"]["trades"],
                    "window_pnl_sol": snapshot["history_window"]["pnl_sol"],
                    "window_win_rate": snapshot["history_window"]["win_rate"],
                },
            }
        )
        print(json.dumps(result, ensure_ascii=True))
        return 0

    _save_state(state)
    if safe_updates or systemic_updates:
        status = "applied"
    else:
        status = "hold"
        reason = "no updates required"

    output = {
        "status": status,
        "source": decision_source,
        "current_profile": current_profile,
        "target_profile": target_profile,
        "systemic_applied": systemic_applied,
        "systemic_skipped_reason": systemic_skipped_reason,
        "updates": safe_updates,
        "systemic_updates": systemic_updates,
        "rationale": rationale[:300],
    }
    if reason:
        output["reason"] = reason

    _append_diag(
        {
            "ts": now_ts,
            "status": status,
            "source": decision_source,
            "reason": reason,
            "rationale": rationale[:300],
            "current_profile": current_profile,
            "target_profile": target_profile,
            "systemic_applied": systemic_applied,
            "systemic_skipped_reason": systemic_skipped_reason,
            "safe_updates": safe_updates,
            "systemic_updates": systemic_updates,
            "snapshot_summary": {
                "suppressed_ratio": snapshot["chart_watch"]["suppressed_ratio"],
                "entry_near_count": snapshot["chart_watch"]["entry_near_count"],
                "reason_mix": snapshot["chart_watch"]["reason_mix"],
                "window_trades": snapshot["history_window"]["trades"],
                "window_pnl_sol": snapshot["history_window"]["pnl_sol"],
                "window_win_rate": snapshot["history_window"]["win_rate"],
            },
        }
    )
    print(json.dumps(output, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
