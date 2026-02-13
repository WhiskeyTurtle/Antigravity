import argparse
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _health_ok(url: str, timeout_sec: float) -> bool:
    try:
        req = urllib.request.Request(url=url, method="GET")
        with urllib.request.urlopen(req, timeout=float(timeout_sec)) as resp:
            status = int(getattr(resp, "status", 0) or 0)
            return 200 <= status < 300
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return False
    except Exception:
        return False


def _stop_child(proc: subprocess.Popen, grace_sec: float = 10.0) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except Exception:
        pass
    deadline = time.monotonic() + max(1.0, float(grace_sec))
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.2)
    try:
        proc.kill()
    except Exception:
        pass


def _build_command(args: argparse.Namespace) -> list[str]:
    cmd = [
        args.python,
        "-m",
        "uvicorn",
        args.app,
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]
    for extra in args.extra_arg:
        if extra:
            cmd.append(extra)
    return cmd


def _spawn(args: argparse.Namespace, env: dict) -> subprocess.Popen:
    cmd = _build_command(args)
    _log(f"Starting server: {' '.join(cmd)}")
    return subprocess.Popen(
        cmd,
        cwd=args.workdir,
        env=env,
    )


def run_guardian(args: argparse.Namespace) -> int:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    for kv in args.env:
        if "=" not in kv:
            _log(f"Ignoring invalid --env '{kv}' (expected KEY=VALUE)")
            continue
        k, v = kv.split("=", 1)
        env[k.strip()] = v

    restarts = 0
    fail_streak = 0
    backoff = max(1.0, float(args.restart_backoff_sec))
    start_guardian = time.monotonic()
    next_health_at = time.monotonic() + max(0.0, float(args.startup_grace_sec))
    child = _spawn(args, env)

    try:
        while True:
            if args.max_runtime_sec > 0:
                if (time.monotonic() - start_guardian) >= float(args.max_runtime_sec):
                    _log("Max guardian runtime reached, stopping.")
                    _stop_child(child)
                    return 0

            code = child.poll()
            if code is not None:
                restarts += 1
                _log(f"Server exited with code {code}. Restart #{restarts}.")
                if args.max_restarts > 0 and restarts > args.max_restarts:
                    _log("Max restart limit reached. Exiting guardian.")
                    return 1
                time.sleep(backoff)
                backoff = min(float(args.restart_backoff_max_sec), backoff * 2.0)
                fail_streak = 0
                next_health_at = time.monotonic() + max(5.0, float(args.startup_grace_sec))
                child = _spawn(args, env)
                continue

            if (not args.no_healthcheck) and time.monotonic() >= next_health_at:
                ok = _health_ok(args.health_url, args.health_timeout_sec)
                if ok:
                    fail_streak = 0
                    backoff = max(1.0, float(args.restart_backoff_sec))
                else:
                    fail_streak += 1
                    _log(
                        f"Health check failed ({fail_streak}/{args.max_health_failures}) "
                        f"for {args.health_url}"
                    )
                    if fail_streak >= args.max_health_failures:
                        restarts += 1
                        _log(f"Restarting due to health failures. Restart #{restarts}.")
                        if args.max_restarts > 0 and restarts > args.max_restarts:
                            _log("Max restart limit reached. Exiting guardian.")
                            _stop_child(child)
                            return 2
                        _stop_child(child)
                        time.sleep(backoff)
                        backoff = min(float(args.restart_backoff_max_sec), backoff * 2.0)
                        fail_streak = 0
                        next_health_at = time.monotonic() + max(5.0, float(args.startup_grace_sec))
                        child = _spawn(args, env)
                        continue

                next_health_at = time.monotonic() + max(5.0, float(args.health_interval_sec))

            time.sleep(1.0)
    except KeyboardInterrupt:
        _log("Guardian interrupted. Stopping child process.")
        _stop_child(child)
        return 0
    except Exception as exc:
        _log(f"Guardian fatal error: {exc}")
        _stop_child(child)
        return 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run uvicorn under a local watchdog that auto-restarts on crash or health failures.",
    )
    parser.add_argument("--python", default=sys.executable, help="Python executable to run uvicorn.")
    parser.add_argument("--app", default="main:app", help="ASGI app path for uvicorn.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--workdir",
        default=os.path.dirname(os.path.abspath(__file__)),
        help="Working directory for uvicorn process.",
    )
    parser.add_argument("--health-path", default="/api/chart-watch")
    parser.add_argument("--health-interval-sec", type=float, default=20.0)
    parser.add_argument("--health-timeout-sec", type=float, default=6.0)
    parser.add_argument("--startup-grace-sec", type=float, default=12.0)
    parser.add_argument("--max-health-failures", type=int, default=3)
    parser.add_argument("--restart-backoff-sec", type=float, default=3.0)
    parser.add_argument("--restart-backoff-max-sec", type=float, default=60.0)
    parser.add_argument("--max-restarts", type=int, default=0, help="0 means unlimited restarts.")
    parser.add_argument("--max-runtime-sec", type=float, default=0.0, help="0 means no runtime cap.")
    parser.add_argument("--no-healthcheck", action="store_true")
    parser.add_argument("--extra-arg", action="append", default=[], help="Extra uvicorn args (repeatable).")
    parser.add_argument("--env", action="append", default=[], help="Extra env vars in KEY=VALUE format.")
    args = parser.parse_args()
    args.max_health_failures = max(1, int(args.max_health_failures))
    args.port = max(1, int(args.port))
    args.health_url = f"http://{args.host}:{args.port}{args.health_path}"
    return args


if __name__ == "__main__":
    raise SystemExit(run_guardian(parse_args()))
