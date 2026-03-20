from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_ready(url: str, timeout: float = 20.0) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"service not ready: {url}")


def choose_ports(api_port: int, random_ports: bool) -> int:
    if random_ports:
        return free_port()
    return api_port


def run(keep_running: bool, api_port: int, random_ports: bool) -> int:
    api_port = choose_ports(api_port, random_ports)

    api_base = f"http://127.0.0.1:{api_port}"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)

    api = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "--app-dir", str(ROOT), "app.main:app", "--host", "127.0.0.1", "--port", str(api_port)],
        cwd=ROOT,
        env=env,
    )

    try:
        wait_ready(f"{api_base}/api/dashboard")
        with httpx.Client(timeout=httpx.Timeout(60.0, connect=5.0)) as client:
            dash = client.get(f"{api_base}/api/dashboard").json()
            tasks = client.get(f"{api_base}/api/tasks").json()
            nodes = client.get(f"{api_base}/api/nodes").json()
            instances = client.get(f"{api_base}/api/prometheus/instances").json()

        result = {
            "api_base": api_base,
            "dashboard": dash,
            "task0": tasks[0] if tasks else None,
            "node0": nodes[0] if nodes else None,
            "instance0": instances[0] if instances else None,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))

        if keep_running:
            print("Services are still running:")
            print(f"  UI/API: {api_base}/")
            print("Press Ctrl+C to stop all services.")
            while True:
                time.sleep(1)
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        for proc in (api,):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run local API smoke acceptance checks")
    parser.add_argument("--keep-running", action="store_true", help="Keep services alive after acceptance checks")
    parser.add_argument("--api-port", type=int, default=18080, help="API server port (default: 18080)")
    parser.add_argument("--random-ports", action="store_true", help="Use random free ports instead of fixed ports")
    args = parser.parse_args()
    raise SystemExit(
        run(
            keep_running=args.keep_running,
            api_port=args.api_port,
            random_ports=args.random_ports,
        )
    )
