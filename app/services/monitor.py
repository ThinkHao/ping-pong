from __future__ import annotations

import json
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from app.db import get_conn


MONITOR_INTERVAL_SECONDS = 30
_stop_event = threading.Event()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _upsert_alert(alert_type: str, severity: str, message: str, source_type: str, source_id: int | None) -> None:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id FROM alerts WHERE alert_type=? AND source_type=? AND source_id=? AND status='open'",
            (alert_type, source_type, source_id),
        ).fetchone()
        if existing:
            return
        conn.execute(
            "INSERT INTO alerts(alert_type, severity, message, source_type, source_id, status, created_at) VALUES(?,?,?,?,?,?,?)",
            (alert_type, severity, message, source_type, source_id, "open", utc_now()),
        )
        conn.commit()


def _resolve_alert(alert_type: str, source_type: str, source_id: int | None) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE alerts SET status='resolved', resolved_at=? WHERE alert_type=? AND source_type=? AND source_id=? AND status='open'",
            (utc_now(), alert_type, source_type, source_id),
        )
        conn.commit()


def check_nodes() -> None:
    with get_conn() as conn:
        nodes = conn.execute("SELECT * FROM exporter_nodes").fetchall()

    for node in nodes:
        node_id = node["id"]
        base_url = node["base_url"].rstrip("/")
        url = f"{base_url}/metrics"
        status = "offline"
        version = node["version"]
        try:
            with httpx.Client(timeout=5.0) as client:
                r = client.get(url)
                if r.status_code == 200:
                    status = "online"
                    # Best-effort version extract from metric comments
                    for line in r.text.splitlines():
                        if "blackbox_exporter" in line and "version" in line.lower():
                            version = line[:120]
                            break
        except Exception:
            status = "offline"

        with get_conn() as conn:
            conn.execute(
                "UPDATE exporter_nodes SET status=?, last_heartbeat=?, version=?, updated_at=? WHERE id=?",
                (status, utc_now(), version, utc_now(), node_id),
            )
            conn.commit()

        if status == "offline":
            _upsert_alert("node_offline", "critical", f"Exporter node {node['name']} is offline", "exporter_node", node_id)
        else:
            _resolve_alert("node_offline", "exporter_node", node_id)


def _tcp_probe(host: str, port: int, timeout_s: int) -> tuple[str, float]:
    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return "success", (time.perf_counter() - start) * 1000
    except Exception:
        return "failed", (time.perf_counter() - start) * 1000


def _http_probe(url: str, timeout_s: int) -> tuple[str, float]:
    start = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout_s) as client:
            resp = client.get(url)
            if 200 <= resp.status_code < 400:
                return "success", (time.perf_counter() - start) * 1000
            return "failed", (time.perf_counter() - start) * 1000
    except Exception:
        return "failed", (time.perf_counter() - start) * 1000


def _blackbox_probe(node_base_url: str, module: str, target: str, timeout_s: int) -> tuple[str, float]:
    start = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout_s) as client:
            resp = client.get(f"{node_base_url.rstrip('/')}/probe", params={"module": module, "target": target})
            body = resp.text
            success = "probe_success 1" in body
            return ("success" if success else "failed"), (time.perf_counter() - start) * 1000
    except Exception:
        return "failed", (time.perf_counter() - start) * 1000


def check_tasks() -> None:
    with get_conn() as conn:
        tasks = conn.execute(
            """
            SELECT t.*, n.base_url AS node_base_url
            FROM probe_tasks t
            LEFT JOIN exporter_nodes n ON n.id = t.node_id
            WHERE t.status='active'
            """
        ).fetchall()

    for task in tasks:
        protocol = task["protocol"]
        timeout_s = int(task["timeout_seconds"])
        node_base_url = task["node_base_url"]
        with get_conn() as conn:
            targets = [r["target"] for r in conn.execute("SELECT target FROM probe_task_targets WHERE task_id=?", (task["id"],)).fetchall()]
        if not targets and task["target"]:
            targets = [task["target"]]

        if not node_base_url or not targets:
            result, latency = ("failed", 0.0)
        else:
            latencies = []
            failed = 0
            for target in targets:
                if protocol in {"icmp", "tcp", "http"}:
                    single_result, single_latency = _blackbox_probe(node_base_url, protocol, target, timeout_s)
                else:
                    single_result, single_latency = ("failed", 0.0)
                latencies.append(single_latency)
                if single_result != "success":
                    failed += 1
            result = "success" if failed == 0 else "failed"
            latency = sum(latencies) / len(latencies) if latencies else 0.0

        with get_conn() as conn:
            conn.execute(
                "UPDATE probe_tasks SET last_result=?, last_latency_ms=?, last_checked_at=?, updated_at=? WHERE id=?",
                (result, latency, utc_now(), utc_now(), task["id"]),
            )
            conn.commit()

        if result == "failed":
            _upsert_alert("task_failed", "warning", f"Probe task {task['name']} failed", "probe_task", task["id"])
        else:
            _resolve_alert("task_failed", "probe_task", task["id"])


def monitor_tick() -> None:
    check_nodes()
    check_tasks()


def monitor_loop() -> None:
    while not _stop_event.is_set():
        try:
            monitor_tick()
        except Exception:
            pass
        _stop_event.wait(MONITOR_INTERVAL_SECONDS)


def start_monitor() -> None:
    thread = threading.Thread(target=monitor_loop, daemon=True)
    thread.start()


def stop_monitor() -> None:
    _stop_event.set()


def dashboard_summary() -> dict[str, Any]:
    with get_conn() as conn:
        node_total = conn.execute("SELECT COUNT(*) AS c FROM exporter_nodes").fetchone()["c"]
        node_online = conn.execute("SELECT COUNT(*) AS c FROM exporter_nodes WHERE status='online'").fetchone()["c"]
        task_total = conn.execute("SELECT COUNT(*) AS c FROM probe_tasks WHERE status='active'").fetchone()["c"]
        task_success = conn.execute("SELECT COUNT(*) AS c FROM probe_tasks WHERE status='active' AND last_result='success'").fetchone()["c"]
        avg_latency = conn.execute("SELECT AVG(last_latency_ms) AS l FROM probe_tasks WHERE status='active'").fetchone()["l"]
        open_alerts = conn.execute("SELECT COUNT(*) AS c FROM alerts WHERE status='open'").fetchone()["c"]
        by_region = [dict(r) for r in conn.execute("SELECT region, COUNT(*) as total FROM probe_tasks GROUP BY region").fetchall()]
        by_business = [dict(r) for r in conn.execute("SELECT business, COUNT(*) as total FROM probe_tasks GROUP BY business").fetchall()]

    success_rate = (task_success / task_total * 100.0) if task_total else 0.0
    return {
        "node_total": node_total,
        "node_online": node_online,
        "task_total": task_total,
        "task_success": task_success,
        "success_rate": round(success_rate, 2),
        "avg_latency_ms": round(avg_latency or 0.0, 2),
        "open_alerts": open_alerts,
        "distribution_by_region": by_region,
        "distribution_by_business": by_business,
    }
