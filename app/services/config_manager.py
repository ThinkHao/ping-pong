from __future__ import annotations

import difflib
from datetime import datetime, timezone
from pathlib import Path
import json
import shutil
from typing import Any

import httpx
import yaml

from app.db import get_conn

ROOT = Path(__file__).resolve().parents[2]
GENERATED_DIR = ROOT / "generated"
PUBLISHED_DIR = ROOT / "published"
HISTORY_DIR = ROOT / "history"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_active_tasks() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT t.*, n.base_url AS node_base_url, n.name AS node_name
            FROM probe_tasks t
            LEFT JOIN exporter_nodes n ON n.id = t.node_id
            WHERE t.status = 'active'
            ORDER BY t.business, t.region, t.name
            """
        ).fetchall()
    return [dict(r) for r in rows]


def _load_task_labels_map() -> dict[int, dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT task_id, key, value FROM task_labels").fetchall()
    out: dict[int, dict] = {}
    for item in rows:
        out.setdefault(item["task_id"], {})[item["key"]] = item["value"]
    return out


def _load_active_task_targets() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                t.id AS task_id,
                t.name AS task_name,
                t.protocol,
                t.business,
                t.region,
                t.node_id,
                n.base_url AS node_base_url,
                n.name AS node_name,
                tt.target,
                tt.labels_json
            FROM probe_tasks t
            JOIN probe_task_targets tt ON tt.task_id=t.id
            LEFT JOIN exporter_nodes n ON n.id=t.node_id
            WHERE t.status='active'
            ORDER BY t.business, t.region, t.name, tt.id
            """
        ).fetchall()
    return [dict(r) for r in rows]


def _filter_tasks(tasks: list[dict], instance: dict[str, Any] | None) -> list[dict]:
    if not instance:
        return tasks
    region = (instance.get("scope_region") or "").strip()
    business = (instance.get("scope_business") or "").strip()
    out = tasks
    if region:
        out = [t for t in out if t.get("region") == region]
    if business:
        out = [t for t in out if t.get("business") == business]
    return out


def generate_prometheus_config(instance: dict[str, Any] | None = None) -> str:
    tasks = _filter_tasks(_load_active_task_targets(), instance)
    task_labels_map = _load_task_labels_map()
    jobs: dict[str, dict] = {}
    for task in tasks:
        if not task["node_base_url"]:
            continue
        safe_node = str(task["node_name"] or task["node_id"]).replace(" ", "-")
        job_name = f"blackbox-{task['business']}-{task['region']}-{task['protocol']}-{safe_node}"
        module = task["protocol"]
        if job_name not in jobs:
            jobs[job_name] = {
                "job_name": job_name,
                "metrics_path": "/probe",
                "params": {"module": [module]},
                "static_configs": [],
                "relabel_configs": [
                    {"source_labels": ["__address__"], "target_label": "__param_target"},
                    {"source_labels": ["__param_target"], "target_label": "instance"},
                    {"target_label": "__address__", "replacement": task["node_base_url"].replace("http://", "").replace("https://", "")},
                ],
            }
        labels = {
            "task": task["task_name"],
            "business": task["business"],
            "region": task["region"],
        }
        labels.update(task_labels_map.get(task["task_id"], {}))
        labels.update(json.loads(task.get("labels_json") or "{}"))
        if task["node_name"]:
            labels["exporter_node"] = task["node_name"]
        jobs[job_name]["static_configs"].append(
            {
                "targets": [task["target"]],
                "labels": labels,
            }
        )

    content = {
        "global": {"scrape_interval": "15s", "evaluation_interval": "15s"},
        "scrape_configs": list(jobs.values()),
    }
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    suffix = f".instance-{instance['id']}" if instance else ""
    text = yaml.safe_dump(content, sort_keys=False, allow_unicode=True)
    (GENERATED_DIR / f"prometheus.generated{suffix}.yml").write_text(text, encoding="utf-8")
    return text


def get_latest_published(path: Path | None = None) -> str:
    target = path or (PUBLISHED_DIR / "prometheus.yml")
    if target.exists():
        return target.read_text(encoding="utf-8")
    return ""


def diff_against_published(new_content: str, published_path: Path | None = None, generated_name: str = "generated/prometheus.generated.yml") -> str:
    old_content = get_latest_published(published_path).splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)
    from_name = str(published_path) if published_path else "published/prometheus.yml"
    return "".join(difflib.unified_diff(old_content, new_lines, fromfile=from_name, tofile=generated_name))


def validate_yaml(content: str) -> tuple[bool, str]:
    try:
        yaml.safe_load(content)
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def _write_published(content: str, output_path: str | None) -> Path:
    PUBLISHED_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    target = Path(output_path) if output_path else (PUBLISHED_DIR / "prometheus.yml")
    if not target.is_absolute():
        target = ROOT / target
    target.parent.mkdir(parents=True, exist_ok=True)

    version = datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")
    if target.exists():
        backup = HISTORY_DIR / f"{version}-before-{target.name}"
        shutil.copy2(target, backup)

    target.write_text(content, encoding="utf-8")
    return target


def publish(content: str, note: str | None = None) -> str:
    target = _write_published(content, None)
    version = datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO config_versions(version, content, status, note, created_at) VALUES(?,?,?,?,?)",
            (version, content, "published", note or f"published to {target}", utc_now()),
        )
        conn.commit()
    return version


def publish_to_instance(instance: dict[str, Any], content: str, note: str | None = None) -> tuple[bool, str, str]:
    ok, message = validate_yaml(content)
    if not ok:
        return False, f"yaml invalid: {message}", ""

    target = _write_published(content, instance.get("output_path") or f"published/prometheus.instance-{instance['id']}.yml")
    reload_url = f"{instance['base_url'].rstrip('/')}{instance['reload_path']}"
    auth_type = (instance.get("auth_type") or "none").lower()

    try:
        with httpx.Client(timeout=8.0) as client:
            kwargs: dict[str, Any] = {}
            if auth_type == "basic" and instance.get("username") is not None:
                kwargs["auth"] = (instance.get("username") or "", instance.get("password") or "")
            if auth_type == "bearer" and instance.get("bearer_token"):
                kwargs["headers"] = {"Authorization": f"Bearer {instance['bearer_token']}"}
            resp = client.post(reload_url, **kwargs)
            if resp.status_code >= 300:
                return False, f"reload failed {resp.status_code}: {resp.text[:120]}", ""
    except Exception as exc:
        return False, f"reload request error: {exc}", ""

    version = datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO config_versions(version, content, status, note, created_at) VALUES(?,?,?,?,?)",
            (version, content, "published", note or f"instance={instance['name']} target={target}", utc_now()),
        )
        conn.commit()
    return True, f"published to {target} and reloaded {reload_url}", version


def rollback(version: str) -> bool:
    with get_conn() as conn:
        row = conn.execute("SELECT content FROM config_versions WHERE version=?", (version,)).fetchone()
        if not row:
            return False
        content = row["content"]
        PUBLISHED_DIR.mkdir(parents=True, exist_ok=True)
        (PUBLISHED_DIR / "prometheus.yml").write_text(content, encoding="utf-8")
        conn.execute(
            "INSERT INTO config_versions(version, content, status, note, created_at) VALUES(?,?,?,?,?)",
            (f"rollback-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}", content, "rollback", f"rollback to {version}", utc_now()),
        )
        conn.commit()
        return True
