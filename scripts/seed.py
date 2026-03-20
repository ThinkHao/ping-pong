from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from app.db import get_conn, init_db


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    init_db()
    exporter_url = os.getenv("MOCK_EXPORTER_URL", "http://127.0.0.1:19115")
    prometheus_url = os.getenv("MOCK_PROM_URL", "http://127.0.0.1:19090")
    with get_conn() as conn:
        ts = now()
        conn.execute(
            """
            INSERT INTO exporter_nodes(name, region, base_url, status, tags, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET
                region=excluded.region,
                base_url=excluded.base_url,
                tags=excluded.tags,
                updated_at=excluded.updated_at
            """,
            ("node-local", "cn-east", exporter_url, "unknown", json.dumps({"env": "dev"}), ts, ts),
        )
        conn.execute(
            """
            INSERT INTO prometheus_instances(
                name, base_url, reload_path, auth_type, scope_region, output_path, status, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET
                base_url=excluded.base_url,
                reload_path=excluded.reload_path,
                auth_type=excluded.auth_type,
                scope_region=excluded.scope_region,
                output_path=excluded.output_path,
                status=excluded.status,
                updated_at=excluded.updated_at
            """,
            ("prom-local", prometheus_url, "/-/reload", "none", "cn-east", "published/prometheus.instance-1.yml", "active", ts, ts),
        )
        node = conn.execute("SELECT id FROM exporter_nodes WHERE name='node-local'").fetchone()
        node_id = node["id"] if node else None
        conn.execute(
            """
            INSERT OR IGNORE INTO probe_tasks(
                name,target,protocol,interval_seconds,timeout_seconds,business,region,node_id,labels_json,status,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "dns-google-icmp",
                "8.8.8.8",
                "icmp",
                60,
                5,
                "core-net",
                "cn-east",
                node_id,
                json.dumps({"service": "api"}),
                "active",
                ts,
                ts,
            ),
        )
        task_row = conn.execute("SELECT id FROM probe_tasks WHERE name='dns-google-icmp'").fetchone()
        task_id = task_row["id"] if task_row else None
        if task_id:
            conn.execute("DELETE FROM probe_task_targets WHERE task_id=?", (task_id,))
            conn.execute(
                "INSERT OR REPLACE INTO probe_task_targets(task_id, target, labels_json, created_at, updated_at) VALUES(?,?,?,?,?)",
                (task_id, "8.8.8.8", json.dumps({"tag": "google-dns"}), ts, ts),
            )
            conn.execute(
                "INSERT OR REPLACE INTO task_labels(task_id, key, value, created_at, updated_at) VALUES(?,?,?,?,?)",
                (task_id, "service", "api", ts, ts),
            )
        conn.commit()
    print("Seed done")


if __name__ == "__main__":
    main()
