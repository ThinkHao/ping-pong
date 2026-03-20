from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "app.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS exporter_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                region TEXT NOT NULL,
                base_url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'unknown',
                last_heartbeat TEXT,
                version TEXT,
                tags TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS label_schemas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL UNIQUE,
                required INTEGER NOT NULL DEFAULT 0,
                enum_values TEXT,
                regex_pattern TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS probe_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                target TEXT,
                protocol TEXT NOT NULL,
                interval_seconds INTEGER NOT NULL,
                timeout_seconds INTEGER NOT NULL,
                business TEXT NOT NULL,
                region TEXT NOT NULL,
                node_id INTEGER,
                labels_json TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                last_result TEXT,
                last_latency_ms REAL,
                last_checked_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(node_id) REFERENCES exporter_nodes(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS probe_task_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                target TEXT NOT NULL,
                labels_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(task_id, target),
                FOREIGN KEY(task_id) REFERENCES probe_tasks(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS task_labels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(task_id, key),
                FOREIGN KEY(task_id) REFERENCES probe_tasks(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_id INTEGER,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL,
                resolved_at TEXT
            );

            CREATE TABLE IF NOT EXISTS config_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version TEXT NOT NULL UNIQUE,
                content TEXT NOT NULL,
                status TEXT NOT NULL,
                note TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS prometheus_instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                base_url TEXT NOT NULL,
                reload_path TEXT NOT NULL DEFAULT '/-/reload',
                auth_type TEXT NOT NULL DEFAULT 'none',
                username TEXT,
                password TEXT,
                bearer_token TEXT,
                scope_region TEXT,
                scope_business TEXT,
                output_path TEXT,
                local_sync_path TEXT,
                deploy_host TEXT,
                deploy_port INTEGER,
                deploy_user TEXT,
                deploy_key_path TEXT,
                deploy_password TEXT,
                deploy_use_sudo INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS publish_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id INTEGER NOT NULL,
                version TEXT NOT NULL,
                content TEXT NOT NULL,
                status TEXT NOT NULL,
                requested_by TEXT,
                note TEXT,
                result_message TEXT,
                created_at TEXT NOT NULL,
                reviewed_at TEXT,
                reviewed_by TEXT,
                FOREIGN KEY(instance_id) REFERENCES prometheus_instances(id) ON DELETE CASCADE
            );
            """
        )

        # Backward compatibility: migrate legacy single-target tasks into task target groups.
        legacy_tasks = conn.execute(
            """
            SELECT t.id, t.target
            FROM probe_tasks t
            WHERE t.target IS NOT NULL AND t.target != ''
              AND NOT EXISTS (SELECT 1 FROM probe_task_targets tt WHERE tt.task_id = t.id)
            """
        ).fetchall()
        for item in legacy_tasks:
            now = conn.execute("SELECT datetime('now')").fetchone()[0]
            conn.execute(
                """
                INSERT OR IGNORE INTO probe_task_targets(task_id, target, labels_json, created_at, updated_at)
                VALUES(?,?,?,?,?)
                """,
                (item["id"], item["target"], "{}", now, now),
            )

        # Backward compatibility: add newly introduced columns when upgrading existing DB.
        cols = [r[1] for r in conn.execute("PRAGMA table_info(prometheus_instances)").fetchall()]
        if "local_sync_path" not in cols:
            conn.execute("ALTER TABLE prometheus_instances ADD COLUMN local_sync_path TEXT")
        if "deploy_host" not in cols:
            conn.execute("ALTER TABLE prometheus_instances ADD COLUMN deploy_host TEXT")
        if "deploy_port" not in cols:
            conn.execute("ALTER TABLE prometheus_instances ADD COLUMN deploy_port INTEGER")
        if "deploy_user" not in cols:
            conn.execute("ALTER TABLE prometheus_instances ADD COLUMN deploy_user TEXT")
        if "deploy_key_path" not in cols:
            conn.execute("ALTER TABLE prometheus_instances ADD COLUMN deploy_key_path TEXT")
        if "deploy_password" not in cols:
            conn.execute("ALTER TABLE prometheus_instances ADD COLUMN deploy_password TEXT")
        if "deploy_use_sudo" not in cols:
            conn.execute("ALTER TABLE prometheus_instances ADD COLUMN deploy_use_sudo INTEGER NOT NULL DEFAULT 0")
