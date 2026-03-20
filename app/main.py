from __future__ import annotations

import json
import posixpath
import re
import shlex
import difflib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from concurrent.futures import ThreadPoolExecutor
import httpx
import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.db import get_conn, init_db
from app.services.config_manager import (
    diff_against_published,
    generate_prometheus_config,
    get_latest_published,
    publish,
    publish_to_instance,
    rollback,
    validate_yaml,
)
from app.services.monitor import dashboard_summary, monitor_tick

BASE_DIR = Path(__file__).resolve().parents[1]
init_db()


class NodeIn(BaseModel):
    name: str
    region: str
    base_url: str
    tags: dict[str, Any] | None = None


class TaskIn(BaseModel):
    name: str
    target: str | None = None
    protocol: str = Field(pattern="^(icmp|tcp|http)$")
    interval_seconds: int = Field(ge=10, le=3600)
    timeout_seconds: int = Field(ge=1, le=60)
    business: str
    region: str
    node_id: int = Field(gt=0)
    labels: dict[str, str] | None = None
    targets: list[dict[str, Any]] | None = None


class PublishIn(BaseModel):
    note: str | None = None


class RollbackIn(BaseModel):
    version: str


class TaskLabelsIn(BaseModel):
    labels: dict[str, str]


class TaskTargetsIn(BaseModel):
    targets: list[dict[str, Any]]


class PromInstanceIn(BaseModel):
    name: str
    base_url: str
    reload_path: str = "/-/reload"
    auth_type: str = Field(pattern="^(none|basic|bearer)$")
    username: str | None = None
    password: str | None = None
    bearer_token: str | None = None
    scope_region: str | None = None
    scope_business: str | None = None
    remote_prom_path: str | None = None
    local_sync_path: str | None = None
    deploy_host: str | None = None
    deploy_port: int | None = 22
    deploy_user: str | None = None
    deploy_key_path: str | None = None
    deploy_password: str | None = None
    deploy_use_sudo: bool | None = False
    output_path: str | None = None
    status: str = Field(pattern="^(active|paused)$")


class IcmpTaskIn(BaseModel):
    job_name: str
    file_name: str
    node_id: int | None = None
    exporter_address: str | None = None
    scrape_interval: str | None = None
    targets: list[dict[str, Any]]


class PublishRequestIn(BaseModel):
    instance_id: int
    requested_by: str = "admin"
    note: str | None = None


class ReviewIn(BaseModel):
    reviewed_by: str = "admin"
    comment: str | None = None


app = FastAPI(title="Ping Pong Control Plane", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=BASE_DIR / "app" / "static"), name="static")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rows(sql: str, params: tuple = ()) -> list[dict]:
    with get_conn() as conn:
        data = conn.execute(sql, params).fetchall()
    return [dict(r) for r in data]


def row(sql: str, params: tuple = ()) -> dict | None:
    with get_conn() as conn:
        result = conn.execute(sql, params).fetchone()
    return dict(result) if result else None


def validate_node_binding(node_id: int) -> None:
    node = row("SELECT id FROM exporter_nodes WHERE id=?", (node_id,))
    if not node:
        raise HTTPException(status_code=400, detail=f"node_id `{node_id}` does not exist")


def get_task_labels(task_id: int) -> dict[str, str]:
    task_rows = rows("SELECT key, value FROM task_labels WHERE task_id=? ORDER BY key", (task_id,))
    return {r["key"]: r["value"] for r in task_rows}


def replace_task_labels(task_id: int, labels: dict[str, str]) -> None:
    now = utc_now()
    with get_conn() as conn:
        conn.execute("DELETE FROM task_labels WHERE task_id=?", (task_id,))
        for key, value in labels.items():
            conn.execute(
                "INSERT INTO task_labels(task_id, key, value, created_at, updated_at) VALUES(?,?,?,?,?)",
                (task_id, key, value, now, now),
            )
        conn.execute("UPDATE probe_tasks SET labels_json=?, updated_at=? WHERE id=?", (json.dumps(labels), now, task_id))
        conn.commit()


def get_task_targets(task_id: int) -> list[dict[str, Any]]:
    items = rows("SELECT target, labels_json FROM probe_task_targets WHERE task_id=? ORDER BY id", (task_id,))
    return [{"target": item["target"], "labels": json.loads(item["labels_json"] or "{}")} for item in items]


def replace_task_targets(task_id: int, targets: list[dict[str, Any]]) -> None:
    if not targets:
        raise HTTPException(status_code=400, detail="task must include at least one target")
    now = utc_now()
    with get_conn() as conn:
        conn.execute("DELETE FROM probe_task_targets WHERE task_id=?", (task_id,))
        for item in targets:
            target = str(item.get("target", "")).strip()
            if not target:
                continue
            labels = item.get("labels") or {}
            conn.execute(
                """
                INSERT INTO probe_task_targets(task_id, target, labels_json, created_at, updated_at)
                VALUES(?,?,?,?,?)
                """,
                (task_id, target, json.dumps(labels), now, now),
            )
        first = conn.execute("SELECT target FROM probe_task_targets WHERE task_id=? ORDER BY id LIMIT 1", (task_id,)).fetchone()
        conn.execute("UPDATE probe_tasks SET target=?, updated_at=? WHERE id=?", (first["target"] if first else None, now, task_id))
        conn.commit()


def get_instance_or_404(instance_id: int) -> dict:
    instance = row("SELECT * FROM prometheus_instances WHERE id=?", (instance_id,))
    if not instance:
        raise HTTPException(status_code=404, detail="prometheus instance not found")
    return instance


def instance_request(instance: dict, method: str, path: str, timeout: float = 2.5) -> tuple[bool, dict]:
    base = instance["base_url"].rstrip("/")
    url = f"{base}{path}"
    auth_type = (instance.get("auth_type") or "none").lower()
    kwargs: dict[str, Any] = {}
    if auth_type == "basic" and instance.get("username") is not None:
        kwargs["auth"] = (instance.get("username") or "", instance.get("password") or "")
    if auth_type == "bearer" and instance.get("bearer_token"):
        kwargs["headers"] = {"Authorization": f"Bearer {instance['bearer_token']}"}
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.request(method, url, **kwargs)
            if resp.status_code >= 300:
                return False, {"status_code": resp.status_code, "text": resp.text[:300]}
            ctype = resp.headers.get("content-type", "")
            if "application/json" in ctype:
                return True, resp.json()
            return True, {"text": resp.text}
    except Exception as exc:
        return False, {"error": str(exc)}


def parse_blackbox_version(metrics_text: str) -> str:
    for line in metrics_text.splitlines():
        if line.startswith("blackbox_exporter_build_info"):
            marker = 'version="'
            idx = line.find(marker)
            if idx >= 0:
                remain = line[idx + len(marker):]
                end = remain.find('"')
                if end >= 0:
                    return remain[:end]
    return ""


def normalize_target_address(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    return value.replace("http://", "").replace("https://", "").strip("/")


def resolve_instance_prom_path(instance: dict) -> Path:
    local_sync_path = (instance.get("local_sync_path") or "").strip()
    if not local_sync_path:
        raise HTTPException(status_code=400, detail="该实例未配置 local_sync_path（本地同步路径），无法本地读写配置文件")
    path = Path(local_sync_path)
    if not path.is_absolute():
        path = (BASE_DIR / path).resolve()
    return path


def load_yaml_file(path: Path) -> Any:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        return None
    return yaml.safe_load(text)


def save_yaml_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    path.write_text(text, encoding="utf-8")


def ensure_prometheus_file(path: Path) -> bool:
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    base = {
        "global": {"scrape_interval": "15s", "evaluation_interval": "15s"},
        "scrape_configs": [],
    }
    save_yaml_file(path, base)
    return True


def fetch_remote_prometheus_yaml(instance: dict) -> str:
    ok, resp = instance_request(instance, "GET", "/api/v1/status/config", timeout=4.0)
    if not ok or not isinstance(resp, dict):
        return ""
    data = resp.get("data", {}) if isinstance(resp.get("data"), dict) else {}
    text = data.get("yaml", "") or resp.get("text", "")
    return str(text or "")


def sync_local_prometheus_from_remote(instance: dict, prom_path: Path) -> tuple[bool, str]:
    text = fetch_remote_prometheus_yaml(instance).strip()
    if not text:
        return False, "远端未返回配置"
    try:
        parsed = yaml.safe_load(text)
    except Exception as exc:
        return False, f"远端配置解析失败: {exc}"
    if not isinstance(parsed, dict):
        return False, "远端配置不是合法的 Prometheus YAML 对象"
    save_yaml_file(prom_path, parsed)
    return True, "已从远端同步配置到本地"


def load_ssh_client():
    try:
        import paramiko  # type: ignore
    except Exception:
        raise HTTPException(status_code=500, detail="缺少 paramiko 依赖，请先安装 requirements.txt")
    return paramiko


def resolve_key_path(raw: str | None) -> str | None:
    path = (raw or "").strip()
    if not path:
        return None
    p = Path(path)
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    return str(p)


def is_windows_abs_path(path_text: str) -> bool:
    return bool(re.match(r"^[A-Za-z]:[\\/]", str(path_text or "").strip()))


def is_posix_abs_path(path_text: str) -> bool:
    return str(path_text or "").strip().startswith("/")


def normalize_sd_ref_for_edit(file_item: str) -> str:
    raw = str(file_item or "").strip().replace("\\", "/")
    if not raw:
        return ""
    if is_windows_abs_path(raw) or is_posix_abs_path(raw):
        return Path(raw).name
    return raw


def resolve_local_sd_path(file_item: str, prom_path: Path) -> Path:
    raw = str(file_item or "").strip().replace("\\", "/")
    if not raw:
        return (prom_path.parent / "ping_status.yml").resolve()
    if is_windows_abs_path(raw):
        return Path(raw)
    if is_posix_abs_path(raw):
        # Remote absolute path in Prometheus config; map to local sync dir by filename.
        return (prom_path.parent / Path(raw).name).resolve()
    return (prom_path.parent / raw).resolve()


def collect_local_publish_files(local_prom_path: Path, include_main: bool = True) -> list[tuple[Path, str]]:
    config = load_yaml_file(local_prom_path) or {}
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="本地 prometheus.yml 格式错误，无法发布")
    base_dir = local_prom_path.parent
    items: list[tuple[Path, str]] = []
    if include_main:
        items.append((local_prom_path, local_prom_path.name))
    seen = {str(local_prom_path.resolve())}
    for job in (config.get("scrape_configs") or []):
        if not isinstance(job, dict):
            continue
        for entry in (job.get("file_sd_configs") or []):
            if not isinstance(entry, dict):
                continue
            for raw in (entry.get("files") or []):
                rel = str(raw or "").replace("\\", "/").strip()
                if not rel:
                    continue
                local_path = resolve_local_sd_path(rel, local_prom_path)
                key = str(local_path)
                if key in seen:
                    continue
                seen.add(key)
                if not local_path.exists():
                    # missing file can happen during transition; skip with warning behavior handled by caller
                    continue
                remote_rel = normalize_sd_ref_for_edit(rel) or local_path.name
                items.append((local_path, remote_rel))
    return items


def fetch_active_config_file(instance: dict) -> str:
    ok, resp = instance_request(instance, "GET", "/api/v1/status/flags", timeout=4.0)
    if not ok or not isinstance(resp, dict):
        return ""
    data = resp.get("data", {}) if isinstance(resp.get("data"), dict) else {}
    path = data.get("config.file", "") or data.get("config.file".replace(".", "_"), "")
    return str(path or "").strip()


def infer_default_main_config(remote_prom: str) -> str:
    remote_prom = (remote_prom or "").strip()
    if not remote_prom:
        return ""
    if remote_prom.endswith("/prometheus.yml"):
        return remote_prom
    d = posixpath.dirname(remote_prom) or "."
    return posixpath.join(d, "prometheus.yml")


def remote_file_exists(client, path: str, use_sudo: bool) -> bool:
    test_cmd = f"test -f {shlex.quote(path)}"
    cmd = f"sudo sh -lc {shlex.quote(test_cmd)}" if use_sudo else test_cmd
    stdin, stdout, stderr = client.exec_command(cmd, timeout=8)
    code = stdout.channel.recv_exit_status()
    return code == 0


def merge_managed_icmp_jobs(active_config: dict, local_config: dict) -> tuple[dict, int]:
    base_jobs = active_config.get("scrape_configs")
    if not isinstance(base_jobs, list):
        base_jobs = []
    overlay_jobs = local_config.get("scrape_configs")
    if not isinstance(overlay_jobs, list):
        overlay_jobs = []
    managed = [j for j in overlay_jobs if isinstance(j, dict) and is_icmp_job(j)]
    idx_by_name = {
        str(j.get("job_name") or ""): idx
        for idx, j in enumerate(base_jobs)
        if isinstance(j, dict) and str(j.get("job_name") or "")
    }
    changed = 0
    for job in managed:
        name = str(job.get("job_name") or "")
        if not name:
            continue
        if name in idx_by_name:
            base_jobs[idx_by_name[name]] = job
        else:
            base_jobs.append(job)
            idx_by_name[name] = len(base_jobs) - 1
        changed += 1
    out = dict(active_config)
    out["scrape_configs"] = base_jobs
    return out, changed


def run_remote_cmd(client, cmd: str, timeout: int = 20) -> None:
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    code = stdout.channel.recv_exit_status()
    if code != 0:
        err = (stderr.read().decode("utf-8", errors="ignore") or "").strip()
        raise HTTPException(status_code=400, detail=f"远端执行失败: {err or cmd}")


def upload_remote_file(client, sftp, local_path: Path, remote_target: str, use_sudo: bool) -> None:
    remote_target = remote_target.replace("\\", "/")
    remote_target_dir = posixpath.dirname(remote_target) or "."
    remote_tmp = f"/tmp/pingpong.{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.{Path(remote_target).name}"
    sftp.put(str(local_path), remote_tmp)
    mkdir_cmd = f"mkdir -p {shlex.quote(remote_target_dir)}"
    move_cmd = f"cp {shlex.quote(remote_tmp)} {shlex.quote(remote_target)} && rm -f {shlex.quote(remote_tmp)}"
    cmd = f"{mkdir_cmd} && {move_cmd}"
    if use_sudo:
        cmd = f"sudo sh -lc {shlex.quote(cmd)}"
    run_remote_cmd(client, cmd, timeout=30)


def resolve_active_config_path(instance: dict, client=None) -> str:
    remote_prom = (instance.get("output_path") or "").strip()
    by_flags = fetch_active_config_file(instance)
    if by_flags:
        return by_flags
    candidate = infer_default_main_config(remote_prom)
    if client and candidate:
        try:
            if remote_file_exists(client, candidate, bool(instance.get("deploy_use_sudo"))):
                return candidate
        except Exception:
            pass
    return remote_prom or candidate


def build_remote_publish_preview(instance: dict) -> dict:
    local_prom = resolve_instance_prom_path(instance)
    ensure_prometheus_file(local_prom)
    local_cfg = load_yaml_file(local_prom) or {}
    if not isinstance(local_cfg, dict):
        raise HTTPException(status_code=400, detail="本地 prometheus.yml 格式错误，无法预览发布")
    remote_prom = (instance.get("output_path") or "").strip()
    if not remote_prom:
        raise HTTPException(status_code=400, detail="请先配置 remote_prom_path")

    active_config_path = resolve_active_config_path(instance)
    active_yaml = fetch_remote_prometheus_yaml(instance).strip()
    active_cfg = yaml.safe_load(active_yaml) if active_yaml else {}
    if not isinstance(active_cfg, dict):
        active_cfg = {}

    if active_config_path == remote_prom:
        next_cfg = local_cfg
    else:
        next_cfg, _ = merge_managed_icmp_jobs(active_cfg, local_cfg)

    before_text = yaml.safe_dump(active_cfg, sort_keys=False, allow_unicode=True) if active_cfg else ""
    after_text = yaml.safe_dump(next_cfg, sort_keys=False, allow_unicode=True)
    diff = "".join(
        difflib.unified_diff(
            before_text.splitlines(keepends=True),
            after_text.splitlines(keepends=True),
            fromfile=f"active:{active_config_path}",
            tofile=f"publish:{active_config_path}",
        )
    )
    local_files = collect_local_publish_files(local_prom, include_main=False)
    upload_files = [{"local": str(local_prom), "remote": remote_prom}]
    remote_dir = posixpath.dirname(remote_prom) or "."
    for p, rel in local_files:
        upload_files.append({"local": str(p), "remote": posixpath.join(remote_dir, rel.replace("\\", "/"))})
    return {
        "ok": True,
        "active_config_path": active_config_path,
        "remote_prom_path": remote_prom,
        "will_merge_to_active": active_config_path != remote_prom,
        "upload_files": upload_files,
        "diff": diff or "(无差异)",
    }


def publish_local_to_remote(instance: dict) -> dict:
    local_prom = resolve_instance_prom_path(instance)
    ensure_prometheus_file(local_prom)
    remote_prom = (instance.get("output_path") or "").strip()
    if not remote_prom:
        raise HTTPException(status_code=400, detail="请先配置 remote_prom_path")
    deploy_host = (instance.get("deploy_host") or "").strip()
    deploy_user = (instance.get("deploy_user") or "").strip()
    if not deploy_host or not deploy_user:
        raise HTTPException(status_code=400, detail="请先配置 deploy_host 和 deploy_user")

    files = collect_local_publish_files(local_prom, include_main=False)
    remote_dir = posixpath.dirname(remote_prom) or "."
    use_sudo = bool(instance.get("deploy_use_sudo"))
    port = int(instance.get("deploy_port") or 22)
    key_path = resolve_key_path(instance.get("deploy_key_path"))
    password = instance.get("deploy_password") or None
    local_config = load_yaml_file(local_prom) or {}
    if not isinstance(local_config, dict):
        raise HTTPException(status_code=400, detail="本地 prometheus.yml 格式错误")
    active_config_path = remote_prom

    paramiko = load_ssh_client()
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        connect_kwargs: dict[str, Any] = {
            "hostname": deploy_host,
            "port": port,
            "username": deploy_user,
            "timeout": 10,
            "look_for_keys": key_path is None and password is None,
        }
        if key_path:
            connect_kwargs["key_filename"] = key_path
        if password:
            connect_kwargs["password"] = password
        client.connect(**connect_kwargs)
        sftp = client.open_sftp()
        active_config_path = resolve_active_config_path(instance, client)
        transferred: list[dict[str, str]] = []
        missing: list[str] = []
        # Always publish the local main config to configured remote_prom_path.
        upload_remote_file(client, sftp, local_prom, remote_prom, use_sudo)
        transferred.append({"local": str(local_prom), "remote": remote_prom})
        for local_path, remote_rel in files:
            if not local_path.exists():
                missing.append(str(local_path))
                continue
            rel = remote_rel.replace("\\", "/")
            remote_target = posixpath.join(remote_dir, rel)
            upload_remote_file(client, sftp, local_path, remote_target, use_sudo)
            transferred.append({"local": str(local_path), "remote": remote_target})

        # If Prometheus actually reads another config file, merge managed icmp jobs into active one.
        merged_jobs = 0
        merged_written = False
        if active_config_path and active_config_path != remote_prom:
            active_yaml = fetch_remote_prometheus_yaml(instance).strip()
            if active_yaml:
                try:
                    active_cfg = yaml.safe_load(active_yaml) or {}
                    if isinstance(active_cfg, dict):
                        merged_cfg, merged_jobs = merge_managed_icmp_jobs(active_cfg, local_config)
                        tmp_local = BASE_DIR / "generated" / f"prometheus.active-merge.instance-{instance['id']}.yml"
                        save_yaml_file(tmp_local, merged_cfg)
                        upload_remote_file(client, sftp, tmp_local, active_config_path, use_sudo)
                        transferred.append({"local": str(tmp_local), "remote": active_config_path})
                        merged_written = True
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=f"合并到生效主配置失败: {exc}")
        sftp.close()
    finally:
        client.close()

    ok_reload, reload_resp = instance_request(instance, "POST", instance.get("reload_path") or "/-/reload", timeout=6.0)
    return {
        "ok": True,
        "remote_prom_path": remote_prom,
        "active_config_path": active_config_path,
        "remote_dir": remote_dir,
        "transferred": transferred,
        "missing_local_files": missing,
        "merged_to_active": merged_written,
        "merged_job_count": merged_jobs,
        "reload_ok": ok_reload,
        "reload_detail": reload_resp,
    }


def fetch_runtime_targets_map(instance: dict) -> dict[str, list[dict[str, Any]]]:
    ok, resp = instance_request(instance, "GET", "/api/v1/targets?state=active", timeout=5.0)
    if not ok or not isinstance(resp, dict):
        return {}
    data = resp.get("data", {}) if isinstance(resp.get("data"), dict) else {}
    active = data.get("activeTargets") or []
    if not isinstance(active, list):
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    system_keys = {
        "job",
        "instance",
        "business",
        "region",
        "task",
        "exporter_node",
        "module",
        "ping",
    }
    for item in active:
        if not isinstance(item, dict):
            continue
        labels = item.get("labels") if isinstance(item.get("labels"), dict) else {}
        job_name = str(labels.get("job") or "").strip()
        target = str(labels.get("instance") or "").strip()
        if not job_name or not target:
            continue
        extra = {
            str(k): str(v)
            for k, v in labels.items()
            if not str(k).startswith("__") and str(k) not in system_keys
        }
        out.setdefault(job_name, []).append({"target": target, "labels": extra})
    return out


def is_icmp_job(job: dict[str, Any]) -> bool:
    if not isinstance(job, dict):
        return False
    if str(job.get("metrics_path") or "") != "/probe":
        return False
    params = job.get("params") or {}
    module = params.get("module")
    modules: list[str] = []
    if isinstance(module, list):
        modules = [str(x).lower() for x in module]
    elif module is not None:
        modules = [str(module).lower()]
    return "icmp" in modules


def find_replacement_address(job: dict[str, Any]) -> str:
    for rule in (job.get("relabel_configs") or []):
        if rule.get("target_label") == "__address__":
            return str(rule.get("replacement") or "")
    return ""


def target_records_from_sd_file(sd_path: Path) -> list[dict[str, Any]]:
    data = load_yaml_file(sd_path)
    if not data:
        return []
    if not isinstance(data, list):
        raise HTTPException(status_code=400, detail=f"目标文件格式错误: {sd_path}")
    out: list[dict[str, Any]] = []
    for block in data:
        if not isinstance(block, dict):
            continue
        labels = block.get("labels") or {}
        if not isinstance(labels, dict):
            labels = {}
        for target in block.get("targets") or []:
            target_text = str(target).strip()
            if not target_text:
                continue
            out.append({"target": target_text, "labels": {str(k): str(v) for k, v in labels.items()}})
    return out


def normalize_label_key(raw_key: str) -> str:
    key = str(raw_key or "").strip()
    if not key:
        return ""
    # Prometheus label name: [a-zA-Z_][a-zA-Z0-9_]*
    key = re.sub(r"[^a-zA-Z0-9_]", "_", key)
    if not re.match(r"^[a-zA-Z_]", key):
        key = f"_{key}"
    key = re.sub(r"_+", "_", key)
    if key.startswith("__"):
        key = f"user{key}"
    return key


def normalize_labels_map(labels: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (labels or {}).items():
        nk = normalize_label_key(str(k))
        if not nk:
            continue
        out[nk] = str(v)
    return out


def write_sd_file(sd_path: Path, targets: list[dict[str, Any]]) -> None:
    if not targets:
        raise HTTPException(status_code=400, detail="targets 不能为空")
    payload: list[dict[str, Any]] = []
    for item in targets:
        target = str(item.get("target") or "").strip()
        if not target:
            continue
        labels = item.get("labels") or {}
        if not isinstance(labels, dict):
            labels = {}
        payload.append({"targets": [target], "labels": normalize_labels_map(labels)})
    if not payload:
        raise HTTPException(status_code=400, detail="targets 不能为空")
    save_yaml_file(sd_path, payload)


def relative_to_prom_dir(path: Path, prom_path: Path) -> str:
    try:
        return str(path.relative_to(prom_path.parent)).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def sanitize_file_name(file_name: str) -> str:
    name = str(file_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="file_name 不能为空")
    if re.search(r"[<>:\"|?*]", name):
        raise HTTPException(status_code=400, detail="file_name 包含非法字符")
    if ".." in name:
        raise HTTPException(status_code=400, detail="file_name 不允许包含 ..")
    if not name.endswith((".yml", ".yaml")):
        raise HTTPException(status_code=400, detail="file_name 必须是 .yml 或 .yaml")
    return name.replace("\\", "/")


def resolve_icmp_exporter(instance: dict, payload: IcmpTaskIn) -> tuple[str, int | None]:
    if payload.node_id:
        validate_node_binding(payload.node_id)
        node = row("SELECT * FROM exporter_nodes WHERE id=?", (payload.node_id,))
        if not node:
            raise HTTPException(status_code=400, detail="无效 node_id")
        return normalize_target_address(node["base_url"]), int(node["id"])
    if payload.exporter_address:
        address = normalize_target_address(payload.exporter_address)
        if not address:
            raise HTTPException(status_code=400, detail="exporter_address 无效")
        node_guess = row("SELECT id FROM exporter_nodes WHERE replace(replace(base_url,'http://',''),'https://','')=?", (address,))
        return address, (int(node_guess["id"]) if node_guess else None)
    raise HTTPException(status_code=400, detail="请提供 node_id 或 exporter_address")


def load_prom_icmp_tasks(instance: dict, sync_remote: bool = False) -> dict:
    prom_path = resolve_instance_prom_path(instance)
    synced = False
    sync_message = ""
    if sync_remote:
        synced, sync_message = sync_local_prometheus_from_remote(instance, prom_path)
    created = ensure_prometheus_file(prom_path)
    config = load_yaml_file(prom_path) or {}
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="prometheus.yml 格式错误")
    jobs = config.get("scrape_configs") or []
    if not isinstance(jobs, list):
        jobs = []
    runtime_targets_map = fetch_runtime_targets_map(instance)
    nodes = rows("SELECT id, name, region, base_url FROM exporter_nodes ORDER BY id DESC")
    node_by_addr = {normalize_target_address(n["base_url"]): n for n in nodes}
    tasks: list[dict[str, Any]] = []
    for job in jobs:
        if not is_icmp_job(job):
            continue
        exporter_address = find_replacement_address(job)
        node = node_by_addr.get(exporter_address)
        file_sd = job.get("file_sd_configs") or []
        files: list[str] = []
        for entry in file_sd:
            if isinstance(entry, dict):
                files.extend([str(f) for f in (entry.get("files") or []) if str(f).strip()])
        if not files:
            static_targets: list[dict[str, Any]] = []
            for item in (job.get("static_configs") or []):
                labels = item.get("labels") if isinstance(item, dict) else {}
                labels = labels if isinstance(labels, dict) else {}
                for target in (item.get("targets") or []):
                    static_targets.append({"target": str(target), "labels": {str(k): str(v) for k, v in labels.items()}})
            tasks.append(
                {
                    "job_name": job.get("job_name"),
                    "file_name": "",
                    "file_path": "",
                    "editable": False,
                    "reason": "该任务使用 static_configs，不是 file_sd 目标组",
                    "module": "icmp",
                    "scrape_interval": job.get("scrape_interval") or "",
                    "exporter_address": exporter_address,
                    "node_id": node["id"] if node else None,
                    "node_name": node["name"] if node else "",
                    "targets": static_targets,
                }
            )
            continue
        for file_item in files:
            sd_path = resolve_local_sd_path(file_item, prom_path)
            targets = target_records_from_sd_file(sd_path) if sd_path.exists() else []
            if not targets:
                targets = runtime_targets_map.get(str(job.get("job_name") or ""), [])
            tasks.append(
                {
                    "job_name": job.get("job_name"),
                    "file_name": normalize_sd_ref_for_edit(file_item),
                    "file_path": str(sd_path),
                    "editable": True,
                    "module": "icmp",
                    "scrape_interval": job.get("scrape_interval") or "",
                    "exporter_address": exporter_address,
                    "node_id": node["id"] if node else None,
                    "node_name": node["name"] if node else "",
                    "targets": targets,
                }
            )
    return {
        "prometheus_path": str(prom_path),
        "task_count": len(tasks),
        "tasks": tasks,
        "auto_initialized": created,
        "synced_from_remote": synced,
        "message": sync_message or ("本地文件不存在，已自动初始化空配置" if created else "ok"),
    }


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.on_event("shutdown")
def shutdown() -> None:
    return None


@app.get("/")
def index() -> FileResponse:
    return FileResponse(BASE_DIR / "app" / "static" / "index.html")


@app.get("/api/nodes")
def list_nodes() -> list[dict]:
    return rows("SELECT * FROM exporter_nodes ORDER BY id DESC")


@app.post("/api/nodes")
def create_node(payload: NodeIn) -> dict:
    now = utc_now()
    with get_conn() as conn:
        try:
            cur = conn.execute(
                """
                INSERT INTO exporter_nodes(name, region, base_url, status, tags, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?)
                """,
                (payload.name, payload.region, payload.base_url, "unknown", json.dumps(payload.tags or {}), now, now),
            )
            conn.commit()
            new_id = cur.lastrowid
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    return {"id": new_id}


@app.put("/api/nodes/{node_id}")
def update_node(node_id: int, payload: NodeIn) -> dict:
    with get_conn() as conn:
        conn.execute(
            "UPDATE exporter_nodes SET name=?, region=?, base_url=?, tags=?, updated_at=? WHERE id=?",
            (payload.name, payload.region, payload.base_url, json.dumps(payload.tags or {}), utc_now(), node_id),
        )
        conn.commit()
    return {"ok": True}


@app.delete("/api/nodes/{node_id}")
def delete_node(node_id: int) -> dict:
    with get_conn() as conn:
        conn.execute("DELETE FROM exporter_nodes WHERE id=?", (node_id,))
        conn.commit()
    return {"ok": True}


@app.get("/api/nodes/{node_id}/status")
def node_status(node_id: int) -> dict:
    node = row("SELECT * FROM exporter_nodes WHERE id=?", (node_id,))
    if not node:
        raise HTTPException(status_code=404, detail="node not found")
    base_url = str(node["base_url"]).rstrip("/")
    ok, resp = instance_request({"base_url": base_url, "auth_type": "none"}, "GET", "/metrics", timeout=2.5)
    metrics_text = resp.get("text", "") if isinstance(resp, dict) else ""
    version = parse_blackbox_version(metrics_text) if ok else ""
    preview_lines = "\n".join(metrics_text.splitlines()[:80]) if metrics_text else ""
    return {
        "node": {
            "id": node["id"],
            "name": node["name"],
            "region": node["region"],
            "base_url": node["base_url"],
            "status": node["status"],
            "last_heartbeat": node["last_heartbeat"],
            "version": version or node.get("version"),
        },
        "connectivity": {
            "online": ok,
            "detail": resp,
        },
        "metrics_preview": preview_lines,
    }


@app.get("/api/tasks")
def list_tasks() -> list[dict]:
    result = rows(
        """
        SELECT t.*, n.name AS node_name,
               (SELECT COUNT(*) FROM probe_task_targets tt WHERE tt.task_id=t.id) AS target_count
        FROM probe_tasks t
        LEFT JOIN exporter_nodes n ON n.id = t.node_id
        ORDER BY t.id DESC
        """
    )
    for item in result:
        item["labels"] = get_task_labels(item["id"])
        item["targets"] = get_task_targets(item["id"])
    return result


@app.post("/api/tasks")
def create_task(payload: TaskIn) -> dict:
    validate_node_binding(payload.node_id)
    targets = payload.targets or ([{"target": payload.target, "labels": {}}] if payload.target else [])
    if not targets:
        raise HTTPException(status_code=400, detail="task must include targets")
    now = utc_now()
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO probe_tasks(
                name, target, protocol, interval_seconds, timeout_seconds,
                business, region, node_id, labels_json, status, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                payload.name,
                targets[0]["target"],
                payload.protocol,
                payload.interval_seconds,
                payload.timeout_seconds,
                payload.business,
                payload.region,
                payload.node_id,
                json.dumps(payload.labels or {}),
                "active",
                now,
                now,
            ),
        )
        conn.commit()
        task_id = cur.lastrowid
    replace_task_targets(task_id, targets)
    replace_task_labels(task_id, payload.labels or {})
    return {"id": task_id}


@app.put("/api/tasks/{task_id}")
def update_task(task_id: int, payload: TaskIn) -> dict:
    validate_node_binding(payload.node_id)
    targets = payload.targets or ([{"target": payload.target, "labels": {}}] if payload.target else [])
    if not targets:
        raise HTTPException(status_code=400, detail="task must include targets")
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE probe_tasks SET
                name=?, target=?, protocol=?, interval_seconds=?, timeout_seconds=?,
                business=?, region=?, node_id=?, labels_json=?, updated_at=?
            WHERE id=?
            """,
            (
                payload.name,
                payload.target,
                payload.protocol,
                payload.interval_seconds,
                payload.timeout_seconds,
                payload.business,
                payload.region,
                payload.node_id,
                json.dumps(payload.labels or {}),
                utc_now(),
                task_id,
            ),
        )
        conn.commit()
    replace_task_targets(task_id, targets)
    replace_task_labels(task_id, payload.labels or {})
    return {"ok": True}


@app.delete("/api/tasks/{task_id}")
def delete_task(task_id: int) -> dict:
    with get_conn() as conn:
        conn.execute("DELETE FROM probe_tasks WHERE id=?", (task_id,))
        conn.commit()
    return {"ok": True}


@app.get("/api/tasks/{task_id}/labels")
def list_task_labels(task_id: int) -> dict:
    exists = row("SELECT id FROM probe_tasks WHERE id=?", (task_id,))
    if not exists:
        raise HTTPException(status_code=404, detail="task not found")
    return {"task_id": task_id, "labels": get_task_labels(task_id)}


@app.get("/api/tasks/{task_id}/targets")
def list_task_targets(task_id: int) -> dict:
    exists = row("SELECT id FROM probe_tasks WHERE id=?", (task_id,))
    if not exists:
        raise HTTPException(status_code=404, detail="task not found")
    return {"task_id": task_id, "targets": get_task_targets(task_id)}


@app.put("/api/tasks/{task_id}/targets")
def put_task_targets(task_id: int, payload: TaskTargetsIn) -> dict:
    exists = row("SELECT id FROM probe_tasks WHERE id=?", (task_id,))
    if not exists:
        raise HTTPException(status_code=404, detail="task not found")
    replace_task_targets(task_id, payload.targets)
    return {"ok": True}


@app.put("/api/tasks/{task_id}/labels")
def put_task_labels(task_id: int, payload: TaskLabelsIn) -> dict:
    exists = row("SELECT id FROM probe_tasks WHERE id=?", (task_id,))
    if not exists:
        raise HTTPException(status_code=404, detail="task not found")
    replace_task_labels(task_id, payload.labels)
    return {"ok": True}


@app.delete("/api/tasks/{task_id}/labels/{label_key}")
def delete_task_label(task_id: int, label_key: str) -> dict:
    exists = row("SELECT id FROM probe_tasks WHERE id=?", (task_id,))
    if not exists:
        raise HTTPException(status_code=404, detail="task not found")
    labels = get_task_labels(task_id)
    if label_key in labels:
        del labels[label_key]
        replace_task_labels(task_id, labels)
    return {"ok": True}


@app.post("/api/monitor/tick")
def monitor_once() -> dict:
    monitor_tick()
    return {"ok": True}


@app.get("/api/alerts")
def list_alerts() -> list[dict]:
    return rows("SELECT * FROM alerts ORDER BY id DESC LIMIT 200")


@app.post("/api/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: int) -> dict:
    with get_conn() as conn:
        conn.execute("UPDATE alerts SET status='resolved', resolved_at=? WHERE id=?", (utc_now(), alert_id))
        conn.commit()
    return {"ok": True}


@app.get("/api/dashboard")
def dashboard() -> dict:
    return dashboard_summary()


@app.get("/api/prometheus/instances")
def list_prom_instances() -> list[dict]:
    result = rows("SELECT * FROM prometheus_instances ORDER BY id DESC")
    for item in result:
        item["remote_prom_path"] = item.get("output_path")
        item["local_sync_path"] = item.get("local_sync_path")
        if item.get("password"):
            item["password"] = "******"
        if item.get("bearer_token"):
            item["bearer_token"] = "******"
        if item.get("deploy_password"):
            item["deploy_password"] = "******"
    return result


@app.post("/api/prometheus/instances")
def create_prom_instance(payload: PromInstanceIn) -> dict:
    now = utc_now()
    remote_prom_path = (payload.remote_prom_path or payload.output_path or "").strip() or None
    local_sync_path = (payload.local_sync_path or "").strip() or None
    deploy_key_path = (payload.deploy_key_path or "").strip() or None
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO prometheus_instances(
                name, base_url, reload_path, auth_type, username, password, bearer_token,
                scope_region, scope_business, output_path, local_sync_path,
                deploy_host, deploy_port, deploy_user, deploy_key_path, deploy_password, deploy_use_sudo,
                status, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                payload.name,
                payload.base_url,
                payload.reload_path,
                payload.auth_type,
                payload.username,
                payload.password,
                payload.bearer_token,
                payload.scope_region,
                payload.scope_business,
                remote_prom_path,
                local_sync_path,
                (payload.deploy_host or "").strip() or None,
                payload.deploy_port or 22,
                (payload.deploy_user or "").strip() or None,
                deploy_key_path,
                payload.deploy_password,
                1 if payload.deploy_use_sudo else 0,
                payload.status,
                now,
                now,
            ),
        )
        conn.commit()
    return {"id": cur.lastrowid}


@app.put("/api/prometheus/instances/{instance_id}")
def update_prom_instance(instance_id: int, payload: PromInstanceIn) -> dict:
    get_instance_or_404(instance_id)
    remote_prom_path = (payload.remote_prom_path or payload.output_path or "").strip() or None
    local_sync_path = (payload.local_sync_path or "").strip() or None
    deploy_key_path = (payload.deploy_key_path or "").strip() or None
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE prometheus_instances SET
                name=?, base_url=?, reload_path=?, auth_type=?, username=?, password=?, bearer_token=?,
                scope_region=?, scope_business=?, output_path=?, local_sync_path=?,
                deploy_host=?, deploy_port=?, deploy_user=?, deploy_key_path=?, deploy_password=?, deploy_use_sudo=?,
                status=?, updated_at=?
            WHERE id=?
            """,
            (
                payload.name,
                payload.base_url,
                payload.reload_path,
                payload.auth_type,
                payload.username,
                payload.password,
                payload.bearer_token,
                payload.scope_region,
                payload.scope_business,
                remote_prom_path,
                local_sync_path,
                (payload.deploy_host or "").strip() or None,
                payload.deploy_port or 22,
                (payload.deploy_user or "").strip() or None,
                deploy_key_path,
                payload.deploy_password,
                1 if payload.deploy_use_sudo else 0,
                payload.status,
                utc_now(),
                instance_id,
            ),
        )
        conn.commit()
    return {"ok": True}


@app.delete("/api/prometheus/instances/{instance_id}")
def delete_prom_instance(instance_id: int) -> dict:
    with get_conn() as conn:
        conn.execute("DELETE FROM prometheus_instances WHERE id=?", (instance_id,))
        conn.commit()
    return {"ok": True}


@app.post("/api/prometheus/instances/{instance_id}/publish-remote")
def publish_remote(instance_id: int) -> dict:
    instance = get_instance_or_404(instance_id)
    return publish_local_to_remote(instance)


@app.get("/api/prometheus/instances/{instance_id}/publish-remote/preview")
def publish_remote_preview(instance_id: int) -> dict:
    instance = get_instance_or_404(instance_id)
    return build_remote_publish_preview(instance)


@app.get("/api/prometheus/instances/{instance_id}/publish-preview")
def publish_remote_preview_alias(instance_id: int) -> dict:
    instance = get_instance_or_404(instance_id)
    return build_remote_publish_preview(instance)


@app.get("/api/prometheus/instances/{instance_id}/status")
def prom_instance_status(instance_id: int) -> dict:
    instance = get_instance_or_404(instance_id)
    with ThreadPoolExecutor(max_workers=4) as executor:
        fut_healthy = executor.submit(instance_request, instance, "GET", "/-/healthy")
        fut_build = executor.submit(instance_request, instance, "GET", "/api/v1/status/buildinfo")
        fut_runtime = executor.submit(instance_request, instance, "GET", "/api/v1/status/runtimeinfo")
        fut_config = executor.submit(instance_request, instance, "GET", "/api/v1/status/config")
        healthy_ok, healthy = fut_healthy.result()
        build_ok, build = fut_build.result()
        runtime_ok, runtime = fut_runtime.result()
        config_ok, config_data = fut_config.result()

    build_info = build.get("data", {}) if isinstance(build, dict) else {}
    runtime_info = runtime.get("data", {}) if isinstance(runtime, dict) else {}
    config_text = ""
    if isinstance(config_data, dict):
        config_text = config_data.get("data", {}).get("yaml", "") or config_data.get("text", "")
    local_config = ""
    local_path = ""
    try:
        prom_path = resolve_instance_prom_path(instance)
        local_path = str(prom_path)
        if prom_path.exists():
            local_config = prom_path.read_text(encoding="utf-8")
    except HTTPException:
        pass

    return {
        "instance": {
            "id": instance["id"],
            "name": instance["name"],
            "base_url": instance["base_url"],
            "remote_prom_path": instance.get("output_path"),
            "local_sync_path": instance.get("local_sync_path"),
            "active_config_path": resolve_active_config_path(instance),
            "scope_region": instance.get("scope_region"),
            "scope_business": instance.get("scope_business"),
            "status": instance["status"],
        },
        "connectivity": {
            "healthy": healthy_ok,
            "detail": healthy,
        },
        "buildinfo": build_info,
        "runtimeinfo": runtime_info,
        "config": {
            "available": config_ok,
            "preview": config_text,
            "local_path": local_path,
            "local_preview": local_config,
        },
    }


@app.post("/api/prometheus/instances/{instance_id}/reload")
def reload_prom_instance(instance_id: int) -> dict:
    instance = get_instance_or_404(instance_id)
    ok, resp = instance_request(instance, "POST", instance.get("reload_path") or "/-/reload", timeout=4.0)
    if not ok:
        raise HTTPException(status_code=400, detail=f"reload 失败: {resp}")
    return {"ok": True, "message": "reload requested", "detail": resp}


@app.get("/api/prometheus/instances/{instance_id}/icmp-tasks")
def list_prom_icmp_tasks(instance_id: int, sync_remote: bool = Query(default=False)) -> dict:
    instance = get_instance_or_404(instance_id)
    try:
        data = load_prom_icmp_tasks(instance, sync_remote=sync_remote)
        data["manageable"] = True
        return data
    except HTTPException as exc:
        detail = str(exc.detail)
        if "local_sync_path" in detail:
            return {
                "prometheus_path": "",
                "task_count": 0,
                "tasks": [],
                "manageable": False,
                "message": "当前实例未配置本地同步路径，ICMP任务仅可查看状态；如需编辑请配置 local_sync_path",
            }
        if "prometheus.yml 不存在" in detail:
            return {
                "prometheus_path": "",
                "task_count": 0,
                "tasks": [],
                "manageable": False,
                "message": f"{detail}。请确认 local_sync_path 指向本机存在的文件，或先创建该文件。",
            }
        raise


@app.post("/api/prometheus/instances/{instance_id}/icmp-tasks")
def create_prom_icmp_task(instance_id: int, payload: IcmpTaskIn) -> dict:
    instance = get_instance_or_404(instance_id)
    prom_path = resolve_instance_prom_path(instance)
    config = load_yaml_file(prom_path) or {}
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="prometheus.yml 格式错误")
    scrape_configs = config.get("scrape_configs")
    if scrape_configs is None:
        scrape_configs = []
        config["scrape_configs"] = scrape_configs
    if not isinstance(scrape_configs, list):
        raise HTTPException(status_code=400, detail="prometheus.yml 中 scrape_configs 必须是列表")
    if any(str(j.get("job_name") or "") == payload.job_name for j in scrape_configs if isinstance(j, dict)):
        raise HTTPException(status_code=400, detail=f"job_name 已存在: {payload.job_name}")

    file_name = sanitize_file_name(payload.file_name)
    sd_path = (prom_path.parent / file_name).resolve()
    exporter_address, node_id = resolve_icmp_exporter(instance, payload)
    write_sd_file(sd_path, payload.targets)

    job: dict[str, Any] = {
        "job_name": payload.job_name,
        "metrics_path": "/probe",
        "params": {"module": ["icmp"]},
        "file_sd_configs": [{"files": [relative_to_prom_dir(sd_path, prom_path)]}],
        "relabel_configs": [
            {"source_labels": ["__address__"], "target_label": "__param_target"},
            {"source_labels": ["__param_target"], "target_label": "instance"},
            {"target_label": "__address__", "replacement": exporter_address},
        ],
    }
    if payload.scrape_interval:
        job["scrape_interval"] = payload.scrape_interval
    scrape_configs.append(job)
    save_yaml_file(prom_path, config)
    return {
        "ok": True,
        "job_name": payload.job_name,
        "file_name": relative_to_prom_dir(sd_path, prom_path),
        "node_id": node_id,
        "exporter_address": exporter_address,
    }


@app.put("/api/prometheus/instances/{instance_id}/icmp-tasks/{job_name}")
def update_prom_icmp_task(instance_id: int, job_name: str, payload: IcmpTaskIn) -> dict:
    instance = get_instance_or_404(instance_id)
    prom_path = resolve_instance_prom_path(instance)
    config = load_yaml_file(prom_path) or {}
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="prometheus.yml 格式错误")
    scrape_configs = config.get("scrape_configs") or []
    if not isinstance(scrape_configs, list):
        raise HTTPException(status_code=400, detail="prometheus.yml 中 scrape_configs 必须是列表")

    matched: dict[str, Any] | None = None
    for j in scrape_configs:
        if isinstance(j, dict) and str(j.get("job_name") or "") == job_name:
            matched = j
            break
    if not matched:
        raise HTTPException(status_code=404, detail=f"job 不存在: {job_name}")
    if not is_icmp_job(matched):
        raise HTTPException(status_code=400, detail="仅支持编辑 icmp /probe 任务")

    file_name = sanitize_file_name(payload.file_name)
    sd_path = (prom_path.parent / file_name).resolve()
    write_sd_file(sd_path, payload.targets)

    exporter_address, node_id = resolve_icmp_exporter(instance, payload)
    matched["file_sd_configs"] = [{"files": [relative_to_prom_dir(sd_path, prom_path)]}]
    matched["metrics_path"] = "/probe"
    matched["params"] = {"module": ["icmp"]}
    matched["relabel_configs"] = [
        {"source_labels": ["__address__"], "target_label": "__param_target"},
        {"source_labels": ["__param_target"], "target_label": "instance"},
        {"target_label": "__address__", "replacement": exporter_address},
    ]
    if payload.scrape_interval:
        matched["scrape_interval"] = payload.scrape_interval
    else:
        matched.pop("scrape_interval", None)
    save_yaml_file(prom_path, config)
    return {
        "ok": True,
        "job_name": job_name,
        "file_name": relative_to_prom_dir(sd_path, prom_path),
        "node_id": node_id,
        "exporter_address": exporter_address,
    }


@app.delete("/api/prometheus/instances/{instance_id}/icmp-tasks/{job_name}")
def delete_prom_icmp_task(instance_id: int, job_name: str, remove_file: bool = Query(default=False)) -> dict:
    instance = get_instance_or_404(instance_id)
    prom_path = resolve_instance_prom_path(instance)
    config = load_yaml_file(prom_path) or {}
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="prometheus.yml 格式错误")
    scrape_configs = config.get("scrape_configs") or []
    if not isinstance(scrape_configs, list):
        raise HTTPException(status_code=400, detail="prometheus.yml 中 scrape_configs 必须是列表")
    target_idx = None
    target_job: dict[str, Any] | None = None
    for idx, job in enumerate(scrape_configs):
        if isinstance(job, dict) and str(job.get("job_name") or "") == job_name:
            target_idx = idx
            target_job = job
            break
    if target_idx is None or target_job is None:
        raise HTTPException(status_code=404, detail=f"job 不存在: {job_name}")
    if not is_icmp_job(target_job):
        raise HTTPException(status_code=400, detail="仅支持删除 icmp /probe 任务")

    removed_file = ""
    if remove_file:
        for entry in (target_job.get("file_sd_configs") or []):
            if not isinstance(entry, dict):
                continue
            for f in (entry.get("files") or []):
                path = Path(str(f))
                if not path.is_absolute():
                    path = (prom_path.parent / path).resolve()
                if path.exists():
                    path.unlink()
                    removed_file = str(path)
                    break
            if removed_file:
                break
    scrape_configs.pop(target_idx)
    save_yaml_file(prom_path, config)
    return {"ok": True, "job_name": job_name, "removed_file": removed_file}

@app.post("/api/publish/requests")
def create_publish_request(payload: PublishRequestIn) -> dict:
    instance = get_instance_or_404(payload.instance_id)
    if instance["status"] != "active":
        raise HTTPException(status_code=400, detail="instance is not active")
    content = generate_prometheus_config(instance)
    ok, message = validate_yaml(content)
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    version = f"req-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO publish_requests(instance_id, version, content, status, requested_by, note, created_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (payload.instance_id, version, content, "pending", payload.requested_by, payload.note, utc_now()),
        )
        conn.commit()
    return {"id": cur.lastrowid, "status": "pending"}


@app.get("/api/publish/requests")
def list_publish_requests() -> list[dict]:
    return rows(
        """
        SELECT r.*, i.name AS instance_name
        FROM publish_requests r
        JOIN prometheus_instances i ON i.id = r.instance_id
        ORDER BY r.id DESC
        """
    )


@app.post("/api/publish/requests/{request_id}/approve")
def approve_publish_request(request_id: int, payload: ReviewIn) -> dict:
    req = row("SELECT * FROM publish_requests WHERE id=?", (request_id,))
    if not req:
        raise HTTPException(status_code=404, detail="publish request not found")
    if req["status"] != "pending":
        raise HTTPException(status_code=400, detail="request is not pending")
    instance = get_instance_or_404(req["instance_id"])
    ok, message, version = publish_to_instance(instance, req["content"], req["note"])
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE publish_requests SET
                status=?, result_message=?, reviewed_at=?, reviewed_by=?, version=?
            WHERE id=?
            """,
            ("executed" if ok else "failed", message, utc_now(), payload.reviewed_by, version or req["version"], request_id),
        )
        conn.commit()
    return {"ok": ok, "message": message, "version": version}


@app.post("/api/publish/requests/{request_id}/reject")
def reject_publish_request(request_id: int, payload: ReviewIn) -> dict:
    req = row("SELECT * FROM publish_requests WHERE id=?", (request_id,))
    if not req:
        raise HTTPException(status_code=404, detail="publish request not found")
    if req["status"] != "pending":
        raise HTTPException(status_code=400, detail="request is not pending")
    with get_conn() as conn:
        conn.execute(
            "UPDATE publish_requests SET status='rejected', result_message=?, reviewed_at=?, reviewed_by=? WHERE id=?",
            (payload.comment or "rejected", utc_now(), payload.reviewed_by, request_id),
        )
        conn.commit()
    return {"ok": True}


@app.get("/api/config/generate")
def config_generate(instance_id: int | None = Query(default=None)) -> dict:
    instance = get_instance_or_404(instance_id) if instance_id else None
    content = generate_prometheus_config(instance)
    return {"content": content}


@app.get("/api/config/diff")
def config_diff(instance_id: int | None = Query(default=None)) -> dict:
    instance = get_instance_or_404(instance_id) if instance_id else None
    generated = generate_prometheus_config(instance)
    published_path = None
    generated_name = "generated/prometheus.generated.yml"
    if instance_id:
        published_path = BASE_DIR / "published" / f"prometheus.instance-{instance_id}.yml"
        generated_name = f"generated/prometheus.generated.instance-{instance_id}.yml"
    return {"diff": diff_against_published(generated, published_path=published_path, generated_name=generated_name)}


@app.get("/api/config/validate")
def config_validate(instance_id: int | None = Query(default=None)) -> dict:
    instance = get_instance_or_404(instance_id) if instance_id else None
    generated = generate_prometheus_config(instance)
    ok, message = validate_yaml(generated)
    return {"ok": ok, "message": message}


@app.post("/api/config/publish")
def config_publish(payload: PublishIn) -> dict:
    content = generate_prometheus_config()
    ok, message = validate_yaml(content)
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    version = publish(content, payload.note)
    return {"ok": True, "version": version}


@app.post("/api/config/rollback")
def config_rollback(payload: RollbackIn) -> dict:
    if not rollback(payload.version):
        raise HTTPException(status_code=404, detail="version not found")
    return {"ok": True}


@app.get("/api/config/published")
def config_published() -> dict:
    return {"content": get_latest_published()}


@app.get("/api/config/versions")
def config_versions() -> list[dict]:
    return rows("SELECT * FROM config_versions ORDER BY id DESC LIMIT 100")
