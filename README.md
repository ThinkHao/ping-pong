# Ping Pong Control Plane (Phase-2)

统一管理 Prometheus + blackbox exporter 探测任务的二期实现，覆盖：
- Exporter 节点管理
- 探测任务管理
- 探测任务目标组管理（一个任务可包含多个目标）
- 任务级标签管理（每个任务独立标签）
- Prometheus 实例管理（地址、reload入口、业务/区域范围）
- Prometheus 配置生成、Diff、校验、发布、回滚

## Quick Start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 18080 --reload
```

访问：[http://127.0.0.1:18080/](http://127.0.0.1:18080/)

## One-Command Acceptance

```powershell
python scripts/acceptance.py
# 验收后保持服务不退出：
python scripts/acceptance.py --keep-running
# 如需避免端口冲突可用随机端口：
python scripts/acceptance.py --random-ports --keep-running
```

说明：`acceptance.py` 不再自动 seed，也不再自动拉起 mock exporter / mock prometheus。

## API (核心)

- `GET /api/nodes` / `POST /api/nodes`
- `GET /api/tasks` / `POST /api/tasks`
- `GET /api/tasks/{task_id}/labels`
- `PUT /api/tasks/{task_id}/labels`
- `DELETE /api/tasks/{task_id}/labels/{label_key}`
- `GET /api/tasks/{task_id}/targets`
- `PUT /api/tasks/{task_id}/targets`
- `GET /api/prometheus/instances` / `POST /api/prometheus/instances`
- `GET /api/prometheus/instances/{id}/status`
- `POST /api/publish/requests`
- `GET /api/publish/requests`
- `POST /api/publish/requests/{id}/approve`
- `POST /api/publish/requests/{id}/reject`
- `POST /api/monitor/tick`
- `GET /api/dashboard`
- `GET /api/config/generate`
- `GET /api/config/diff`
- `GET /api/config/validate`
- `POST /api/config/publish`
- `POST /api/config/rollback`
- `GET /api/config/versions`

## 验收口径映射

1. 新增任务后可进入配置生成并发布。
2. 一个任务可配置多个探测目标，并可为每个目标维护独立标签（如 `tag`）。
3. 探测任务必须绑定 exporter 节点，执行与配置都按绑定节点生效。
4. 节点离线会产生 `node_offline` 告警。
5. 发布失败阻断；历史版本支持回滚。
6. 仪表盘展示成功率、延迟、按区域/业务分布。
7. 配置发布支持“申请 -> 审批 -> 调用 Prometheus reload -> 审计记录”。

任务目标录入示例（前端每行一个）：
`10.12.101.34 | tag=天津-北京,site=bj`
