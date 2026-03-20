from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()
reload_count = 0


@app.post('/-/reload')
def reload_config():
    global reload_count
    reload_count += 1
    return PlainTextResponse('reloaded', status_code=200)


@app.get('/status')
def status():
    return {'reload_count': reload_count}


@app.get('/-/healthy')
def healthy():
    return PlainTextResponse('Prometheus is Healthy.')


@app.get('/api/v1/status/buildinfo')
def buildinfo():
    return {
        "status": "success",
        "data": {
            "version": "2.53.0-mock",
            "revision": "mock-rev",
            "branch": "main",
            "buildUser": "codex",
            "buildDate": "2026-03-16T00:00:00Z",
            "goVersion": "go1.22.0",
        },
    }


@app.get('/api/v1/status/runtimeinfo')
def runtimeinfo():
    return {
        "status": "success",
        "data": {
            "startTime": "2026-03-16T00:00:00Z",
            "CWD": "/prometheus",
            "reloadConfigSuccess": True,
            "lastConfigTime": "2026-03-16T00:00:00Z",
            "goroutineCount": 42,
            "GOMAXPROCS": 8,
            "GOGC": "100",
            "storageRetention": "15d",
        },
    }


@app.get('/api/v1/status/config')
def config():
    return {
        "status": "success",
        "data": {
            "yaml": "global:\\n  scrape_interval: 15s\\nscrape_configs:\\n- job_name: mock\\n  static_configs:\\n  - targets: ['127.0.0.1:9090']\\n"
        },
    }
