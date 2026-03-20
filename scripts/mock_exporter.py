from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse

app = FastAPI()


@app.get('/metrics')
def metrics():
    return PlainTextResponse('# HELP blackbox_exporter_build_info A metric with a constant \\"1\\" value\n# TYPE blackbox_exporter_build_info gauge\nblackbox_exporter_build_info{version=\\"0.26.0\\"} 1\n')


@app.get('/probe')
def probe(module: str = Query('icmp'), target: str = Query(...)):
    ok = '0'
    if target and target != '0.0.0.0':
        ok = '1'
    return PlainTextResponse(f'# module={module}\nprobe_success {ok}\nprobe_duration_seconds 0.01\n')
