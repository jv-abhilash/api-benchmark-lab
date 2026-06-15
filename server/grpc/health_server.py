"""
Thin FastAPI wrapper so the docker-compose health check and
/metrics endpoint work over HTTP alongside the gRPC port.
"""
import time, os, sys, threading
from collections import defaultdict
from fastapi import FastAPI
import uvicorn
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

app = FastAPI(title="gRPC Health/Metrics", version="1.0.0")
_start_ts = time.time()

@app.get("/health")
def health():
    return {"status": "ok", "service": "grpc", "port": 50051}

@app.get("/metrics")
def metrics():
    # import counters from main servicer at runtime
    try:
        from server.grpc.main import _counters
        counters = dict(_counters)
    except Exception:
        counters = {}
    uptime = round(time.time() - _start_ts, 2)
    return {
        "service"       : "grpc",
        "uptime_seconds": uptime,
        "counters"      : counters,
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8007)
