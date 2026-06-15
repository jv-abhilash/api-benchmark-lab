"""
server/rest/main.py
REST API — port 8001
Duplex:    Half  |  Stateless: Yes  |  Auth: JWT Bearer
"""
import time, os, sys
from collections import defaultdict
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
import jwt
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from server.shared.iot_feed import next_reading, reading_batch, next_reading_padded, PAD_SIZES, SENSOR_IDS

app = FastAPI(title="REST Sensor API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SECRET      = os.getenv("JWT_SECRET", "api-bench-secret")
ALGO        = "HS256"
VALID_TOKEN = jwt.encode({"sub": "benchmark-client", "role": "reader"}, SECRET, algorithm=ALGO)
security    = HTTPBearer(auto_error=False)
_counters   = defaultdict(int)
_start_ts   = time.time()

def verify_token(creds: HTTPAuthorizationCredentials = Depends(security)):
    if creds is None:
        raise HTTPException(401, "Missing Authorization header")
    try:
        jwt.decode(creds.credentials, SECRET, algorithms=[ALGO])
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid or expired token")

@app.get("/health")
def health():
    return {"status": "ok", "service": "rest", "port": 8001}

@app.get("/token")
def get_token():
    return {"token": VALID_TOKEN, "type": "Bearer"}

@app.get("/sensor", dependencies=[Depends(verify_token)])
def get_latest():
    _counters["requests"] += 1
    return next_reading().to_dict()

@app.get("/sensor/{sensor_id}", dependencies=[Depends(verify_token)])
def get_sensor(sensor_id: str):
    _counters["requests"] += 1
    return next_reading(sensor_id).to_dict()

@app.get("/sensors", dependencies=[Depends(verify_token)])
def get_all():
    _counters["requests"] += 1
    return [next_reading(sid).to_dict() for sid in SENSOR_IDS]

@app.post("/sensor/batch", dependencies=[Depends(verify_token)])
def get_batch(n: int = Query(default=100, ge=1, le=10000)):
    _counters["requests"] += 1
    _counters["batch_requests"] += 1
    return [r.to_dict() for r in reading_batch(size=n)]

@app.get("/sensor/padded/{size_label}", dependencies=[Depends(verify_token)])
def get_padded(size_label: str):
    if size_label not in PAD_SIZES:
        raise HTTPException(400, f"Choose from {list(PAD_SIZES)}")
    _counters["requests"] += 1
    return next_reading_padded(pad_bytes=PAD_SIZES[size_label]).to_dict()

@app.get("/metrics")
def get_metrics():
    uptime = round(time.time() - _start_ts, 2)
    return {
        "service"          : "rest",
        "uptime_seconds"   : uptime,
        "total_requests"   : _counters["requests"],
        "requests_per_sec" : round(_counters["requests"] / max(uptime, 1), 2),
        "counters"         : dict(_counters),
    }
