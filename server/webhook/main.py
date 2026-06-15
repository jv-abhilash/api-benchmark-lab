"""
server/webhook/main.py
Webhook emitter — port 8005
Duplex:    Simplex (server → client only, no reply channel)
Stateless: Yes — each POST is independent
Auth:      HMAC-SHA256 signature on every payload
Async:     Yes — server pushes when events occur, client never polls

How it works:
  1. Client registers a receiver URL (POST /webhook/register)
  2. Server emits sensor readings to that URL on a schedule
  3. Every POST is signed with HMAC-SHA256
  4. Client verifies the signature — rejects if invalid
  5. If receiver is down, server queues and retries (Phase 5 chaos test)
"""
import asyncio, hashlib, hmac, json, time, os, sys
from collections import defaultdict, deque
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import httpx
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from server.shared.iot_feed import next_reading, SENSOR_IDS

app = FastAPI(title="Webhook Emitter", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

SECRET    = os.getenv("WEBHOOK_SECRET", "webhook-hmac-secret")
_counters = defaultdict(int)
_start_ts = time.time()

# ── Receiver registry ────────────────────────────────────────
# { url: { interval_ms, sensor_id, active, retry_queue } }
_receivers: dict[str, dict] = {}

# ── HMAC signing ─────────────────────────────────────────────
def sign_payload(payload: bytes) -> str:
    """HMAC-SHA256 signature — receiver must verify this."""
    return hmac.new(SECRET.encode(), payload, hashlib.sha256).hexdigest()

def make_event(sensor_id: str = None) -> tuple[dict, bytes]:
    r       = next_reading(sensor_id)
    payload = {
        "event"     : "sensor.reading",
        "timestamp" : r.timestamp,
        "data"      : r.to_dict(),
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return payload, raw

# ── Emitter loop ─────────────────────────────────────────────
async def emit_to_receiver(url: str, cfg: dict):
    """
    Continuously POSTs signed sensor events to a registered URL.
    If the receiver is down: queues up to 100 events, retries on restore.
    This is Phase 5 reliability behaviour.
    """
    interval = cfg.get("interval_ms", 1000) / 1000.0
    sensor_id = cfg.get("sensor_id")
    retry_q: deque = cfg.setdefault("retry_queue", deque(maxlen=100))

    async with httpx.AsyncClient(timeout=5.0) as client:
        while cfg.get("active", False):
            payload, raw = make_event(sensor_id)
            sig          = sign_payload(raw)
            headers      = {
                "Content-Type"      : "application/json",
                "X-Webhook-Sig"     : f"sha256={sig}",
                "X-Webhook-Event"   : "sensor.reading",
                "X-Delivery-Seq"    : str(_counters["emitted"]),
            }

            # drain retry queue first
            while retry_q:
                queued_raw, queued_headers = retry_q[0]
                try:
                    resp = await client.post(url, content=queued_raw,
                                             headers=queued_headers)
                    if resp.status_code < 500:
                        retry_q.popleft()
                        _counters["retried"] += 1
                except Exception:
                    break

            # send current event
            try:
                resp = await client.post(url, content=raw, headers=headers)
                if resp.status_code < 500:
                    _counters["emitted"] += 1
                else:
                    retry_q.append((raw, headers))
                    _counters["queued"] += 1
            except Exception:
                # receiver down — queue the event
                retry_q.append((raw, headers))
                _counters["queued"] += 1
                _counters["failures"] += 1

            await asyncio.sleep(interval)

# ── Routes ───────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status"    : "ok",
        "service"   : "webhook",
        "port"      : 8005,
        "receivers" : len(_receivers),
    }

@app.get("/secret")
def get_secret():
    """Returns HMAC secret so benchmark client can verify signatures."""
    return {"secret": SECRET, "algorithm": "HMAC-SHA256"}

@app.post("/webhook/register")
async def register(request: Request, background_tasks: BackgroundTasks):
    """
    Register a receiver URL.
    Body: { "url": "http://machine-b:9000/webhook",
            "sensor_id": "T-01",   (optional)
            "interval_ms": 500 }   (optional, default 1000)
    """
    body = await request.json()
    url  = body.get("url")
    if not url:
        raise HTTPException(400, "url is required")

    cfg = {
        "url"        : url,
        "sensor_id"  : body.get("sensor_id"),
        "interval_ms": body.get("interval_ms", 1000),
        "active"     : True,
        "retry_queue": deque(maxlen=100),
        "registered_at": time.time(),
    }
    _receivers[url] = cfg
    _counters["registrations"] += 1
    background_tasks.add_task(emit_to_receiver, url, cfg)
    return {"status": "registered", "url": url, "interval_ms": cfg["interval_ms"]}

@app.delete("/webhook/unregister")
async def unregister(url: str):
    if url not in _receivers:
        raise HTTPException(404, "URL not registered")
    _receivers[url]["active"] = False
    del _receivers[url]
    return {"status": "unregistered", "url": url}

@app.get("/webhook/receivers")
def list_receivers():
    return [
        {
            "url"          : url,
            "interval_ms"  : cfg["interval_ms"],
            "sensor_id"    : cfg["sensor_id"],
            "active"       : cfg["active"],
            "retry_queued" : len(cfg.get("retry_queue", [])),
        }
        for url, cfg in _receivers.items()
    ]

@app.post("/webhook/test-fire")
async def test_fire(request: Request):
    """Fire one event to a URL immediately — for milestone testing."""
    body      = await request.json()
    url       = body.get("url")
    payload, raw = make_event(body.get("sensor_id"))
    sig       = sign_payload(raw)
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp  = await client.post(url, content=raw, headers={
            "Content-Type"   : "application/json",
            "X-Webhook-Sig"  : f"sha256={sig}",
            "X-Webhook-Event": "sensor.reading",
        })
    return {
        "fired_to"    : url,
        "status_code" : resp.status_code,
        "signature"   : f"sha256={sig}",
        "payload_size": len(raw),
    }

@app.get("/metrics")
def get_metrics():
    uptime = round(time.time() - _start_ts, 2)
    return {
        "service"         : "webhook",
        "uptime_seconds"  : uptime,
        "total_emitted"   : _counters["emitted"],
        "total_queued"    : _counters["queued"],
        "total_retried"   : _counters["retried"],
        "total_failures"  : _counters["failures"],
        "registrations"   : _counters["registrations"],
        "events_per_sec"  : round(_counters["emitted"] / max(uptime, 1), 2),
        "active_receivers": len(_receivers),
    }
