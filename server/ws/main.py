"""
server/ws/main.py
WebSocket service — port 8003
Duplex:    Full  |  Stateless: No (persistent connection)  |  Auth: token in handshake
One persistent connection replaces thousands of REST requests.
Server pushes readings continuously — client never needs to ask again.
"""
import asyncio, time, os, sys, json
from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, status
from fastapi.middleware.cors import CORSMiddleware
import jwt
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from server.shared.iot_feed import stream_readings, next_reading, PAD_SIZES

app = FastAPI(title="WebSocket Sensor Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SECRET    = os.getenv("JWT_SECRET", "api-bench-secret")
ALGO      = "HS256"
_counters = defaultdict(int)
_start_ts = time.time()
# track active connections for the metrics endpoint
_active_connections: set[WebSocket] = set()

def verify_token(token: str) -> bool:
    try:
        jwt.decode(token, SECRET, algorithms=[ALGO])
        return True
    except jwt.InvalidTokenError:
        return False

@app.get("/health")
def health():
    return {
        "status"             : "ok",
        "service"            : "websocket",
        "port"               : 8003,
        "active_connections" : len(_active_connections),
    }

@app.get("/metrics")
def get_metrics():
    uptime = round(time.time() - _start_ts, 2)
    return {
        "service"            : "websocket",
        "uptime_seconds"     : uptime,
        "active_connections" : len(_active_connections),
        "total_messages_sent": _counters["messages_sent"],
        "total_connections"  : _counters["connections"],
        "auth_failures"      : _counters["auth_failures"],
        "messages_per_sec"   : round(_counters["messages_sent"] / max(uptime, 1), 2),
    }

# ── ws://host:8003/ws/stream ─────────────────────────────────
# Main streaming endpoint — server pushes sensor readings
# Query params:
#   token       : JWT (required)
#   sensor_id   : which sensor (optional, default random)
#   interval_ms : push cadence (default 100ms = 10/sec)
#   pad_size    : payload size label for Phase 3 (default 64B)
@app.websocket("/ws/stream")
async def ws_stream(
    websocket   : WebSocket,
    token       : str  = Query(...),
    sensor_id   : str  = Query(default=None),
    interval_ms : int  = Query(default=100),
    pad_size    : str  = Query(default="64B"),
):
    # auth before accepting
    if not verify_token(token):
        _counters["auth_failures"] += 1
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    _active_connections.add(websocket)
    _counters["connections"] += 1
    pad_bytes = PAD_SIZES.get(pad_size, 0)

    try:
        async for reading in stream_readings(
            sensor_id    = sensor_id,
            interval_ms  = interval_ms,
            pad_bytes    = pad_bytes,
        ):
            msg = reading.to_dict() if hasattr(reading, "to_dict") else reading.reading.to_dict()
            await websocket.send_json(msg)
            _counters["messages_sent"] += 1

    except WebSocketDisconnect:
        pass
    finally:
        _active_connections.discard(websocket)

# ── ws://host:8003/ws/bidi ───────────────────────────────────
# Full-duplex endpoint — server streams while client sends commands
# This is Phase 4 duplex flood test endpoint
# Client can send: {"cmd": "pause"} | {"cmd": "resume"} | {"cmd": "ping"}
@app.websocket("/ws/bidi")
async def ws_bidi(
    websocket : WebSocket,
    token     : str = Query(...),
):
    if not verify_token(token):
        _counters["auth_failures"] += 1
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    _active_connections.add(websocket)
    _counters["connections"] += 1

    paused    = False
    interval  = 0.1   # 100ms default

    async def sender():
        """Server → client: pushes sensor readings continuously."""
        nonlocal paused, interval
        while True:
            if not paused:
                r = next_reading()
                await websocket.send_json({**r.to_dict(), "_dir": "server→client"})
                _counters["messages_sent"] += 1
            await asyncio.sleep(interval)

    async def receiver():
        """Client → server: handles control commands."""
        nonlocal paused, interval
        while True:
            try:
                data = await websocket.receive_json()
                cmd  = data.get("cmd", "")
                if cmd == "pause":
                    paused = True
                    await websocket.send_json({"ack": "paused"})
                elif cmd == "resume":
                    paused   = False
                    interval = data.get("interval_ms", 100) / 1000.0
                    await websocket.send_json({"ack": "resumed"})
                elif cmd == "ping":
                    # echo client_ts back unchanged so client can measure true RTT
                    # client sends: {"cmd": "ping", "client_ts": time.perf_counter()}
                    # client RTT = time.perf_counter() - pong["client_ts"]
                    client_ts = data.get("client_ts", time.time())
                    await websocket.send_json({"ack": "pong", "client_ts": client_ts})
                _counters["client_commands"] += 1
            except Exception:
                break

    try:
        await asyncio.gather(sender(), receiver())
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _active_connections.discard(websocket)
