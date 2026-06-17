"""
server/webrtc/main.py
WebRTC signalling server — port 8006
Duplex:    Full P2P (once connected, server is OUT of data path)
Stateless: No — signalling maintains session state during ICE exchange
Auth:      JWT on signalling channel
Transport: DTLS (mandatory, built into WebRTC)

How WebRTC works in 3 stages:
  Stage 1 — Signalling (this server):
    Peers exchange SDP offer/answer and ICE candidates via HTTP
    Server just relays these — never sees the actual data
  Stage 2 — ICE negotiation:
    Peers find a direct path to each other (LAN = direct IP, WAN = STUN/TURN)
  Stage 3 — P2P data channel:
    Data flows directly peer-to-peer, server completely bypassed
    DTLS encryption is mandatory — always encrypted

On LAN: ICE finds direct path immediately, latency = pure network RTT
On WAN: needs STUN server to punch through NAT
"""
import asyncio, json, time, os, sys, uuid
from collections import defaultdict
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from server.shared.iot_feed import next_reading

app = FastAPI(title="WebRTC Signalling Server", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

SECRET    = os.getenv("JWT_SECRET", "api-bench-secret")
ALGO      = "HS256"
security  = HTTPBearer(auto_error=False)
_counters = defaultdict(int)
_start_ts = time.time()

_sessions:  dict[str, dict]    = {}
_ws_peers:  dict[str, WebSocket] = {}

def verify_token(creds: HTTPAuthorizationCredentials = Depends(security)):
    if creds is None:
        raise HTTPException(401, "Missing token")
    try:
        jwt.decode(creds.credentials, SECRET, algorithms=[ALGO])
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")

def verify_token_str(token: str) -> bool:
    try:
        jwt.decode(token, SECRET, algorithms=[ALGO])
        return True
    except jwt.InvalidTokenError:
        return False

@app.get("/health")
def health():
    return {
        "status"          : "ok",
        "service"         : "webrtc",
        "port"            : 8006,
        "active_sessions" : len(_sessions),
    }

@app.post("/webrtc/session", dependencies=[Depends(verify_token)])
def create_session():
    session_id = str(uuid.uuid4())[:8]
    _sessions[session_id] = {
        "id"           : session_id,
        "created_at"   : time.time(),
        "offer"        : None,
        "answer"       : None,
        "candidates_a" : [],
        "candidates_b" : [],
        "state"        : "waiting",
    }
    _counters["sessions_created"] += 1
    return {"session_id": session_id, "state": "waiting"}

@app.post("/webrtc/offer/{session_id}", dependencies=[Depends(verify_token)])
async def post_offer(session_id: str, request_data: dict):
    if session_id not in _sessions:
        raise HTTPException(404, "Session not found")
    _sessions[session_id]["offer"] = request_data.get("sdp")
    _sessions[session_id]["state"] = "offer_received"
    _counters["offers"] += 1
    if session_id in _ws_peers:
        try:
            await _ws_peers[session_id].send_json({"type": "offer", "sdp": request_data.get("sdp")})
        except Exception:
            pass
    return {"status": "offer_stored", "session_id": session_id}

@app.get("/webrtc/offer/{session_id}", dependencies=[Depends(verify_token)])
def get_offer(session_id: str):
    if session_id not in _sessions:
        raise HTTPException(404, "Session not found")
    offer = _sessions[session_id].get("offer")
    if not offer:
        raise HTTPException(404, "No offer yet")
    return {"sdp": offer, "session_id": session_id}

@app.post("/webrtc/answer/{session_id}", dependencies=[Depends(verify_token)])
async def post_answer(session_id: str, request_data: dict):
    if session_id not in _sessions:
        raise HTTPException(404, "Session not found")
    _sessions[session_id]["answer"] = request_data.get("sdp")
    _sessions[session_id]["state"]  = "answer_received"
    _counters["answers"] += 1
    return {"status": "answer_stored", "session_id": session_id}

@app.post("/webrtc/candidate/{session_id}", dependencies=[Depends(verify_token)])
def post_candidate(session_id: str, request_data: dict):
    if session_id not in _sessions:
        raise HTTPException(404, "Session not found")
    peer = request_data.get("peer", "a")
    _sessions[session_id][f"candidates_{peer}"].append(request_data.get("candidate"))
    _counters["ice_candidates"] += 1
    return {"status": "candidate_stored", "peer": peer,
            "total_candidates": len(_sessions[session_id][f"candidates_{peer}"])}

@app.get("/webrtc/candidates/{session_id}", dependencies=[Depends(verify_token)])
def get_candidates(session_id: str, peer: str = "a"):
    if session_id not in _sessions:
        raise HTTPException(404, "Session not found")
    return {"candidates": _sessions[session_id].get(f"candidates_{peer}", []),
            "session_id": session_id, "peer": peer}

@app.get("/webrtc/session/{session_id}", dependencies=[Depends(verify_token)])
def get_session(session_id: str):
    if session_id not in _sessions:
        raise HTTPException(404, "Session not found")
    s = _sessions[session_id]
    return {
        "session_id"  : session_id,
        "state"       : s["state"],
        "has_offer"   : s["offer"] is not None,
        "has_answer"  : s["answer"] is not None,
        "candidates_a": len(s["candidates_a"]),
        "candidates_b": len(s["candidates_b"]),
        "age_seconds" : round(time.time() - s["created_at"], 1),
    }

@app.websocket("/webrtc/ws/{session_id}")
async def ws_signal(websocket: WebSocket, session_id: str, token: str = Query(...)):
    if not verify_token_str(token):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    _ws_peers[session_id] = websocket
    _counters["ws_connections"] += 1
    try:
        while True:
            data     = await websocket.receive_json()
            msg_type = data.get("type")
            if msg_type == "candidate":
                peer = data.get("peer", "a")
                if session_id in _sessions:
                    _sessions[session_id][f"candidates_{peer}"].append(data.get("candidate"))
                    _counters["ice_candidates"] += 1
            elif msg_type == "ping":
                await websocket.send_json({"type": "pong", "ts": time.time()})
    except WebSocketDisconnect:
        pass
    finally:
        _ws_peers.pop(session_id, None)

@app.websocket("/webrtc/datachannel/{session_id}")
async def simulated_datachannel(
    websocket   : WebSocket,
    session_id  : str,
    token       : str = Query(...),
    interval_ms : int = Query(default=100),
):
    if not verify_token_str(token):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    _counters["datachannel_sessions"] += 1
    stop = asyncio.Event()

    async def sender():
        while not stop.is_set():
            try:
                r = next_reading()
                await websocket.send_json({
                    **r.to_dict(),
                    "_channel": "datachannel",
                    "_note"   : "real P2P would bypass this server entirely",
                })
                _counters["datachannel_messages"] += 1
            except Exception:
                stop.set()
                break
            await asyncio.sleep(interval_ms / 1000.0)

    async def receiver():
        while not stop.is_set():
            try:
                data = await websocket.receive_json()
                if data.get("cmd") == "ping":
                    # echo client_ts back unchanged — client measures true RTT
                    client_ts = data.get("client_ts", time.time())
                    await websocket.send_json({"ack": "pong", "client_ts": client_ts})
                _counters["datachannel_received"] += 1
            except Exception:
                stop.set()
                break

    try:
        await asyncio.gather(sender(), receiver())
    except WebSocketDisconnect:
        pass
    finally:
        stop.set()

@app.get("/metrics")
def get_metrics():
    uptime = round(time.time() - _start_ts, 2)
    return {
        "service"              : "webrtc",
        "uptime_seconds"       : uptime,
        "sessions_created"     : _counters["sessions_created"],
        "offers_exchanged"     : _counters["offers"],
        "answers_exchanged"    : _counters["answers"],
        "ice_candidates"       : _counters["ice_candidates"],
        "ws_connections"       : _counters["ws_connections"],
        "datachannel_sessions" : _counters["datachannel_sessions"],
        "datachannel_messages" : _counters["datachannel_messages"],
    }
