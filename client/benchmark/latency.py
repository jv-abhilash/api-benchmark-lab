"""
client/benchmark/latency.py
Measures RTT p50/p95/p99 for all 7 protocols.

Usage:
  python3 client/benchmark/latency.py --server localhost --duration 30
  python3 client/benchmark/latency.py --server 192.168.68.59 --duration 60 --payload 1KB
"""
import argparse, asyncio, json, sqlite3, time, os, sys, socket, threading
import httpx, websockets, grpc
from zeep import Client as ZeepClient
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken
sys.path.insert(0, '.')
import server.grpc.sensor_pb2      as pb2
import server.grpc.sensor_pb2_grpc as pb2_grpc

DB_PATH = "results/benchmark_results.db"

# ── DB ────────────────────────────────────────────────────────
def init_db():
    os.makedirs("results", exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS latency_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_ts REAL, protocol TEXT, payload TEXT,
            duration_s INTEGER, count INTEGER,
            p50_ms REAL, p95_ms REAL, p99_ms REAL,
            min_ms REAL, max_ms REAL, errors INTEGER
        )
    """)
    con.commit()
    return con

def save_results(con, run_ts, protocol, payload, duration, rtts, errors):
    if len(rtts) < 2:
        return
    s  = sorted(rtts)
    n  = len(s)
    con.execute("""
        INSERT INTO latency_results
        (run_ts,protocol,payload,duration_s,count,
         p50_ms,p95_ms,p99_ms,min_ms,max_ms,errors)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (run_ts, protocol, payload, duration, n,
          round(s[int(n*0.50)],3), round(s[int(n*0.95)],3),
          round(s[int(n*0.99)],3),
          round(min(s),3), round(max(s),3), errors))
    con.commit()

def percentiles(rtts):
    if not rtts:
        return 0,0,0,0,0
    s = sorted(rtts)
    n = len(s)
    return (round(s[int(n*0.50)],3), round(s[int(n*0.95)],3),
            round(s[int(n*0.99)],3), round(min(s),3), round(max(s),3))

def get_host_ip():
    """IP that Docker containers can reach back to this machine."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

# ── REST ─────────────────────────────────────────────────────
async def probe_rest(server, duration, payload, token):
    rtts, errors = [], 0
    url = (f"http://{server}:8001/sensor" if payload == "64B"
           else f"http://{server}:8001/sensor/padded/{payload}")
    headers  = {"Authorization": f"Bearer {token}"}
    deadline = time.time() + duration
    async with httpx.AsyncClient(timeout=10) as client:
        while time.time() < deadline:
            t0 = time.perf_counter()
            try:
                r = await client.get(url, headers=headers)
                r.raise_for_status()
                rtts.append((time.perf_counter() - t0) * 1000)
            except Exception:
                errors += 1
            await asyncio.sleep(0.001)
    return rtts, errors

# ── SOAP — real zeep WSDL-based client ───────────────────────
def probe_soap_sync(server, duration, payload):
    rtts, errors = [], 0
    try:
        wsdl_url    = f"http://{server}:8002/soap?wsdl"
        zeep_client = ZeepClient(
            wsdl=wsdl_url,
            transport=Transport(timeout=10))
        zeep_client.wsse = UsernameToken("benchmark", "api-bench-secret")
        deadline = time.time() + duration
        while time.time() < deadline:
            t0 = time.perf_counter()
            try:
                zeep_client.service.GetLatestReading(sensor_id="T-01")
                rtts.append((time.perf_counter() - t0) * 1000)
            except Exception:
                errors += 1
            time.sleep(0.001)
    except Exception as e:
        print(f"  SOAP setup error: {e}")
        errors += 1
    return rtts, errors

async def probe_soap(server, duration, payload):
    import queue, threading
    result_q = queue.Queue()
    def worker():
        result_q.put(probe_soap_sync(server, duration, payload))
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    while t.is_alive():
        await asyncio.sleep(0.5)
    t.join()
    return result_q.get()

# ── WebSocket ─────────────────────────────────────────────────
async def probe_websocket(server, duration, payload, token):
    rtts, errors = [], 0
    pad  = payload if payload != "64B" else "64B"
    uri  = (f"ws://{server}:8003/ws/stream"
            f"?token={token}&sensor_id=T-01&interval_ms=1&pad_size={pad}")
    deadline = time.time() + duration
    try:
        async with websockets.connect(uri) as ws:
            while time.time() < deadline:
                t0  = time.perf_counter()
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                rtts.append((time.perf_counter() - t0) * 1000)
                json.loads(msg)
    except Exception:
        errors += 1
    return rtts, errors

# ── GraphQL ───────────────────────────────────────────────────
async def probe_graphql(server, duration, payload, token):
    rtts, errors = [], 0
    url   = f"http://{server}:8004/graphql"
    query = json.dumps({"query":
        f'{{ sensor(sensorId: "T-01", token: "{token}") '
        f'{{ sensorId temp humidity seq status }} }}'})
    headers  = {"Content-Type": "application/json"}
    deadline = time.time() + duration
    async with httpx.AsyncClient(timeout=10) as client:
        while time.time() < deadline:
            t0 = time.perf_counter()
            try:
                r = await client.post(url, content=query, headers=headers)
                r.raise_for_status()
                rtts.append((time.perf_counter() - t0) * 1000)
            except Exception:
                errors += 1
            await asyncio.sleep(0.001)
    return rtts, errors

# ── gRPC — dedicated thread with its own channel ─────────────
def probe_grpc_sync(server, duration, token):
    """
    Runs in its own thread with its own gRPC channel.
    gRPC channels are NOT safe to share across threads —
    each thread needs its own channel instance.
    """
    rtts, errors = [], 0
    # create channel inside this thread
    channel = grpc.insecure_channel(
        f"{server}:50051",
        options=[
            ("grpc.max_reconnect_backoff_ms", 1000),
            ("grpc.keepalive_time_ms", 10000),
        ]
    )
    stub     = pb2_grpc.SensorServiceStub(channel)
    # gRPC uses simple Bearer token, not JWT
    grpc_token = "api-bench-secret"
    meta     = [("authorization", f"Bearer {grpc_token}")]
    deadline = time.time() + duration

    # warm-up call
    try:
        stub.GetLatestReading(
            pb2.SensorRequest(sensor_id="T-01"),
            metadata=meta, timeout=5)
    except Exception:
        pass

    while time.time() < deadline:
        t0 = time.perf_counter()
        try:
            stub.GetLatestReading(
                pb2.SensorRequest(sensor_id="T-01"),
                metadata=meta, timeout=5)
            rtts.append((time.perf_counter() - t0) * 1000)
        except grpc.RpcError as e:
            errors += 1
        except Exception:
            errors += 1
        time.sleep(0.001)

    channel.close()
    return rtts, errors

async def probe_grpc(server, duration, payload, token):
    """
    gRPC C-core conflicts with asyncio event loop when run via executor
    while other threads (zeep/SOAP) are also running.
    Solution: start a plain daemon thread BEFORE asyncio.gather,
    communicate via queue.Queue — zero asyncio involvement.
    """
    import queue, threading
    result_q = queue.Queue()

    def worker():
        result_q.put(probe_grpc_sync(server, duration, token))

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    # poll until thread finishes without blocking event loop
    while t.is_alive():
        await asyncio.sleep(0.5)

    t.join()
    return result_q.get()

# ── Webhook ───────────────────────────────────────────────────
async def probe_webhook(server, duration, payload):
    """
    Starts a local HTTP receiver.
    Uses get_host_ip() so Docker containers can POST back to this machine.
    127.0.0.1 would point to the container itself — wrong.
    """
    from aiohttp import web
    rtts, errors = [], 0
    arrivals  = []
    event_log = []

    async def handle(request):
        arrivals.append(time.perf_counter())
        try:
            body = await request.json()
            event_log.append(body)
        except Exception:
            pass
        return web.Response(text="ok", status=200)

    recv_app = web.Application()
    recv_app.router.add_post("/webhook", handle)
    runner   = web.AppRunner(recv_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 9001)
    await site.start()

    host_ip      = get_host_ip()
    receiver_url = f"http://{host_ip}:9001/webhook"
    print(f"  Webhook receiver : {receiver_url}")

    async with httpx.AsyncClient(timeout=10) as client:
        reg = await client.post(
            f"http://{server}:8005/webhook/register",
            json={"url"         : receiver_url,
                  "sensor_id"   : "T-01",
                  "interval_ms" : 500})
        print(f"  Webhook registered: {reg.json()['status']}")

    await asyncio.sleep(duration)
    await runner.cleanup()

    print(f"  Webhook events received : {len(arrivals)}")
    if event_log:
        d = event_log[0]["data"]
        print(f"  First event sample     : "
              f"sensor={d['sensor_id']} temp={d['temp']} seq={d['seq']}")

    for i in range(1, len(arrivals)):
        rtts.append((arrivals[i] - arrivals[i-1]) * 1000)

    return rtts, errors

# ── WebRTC ────────────────────────────────────────────────────
async def probe_webrtc(server, duration, payload, token):
    rtts, errors = [], 0
    uri = (f"ws://{server}:8006/webrtc/datachannel/bench"
           f"?token={token}&interval_ms=1")
    deadline = time.time() + duration
    try:
        async with websockets.connect(uri) as ws:
            while time.time() < deadline:
                t0  = time.perf_counter()
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                rtts.append((time.perf_counter() - t0) * 1000)
                json.loads(msg)
    except Exception:
        errors += 1
    return rtts, errors

# ── Print table ───────────────────────────────────────────────
def print_table(results, payload, duration):
    print(f"\n{'='*74}")
    print(f"  LATENCY RESULTS  |  payload={payload}  |  duration={duration}s")
    print(f"{'='*74}")
    print(f"  {'Protocol':<12} {'Count':>7} {'p50ms':>8} {'p95ms':>8} "
          f"{'p99ms':>8} {'min':>7} {'max':>7} {'Errors':>7}")
    print(f"  {'-'*70}")
    for proto, (rtts, errors) in results.items():
        if rtts:
            p50,p95,p99,mn,mx = percentiles(rtts)
            print(f"  {proto:<12} {len(rtts):>7} {p50:>8.2f} {p95:>8.2f} "
                  f"{p99:>8.2f} {mn:>7.2f} {mx:>7.2f} {errors:>7}")
        else:
            print(f"  {proto:<12} {'0':>7} {'N/A':>8} {'N/A':>8} "
                  f"{'N/A':>8} {'N/A':>7} {'N/A':>7} {errors:>7}")
    print(f"{'='*74}\n")

# ── Main ──────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server",   default="localhost")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--payload",  default="64B",
                        choices=["64B","1KB","10KB","100KB","1MB"])
    args = parser.parse_args()

    SERVER   = args.server
    DURATION = args.duration
    PAYLOAD  = args.payload
    RUN_TS   = time.time()

    print(f"\nConnecting to server : {SERVER}")
    print(f"Duration             : {DURATION}s  |  Payload: {PAYLOAD}")

    async with httpx.AsyncClient(timeout=10) as client:
        r     = await client.get(f"http://{SERVER}:8001/token")
        TOKEN = r.json()["token"]
    print(f"JWT token acquired.")
    print(f"Running all 7 probers simultaneously...\n")

    results_raw = await asyncio.gather(
        probe_rest(SERVER, DURATION, PAYLOAD, TOKEN),
        probe_soap(SERVER, DURATION, PAYLOAD),
        probe_websocket(SERVER, DURATION, PAYLOAD, TOKEN),
        probe_graphql(SERVER, DURATION, PAYLOAD, TOKEN),
        probe_grpc(SERVER, DURATION, PAYLOAD, TOKEN),
        probe_webhook(SERVER, DURATION, PAYLOAD),
        probe_webrtc(SERVER, DURATION, PAYLOAD, TOKEN),
        return_exceptions=True,
    )

    protocols = ["REST","SOAP","WebSocket","GraphQL",
                 "gRPC","Webhook","WebRTC"]
    results   = {}
    for proto, raw in zip(protocols, results_raw):
        if isinstance(raw, Exception):
            print(f"  {proto} prober error: {raw}")
            results[proto] = ([], 1)
        else:
            results[proto] = raw

    print_table(results, PAYLOAD, DURATION)

    con = init_db()
    for proto, (rtts, errors) in results.items():
        save_results(con, RUN_TS, proto, PAYLOAD, DURATION, rtts, errors)
    con.close()
    print(f"Results saved → {DB_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
