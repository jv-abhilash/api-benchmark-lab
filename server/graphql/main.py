"""
server/graphql/main.py
GraphQL service — port 8004
Duplex:    Half (query/mutation) + push via subscription over WebSocket
Stateless: Yes for queries  |  No for subscriptions (persistent WS)
Auth:      Bearer token in HTTP header
Key demo:  same server, two modes — query fetches exact fields you ask for,
           subscription pushes continuously without polling
"""
import asyncio, time, os, sys
from collections import defaultdict
from typing import AsyncGenerator, Optional
import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import jwt
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from server.shared.iot_feed import next_reading, SENSOR_IDS, stream_readings, PAD_SIZES

SECRET    = os.getenv("JWT_SECRET", "api-bench-secret")
ALGO      = "HS256"
_counters = defaultdict(int)
_start_ts = time.time()

def verify(token: str) -> bool:
    try:
        jwt.decode(token, SECRET, algorithms=[ALGO])
        return True
    except jwt.InvalidTokenError:
        return False

# ── GraphQL types ────────────────────────────────────────────
@strawberry.type
class SensorReadingType:
    sensor_id : str
    temp      : float
    humidity  : float
    timestamp : float
    seq       : int
    location  : str
    status    : str

def reading_to_gql(r) -> SensorReadingType:
    return SensorReadingType(**r.to_dict())

# ── Query — half-duplex, client asks, server answers once ────
@strawberry.type
class Query:
    @strawberry.field
    def sensor(self, sensor_id: Optional[str] = None,
               token: str = "") -> SensorReadingType:
        """Fetch latest reading for one sensor (or random if omitted)."""
        if not verify(token):
            raise HTTPException(401, "Invalid token")
        _counters["queries"] += 1
        return reading_to_gql(next_reading(sensor_id))

    @strawberry.field
    def sensors(self, token: str = "") -> list[SensorReadingType]:
        """Fetch all 8 sensors in one query — compare with REST /sensors."""
        if not verify(token):
            raise HTTPException(401, "Invalid token")
        _counters["queries"] += 1
        return [reading_to_gql(next_reading(sid)) for sid in SENSOR_IDS]

    @strawberry.field
    def sensor_temp_only(self, sensor_id: str,
                         token: str = "") -> SensorReadingType:
        """
        Demonstrates GraphQL's over-fetch prevention.
        Client asks for only temp — resolver still fetches full reading
        but GraphQL layer strips unrequested fields before responding.
        In the query: { sensorTempOnly(sensorId:"T-01", token:"...") { temp } }
        Only 'temp' comes back — not humidity, seq, location etc.
        """
        if not verify(token):
            raise HTTPException(401, "Invalid token")
        _counters["queries"] += 1
        _counters["field_selective"] += 1
        return reading_to_gql(next_reading(sensor_id))

# ── Subscription — push via WebSocket, no polling needed ─────
@strawberry.type
class Subscription:
    @strawberry.subscription
    async def sensor_stream(
        self,
        token       : str,
        sensor_id   : Optional[str] = None,
        interval_ms : int = 100,
    ) -> AsyncGenerator[SensorReadingType, None]:
        """
        Push readings continuously without client asking each time.
        This is what makes GraphQL subscriptions useful for live data.
        Runs over WebSocket under the hood (strawberry handles this).
        """
        if not verify(token):
            return
        async for r in stream_readings(sensor_id, interval_ms=interval_ms):
            _counters["subscription_messages"] += 1
            yield reading_to_gql(r)

    @strawberry.subscription
    async def multi_sensor_stream(
        self,
        token       : str,
        interval_ms : int = 500,
    ) -> AsyncGenerator[SensorReadingType, None]:
        """Round-robins through all 8 sensors — shows pub/sub fan-out."""
        if not verify(token):
            return
        idx = 0
        while True:
            sid = SENSOR_IDS[idx % len(SENSOR_IDS)]
            r   = next_reading(sid)
            _counters["subscription_messages"] += 1
            yield reading_to_gql(r)
            idx += 1
            await asyncio.sleep(interval_ms / 1000.0)

# ── App assembly ─────────────────────────────────────────────
schema = strawberry.Schema(query=Query, subscription=Subscription)
graphql_router = GraphQLRouter(
    schema,
    subscription_protocols=[GRAPHQL_TRANSPORT_WS_PROTOCOL],
)

app = FastAPI(title="GraphQL Sensor Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])
app.include_router(graphql_router, prefix="/graphql")

@app.get("/health")
def health():
    return {"status": "ok", "service": "graphql", "port": 8004}

@app.get("/token")
def get_token():
    import jwt as _jwt
    token = _jwt.encode({"sub": "benchmark-client", "role": "reader"},
                        SECRET, algorithm=ALGO)
    return {"token": token, "type": "Bearer"}

@app.get("/metrics")
def get_metrics():
    uptime = round(time.time() - _start_ts, 2)
    return {
        "service"                 : "graphql",
        "uptime_seconds"          : uptime,
        "total_queries"           : _counters["queries"],
        "subscription_messages"   : _counters["subscription_messages"],
        "field_selective_queries" : _counters["field_selective"],
        "queries_per_sec"         : round(_counters["queries"] / max(uptime, 1), 2),
    }
