"""
server/shared/iot_feed.py
─────────────────────────────────────────────────────────────────
Shared IoT sensor data generator used by all 7 API services.

Every protocol in this project delivers the SAME payload so that
benchmark differences reflect the protocol, not the data.

Payload shape (mirrors sensor.proto SensorReading):
  {
    "sensor_id" : "T-01",
    "temp"      : 28.4,
    "humidity"  : 62.1,
    "timestamp" : 1718432100.4,
    "seq"       : 42,
    "location"  : "lab-room-1",
    "status"    : "NORMAL"
  }

Usage
─────
  # one reading (dict)
  from shared.iot_feed import next_reading
  reading = next_reading("T-01")

  # async generator — yields every interval_ms milliseconds
  async for reading in stream_readings("T-01", interval_ms=100):
      ...

  # sync generator — for SOAP / blocking contexts
  for reading in stream_readings_sync("T-01", interval_ms=100):
      ...

  # padded reading for Phase 3 payload stress test
  reading = next_reading_padded("T-01", pad_bytes=1024)

Run standalone to verify output:
  python iot_feed.py
"""

import asyncio
import json
import math
import os
import random
import time
from dataclasses import dataclass, asdict
from typing import AsyncGenerator, Generator, Optional


# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────

SENSOR_IDS = [f"T-{i:02d}" for i in range(1, 9)]   # T-01 … T-08
LOCATIONS  = [
    "lab-room-1", "lab-room-2", "server-rack",
    "outdoor-north", "outdoor-south", "hallway",
]
STATUS_WEIGHTS = [0.85, 0.12, 0.03]   # NORMAL / WARNING / CRITICAL
STATUSES       = ["NORMAL", "WARNING", "CRITICAL"]


# ─────────────────────────────────────────────────────────────
# Dataclass — mirrors proto SensorReading exactly
# ─────────────────────────────────────────────────────────────

@dataclass
class SensorReading:
    sensor_id : str
    temp      : float   # Celsius
    humidity  : float   # percent
    timestamp : float   # Unix epoch
    seq       : int     # monotonic counter — detects lost messages
    location  : str
    status    : str     # NORMAL / WARNING / CRITICAL

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    def to_json_bytes(self) -> bytes:
        return self.to_json().encode()


@dataclass
class SensorReadingPadded:
    """Phase 3: same reading + padding bytes to hit target size."""
    reading   : SensorReading
    pad_bytes : int
    _pad      : bytes = b""

    def __post_init__(self):
        self._pad = os.urandom(self.pad_bytes)

    def to_dict(self) -> dict:
        d = self.reading.to_dict()
        d["_pad"] = self._pad.hex()   # hex string so JSON-serialisable
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_json_bytes(self) -> bytes:
        return self.to_json().encode()


# ─────────────────────────────────────────────────────────────
# Sequence counter (per sensor, persistent within process)
# ─────────────────────────────────────────────────────────────

_seq_counters: dict[str, int] = {}

def _next_seq(sensor_id: str) -> int:
    _seq_counters[sensor_id] = _seq_counters.get(sensor_id, 0) + 1
    return _seq_counters[sensor_id]


# ─────────────────────────────────────────────────────────────
# Realistic sensor simulation
# Uses a sine wave + noise so values look real on charts
# ─────────────────────────────────────────────────────────────

def _simulate_temp(sensor_id: str, t: float) -> float:
    """22–35 °C with slow drift + noise, unique phase per sensor."""
    seed  = abs(hash(sensor_id)) % 100
    base  = 28.0 + 4.0 * math.sin(t / 300 + seed)   # 5-min cycle
    noise = random.gauss(0, 0.3)
    return round(base + noise, 2)

def _simulate_humidity(sensor_id: str, t: float) -> float:
    """45–75 % with slow drift + noise, anti-correlated with temp."""
    seed  = abs(hash(sensor_id + "h")) % 100
    base  = 60.0 - 8.0 * math.sin(t / 300 + seed)
    noise = random.gauss(0, 0.5)
    return round(max(0.0, min(100.0, base + noise)), 2)

def _simulate_status(temp: float, humidity: float) -> str:
    if temp > 33.0 or humidity > 72.0:
        return "CRITICAL"
    if temp > 31.0 or humidity > 68.0:
        return "WARNING"
    return "NORMAL"


# ─────────────────────────────────────────────────────────────
# Core factory
# ─────────────────────────────────────────────────────────────

def next_reading(sensor_id: Optional[str] = None) -> SensorReading:
    """Return one SensorReading for the given sensor (or a random one)."""
    sid = sensor_id or random.choice(SENSOR_IDS)
    t   = time.time()
    temp     = _simulate_temp(sid, t)
    humidity = _simulate_humidity(sid, t)
    return SensorReading(
        sensor_id = sid,
        temp      = temp,
        humidity  = humidity,
        timestamp = round(t, 4),
        seq       = _next_seq(sid),
        location  = LOCATIONS[abs(hash(sid)) % len(LOCATIONS)],
        status    = _simulate_status(temp, humidity),
    )


def next_reading_padded(
    sensor_id: Optional[str] = None,
    pad_bytes: int = 0,
) -> SensorReadingPadded:
    """Phase 3: reading + pad_bytes of random data."""
    return SensorReadingPadded(
        reading   = next_reading(sensor_id),
        pad_bytes = pad_bytes,
    )


# ─────────────────────────────────────────────────────────────
# Async generator — used by FastAPI / WebSocket / GraphQL / gRPC
# ─────────────────────────────────────────────────────────────

async def stream_readings(
    sensor_id   : Optional[str] = None,
    interval_ms : int  = 100,
    max_messages: int  = 0,
    pad_bytes   : int  = 0,
) -> AsyncGenerator[SensorReading | SensorReadingPadded, None]:
    """
    Async generator — yields a reading every interval_ms milliseconds.

    Args:
        sensor_id   : specific sensor or None for random rotation
        interval_ms : push cadence in milliseconds (default 100 = 10/sec)
        max_messages: stop after N messages; 0 = stream forever
        pad_bytes   : >0 yields SensorReadingPadded (Phase 3)
    """
    count    = 0
    interval = interval_ms / 1000.0
    while max_messages == 0 or count < max_messages:
        if pad_bytes > 0:
            yield next_reading_padded(sensor_id, pad_bytes)
        else:
            yield next_reading(sensor_id)
        count += 1
        await asyncio.sleep(interval)


# ─────────────────────────────────────────────────────────────
# Sync generator — used by SOAP / blocking contexts
# ─────────────────────────────────────────────────────────────

def stream_readings_sync(
    sensor_id   : Optional[str] = None,
    interval_ms : int  = 100,
    max_messages: int  = 0,
    pad_bytes   : int  = 0,
) -> Generator[SensorReading | SensorReadingPadded, None, None]:
    """Blocking version of stream_readings for sync services."""
    count    = 0
    interval = interval_ms / 1000.0
    while max_messages == 0 or count < max_messages:
        if pad_bytes > 0:
            yield next_reading_padded(sensor_id, pad_bytes)
        else:
            yield next_reading(sensor_id)
        count += 1
        time.sleep(interval)


# ─────────────────────────────────────────────────────────────
# Batch factory — used by gRPC client-streaming test
# ─────────────────────────────────────────────────────────────

def reading_batch(
    size      : int = 100,
    sensor_id : Optional[str] = None,
) -> list[SensorReading]:
    """Return a list of readings instantly (no sleep). For batch upload."""
    return [next_reading(sensor_id) for _ in range(size)]


# ─────────────────────────────────────────────────────────────
# Phase 3 pad-size presets
# ─────────────────────────────────────────────────────────────

PAD_SIZES = {
    "64B"  :       0,    # no padding — baseline reading ~120 bytes JSON
    "1KB"  :     900,    # reading + 900 bytes pad ≈ 1 KB JSON
    "10KB" :    9900,
    "100KB":   99900,
    "1MB"  :  999900,
}


# ─────────────────────────────────────────────────────────────
# Standalone verification
# Run: python iot_feed.py
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("IoT Feed Generator — standalone verification")
    print("=" * 60)

    # 1. Single readings for each sensor
    print("\n[1] One reading per sensor:")
    for sid in SENSOR_IDS:
        r = next_reading(sid)
        print(f"  {r.sensor_id}  temp={r.temp}°C  "
              f"hum={r.humidity}%  seq={r.seq}  status={r.status}")

    # 2. JSON output
    print("\n[2] JSON output of one reading:")
    r = next_reading("T-01")
    print(f"  {r.to_json()}")

    # 3. Sequence counter check
    print("\n[3] Sequence counter (T-01 × 5):")
    for _ in range(5):
        r = next_reading("T-01")
        print(f"  seq={r.seq}", end="  ")
    print()

    # 4. Padded reading sizes
    print("\n[4] Padded reading sizes for Phase 3:")
    for label, pad in PAD_SIZES.items():
        rp = next_reading_padded("T-01", pad_bytes=pad)
        size = len(rp.to_json_bytes())
        print(f"  {label:6s}  pad={pad:7d} bytes  "
              f"total JSON size = {size:8,d} bytes")

    # 5. Batch factory
    print("\n[5] Batch of 10 readings (instant, no sleep):")
    batch = reading_batch(size=10, sensor_id="T-02")
    for r in batch:
        print(f"  seq={r.seq}  temp={r.temp}")

    # 6. Sync stream — 5 readings at 200ms interval
    print("\n[6] Sync stream — 5 readings at 200ms interval:")
    for r in stream_readings_sync("T-03", interval_ms=200, max_messages=5):
        print(f"  {r.timestamp:.3f}  seq={r.seq}  temp={r.temp}")

    # 7. Async stream — 5 readings at 100ms interval
    print("\n[7] Async stream — 5 readings at 100ms interval:")
    async def _test_async():
        async for r in stream_readings("T-04", interval_ms=100, max_messages=5):
            print(f"  {r.timestamp:.3f}  seq={r.seq}  temp={r.temp}")
    asyncio.run(_test_async())

    print("\n" + "=" * 60)
    print("All checks passed. iot_feed.py is ready.")
    print("=" * 60)
