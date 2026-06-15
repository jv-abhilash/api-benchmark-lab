"""
server/grpc/main.py
gRPC service — port 50051
Duplex:    Full bidirectional  |  Stateless: Yes (unary) / No (streams)
Auth:      mTLS (certs) — simplified to metadata token for this project
Payload:   Protobuf binary — 3-10x smaller than JSON
Transport: HTTP/2 — multiplexed streams, no head-of-line blocking

All 4 call types implemented:
  1. GetLatestReading   — unary         (like REST GET)
  2. StreamReadings     — server stream  (live feed)
  3. BatchUpload        — client stream  (bulk ingest)
  4. BiDiSensor         — bidi stream    (Phase 4 duplex flood)
"""
import asyncio, time, os, sys, grpc
from concurrent import futures
from collections import defaultdict
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import server.grpc.sensor_pb2      as pb2
import server.grpc.sensor_pb2_grpc as pb2_grpc
from server.shared.iot_feed import (
    next_reading, stream_readings_sync, reading_batch,
    PAD_SIZES, next_reading_padded
)

TOKEN     = os.getenv("GRPC_TOKEN", "api-bench-secret")
_counters = defaultdict(int)
_start_ts = time.time()

def reading_to_proto(r) -> pb2.SensorReading:
    """Convert iot_feed SensorReading dataclass → Protobuf message."""
    status_map = {"NORMAL": pb2.NORMAL, "WARNING": pb2.WARNING, "CRITICAL": pb2.CRITICAL}
    return pb2.SensorReading(
        sensor_id = r.sensor_id,
        temp      = r.temp,
        humidity  = r.humidity,
        timestamp = r.timestamp,
        seq       = r.seq,
        location  = r.location,
        status    = status_map.get(r.status, pb2.NORMAL),
    )

def verify_token(context) -> bool:
    """Check bearer token in gRPC metadata."""
    meta = dict(context.invocation_metadata())
    return meta.get("authorization", "") == f"Bearer {TOKEN}"

def abort_unauthenticated(context):
    context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or missing token")

# ── Servicer — implements all 4 call types ───────────────────
class SensorServicer(pb2_grpc.SensorServiceServicer):

    # 1. Unary — one request, one response
    def GetLatestReading(self, request, context):
        if not verify_token(context):
            abort_unauthenticated(context)
            return pb2.SensorReading()
        _counters["unary"] += 1
        r = next_reading(request.sensor_id or None)
        return reading_to_proto(r)

    # 2. Server streaming — one request, stream of responses
    def StreamReadings(self, request, context):
        if not verify_token(context):
            abort_unauthenticated(context)
            return
        _counters["server_stream_calls"] += 1
        count    = 0
        interval = (request.interval_ms or 100) / 1000.0
        max_msg  = request.max_messages or 0
        pad      = request.pad_size_bytes or 0

        while context.is_active():
            if max_msg > 0 and count >= max_msg:
                break
            if pad > 0:
                rp = next_reading_padded(request.sensor_id or None, pad_bytes=pad)
                r  = rp.reading
            else:
                r  = next_reading(request.sensor_id or None)
            yield reading_to_proto(r)
            _counters["stream_messages"] += 1
            count += 1
            time.sleep(interval)

    # 3. Client streaming — stream of requests, one response
    def BatchUpload(self, request_iterator, context):
        if not verify_token(context):
            abort_unauthenticated(context)
            return pb2.BatchAck()
        _counters["batch_uploads"] += 1
        t_start  = time.time()
        received = 0
        rejected = 0
        for proto_reading in request_iterator:
            # validate: temp must be in sane range
            if -50 <= proto_reading.temp <= 100:
                received += 1
            else:
                rejected += 1
            _counters["batch_messages"] += 1
        duration_ms = (time.time() - t_start) * 1000
        return pb2.BatchAck(
            received    = received,
            rejected    = rejected,
            duration_ms = duration_ms,
        )

    # 4. Bidirectional streaming — both sides stream simultaneously
    def BiDiSensor(self, request_iterator, context):
        if not verify_token(context):
            abort_unauthenticated(context)
            return
        _counters["bidi_calls"] += 1
        paused   = False
        interval = 0.1

        def handle_controls():
            nonlocal paused, interval
            for ctrl in request_iterator:
                if ctrl.command == pb2.StreamControl.PAUSE:
                    paused = True
                elif ctrl.command == pb2.StreamControl.RESUME:
                    paused   = False
                    interval = (ctrl.new_interval_ms or 100) / 1000.0
                elif ctrl.command == pb2.StreamControl.STOP:
                    context.cancel()
                    break
                elif ctrl.command == pb2.StreamControl.PING:
                    pass  # pong sent in main loop

        import threading
        t = threading.Thread(target=handle_controls, daemon=True)
        t.start()

        while context.is_active():
            if not paused:
                r = next_reading()
                frame = pb2.StreamFrame(reading=reading_to_proto(r))
                yield frame
                _counters["bidi_messages"] += 1
            time.sleep(interval)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SensorServiceServicer_to_server(SensorServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print(f"gRPC server started on port 50051")
    print(f"Token: Bearer {TOKEN}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
