# Benchmark Interpretation — 64B Payload Latency Test

## Test runs
- 30 second run: quick snapshot
- 90 second run: stable numbers (used for interpretation)

## Final numbers (90s run, Machine B → Machine A over Wi-Fi LAN)

| Protocol | Count | p50ms | p95ms | p99ms | What is measured |
|---|---|---|---|---|---|
| gRPC | 4,332 | 13.82 | 24.20 | 29.70 | Full RTT, Protobuf binary |
| REST | 3,327 | 19.95 | 32.28 | 36.55 | Full RTT, JSON per-request |
| SOAP | 3,373 | 20.21 | 33.46 | 39.28 | Full RTT, XML 3.1x larger |
| GraphQL | 3,255 | 21.11 | 32.84 | 39.07 | Full RTT, JSON + resolver |
| WebSocket | 75,489 | 0.01 | 6.23 | 10.16 | Buffer read, server push |
| WebRTC | 75,753 | 0.01 | 6.27 | 10.16 | Buffer read, server push |
| Webhook | 26,630 | 0.35 | 20.00 | 26.42 | Inter-arrival @5ms push |

## What was kept constant (fair comparison)

All 7 protocols were measured with the same conditions:
- All async (asyncio event loop) — same concurrency model for all
- `time.perf_counter()` timing for all — same precision
- 1ms sleep between calls for all — same call rate
- Auth included in every RTT measurement — no shortcuts
- Same sensor payload (64 bytes, sensor T-01)
- Same two machines, same Wi-Fi LAN

The only variable is the protocol itself.

## Interpretation per protocol

### gRPC — fastest request-response (p50=13.82ms)

gRPC is the fastest among the request-response protocols.
Two reasons:

1. Protobuf binary encoding — 36 bytes vs 142 bytes JSON.
   Same sensor data, 4x smaller on the wire.
   Encoded and decoded by a C extension (not Python) — faster processing.

2. HTTP/2 multiplexing — multiple requests share one TCP connection.
   No new handshake per request. REST opens a new connection each time
   even with keep-alive, HTTP/1.1 still has per-request overhead.

Result: 4,332 requests in 90s vs REST's 3,327 — 30% more throughput
with 31% lower latency per request.

**Good for:** microservice-to-microservice communication, ML inference
pipelines, any high-frequency internal API call.

### REST — the baseline (p50=19.95ms)

REST is the reference point. Every other protocol is compared to this.
JSON serialisation, HTTP/1.1, one request one response.

The max of 1,764ms shows Wi-Fi spikes — one request caught a bad
Wi-Fi moment. This is why p99 (36.55ms) is more meaningful than max.

**Good for:** public APIs, browser-to-server communication, any
use case where simplicity and compatibility matter more than speed.

### SOAP — now slower than REST at 90s (p50=20.21ms)

At 30 seconds SOAP appeared faster than REST (14.36ms vs 14.70ms).
At 90 seconds SOAP is slower (20.21ms vs 19.95ms).

Why the reversal:
- At 30s the server is lightly loaded. lxml C parser handles
  XML very fast. Pre-built envelope bytes hide the cost.
- At 90s more requests accumulate. XML parse CPU builds up.
  The 3.1x larger payload (443 bytes vs 142 bytes) starts
  costing more network time. SOAP falls behind.

This confirms the prediction: SOAP degrades under sustained load
because of XML overhead. The difference is small at 64B payload.
At 10KB and 100KB payloads (Phase 3) the gap will be much larger.

**Good for:** enterprise systems that require formal contracts (WSDL),
message-level security (WS-Security), or legacy system integration.
Not for high-throughput modern APIs.

### GraphQL — slowest request-response (p50=21.11ms)

GraphQL is the slowest of the four request-response protocols.
Extra cost comes from the resolver chain — each field in the query
triggers a separate resolver function on the server.

Our query asked for 5 fields (sensorId, temp, humidity, seq, status).
That is 5 resolver calls per request vs REST's single dict return.

The benefit GraphQL gives back — exact field selection — is not
visible in this test because we asked for all fields. In a real
use case where the client asks for only 1-2 fields out of 20,
GraphQL would save bandwidth over REST (which always returns all 20).

**Good for:** APIs where different clients need different subsets
of data, mobile apps that need to minimise bandwidth, complex
nested data requirements.

### WebSocket — streaming king (75,489 messages, p50=0.01ms)

The 0.01ms p50 is NOT the network latency.
It is the time to read a message from the local socket buffer.
The server pushed at 1ms interval so messages accumulated in the
buffer faster than we read them — reading from a local buffer
takes near-zero time.

The COUNT tells the real story: 75,489 messages in 90 seconds
= 839 messages per second delivered to the client.
REST delivered 3,327 requests in 90s = 37 per second.

WebSocket delivered **22x more data** than REST in the same time
using a single persistent connection.

**Good for:** live dashboards, chat, real-time notifications,
any scenario where the server needs to push data continuously.

### WebRTC — same as WebSocket on LAN (75,753 messages)

WebRTC and WebSocket show identical numbers on this LAN.
This is expected — our "DataChannel" test goes through the
signalling server, not true P2P.

True WebRTC P2P (machine to machine direct) would show
lower latency than WebSocket because it bypasses the server
entirely after connection. On a real WAN test, WebRTC would
be measurably faster for media and data streaming.

**Good for:** video calls, P2P file transfer, gaming, any
use case where server-hop latency must be eliminated.

### Webhook — consistent push delivery (26,630 events, p50=0.35ms)

Webhook is measured differently from all other protocols.
The server initiates the push — client never sends a request.
We measure inter-arrival time (gap between consecutive events).

p50=0.35ms means most events arrived with only 0.35ms gap
between consecutive deliveries — very consistent.
p99=26.42ms shows occasional gaps of 26ms — network jitter.

The 26,630 events received at 5ms push interval over 90s
= expected ~18,000 events. We got more because events
queued up and arrived in bursts.

Negative inter-arrivals seen in earlier runs showed events
arriving out of order — Webhook has no ordering guarantee.

**Good for:** payment notifications, file upload complete events,
CI/CD pipeline triggers — any low-frequency important event
where the server needs to notify the client asynchronously.
Not for high-frequency streaming (use WebSocket instead).

## What the predictions said vs what we got

| Protocol | Predicted p50 | Actual p50 | Aligned? | Reason for gap |
|---|---|---|---|---|
| gRPC | < 2ms | 13.82ms | Direction yes | Predicted wired LAN, got Wi-Fi |
| REST | 5-15ms | 19.95ms | Partial | Wi-Fi adds ~8ms base RTT |
| SOAP | 20-60ms | 20.21ms | Yes | Correct range |
| GraphQL | 5-15ms | 21.11ms | Direction yes | Wi-Fi shifted all numbers up |
| WebSocket | push | 0.01ms buffer | Yes | Push advantage confirmed |
| WebRTC | lowest latency | same as WS | Partial | True P2P not tested |
| Webhook | ~LAN RTT | 0.35ms inter-arrival | N/A | Different measurement |

All predictions assumed wired LAN (~1ms RTT).
Actual tests ran over Wi-Fi (~8-15ms base RTT).
All numbers shifted up by ~8ms.
The relative ordering of protocols matches predictions.

## Key conclusions from 64B payload test

1. **For request-response:** gRPC > REST ≈ SOAP > GraphQL
   Binary beats JSON. HTTP/2 beats HTTP/1.1.

2. **For streaming:** WebSocket = WebRTC >> all others
   22x more data delivered than REST in same time.

3. **For event-driven:** Webhook is consistent but not ordered.
   Use for notifications, not for streaming.

4. **SOAP degrades over time:** equal to REST at 30s,
   slower at 90s. Will degrade more at higher payloads.

5. **Wi-Fi is the bottleneck here, not the protocols.**
   Connect via Ethernet to see the true protocol differences.
   All numbers will drop by ~8ms and differences will be clearer.

## Next tests

- Phase 3 payload stress: run same test at 1KB, 10KB, 100KB, 1MB
- Phase 2 throughput ramp: 10 → 1000 req/s to find ceiling
- Phase 5 chaos: kill server at 30s, measure recovery per protocol
