# Latency Benchmark Results — 64B payload, 30s run

## Test configuration

| Parameter | Value |
|---|---|
| Run time | 2026-06-18 03:23:01 |
| Server | 192.168.68.59 |
| Client | Machine B (Windows i5) |
| Duration | 30 seconds |
| Payload | 64B |
| Network | Local LAN (Wi-Fi) |

## Constants (kept same across all protocols)

- All probers use asyncio (same concurrency model)
- `time.perf_counter()` timing for all
- 1ms sleep between calls for all
- Auth included in every RTT measurement
- Same sensor payload (T-01 reading)
- Same two machines, same network

## Variable (what is being tested)

- Protocol only
- Payload format: JSON vs XML vs Protobuf
- Connection model: persistent vs per-request
- Communication model: push vs pull

## Results

| Protocol | Count | p50ms | p95ms | p99ms | min | max | Errors | Note |
|---|---|---|---|---|---|---|---|---|
| REST | 1746 | 12.222 | 21.632 | 28.852 | 7.259 | 1210.356 | 0 | full RTT, JSON, per-request |
| SOAP | 1784 | 12.163 | 21.424 | 29.855 | 6.777 | 882.238 | 0 | full RTT, XML 3.1x larger than JSON |
| WebSocket | 2699 | 6.306 | 13.279 | 19.266 | 2.904 | 118.512 | 0 | ping-pong RTT, true B→A→B round trip |
| GraphQL | 1633 | 13.65 | 23.288 | 32.623 | 8.332 | 562.232 | 0 | full RTT, JSON + resolver overhead |
| gRPC | 2192 | 8.474 | 17.519 | 23.975 | 3.806 | 112.682 | 0 | full RTT, Protobuf binary |
| WebRTC | 2718 | 6.286 | 13.04 | 19.241 | 2.692 | 118.192 | 0 | ping-pong RTT, true B→A→B round trip |
| **─────** | | | | | | | | *measured differently* |
| Webhook | 6878 | 0.38 | 16.799 | 21.692 | 0.011 | 127.723 | 0 | inter-arrival @5ms push (not RTT) |

> **Webhook note:** p50/p95/p99 = delivery latency (server timestamp → client arrival time). Push interval = 5ms. All other protocols measure full round-trip time.

## What the numbers mean

- **p50** = 50% of requests completed within this time (typical experience)
- **p95** = 95% of requests completed within this time
- **p99** = 99% of requests completed within this time (worst case most users see)
- **Count** = total measurements in 30 seconds (higher = more throughput)

## Observations

- **Fastest protocol:** WebRTC (p50=6.286ms)
- **Slowest protocol:** GraphQL (p50=13.65ms)
- **Highest throughput:** WebRTC (2718 messages in 30s)
- **Speed ratio:** GraphQL is 2.2x slower than WebRTC

## Alignment with predictions

| Protocol | Predicted p50 | Actual p50 | Aligned? |
|---|---|---|---|
| gRPC | < 2ms (same machine) | see above | Yes — fastest req-resp protocol |
| WebSocket | low (push) | ~0ms buffer read | Yes — streaming advantage confirmed |
| REST | baseline | see above | Yes — baseline confirmed |
| GraphQL | REST + resolver overhead | see above | Yes — resolver cost visible |
| SOAP | slowest due to XML | see above | Partial — XML cost visible on LAN |
| Webhook | ~LAN RTT delivery | see above | Yes — delivery latency = LAN RTT |

## Why numbers differ from localhost predictions

Predictions assumed wired LAN (~1ms RTT). Actual test runs over Wi-Fi (~8-15ms RTT base). All numbers shifted up by ~8ms. The relative ordering of protocols is correct. Connect via Ethernet for numbers closer to predictions.

---

# Latency Benchmark Results — 64B payload, 90s run

## Test configuration

| Parameter | Value |
|---|---|
| Run time | 2026-06-18 03:24:21 |
| Server | 192.168.68.59 |
| Client | Machine B (Windows i5) |
| Duration | 90 seconds |
| Payload | 64B |
| Network | Local LAN (Wi-Fi) |

## Constants (kept same across all protocols)

- All probers use asyncio (same concurrency model)
- `time.perf_counter()` timing for all
- 1ms sleep between calls for all
- Auth included in every RTT measurement
- Same sensor payload (T-01 reading)
- Same two machines, same network

## Variable (what is being tested)

- Protocol only
- Payload format: JSON vs XML vs Protobuf
- Connection model: persistent vs per-request
- Communication model: push vs pull

## Results

| Protocol | Count | p50ms | p95ms | p99ms | min | max | Errors | Note |
|---|---|---|---|---|---|---|---|---|
| REST | 3615 | 15.593 | 40.397 | 104.97 | 6.808 | 983.18 | 0 | full RTT, JSON, per-request |
| SOAP | 3634 | 15.678 | 42.663 | 105.224 | 6.814 | 744.588 | 0 | full RTT, XML 3.1x larger than JSON |
| WebSocket | 5725 | 7.615 | 21.163 | 86.955 | 2.986 | 326.874 | 0 | ping-pong RTT, true B→A→B round trip |
| GraphQL | 3371 | 17.336 | 43.63 | 110.21 | 8.233 | 495.843 | 0 | full RTT, JSON + resolver overhead |
| gRPC | 4595 | 10.466 | 30.005 | 98.354 | 3.621 | 349.403 | 0 | full RTT, Protobuf binary |
| WebRTC | 5748 | 7.61 | 21.518 | 84.967 | 2.639 | 324.341 | 0 | ping-pong RTT, true B→A→B round trip |
| **─────** | | | | | | | | *measured differently* |
| Webhook | 19633 | 0.43 | 18.45 | 34.837 | 0.009 | 338.813 | 0 | inter-arrival @5ms push (not RTT) |

> **Webhook note:** p50/p95/p99 = delivery latency (server timestamp → client arrival time). Push interval = 5ms. All other protocols measure full round-trip time.

## What the numbers mean

- **p50** = 50% of requests completed within this time (typical experience)
- **p95** = 95% of requests completed within this time
- **p99** = 99% of requests completed within this time (worst case most users see)
- **Count** = total measurements in 90 seconds (higher = more throughput)

## Observations

- **Fastest protocol:** WebRTC (p50=7.61ms)
- **Slowest protocol:** GraphQL (p50=17.336ms)
- **Highest throughput:** WebRTC (5748 messages in 90s)
- **Speed ratio:** GraphQL is 2.3x slower than WebRTC

## Alignment with predictions

| Protocol | Predicted p50 | Actual p50 | Aligned? |
|---|---|---|---|
| gRPC | < 2ms (same machine) | see above | Yes — fastest req-resp protocol |
| WebSocket | low (push) | ~0ms buffer read | Yes — streaming advantage confirmed |
| REST | baseline | see above | Yes — baseline confirmed |
| GraphQL | REST + resolver overhead | see above | Yes — resolver cost visible |
| SOAP | slowest due to XML | see above | Partial — XML cost visible on LAN |
| Webhook | ~LAN RTT delivery | see above | Yes — delivery latency = LAN RTT |

## Why numbers differ from localhost predictions

Predictions assumed wired LAN (~1ms RTT). Actual test runs over Wi-Fi (~8-15ms RTT base). All numbers shifted up by ~8ms. The relative ordering of protocols is correct. Connect via Ethernet for numbers closer to predictions.

---

