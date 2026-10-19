# Backpressure demo

A small job-ingestion server that stays stable under overload, and a load generator that reacts to its overload signals. It uses Python 3 and the standard library only, with no dependencies to install.

## Architecture

```
Client ──▶ Token bucket ──▶ Bounded queue ──▶ Worker pool ──▶ Circuit breaker ──▶ Fake DB
             │ 429             │ 503 / block      ▲ pull            │ fail fast
             │                 │ / drop-oldest    │ autoscale       │ (1 probe in half-open)
             │                 ▼                  │ on depth        │
             │          Watermarks (80% / 30%)  ──┘                 │
             │          X-Backpressure: throttled                   │
```

| Layer | What it does | Signal to the client |
|---|---|---|
| Token bucket | Caps the request rate and allows short bursts | `429` + `Retry-After: 1` |
| Bounded queue | Fixed capacity; the overflow policy decides what happens when it is full | `503` + `Retry-After: 2` (`reject` policy) |
| Watermarks | Turn on at the high mark and turn off only below the low mark, so the signal doesn't flap | `X-Backpressure: throttled` on `202` |
| Worker pool | Workers pull jobs only when free; the autoscaler adds workers above the high mark and removes them below the low mark | — |
| Circuit breaker | Opens after 5 consecutive failures; after 2s it lets **one** probe through | Jobs are requeued (counted as failed only if the queue is full) |

### Overflow policies (`--policy`)

- `reject` (default): returns `503` immediately. Use it for public APIs where clients retry.
- `block`: waits up to 1s for space, then returns `503`. Use it for internal pipelines.
- `drop-oldest`: evicts the head of the queue to make room. Use it for telemetry or metrics where the latest data matters most.

## Running

```bash
# Terminal 1: the server prints a status line every second
python3 server.py

# Terminal 2: a client that reacts to backpressure
python3 loadgen.py

# ...or a client that ignores it, for comparison
python3 loadgen.py --naive
```

Server output:

```
depth= 494/500 THROTTLED workers= 3 breaker=closed    | in= 118/s ok=  85 429=   0 503=  33 ...
depth= 146/500        ok workers= 5 breaker=closed    | in=   8/s ok=   8 429=   0 503=   0 ...
```

### Trying failure scenarios

```bash
curl -X POST 'localhost:8080/admin/db?fail_rate=0.9'   # DB fails, breaker opens
curl -X POST 'localhost:8080/admin/db?fail_rate=0'     # DB recovers, probe closes breaker
curl -X POST 'localhost:8080/admin/db?latency=0.3'     # slow DB, queue fills, throttling starts
curl localhost:8080/metrics                            # JSON snapshot
```

## API

| Method | Path | Responses |
|---|---|---|
| `POST` | `/jobs` | `202` accepted · `429` rate limited · `503` queue full |
| `GET` | `/metrics` | Queue depth, throttle state, worker count, breaker state, counters |
| `POST` | `/admin/db?fail_rate=&latency=` | Tunes the fake DB at runtime |

## How the load generator reacts

- **AIMD rate control** (additive increase, multiplicative decrease): it adds about 1 req/s per second while requests succeed. It multiplies its rate by 0.8 when it sees `throttled` and by 0.5 on a `429` or `503`, at most once per 0.5s.
- **Retries:** it waits `max(Retry-After, random(0, min(5s, 0.1s · 2^attempt)))` (exponential backoff with full jitter), for up to 5 attempts per job.

In a sample run, the cooperative client got 96% of its requests accepted. The `--naive` client, sending a fixed 500 req/s, got about 35% accepted.

## Configuration

`python3 server.py --help`

| Flag | Default | |
|---|---|---|
| `--port` | 8080 | |
| `--rate` / `--burst` | 300 / 100 | Token bucket refill rate (req/s) and capacity |
| `--capacity` | 500 | Queue size |
| `--low` / `--high` | 0.3 / 0.8 | Watermarks, as a fraction of capacity |
| `--policy` | reject | `reject` \| `block` \| `drop-oldest` |
| `--min-workers` / `--max-workers` | 2 / 16 | Autoscaler bounds |
| `--db-latency` / `--db-fail-rate` | 0.05 / 0.0 | Fake DB behaviour |

`python3 loadgen.py --help`: `--rate`, `--max-rate`, `--concurrency`, `--duration`, `--max-attempts`, `--naive`.

## Tests

```bash
python3 -m unittest -v test_backpressure
```

The tests cover the token bucket, watermark hysteresis, every circuit breaker transition (including single-probe half-open and ignoring stale calls), each overflow policy, and the `202`, `429` and `503` HTTP behaviour.

## Limitations

- **The queue is in memory.** Queued jobs are lost when the process restarts. For production, back it with Redis (a list or stream) or Kafka. The other layers don't depend on the queue implementation.
- **State belongs to one process.** The rate limiter, watermarks and circuit breaker are local. With several instances, move them to shared storage such as Redis. For example, the half-open probe could take a lock with `SET probe_lock 1 NX PX 5000`.
