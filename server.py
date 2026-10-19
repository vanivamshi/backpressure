#!/usr/bin/env python3
"""Backpressure demo server (stdlib only).

Layers, from the edge inward:
  1. Token bucket rate limiter        -> 429 + Retry-After
  2. Bounded queue + overflow policy  -> reject (503), block (wait), or drop-oldest
  3. High/low watermarks              -> "X-Backpressure: throttled" so clients slow down early
  4. Pull-based worker pool           -> workers take a job only when free; autoscaled on depth
  5. Circuit breaker                  -> fail fast while the downstream dependency is unhealthy
"""
import argparse
import itertools
import json
import queue
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs


class TokenBucket:
    """Allows `rate` requests/sec on average, with bursts up to `capacity`."""

    def __init__(self, rate, capacity):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def try_acquire(self):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False


class Watermark:
    """Hysteresis: throttle once depth reaches `high`, release only once it falls to `low`."""

    def __init__(self, low, high):
        self.low = low
        self.high = high
        self.throttled = False
        self.lock = threading.Lock()

    def update(self, depth):
        with self.lock:
            if not self.throttled and depth >= self.high:
                self.throttled = True
            elif self.throttled and depth <= self.low:
                self.throttled = False
            return self.throttled


class CircuitOpenError(Exception):
    pass


class CircuitBreaker:
    """CLOSED -> (failures >= threshold) -> OPEN -> (reset_timeout) -> HALF_OPEN.

    HALF_OPEN admits exactly one probe call; everyone else fails fast until it
    finishes. Probe success closes the breaker, probe failure reopens it.
    """

    CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"

    def __init__(self, failure_threshold=5, reset_timeout=2.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.state = self.CLOSED
        self.failures = 0
        self.opened_at = 0.0
        self.probing = False
        self.lock = threading.Lock()

    def call(self, fn, *args):
        with self.lock:
            if self.state == self.OPEN:
                if time.monotonic() - self.opened_at < self.reset_timeout:
                    raise CircuitOpenError()
                self.state = self.HALF_OPEN
            is_probe = self.state == self.HALF_OPEN
            if is_probe:
                if self.probing:
                    raise CircuitOpenError()  # a probe is already in flight
                self.probing = True
        try:
            result = fn(*args)
        except Exception:
            with self.lock:
                # Ignore late failures from calls that started before the breaker opened.
                if is_probe or self.state == self.CLOSED:
                    self.failures += 1
                    if is_probe or self.failures >= self.failure_threshold:
                        self._open()
            raise
        finally:
            if is_probe:
                with self.lock:
                    self.probing = False
        with self.lock:
            # Only the probe (or normal traffic) may close it, not a stale call finishing late.
            if is_probe or self.state == self.CLOSED:
                self.failures = 0
                self.state = self.CLOSED
        return result

    def _open(self):
        self.state = self.OPEN
        self.opened_at = time.monotonic()


ACCEPTED, REJECTED, EVICTED = "accepted", "rejected", "evicted"
POLICIES = ("reject", "block", "drop-oldest")


def enqueue(q, job, policy, block_timeout=1.0):
    """Put `job` on bounded queue `q`, applying the overflow policy when it is full.

    Returns ACCEPTED, REJECTED, or EVICTED (accepted, but the oldest job was dropped).
    """
    if policy == "block":
        try:
            q.put(job, timeout=block_timeout)
            return ACCEPTED
        except queue.Full:
            return REJECTED
    try:
        q.put_nowait(job)
        return ACCEPTED
    except queue.Full:
        if policy == "reject":
            return REJECTED
    # drop-oldest: evict the head to make room for the newest job
    try:
        q.get_nowait()
    except queue.Empty:
        pass
    try:
        q.put_nowait(job)
        return EVICTED
    except queue.Full:
        return REJECTED


class Metrics:
    NAMES = ("received", "accepted", "rate_limited", "queue_full", "dropped",
             "processed", "db_error", "breaker_open", "failed")

    def __init__(self):
        self.counts = dict.fromkeys(self.NAMES, 0)
        self.lock = threading.Lock()

    def inc(self, name, n=1):
        with self.lock:
            self.counts[name] += n

    def snapshot(self):
        with self.lock:
            return dict(self.counts)


class FakeDatabase:
    """Downstream dependency with tunable latency and failure rate."""

    def __init__(self, latency, fail_rate):
        self.latency = latency
        self.fail_rate = fail_rate

    def write(self, job):
        time.sleep(self.latency * random.uniform(0.5, 1.5))
        if random.random() < self.fail_rate:
            raise IOError("db write failed")


class WorkerPool:
    def __init__(self, q, db, breaker, metrics, max_attempts=3):
        self.q = q
        self.db = db
        self.breaker = breaker
        self.metrics = metrics
        self.max_attempts = max_attempts
        self.stops = []  # one stop event per live worker
        self.lock = threading.Lock()

    def size(self):
        with self.lock:
            return len(self.stops)

    def add(self):
        stop = threading.Event()
        with self.lock:
            self.stops.append(stop)
        threading.Thread(target=self._run, args=(stop,), daemon=True).start()

    def remove(self):
        with self.lock:
            if self.stops:
                self.stops.pop().set()

    def _run(self, stop):
        while not stop.is_set():
            try:
                job = self.q.get(timeout=0.5)  # pull: take work only when free
            except queue.Empty:
                continue
            try:
                self.breaker.call(self.db.write, job)
                self.metrics.inc("processed")
            except CircuitOpenError:
                self.metrics.inc("breaker_open")
                self._requeue(job)
                time.sleep(0.1)  # don't spin while downstream is down
            except Exception:
                self.metrics.inc("db_error")
                job["attempts"] += 1
                if job["attempts"] >= self.max_attempts:
                    self.metrics.inc("failed")
                else:
                    self._requeue(job)

    def _requeue(self, job):
        try:
            self.q.put_nowait(job)
        except queue.Full:
            self.metrics.inc("failed")


class App:
    def __init__(self, cfg):
        self.cfg = cfg
        self.policy = cfg.policy
        self.queue = queue.Queue(maxsize=cfg.capacity)
        self.low = int(cfg.capacity * cfg.low)
        self.high = int(cfg.capacity * cfg.high)
        self.bucket = TokenBucket(cfg.rate, cfg.burst)
        self.watermark = Watermark(self.low, self.high)
        self.metrics = Metrics()
        self.db = FakeDatabase(cfg.db_latency, cfg.db_fail_rate)
        self.breaker = CircuitBreaker()
        self.pool = WorkerPool(self.queue, self.db, self.breaker, self.metrics)
        self.ids = itertools.count(1)

    def start(self):
        for _ in range(self.cfg.min_workers):
            self.pool.add()
        threading.Thread(target=self._autoscale, daemon=True).start()
        threading.Thread(target=self._report, daemon=True).start()

    def _autoscale(self):
        while True:
            time.sleep(1.0)
            depth = self.queue.qsize()
            # More workers won't help while the downstream is failing.
            if (depth >= self.high and self.breaker.state == CircuitBreaker.CLOSED
                    and self.pool.size() < self.cfg.max_workers):
                self.pool.add()
            elif depth <= self.low and self.pool.size() > self.cfg.min_workers:
                self.pool.remove()

    def _report(self):
        prev = self.metrics.snapshot()
        while True:
            time.sleep(1.0)
            s = self.status()
            cur = s["counters"]
            d = {k: cur[k] - prev[k] for k in cur}
            prev = cur
            print(f"depth={s['queue_depth']:>4}/{s['queue_capacity']} "
                  f"{'THROTTLED' if s['throttled'] else 'ok':>9} "
                  f"workers={s['workers']:>2} breaker={s['breaker']:<9} | "
                  f"in={d['received']:>4}/s ok={d['accepted']:>4} 429={d['rate_limited']:>4} "
                  f"503={d['queue_full']:>4} drop={d['dropped']:>4} "
                  f"done={d['processed']:>4} fail={d['failed']:>3}", flush=True)

    def status(self):
        depth = self.queue.qsize()
        return {
            "queue_depth": depth,
            "queue_capacity": self.cfg.capacity,
            "throttled": self.watermark.update(depth),
            "workers": self.pool.size(),
            "breaker": self.breaker.state,
            "policy": self.policy,
            "db": {"latency": self.db.latency, "fail_rate": self.db.fail_rate},
            "counters": self.metrics.snapshot(),
        }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            return self._send(200, self.server.app.status())
        self._send(404, {"error": "not found"})

    def do_POST(self):
        path, _, qs = self.path.partition("?")
        if path == "/jobs":
            return self._submit(self.server.app)
        if path == "/admin/db":
            return self._tune_db(self.server.app, parse_qs(qs))
        self._send(404, {"error": "not found"})

    def _submit(self, app):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        app.metrics.inc("received")

        if not app.bucket.try_acquire():
            app.metrics.inc("rate_limited")
            return self._send(429, {"error": "rate limited"}, {"Retry-After": "1"})

        job = {"id": next(app.ids), "body": body.decode(errors="replace"), "attempts": 0}
        outcome = enqueue(app.queue, job, app.policy)
        throttled = app.watermark.update(app.queue.qsize())
        signal = {"X-Backpressure": "throttled" if throttled else "ok"}

        if outcome == REJECTED:
            app.metrics.inc("queue_full")
            return self._send(503, {"error": "queue full"}, {"Retry-After": "2", **signal})
        if outcome == EVICTED:
            app.metrics.inc("dropped")
        app.metrics.inc("accepted")
        self._send(202, {"id": job["id"]}, signal)

    def _tune_db(self, app, params):
        """Change the fake DB at runtime, e.g. POST /admin/db?fail_rate=0.8&latency=0.2"""
        if "fail_rate" in params:
            app.db.fail_rate = float(params["fail_rate"][0])
        if "latency" in params:
            app.db.latency = float(params["latency"][0])
        self._send(200, {"latency": app.db.latency, "fail_rate": app.db.fail_rate})

    def _send(self, code, payload, headers=None):
        data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass  # the per-second report replaces access logs


def make_server(app, host="127.0.0.1", port=8080):
    server = ThreadingHTTPServer((host, port), Handler)
    server.app = app
    return server


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--rate", type=float, default=300, help="token bucket refill (req/s)")
    p.add_argument("--burst", type=int, default=100, help="token bucket capacity")
    p.add_argument("--capacity", type=int, default=500, help="queue size")
    p.add_argument("--low", type=float, default=0.3, help="low watermark (fraction of capacity)")
    p.add_argument("--high", type=float, default=0.8, help="high watermark (fraction of capacity)")
    p.add_argument("--policy", choices=POLICIES, default="reject", help="overflow policy")
    p.add_argument("--min-workers", type=int, default=2)
    p.add_argument("--max-workers", type=int, default=16)
    p.add_argument("--db-latency", type=float, default=0.05, help="seconds per write")
    p.add_argument("--db-fail-rate", type=float, default=0.0)
    return p.parse_args(argv)


def main():
    cfg = parse_args()
    app = App(cfg)
    app.start()
    server = make_server(app, port=cfg.port)
    print(f"listening on :{cfg.port}  policy={cfg.policy} capacity={cfg.capacity} "
          f"watermarks={app.low}/{app.high} rate={cfg.rate}/s", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
