#!/usr/bin/env python3
"""Load generator that reacts to backpressure.

Cooperative mode (default):
  - AIMD rate control: +1 req/s per second while healthy, x0.8 on "X-Backpressure: throttled",
    x0.5 on 429/503 (at most one decrease per 0.5s so concurrent responses don't collapse it).
  - Rejected jobs are retried after max(Retry-After, exponential backoff with full jitter).

Naive mode (--naive): fixed rate, no backoff, no retries. Use it to compare.
"""
import argparse
import random
import threading
import time
import urllib.error
import urllib.request
from collections import Counter


class RateController:
    def __init__(self, rate, min_rate, max_rate, adaptive):
        self.rate = rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.adaptive = adaptive
        self.last_decrease = 0.0
        self.lock = threading.Lock()

    def on_ok(self):
        if not self.adaptive:
            return
        with self.lock:
            # +1/rate per success ~= +1 req/s per second
            self.rate = min(self.max_rate, self.rate + 1.0 / self.rate)

    def on_pressure(self, factor):
        if not self.adaptive:
            return
        with self.lock:
            now = time.monotonic()
            if now - self.last_decrease >= 0.5:
                self.rate = max(self.min_rate, self.rate * factor)
                self.last_decrease = now


def send(url, body, timeout):
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.headers
    except urllib.error.HTTPError as e:
        return e.code, e.headers


def backoff_delay(attempt, retry_after, base=0.1, cap=5.0):
    return max(retry_after, random.uniform(0, min(cap, base * 2 ** attempt)))


def worker(args, ctl, stats, stats_lock, deadline):
    seq = 0
    while time.monotonic() < deadline:
        seq += 1
        body = f"job-{threading.get_ident()}-{seq}".encode()
        for attempt in range(args.max_attempts):
            time.sleep(args.concurrency / ctl.rate)  # pace: threads share the target rate
            try:
                status, headers = send(args.url, body, args.timeout)
            except OSError:
                status, headers = 0, {}

            with stats_lock:
                stats["sent"] += 1
                stats[status] += 1
                throttled = headers.get("X-Backpressure") == "throttled"
                if throttled:
                    stats["throttled"] += 1

            if status == 202:
                ctl.on_pressure(0.8) if throttled else ctl.on_ok()
                break
            if args.naive:
                break
            ctl.on_pressure(0.5)
            retry_after = float(headers.get("Retry-After") or 0)
            time.sleep(backoff_delay(attempt, retry_after))
        else:
            with stats_lock:
                stats["gave_up"] += 1


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default="http://127.0.0.1:8080/jobs")
    p.add_argument("--rate", type=float, default=500, help="initial target req/s")
    p.add_argument("--max-rate", type=float, default=2000)
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--duration", type=float, default=30)
    p.add_argument("--max-attempts", type=int, default=5)
    p.add_argument("--timeout", type=float, default=5)
    p.add_argument("--naive", action="store_true", help="ignore backpressure signals")
    args = p.parse_args()

    ctl = RateController(args.rate, min_rate=5, max_rate=args.max_rate, adaptive=not args.naive)
    stats, stats_lock = Counter(), threading.Lock()
    deadline = time.monotonic() + args.duration
    threads = [threading.Thread(target=worker, args=(args, ctl, stats, stats_lock, deadline), daemon=True)
               for _ in range(args.concurrency)]
    for t in threads:
        t.start()

    prev = Counter()
    while any(t.is_alive() for t in threads):
        time.sleep(1.0)
        with stats_lock:
            cur = Counter(stats)
        d = cur - prev
        prev = cur
        print(f"target={ctl.rate:7.1f}/s sent={d['sent']:>4} 202={d[202]:>4} "
              f"throttled={d['throttled']:>4} 429={d[429]:>4} 503={d[503]:>4} "
              f"err={d[0]:>3} gave_up={d['gave_up']:>3}", flush=True)

    print("\ntotal:", {str(k): v for k, v in sorted(stats.items(), key=str)})


if __name__ == "__main__":
    main()
