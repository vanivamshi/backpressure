import json
import queue
import threading
import time
import unittest
import urllib.error
import urllib.request

from server import (ACCEPTED, EVICTED, REJECTED, App, CircuitBreaker, CircuitOpenError,
                    TokenBucket, Watermark, enqueue, make_server, parse_args)


class TokenBucketTest(unittest.TestCase):
    def test_burst_then_refill(self):
        b = TokenBucket(rate=100, capacity=5)
        self.assertEqual(sum(b.try_acquire() for _ in range(10)), 5)
        time.sleep(0.05)  # ~5 tokens refilled
        self.assertTrue(b.try_acquire())


class WatermarkTest(unittest.TestCase):
    def test_hysteresis(self):
        w = Watermark(low=3, high=8)
        self.assertFalse(w.update(7))
        self.assertTrue(w.update(8))
        self.assertTrue(w.update(5))   # still above low: stays throttled
        self.assertFalse(w.update(3))


class CircuitBreakerTest(unittest.TestCase):
    def test_opens_then_recovers(self):
        cb = CircuitBreaker(failure_threshold=2, reset_timeout=0.05)

        def boom():
            raise IOError()

        for _ in range(2):
            with self.assertRaises(IOError):
                cb.call(boom)
        self.assertEqual(cb.state, CircuitBreaker.OPEN)
        with self.assertRaises(CircuitOpenError):
            cb.call(lambda: 1)
        time.sleep(0.06)
        self.assertEqual(cb.call(lambda: 1), 1)
        self.assertEqual(cb.state, CircuitBreaker.CLOSED)

    def open_breaker(self):
        cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.05)
        with self.assertRaises(IOError):
            cb.call(self.fail)
        time.sleep(0.06)
        return cb

    @staticmethod
    def fail():
        raise IOError()

    def test_half_open_allows_single_probe(self):
        cb = self.open_breaker()
        started, release = threading.Event(), threading.Event()

        def slow_probe():
            started.set()
            release.wait()
            return "ok"

        t = threading.Thread(target=cb.call, args=(slow_probe,))
        t.start()
        started.wait()
        self.assertEqual(cb.state, CircuitBreaker.HALF_OPEN)
        for _ in range(5):  # concurrent callers fail fast while the probe is in flight
            with self.assertRaises(CircuitOpenError):
                cb.call(lambda: "should not run")
        release.set()
        t.join()
        self.assertEqual(cb.state, CircuitBreaker.CLOSED)

    def test_failed_probe_reopens(self):
        cb = self.open_breaker()
        with self.assertRaises(IOError):
            cb.call(self.fail)
        self.assertEqual(cb.state, CircuitBreaker.OPEN)
        self.assertFalse(cb.probing)
        with self.assertRaises(CircuitOpenError):  # timer restarted
            cb.call(lambda: 1)

    def test_stale_success_does_not_close(self):
        cb = CircuitBreaker(failure_threshold=1, reset_timeout=10)
        started, release = threading.Event(), threading.Event()

        def slow_ok():
            started.set()
            release.wait()

        t = threading.Thread(target=cb.call, args=(slow_ok,))
        t.start()  # starts while CLOSED
        started.wait()
        with self.assertRaises(IOError):
            cb.call(self.fail)  # opens the breaker
        release.set()
        t.join()
        self.assertEqual(cb.state, CircuitBreaker.OPEN)


class EnqueueTest(unittest.TestCase):
    def full_queue(self):
        q = queue.Queue(maxsize=2)
        q.put(1)
        q.put(2)
        return q

    def test_reject(self):
        q = self.full_queue()
        self.assertEqual(enqueue(q, 3, "reject"), REJECTED)
        self.assertEqual(list(q.queue), [1, 2])

    def test_drop_oldest(self):
        q = self.full_queue()
        self.assertEqual(enqueue(q, 3, "drop-oldest"), EVICTED)
        self.assertEqual(list(q.queue), [2, 3])

    def test_block_times_out(self):
        q = self.full_queue()
        self.assertEqual(enqueue(q, 3, "block", block_timeout=0.05), REJECTED)

    def test_block_waits_for_space(self):
        q = self.full_queue()
        threading.Timer(0.05, q.get).start()
        self.assertEqual(enqueue(q, 3, "block", block_timeout=1), ACCEPTED)


class HttpTest(unittest.TestCase):
    def start(self, *argv):
        app = App(parse_args(list(argv)))  # app.start() not called: no workers drain the queue
        server = make_server(app, port=0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_address[1]}"

    def post(self, url):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=b"x", method="POST")) as r:
                return r.status, r.headers
        except urllib.error.HTTPError as e:
            return e.code, e.headers

    def test_queue_full_returns_503_and_throttle_signal(self):
        base = self.start("--capacity", "4", "--high", "0.5", "--low", "0.25", "--burst", "100")
        codes = [self.post(base + "/jobs") for _ in range(5)]
        self.assertEqual([c for c, _ in codes], [202, 202, 202, 202, 503])
        self.assertEqual(codes[0][1]["X-Backpressure"], "ok")
        self.assertEqual(codes[1][1]["X-Backpressure"], "throttled")
        self.assertEqual(codes[4][1]["Retry-After"], "2")

    def test_rate_limit_returns_429(self):
        base = self.start("--rate", "1", "--burst", "2")
        codes = [self.post(base + "/jobs")[0] for _ in range(3)]
        self.assertEqual(codes, [202, 202, 429])

    def test_metrics(self):
        base = self.start()
        self.post(base + "/jobs")
        with urllib.request.urlopen(base + "/metrics") as r:
            m = json.load(r)
        self.assertEqual(m["queue_depth"], 1)
        self.assertEqual(m["counters"]["accepted"], 1)


if __name__ == "__main__":
    unittest.main()
