from __future__ import annotations

import threading
import time
import unittest

from app import db


class FakeConnection:
    def __init__(self) -> None:
        self.closed = False
        self.rollback_count = 0

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


class FakeConnectionPool:
    def __init__(self) -> None:
        self.in_use = 0
        self.max_in_use = 0
        self.getconn_calls = 0
        self._lock = threading.Lock()

    def getconn(self):
        with self._lock:
            self.getconn_calls += 1
            if self.in_use >= 1:
                raise AssertionError("Pool called before a capacity slot was available")
            self.in_use += 1
            self.max_in_use = max(self.max_in_use, self.in_use)
        return FakeConnection()

    def putconn(self, _connection, close: bool = False) -> None:
        del close
        with self._lock:
            self.in_use -= 1

    def closeall(self) -> None:
        pass


class DatabasePoolBackpressureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_pool = db._connection_pool
        self.original_slots = db._connection_slots
        self.fake_pool = FakeConnectionPool()
        db._connection_pool = self.fake_pool
        db._connection_slots = threading.BoundedSemaphore(1)

    def tearDown(self) -> None:
        self.assertEqual(self.fake_pool.in_use, 0)
        db._connection_pool = self.original_pool
        db._connection_slots = self.original_slots

    def test_waiter_acquires_connection_after_capacity_is_released(self) -> None:
        first = db.get_connection(timeout_seconds=0.1)
        result: dict[str, object] = {}
        started = threading.Event()

        def acquire_after_release() -> None:
            started.set()
            wait_started_at = time.monotonic()
            connection = db.get_connection(timeout_seconds=0.5)
            result["wait_seconds"] = time.monotonic() - wait_started_at
            connection.close()

        worker = threading.Thread(target=acquire_after_release)
        worker.start()
        self.assertTrue(started.wait(timeout=0.2))
        time.sleep(0.06)
        self.assertTrue(worker.is_alive())

        first.close()
        worker.join(timeout=1)

        self.assertFalse(worker.is_alive())
        self.assertGreaterEqual(float(result["wait_seconds"]), 0.05)
        self.assertEqual(self.fake_pool.max_in_use, 1)
        self.assertEqual(self.fake_pool.getconn_calls, 2)

    def test_acquire_timeout_is_bounded_and_does_not_touch_exhausted_pool(self) -> None:
        first = db.get_connection(timeout_seconds=0.1)
        wait_started_at = time.monotonic()

        with self.assertRaises(db.DatabasePoolExhausted):
            db.get_connection(timeout_seconds=0.06)

        waited = time.monotonic() - wait_started_at
        self.assertGreaterEqual(waited, 0.05)
        self.assertLess(waited, 0.3)
        self.assertEqual(self.fake_pool.getconn_calls, 1)

        first.close()
        recovered = db.get_connection(timeout_seconds=0.1)
        recovered.close()

    def test_exception_and_repeated_close_do_not_leak_capacity(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with db.get_connection(timeout_seconds=0.1):
                raise RuntimeError("boom")

        connection = db.get_connection(timeout_seconds=0.1)
        connection.close()
        connection.close()

        final_connection = db.get_connection(timeout_seconds=0.1)
        final_connection.close()
        self.assertEqual(self.fake_pool.in_use, 0)

    def test_same_wrapper_can_be_reused_across_transaction_batches(self) -> None:
        connection = db.get_connection(timeout_seconds=0.1)

        with connection:
            pass
        self.assertEqual(self.fake_pool.in_use, 0)

        with connection:
            pass
        connection.close()

        self.assertEqual(self.fake_pool.in_use, 0)
        self.assertEqual(self.fake_pool.getconn_calls, 2)


if __name__ == "__main__":
    unittest.main()
