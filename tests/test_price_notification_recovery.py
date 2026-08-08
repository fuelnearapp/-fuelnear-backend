from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest

import httpx
import psycopg2

from app import main
from app.apns_client import parse_apns_response


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class PriceNotificationRecoveryTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-push-recovery-",
            dir="/private/tmp",
        )
        cls.data_dir = Path(cls.temp_dir.name) / "postgres"
        cls.socket_dir = Path(cls.temp_dir.name) / "socket"
        cls.socket_dir.mkdir()
        cls.port = find_free_port()
        cls.pg_ctl = pg_ctl
        subprocess.run(
            [initdb, "-D", str(cls.data_dir), "-A", "trust", "-U", "postgres"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                pg_ctl,
                "-D",
                str(cls.data_dir),
                "-o",
                f"-F -p {cls.port} -k {cls.socket_dir}",
                "-w",
                "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls.connection_kwargs = {
            "dbname": "postgres",
            "user": "postgres",
            "host": str(cls.socket_dir),
            "port": cls.port,
        }
        with cls.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE sent_price_notifications (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        mimit_run_id BIGINT NOT NULL,
                        fuel_type TEXT NOT NULL,
                        station_id BIGINT NULL,
                        price DOUBLE PRECISION NULL,
                        distance_km DOUBLE PRECISION NULL,
                        send_attempts INTEGER NOT NULL DEFAULT 0,
                        processing_attempts INTEGER NOT NULL DEFAULT 0,
                        last_error_temporary BOOLEAN NULL,
                        last_status_code INTEGER NULL,
                        last_reason TEXT NULL,
                        processing_started_at TIMESTAMPTZ NULL,
                        sent_at TIMESTAMPTZ NULL,
                        status TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (user_id, mimit_run_id)
                    );
                    CREATE TABLE user_device_tokens (
                        id BIGSERIAL PRIMARY KEY,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )

    @classmethod
    def tearDownClass(cls) -> None:
        subprocess.run(
            [cls.pg_ctl, "-D", str(cls.data_dir), "-m", "fast", "stop"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls.temp_dir.cleanup()

    @classmethod
    def connect(cls):
        return psycopg2.connect(**cls.connection_kwargs)

    def setUp(self) -> None:
        self.now = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE sent_price_notifications, user_device_tokens RESTART IDENTITY;"
                )

    def insert_record(
        self,
        *,
        user_id: int = 1,
        status: str = "processing",
        started_at: datetime | None | object = ...,
        updated_at: datetime | None = None,
        processing_attempts: int = 1,
        temporary: bool | None = None,
    ) -> int:
        selected_started_at = (
            self.now - timedelta(hours=1)
            if started_at is ...
            else started_at
        )
        selected_updated_at = updated_at or self.now - timedelta(hours=1)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO sent_price_notifications (
                        user_id, mimit_run_id, fuel_type, station_id, price,
                        distance_km, status, processing_started_at,
                        processing_attempts, last_error_temporary, updated_at
                    )
                    VALUES (%s, 77, 'benzina', 10, 1.8, 1.2, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        user_id,
                        status,
                        selected_started_at,
                        processing_attempts,
                        temporary,
                        selected_updated_at,
                    ),
                )
                return int(cur.fetchone()[0])

    def recover(self, *, max_attempts: int = 3) -> dict:
        with self.connect() as conn:
            return main.recover_stale_price_notifications(
                conn,
                77,
                reference_date=self.now,
                stale_seconds=900,
                max_attempts=max_attempts,
            )

    def row(self, record_id: int) -> tuple:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, processing_attempts, last_error_temporary,
                           last_reason, processing_started_at
                    FROM sent_price_notifications
                    WHERE id = %s;
                    """,
                    (record_id,),
                )
                return cur.fetchone()

    def claim(self, user_id: int = 1, max_attempts: int = 3) -> int | None:
        with self.connect() as conn:
            return main.claim_price_notification_record(
                conn,
                user_id=user_id,
                mimit_run_id=77,
                fuel_type="benzina",
                station_id=10,
                price=1.8,
                distance_km=1.2,
                max_attempts=max_attempts,
            )

    def test_recent_processing_is_not_recovered(self):
        record_id = self.insert_record(started_at=self.now - timedelta(minutes=5))
        summary = self.recover()
        self.assertEqual(summary["stale_processing_found_count"], 0)
        self.assertEqual(self.row(record_id)[0], "processing")

    def test_stale_processing_becomes_retryable(self):
        record_id = self.insert_record()
        summary = self.recover()
        self.assertEqual(summary["stale_processing_recovered_count"], 1)
        self.assertEqual(self.row(record_id)[:4], ("failed", 2, True, "StaleProcessingRecovered"))

    def test_multiple_stale_records_are_recovered(self):
        ids = [self.insert_record(user_id=user_id) for user_id in (1, 2, 3)]
        summary = self.recover()
        self.assertEqual(summary["stale_processing_recovered_count"], 3)
        self.assertTrue(all(self.row(record_id)[0] == "failed" for record_id in ids))

    def test_stale_record_at_max_attempts_becomes_terminal(self):
        record_id = self.insert_record(processing_attempts=3)
        summary = self.recover(max_attempts=3)
        self.assertEqual(summary["stale_processing_terminal_count"], 1)
        self.assertEqual(self.row(record_id)[:4], ("failed", 3, False, "ProcessingAttemptsExceeded"))
        self.assertIsNone(self.claim(max_attempts=3))

    def test_two_workers_do_not_double_claim_recovered_record(self):
        self.insert_record()
        with ThreadPoolExecutor(max_workers=2) as executor:
            recoveries = list(executor.map(lambda _value: self.recover(), range(2)))
        self.assertEqual(
            sum(item["stale_processing_recovered_count"] for item in recoveries),
            1,
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(lambda _value: self.claim(), range(2)))
        self.assertEqual(sum(claim is not None for claim in claims), 1)

    def test_next_run_recovers_crash_after_processing_claim(self):
        record_id = self.claim()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE sent_price_notifications
                    SET processing_started_at = %s, updated_at = %s
                    WHERE id = %s;
                    """,
                    (self.now - timedelta(hours=1), self.now - timedelta(hours=1), record_id),
                )
        self.assertEqual(self.recover()["stale_processing_recovered_count"], 1)
        self.assertEqual(self.claim(), record_id)
        self.assertEqual(self.row(record_id)[1], 2)

    def test_normal_temporary_failure_remains_retryable(self):
        record_id = self.insert_record(
            status="failed",
            started_at=None,
            processing_attempts=1,
            temporary=True,
        )
        self.assertEqual(self.claim(), record_id)
        self.assertEqual(self.row(record_id)[:2], ("processing", 2))

    def test_apns_temporary_failure_remains_retryable_in_queue(self):
        temporary = parse_apns_response(
            httpx.Response(503, json={"reason": "ServiceUnavailable"}),
            "production",
            1,
        )
        record_id = self.claim()
        with self.connect() as conn:
            main.finalize_price_notification_record(
                conn,
                notification_id=record_id,
                final_status="failed",
                send_attempts=temporary["attempts"],
                temporary_failure=temporary["temporary_error"],
                last_status_code=temporary["status_code"],
                last_reason=temporary["reason"],
                max_attempts=3,
            )
        self.assertTrue(temporary["temporary_error"])
        self.assertEqual(self.row(record_id)[:3], ("failed", 1, True))
        self.assertEqual(self.claim(), record_id)

    def test_invalid_apns_token_is_disabled_and_record_is_terminal(self):
        invalid = parse_apns_response(
            httpx.Response(410, json={"reason": "Unregistered"}),
            "production",
            1,
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO user_device_tokens DEFAULT VALUES RETURNING id;")
                token_id = int(cur.fetchone()[0])
        record_id = self.claim()
        with self.connect() as conn:
            main.deactivate_invalid_device_token(conn, token_id)
            main.finalize_price_notification_record(
                conn,
                notification_id=record_id,
                final_status="failed",
                send_attempts=invalid["attempts"],
                temporary_failure=invalid["temporary_error"],
                last_status_code=invalid["status_code"],
                last_reason=invalid["reason"],
                max_attempts=3,
            )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT is_active FROM user_device_tokens WHERE id = %s;", (token_id,))
                self.assertFalse(cur.fetchone()[0])
        self.assertTrue(invalid["invalid_token"])
        self.assertFalse(invalid["temporary_error"])
        self.assertEqual(self.row(record_id)[:3], ("failed", 1, False))
        self.assertIsNone(self.claim())

    def test_sent_record_is_never_recovered(self):
        record_id = self.insert_record(status="sent")
        self.assertEqual(self.recover()["stale_processing_found_count"], 0)
        self.assertEqual(self.row(record_id)[0], "sent")

    def test_terminal_failed_record_is_never_recovered(self):
        record_id = self.insert_record(
            status="failed",
            started_at=None,
            processing_attempts=3,
            temporary=False,
        )
        self.assertEqual(self.recover()["stale_processing_found_count"], 0)
        self.assertEqual(self.row(record_id)[0], "failed")

    def test_legacy_processing_without_timestamp_uses_old_updated_at(self):
        record_id = self.insert_record(
            started_at=None,
            updated_at=self.now - timedelta(hours=1),
        )
        self.assertEqual(self.recover()["stale_processing_recovered_count"], 1)
        self.assertEqual(self.row(record_id)[0], "failed")

    def test_recovery_is_idempotent(self):
        record_id = self.insert_record()
        first = self.recover()
        second = self.recover()
        self.assertEqual(first["stale_processing_recovered_count"], 1)
        self.assertEqual(second["stale_processing_found_count"], 0)
        self.assertEqual(self.row(record_id)[1], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
