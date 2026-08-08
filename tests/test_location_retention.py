from __future__ import annotations

from contextlib import redirect_stdout
from datetime import timedelta
from io import StringIO
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import psycopg2

from app import main


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class FakeAPNsPushClient:
    client_reused = True
    jwt_reused = True

    def close(self) -> None:
        pass

    def send_push(self, **_kwargs):
        return {
            "success": True,
            "status_code": 200,
            "reason": "Success",
            "invalid_token": False,
            "temporary_error": False,
            "environment": "sandbox",
            "attempts": 1,
        }


class LocationRetentionTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-location-retention-",
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
            main.ensure_auth_schema(conn)
            main.ensure_mimit_import_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stations (
                        id BIGSERIAL PRIMARY KEY,
                        mimit_id BIGINT NOT NULL UNIQUE,
                        name TEXT,
                        brand TEXT,
                        operator TEXT,
                        address TEXT,
                        city TEXT,
                        province TEXT,
                        latitude DOUBLE PRECISION NOT NULL,
                        longitude DOUBLE PRECISION NOT NULL,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE TABLE IF NOT EXISTS fuel_prices (
                        id BIGSERIAL PRIMARY KEY,
                        station_id BIGINT NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
                        fuel_type TEXT NOT NULL,
                        price DOUBLE PRECISION NOT NULL,
                        is_self_service BOOLEAN NOT NULL,
                        reported_at TIMESTAMPTZ NOT NULL,
                        source TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            main.ensure_sent_price_notifications_schema(conn)

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
        self.original_retention_days = main.USER_LOCATION_RETENTION_DAYS
        main.USER_LOCATION_RETENTION_DAYS = 30
        self.connection_patcher = patch.object(main, "get_connection", side_effect=self.connect)
        self.connection_patcher.start()
        self.addCleanup(self.connection_patcher.stop)
        self.rate_limit_patcher = patch.object(main, "enforce_owner_rate_limit")
        self.rate_limit_patcher.start()
        self.addCleanup(self.rate_limit_patcher.stop)
        self.addCleanup(setattr, main, "USER_LOCATION_RETENTION_DAYS", self.original_retention_days)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE
                        sent_price_notifications,
                        user_device_tokens,
                        user_locations,
                        price_notification_preferences,
                        fuel_prices,
                        stations,
                        mimit_import_runs,
                        user_sessions,
                        email_verification_tokens,
                        rewards,
                        referrals,
                        user_subscriptions,
                        apple_transactions,
                        users
                    RESTART IDENTITY CASCADE;
                    """
                )

    def create_user(self, suffix: str = "user") -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (
                        email, password_hash, display_name, referral_code,
                        is_email_verified, is_active
                    )
                    VALUES (%s, NULL, 'User', %s, TRUE, TRUE)
                    RETURNING id;
                    """,
                    (f"{suffix}@example.com", f"RET{suffix.upper()}"[:12]),
                )
                return int(cur.fetchone()[0])

    def insert_legacy_location(
        self,
        user_id: int,
        *,
        age_days: int | None,
        latitude: float | None = 41.49001,
        longitude: float | None = 12.61001,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO price_notification_preferences (
                        user_id, price_notifications_enabled, fuel_type, radius_km,
                        favorites_only, latitude, longitude, location_updated_at
                    )
                    VALUES (
                        %s, TRUE, 'benzina', 3.0, FALSE, %s, %s,
                        CASE WHEN %s IS NULL THEN NULL ELSE NOW() - (%s * INTERVAL '1 day') END
                    );
                    """,
                    (user_id, latitude, longitude, age_days, age_days),
                )

    def legacy_location(self, user_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT latitude, longitude, location_updated_at, fuel_type, radius_km
                    FROM price_notification_preferences
                    WHERE user_id = %s;
                    """,
                    (user_id,),
                )
                return cur.fetchone()

    def cleanup_legacy(self, user_id: int | None = None) -> int:
        with self.connect() as conn:
            return main.cleanup_expired_legacy_notification_locations(
                conn,
                user_id=user_id,
            )

    def test_recent_legacy_location_is_preserved(self):
        user_id = self.create_user("recent")
        self.insert_legacy_location(user_id, age_days=1)

        self.assertEqual(self.cleanup_legacy(), 0)
        self.assertIsNotNone(self.legacy_location(user_id)[0])

    def test_expired_legacy_location_is_cleared(self):
        user_id = self.create_user("expired")
        self.insert_legacy_location(user_id, age_days=31)

        self.assertEqual(self.cleanup_legacy(), 1)
        row = self.legacy_location(user_id)
        self.assertEqual(row[:3], (None, None, None))
        self.assertEqual(row[3:], ("benzina", 3.0))

    def test_preference_without_location_is_unchanged(self):
        user_id = self.create_user("empty")
        self.insert_legacy_location(user_id, age_days=None, latitude=None, longitude=None)
        before = self.legacy_location(user_id)

        self.assertEqual(self.cleanup_legacy(), 0)
        self.assertEqual(self.legacy_location(user_id), before)

    def test_cleanup_is_idempotent(self):
        user_id = self.create_user("repeat")
        self.insert_legacy_location(user_id, age_days=31)

        self.assertEqual(self.cleanup_legacy(), 1)
        self.assertEqual(self.cleanup_legacy(), 0)

    def test_notifications_continue_using_modern_location_after_cleanup(self):
        user_id = self.create_user("notify")
        self.insert_legacy_location(user_id, age_days=31)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_locations (user_id, lat, lng, accuracy, source)
                    VALUES (%s, 41.4959, 12.6190, 10.0, 'ios');
                    INSERT INTO user_device_tokens (
                        user_id, device_token, platform, environment, is_active
                    ) VALUES (%s, %s, 'ios', 'sandbox', TRUE);
                    INSERT INTO stations (
                        mimit_id, name, brand, address, city, province,
                        latitude, longitude, is_active
                    ) VALUES (
                        1001, 'Station', 'Q8', 'Via Test', 'Anzio', 'RM',
                        41.4959, 12.6190, TRUE
                    ) RETURNING id;
                    """,
                    (user_id, user_id, "ab" * 32),
                )
                station_id = int(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO fuel_prices (
                        station_id, fuel_type, price, is_self_service, reported_at, source
                    ) VALUES (%s, 'benzina', 1.8, TRUE, NOW(), 'mimit');
                    INSERT INTO mimit_import_runs (status, started_at, completed_at)
                    VALUES ('success', NOW() - INTERVAL '1 minute', NOW())
                    RETURNING id;
                    """,
                    (station_id,),
                )
                run_id = int(cur.fetchone()[0])

        with (
            patch.object(main, "apns_is_configured", return_value=True),
            patch.object(main, "APNsPushClient", FakeAPNsPushClient),
        ):
            summary = main.process_price_notifications_for_run(run_id)

        self.assertEqual(summary["stale_legacy_locations_cleaned_count"], 1)
        self.assertEqual(summary["sent_count"], 1)

    def test_delete_account_cascades_all_location_records(self):
        user_id = self.create_user("delete")
        self.insert_legacy_location(user_id, age_days=1)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO user_locations (user_id, lat, lng) VALUES (%s, 41.49, 12.61);",
                    (user_id,),
                )

        with patch.object(main, "get_current_user_from_token", return_value={"id": user_id}):
            self.assertEqual(main.delete_current_account("Bearer token"), {"status": "ok"})

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM user_locations WHERE user_id = %s),
                        (SELECT COUNT(*) FROM price_notification_preferences WHERE user_id = %s),
                        (SELECT COUNT(*) FROM user_device_tokens WHERE user_id = %s);
                    """,
                    (user_id, user_id, user_id),
                )
                self.assertEqual(cur.fetchone(), (0, 0, 0))

    def test_modern_location_update_clears_legacy_copy(self):
        user_id = self.create_user("modern")
        self.insert_legacy_location(user_id, age_days=1)

        with patch.object(main, "get_current_user_from_token", return_value={"id": user_id}):
            result = main.upsert_current_user_location(
                main.UserLocationRequest(
                    lat=41.5,
                    lng=12.6,
                    accuracy=10,
                    source="ios",
                ),
                "Bearer token",
            )

        self.assertTrue(result["has_location"])
        self.assertEqual(self.legacy_location(user_id)[:3], (None, None, None))

    def test_preferences_endpoint_handles_cleared_coordinates(self):
        user_id = self.create_user("preferences")
        self.insert_legacy_location(user_id, age_days=31)

        with patch.object(main, "get_current_user_from_token", return_value={"id": user_id}):
            result = main.get_current_user_notification_preferences("Bearer token")

        preferences = result["preferences"]
        self.assertIsNone(preferences["latitude"])
        self.assertIsNone(preferences["longitude"])
        self.assertIsNone(preferences["location_updated_at"])
        self.assertEqual(preferences["fuel_type"], "benzina")

    def test_cleanup_logs_do_not_include_coordinates(self):
        user_id = self.create_user("logs")
        self.insert_legacy_location(user_id, age_days=31)
        output = StringIO()

        with redirect_stdout(output):
            self.cleanup_legacy()

        logged = output.getvalue()
        self.assertNotIn("41.49001", logged)
        self.assertNotIn("12.61001", logged)
        self.assertIn("stale_legacy_locations_cleaned_count=1", logged)

    def test_modern_and_legacy_retention_share_the_same_threshold(self):
        expired_user = self.create_user("expiredboth")
        recent_user = self.create_user("recentboth")
        self.insert_legacy_location(expired_user, age_days=31)
        self.insert_legacy_location(recent_user, age_days=29)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_locations (user_id, lat, lng, updated_at)
                    VALUES
                        (%s, 41.49, 12.61, NOW() - INTERVAL '31 days'),
                        (%s, 41.49, 12.61, NOW() - INTERVAL '29 days');
                    """,
                    (expired_user, recent_user),
                )
            self.assertEqual(main.cleanup_expired_user_locations(conn), 1)
            self.assertEqual(main.cleanup_expired_legacy_notification_locations(conn), 1)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM user_locations;")
                self.assertEqual(cur.fetchone()[0], 1)
        self.assertIsNotNone(self.legacy_location(recent_user)[0])


if __name__ == "__main__":
    unittest.main()
