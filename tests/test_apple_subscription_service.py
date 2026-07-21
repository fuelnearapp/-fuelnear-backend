from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest
from uuid import uuid4

import psycopg2

import app.apple_subscription_service as service


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class AppleSubscriptionServiceTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-apple-service-",
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
        cls.original_get_connection = service.get_connection
        service.get_connection = cls.connect

        with cls.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE users (id BIGSERIAL PRIMARY KEY, app_account_token UUID UNIQUE);"
                )
                cur.execute(
                    """
                    CREATE TABLE apple_transactions (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NULL REFERENCES users(id) ON DELETE CASCADE,
                        guest_id BIGINT NULL,
                        product_id TEXT NOT NULL,
                        transaction_id TEXT NOT NULL UNIQUE,
                        original_transaction_id TEXT NOT NULL,
                        purchase_date TIMESTAMPTZ NOT NULL,
                        expires_date TIMESTAMPTZ NULL,
                        environment TEXT NOT NULL,
                        ownership_type TEXT NULL,
                        transaction_reason TEXT NULL,
                        revocation_date TIMESTAMPTZ NULL,
                        revocation_reason TEXT NULL,
                        app_account_token UUID NULL,
                        signed_date TIMESTAMPTZ NULL,
                        storefront TEXT NULL,
                        offer_type INTEGER NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )

    @classmethod
    def tearDownClass(cls) -> None:
        service.get_connection = cls.original_get_connection
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
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE apple_transactions, users RESTART IDENTITY CASCADE;")

    def create_user(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users DEFAULT VALUES RETURNING id;")
                return int(cur.fetchone()[0])

    def insert_transaction(
        self,
        user_id: int,
        transaction_id: str,
        original_transaction_id: str,
        purchase_date: datetime,
        expires_date: datetime | None,
        *,
        revocation_date: datetime | None = None,
        signed_date: datetime | None = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO apple_transactions (
                        user_id,
                        product_id,
                        transaction_id,
                        original_transaction_id,
                        purchase_date,
                        expires_date,
                        environment,
                        revocation_date,
                        signed_date
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'Sandbox', %s, %s);
                    """,
                    (
                        user_id,
                        "MB.FuelNear.plus.monthly",
                        transaction_id,
                        original_transaction_id,
                        purchase_date,
                        expires_date,
                        revocation_date,
                        signed_date,
                    ),
                )

    def test_get_transaction(self):
        user_id = self.create_user()
        now = datetime.now(timezone.utc)
        self.insert_transaction(user_id, "tx-1", "original-1", now, now + timedelta(days=30))
        transaction = service.get_transaction("tx-1")
        self.assertIsNotNone(transaction)
        self.assertEqual(transaction["transaction_id"], "tx-1")

    def test_get_user_id_by_app_account_token(self):
        app_account_token = uuid4()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (app_account_token) VALUES (%s) RETURNING id;",
                    (str(app_account_token),),
                )
                user_id = int(cur.fetchone()[0])

        self.assertEqual(
            service.get_user_id_by_app_account_token(app_account_token),
            user_id,
        )
        self.assertIsNone(service.get_user_id_by_app_account_token(uuid4()))

    def test_get_latest_transaction(self):
        user_id = self.create_user()
        now = datetime.now(timezone.utc)
        self.insert_transaction(user_id, "tx-1", "original-1", now, now + timedelta(days=30))
        self.insert_transaction(
            user_id,
            "tx-2",
            "original-1",
            now + timedelta(days=1),
            now + timedelta(days=60),
        )
        latest = service.get_latest_transaction("original-1")
        self.assertEqual(latest["transaction_id"], "tx-2")

    def test_get_transactions_for_user(self):
        user_id = self.create_user()
        now = datetime.now(timezone.utc)
        self.insert_transaction(user_id, "tx-1", "original-1", now, now + timedelta(days=30))
        self.insert_transaction(user_id, "tx-2", "original-1", now + timedelta(days=1), now + timedelta(days=60))
        self.assertEqual(len(service.get_transactions_for_user(user_id)), 2)

    def test_active_subscription(self):
        user_id = self.create_user()
        now = datetime.now(timezone.utc)
        self.insert_transaction(user_id, "tx-1", "original-1", now, now + timedelta(days=30))
        self.assertTrue(service.is_subscription_active("original-1", now))
        active = service.get_active_subscription_for_user(user_id)
        self.assertEqual(active["original_transaction_id"], "original-1")

    def test_expired_subscription(self):
        user_id = self.create_user()
        now = datetime.now(timezone.utc)
        self.insert_transaction(user_id, "tx-1", "original-1", now - timedelta(days=60), now - timedelta(days=1))
        self.assertFalse(service.is_subscription_active("original-1", now))
        self.assertIsNone(service.get_active_subscription_for_user(user_id))

    def test_revoked_subscription(self):
        user_id = self.create_user()
        now = datetime.now(timezone.utc)
        self.insert_transaction(
            user_id,
            "tx-1",
            "original-1",
            now,
            now + timedelta(days=30),
            revocation_date=now,
        )
        self.assertFalse(service.is_subscription_active("original-1", now))
        self.assertIsNone(service.get_active_subscription_for_user(user_id))

    def test_user_without_subscriptions(self):
        user_id = self.create_user()
        self.assertEqual(service.get_transactions_for_user(user_id), [])
        self.assertIsNone(service.get_active_subscription_for_user(user_id))
        self.assertFalse(service.is_subscription_active("missing-original"))


if __name__ == "__main__":
    unittest.main()
