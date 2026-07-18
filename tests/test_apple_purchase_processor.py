from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import psycopg2

import app.apple_purchase_processor as processor
import app.apple_subscription_reconciler as reconciler
import app.apple_subscription_service as service
import app.apple_subscriptions as repository


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ApplePurchaseProcessorTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-apple-processor-",
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
        cls.original_connections = {
            "repository": repository.get_connection,
            "service": service.get_connection,
            "reconciler": reconciler.get_connection,
        }
        repository.get_connection = cls.connect
        service.get_connection = cls.connect
        reconciler.get_connection = cls.connect

        with cls.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE users (id BIGSERIAL PRIMARY KEY);")
                cur.execute(
                    """
                    CREATE TABLE apple_transactions (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
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
                cur.execute(
                    """
                    CREATE TABLE user_subscriptions (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        source TEXT NOT NULL,
                        status TEXT NOT NULL,
                        starts_at TIMESTAMPTZ NOT NULL,
                        expires_at TIMESTAMPTZ NOT NULL,
                        original_transaction_id TEXT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE UNIQUE INDEX idx_user_subscriptions_one_active
                    ON user_subscriptions(user_id)
                    WHERE status = 'active';
                    """
                )

    @classmethod
    def tearDownClass(cls) -> None:
        repository.get_connection = cls.original_connections["repository"]
        service.get_connection = cls.original_connections["service"]
        reconciler.get_connection = cls.original_connections["reconciler"]
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
                cur.execute(
                    "TRUNCATE user_subscriptions, apple_transactions, users RESTART IDENTITY CASCADE;"
                )

    def create_user(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users DEFAULT VALUES RETURNING id;")
                return int(cur.fetchone()[0])

    def transaction(self, user_id: int, **changes) -> repository.AppleTransaction:
        now = datetime.now(timezone.utc)
        base = repository.AppleTransaction(
            user_id=user_id,
            product_id="MB.FuelNear.plus.monthly",
            transaction_id="transaction-1",
            original_transaction_id="original-1",
            purchase_date=now,
            expires_date=now + timedelta(days=30),
            environment="Sandbox",
            signed_date=now,
        )
        return replace(base, **changes)

    def count_rows(self, table: str, user_id: int | None = None) -> int:
        allowed_tables = {"apple_transactions", "user_subscriptions"}
        if table not in allowed_tables:
            raise ValueError("Unsupported test table")
        with self.connect() as conn:
            with conn.cursor() as cur:
                if user_id is None:
                    cur.execute(f"SELECT COUNT(*) FROM {table};")
                else:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE user_id = %s;", (user_id,))
                return int(cur.fetchone()[0])

    def active_expiry(self, user_id: int) -> datetime | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT expires_at
                    FROM user_subscriptions
                    WHERE user_id = %s AND status = 'active';
                    """,
                    (user_id,),
                )
                row = cur.fetchone()
                return row[0] if row else None

    def test_new_transaction_is_saved_and_reconciled(self):
        user_id = self.create_user()
        transaction = self.transaction(user_id)

        result = processor.process_apple_transaction(transaction)

        self.assertTrue(result.created)
        self.assertEqual(result.transaction_id, transaction.transaction_id)
        self.assertEqual(result.original_transaction_id, transaction.original_transaction_id)
        self.assertTrue(result.is_plus)
        self.assertEqual(result.expires_at, transaction.expires_date)
        self.assertTrue(result.changed)
        self.assertEqual(self.count_rows("apple_transactions"), 1)
        self.assertEqual(self.count_rows("user_subscriptions"), 1)

    def test_duplicate_transaction_is_not_inserted_and_is_reconciled(self):
        user_id = self.create_user()
        transaction = self.transaction(user_id)
        processor.process_apple_transaction(transaction)

        with patch.object(
            processor.apple_subscription_reconciler,
            "reconcile_apple_entitlement",
            wraps=reconciler.reconcile_apple_entitlement,
        ) as reconcile_mock:
            result = processor.process_apple_transaction(transaction)

        self.assertFalse(result.created)
        self.assertFalse(result.changed)
        reconcile_mock.assert_called_once_with(user_id)
        self.assertEqual(self.count_rows("apple_transactions"), 1)
        self.assertEqual(self.count_rows("user_subscriptions"), 1)

    def test_original_transaction_conflict_does_not_change_entitlement(self):
        first_user_id = self.create_user()
        second_user_id = self.create_user()
        processor.process_apple_transaction(self.transaction(first_user_id))
        referral_expiry = datetime.now(timezone.utc) + timedelta(days=7)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_subscriptions (
                        user_id, source, status, starts_at, expires_at
                    )
                    VALUES (%s, 'referral_reward', 'active', NOW(), %s);
                    """,
                    (second_user_id, referral_expiry),
                )

        conflicting = self.transaction(
            second_user_id,
            transaction_id="transaction-2",
        )
        with self.assertRaises(repository.AppleOriginalTransactionOwnershipConflict):
            processor.process_apple_transaction(conflicting)

        self.assertEqual(self.count_rows("apple_transactions"), 1)
        self.assertEqual(self.active_expiry(second_user_id), referral_expiry)

    def test_reconciler_is_executed_for_new_transaction(self):
        user_id = self.create_user()
        transaction = self.transaction(user_id)
        with patch.object(
            processor.apple_subscription_reconciler,
            "reconcile_apple_entitlement",
            wraps=reconciler.reconcile_apple_entitlement,
        ) as reconcile_mock:
            processor.process_apple_transaction(transaction)

        reconcile_mock.assert_called_once_with(user_id)

    def test_processing_is_fully_idempotent(self):
        user_id = self.create_user()
        transaction = self.transaction(user_id)

        first = processor.process_apple_transaction(transaction)
        second = processor.process_apple_transaction(transaction)

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertTrue(first.changed)
        self.assertFalse(second.changed)
        self.assertEqual(first.expires_at, second.expires_at)
        self.assertEqual(self.count_rows("apple_transactions"), 1)
        self.assertEqual(self.count_rows("user_subscriptions"), 1)


if __name__ == "__main__":
    unittest.main()
