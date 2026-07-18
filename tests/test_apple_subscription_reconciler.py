from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest

import psycopg2
from psycopg2.extras import RealDictCursor

import app.apple_subscription_reconciler as reconciler
import app.apple_subscription_service as service


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class AppleSubscriptionReconcilerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-apple-reconciler-",
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
        cls.original_service_connection = service.get_connection
        cls.original_reconciler_connection = reconciler.get_connection
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
        service.get_connection = cls.original_service_connection
        reconciler.get_connection = cls.original_reconciler_connection
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
                cur.execute("TRUNCATE user_subscriptions, apple_transactions, users RESTART IDENTITY CASCADE;")

    def create_user(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users DEFAULT VALUES RETURNING id;")
                return int(cur.fetchone()[0])

    def insert_apple_transaction(
        self,
        user_id: int,
        expires_at: datetime,
        *,
        revoked_at: datetime | None = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO apple_transactions (
                        user_id, product_id, transaction_id, original_transaction_id,
                        purchase_date, expires_date, environment, revocation_date, signed_date
                    )
                    VALUES (%s, 'MB.FuelNear.plus.monthly', %s, %s, %s, %s, 'Sandbox', %s, %s);
                    """,
                    (
                        user_id,
                        f"transaction-{user_id}",
                        f"original-{user_id}",
                        expires_at - timedelta(days=30),
                        expires_at,
                        revoked_at,
                        expires_at - timedelta(days=30),
                    ),
                )

    def insert_entitlement(
        self,
        user_id: int,
        source: str,
        expires_at: datetime,
        *,
        original_transaction_id: str | None = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_subscriptions (
                        user_id, source, status, starts_at, expires_at, original_transaction_id
                    )
                    VALUES (%s, %s, 'active', %s, %s, %s);
                    """,
                    (
                        user_id,
                        source,
                        expires_at - timedelta(days=7),
                        expires_at,
                        original_transaction_id,
                    ),
                )

    def get_entitlement(self, user_id: int) -> dict | None:
        with self.connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM user_subscriptions WHERE user_id = %s ORDER BY id DESC LIMIT 1;",
                    (user_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def test_apple_active_without_previous_entitlement(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        apple_expiry = reference + timedelta(days=30)
        self.insert_apple_transaction(user_id, apple_expiry)
        result = reconciler.reconcile_apple_entitlement(user_id, reference)
        self.assertTrue(result.is_plus)
        self.assertTrue(result.apple_active)
        self.assertTrue(result.changed)
        self.assertEqual(result.expires_at, apple_expiry)

    def test_apple_active_extends_shorter_referral(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        referral_expiry = reference + timedelta(days=7)
        apple_expiry = reference + timedelta(days=30)
        self.insert_entitlement(user_id, "referral_reward", referral_expiry)
        self.insert_apple_transaction(user_id, apple_expiry)
        result = reconciler.reconcile_apple_entitlement(user_id, reference)
        self.assertEqual(result.expires_at, apple_expiry)
        self.assertEqual(self.get_entitlement(user_id)["source"], "referral_reward")

    def test_apple_active_preserves_longer_referral(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        referral_expiry = reference + timedelta(days=60)
        self.insert_entitlement(user_id, "referral_reward", referral_expiry)
        self.insert_apple_transaction(user_id, reference + timedelta(days=30))
        result = reconciler.reconcile_apple_entitlement(user_id, reference)
        self.assertEqual(result.expires_at, referral_expiry)
        self.assertFalse(result.changed)

    def test_expired_apple_without_referral_expires_apple_entitlement(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        self.insert_apple_transaction(user_id, reference - timedelta(days=1))
        self.insert_entitlement(user_id, reconciler.APPLE_SUBSCRIPTION_SOURCE, reference + timedelta(days=10))
        result = reconciler.reconcile_apple_entitlement(user_id, reference)
        self.assertFalse(result.is_plus)
        self.assertFalse(result.apple_active)
        self.assertTrue(result.changed)
        self.assertEqual(self.get_entitlement(user_id)["status"], "expired")

    def test_revoked_apple_preserves_valid_referral(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        referral_expiry = reference + timedelta(days=7)
        self.insert_entitlement(user_id, "referral_reward", referral_expiry)
        self.insert_apple_transaction(
            user_id,
            reference + timedelta(days=30),
            revoked_at=reference,
        )
        result = reconciler.reconcile_apple_entitlement(user_id, reference)
        self.assertTrue(result.is_plus)
        self.assertFalse(result.apple_active)
        self.assertEqual(result.expires_at, referral_expiry)

    def test_revoked_apple_preserves_referral_tail_added_to_apple_entitlement(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        apple_expiry = reference + timedelta(days=30)
        self.insert_apple_transaction(
            user_id,
            apple_expiry,
            revoked_at=reference,
        )
        self.insert_entitlement(
            user_id,
            reconciler.APPLE_SUBSCRIPTION_SOURCE,
            apple_expiry + timedelta(days=7),
            original_transaction_id=f"original-{user_id}",
        )

        result = reconciler.reconcile_apple_entitlement(user_id, reference)

        entitlement = self.get_entitlement(user_id)
        self.assertTrue(result.is_plus)
        self.assertFalse(result.apple_active)
        self.assertEqual(result.expires_at, reference + timedelta(days=7))
        self.assertEqual(entitlement["source"], "referral_reward")
        self.assertIsNone(entitlement["original_transaction_id"])

    def test_repeated_reconciliation_is_idempotent(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        self.insert_apple_transaction(user_id, reference + timedelta(days=30))
        first = reconciler.reconcile_apple_entitlement(user_id, reference)
        second = reconciler.reconcile_apple_entitlement(user_id, reference)
        self.assertTrue(first.changed)
        self.assertFalse(second.changed)
        self.assertEqual(first.expires_at, second.expires_at)

    def test_concurrent_reconciliation_keeps_one_active_entitlement(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        self.insert_apple_transaction(user_id, reference + timedelta(days=30))

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(
                executor.map(
                    lambda _: reconciler.reconcile_apple_entitlement(user_id, reference),
                    range(2),
                )
            )

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM user_subscriptions WHERE user_id = %s AND status = 'active';",
                    (user_id,),
                )
                active_count = int(cur.fetchone()[0])
        self.assertEqual(active_count, 1)
        self.assertEqual(sum(result.changed for result in results), 1)


if __name__ == "__main__":
    unittest.main()
