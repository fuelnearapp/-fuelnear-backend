from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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
from psycopg2.extras import RealDictCursor

from app import main
import app.apple_subscription_reconciler as reconciler
import app.apple_subscription_service as service
import app.apple_subscriptions as repository
from app import plus_entitlements


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
                        apple_expires_at TIMESTAMPTZ NULL,
                        referral_expires_at TIMESTAMPTZ NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE UNIQUE INDEX idx_user_subscriptions_one_active
                    ON user_subscriptions(user_id)
                    WHERE status = 'active';
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE rewards (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        referral_id BIGINT NULL,
                        reward_type TEXT NOT NULL,
                        reward_value TEXT NOT NULL,
                        status TEXT NOT NULL,
                        granted_at TIMESTAMPTZ NULL,
                        expires_at TIMESTAMPTZ NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE UNIQUE INDEX idx_rewards_unique_referral_id
                    ON rewards(referral_id)
                    WHERE referral_id IS NOT NULL;
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
                cur.execute(
                    "TRUNCATE rewards, user_subscriptions, apple_transactions, "
                    "users RESTART IDENTITY CASCADE;"
                )

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

    def insert_reward(
        self,
        user_id: int,
        referral_id: int,
        granted_at: datetime,
        *,
        days: int = 7,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rewards (
                        user_id, referral_id, reward_type, reward_value,
                        status, granted_at
                    )
                    VALUES (%s, %s, 'plus_days', %s, 'granted', %s);
                    """,
                    (user_id, referral_id, str(days), granted_at),
                )

    def grant_reward(self, user_id: int, referral_id: int, days: int = 7) -> dict:
        conn = self.connect()
        try:
            with conn:
                return main.grant_plus_days_reward(
                    conn,
                    user_id,
                    referral_id,
                    days,
                )
        finally:
            conn.close()

    def get_rewards(self, user_id: int) -> list[dict]:
        with self.connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, referral_id, reward_value, status, granted_at
                    FROM rewards
                    WHERE user_id = %s
                    ORDER BY id;
                    """,
                    (user_id,),
                )
                return [dict(row) for row in cur.fetchall()]

    def make_apple_transaction(
        self,
        user_id: int,
        *,
        transaction_id: str,
        original_transaction_id: str,
        signed_date: datetime,
        expires_at: datetime,
        purchase_date: datetime | None = None,
        revoked_at: datetime | None = None,
    ) -> repository.AppleTransaction:
        return repository.AppleTransaction(
            user_id=user_id,
            product_id="MB.FuelNear.plus.monthly",
            transaction_id=transaction_id,
            original_transaction_id=original_transaction_id,
            purchase_date=purchase_date or signed_date - timedelta(days=30),
            expires_date=expires_at,
            environment="Sandbox",
            revocation_date=revoked_at,
            signed_date=signed_date,
        )

    def save_and_reconcile(
        self,
        transaction: repository.AppleTransaction,
        reference: datetime,
    ):
        conn = self.connect()
        try:
            repository.save_apple_transaction(conn, transaction)
        finally:
            conn.close()
        return reconciler.reconcile_apple_entitlement(
            transaction.user_id,
            reference,
        )

    def count_apple_transactions(self, user_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM apple_transactions WHERE user_id = %s;",
                    (user_id,),
                )
                return int(cur.fetchone()[0])

    def count_active_entitlements(self, user_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM user_subscriptions
                    WHERE user_id = %s AND status = 'active';
                    """,
                    (user_id,),
                )
                return int(cur.fetchone()[0])

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

    def test_referral_only_creates_independent_component(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        self.insert_reward(user_id, 1001, reference)

        result = reconciler.reconcile_apple_entitlement(user_id, reference)

        entitlement = self.get_entitlement(user_id)
        self.assertTrue(result.is_plus)
        self.assertFalse(result.apple_active)
        self.assertEqual(result.expires_at, reference + timedelta(days=7))
        self.assertEqual(entitlement["source"], "referral_reward")
        self.assertIsNone(entitlement["apple_expires_at"])
        self.assertEqual(
            entitlement["referral_expires_at"],
            reference + timedelta(days=7),
        )

    def test_no_apple_or_referral_has_no_entitlement(self):
        user_id = self.create_user()

        result = reconciler.reconcile_apple_entitlement(user_id)

        self.assertFalse(result.is_plus)
        self.assertFalse(result.apple_active)
        self.assertIsNone(result.expires_at)
        self.assertIsNone(self.get_entitlement(user_id))

    def test_apple_and_referral_are_recorded_as_distinct_components(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        apple_expiry = reference + timedelta(days=30)
        self.insert_apple_transaction(user_id, apple_expiry)
        self.insert_reward(user_id, 1002, reference)

        result = reconciler.reconcile_apple_entitlement(user_id, reference)

        entitlement = self.get_entitlement(user_id)
        self.assertTrue(result.is_plus)
        self.assertTrue(result.apple_active)
        self.assertEqual(entitlement["source"], "combined")
        self.assertEqual(entitlement["apple_expires_at"], apple_expiry)
        self.assertEqual(
            entitlement["referral_expires_at"],
            apple_expiry + timedelta(days=7),
        )
        self.assertEqual(result.expires_at, apple_expiry + timedelta(days=7))

    def test_original_bug_refund_leaves_only_seven_referral_days(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        apple_expiry = reference + timedelta(days=30)
        refund_at = reference + timedelta(hours=1)
        self.insert_reward(user_id, 1003, reference)
        self.insert_apple_transaction(user_id, apple_expiry)
        reconciler.reconcile_apple_entitlement(user_id, reference)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE apple_transactions
                    SET revocation_date = %s,
                        signed_date = %s,
                        updated_at = NOW()
                    WHERE user_id = %s;
                    """,
                    (refund_at, refund_at, user_id),
                )

        result = reconciler.reconcile_apple_entitlement(user_id, refund_at)

        entitlement = self.get_entitlement(user_id)
        self.assertTrue(result.is_plus)
        self.assertFalse(result.apple_active)
        self.assertEqual(result.expires_at, refund_at + timedelta(days=7))
        self.assertEqual(entitlement["source"], "referral_reward")
        self.assertIsNone(entitlement["apple_expires_at"])
        self.assertEqual(
            entitlement["referral_expires_at"],
            refund_at + timedelta(days=7),
        )
        self.assertLess(result.expires_at - refund_at, timedelta(days=8))

    def test_expired_apple_activates_preserved_referral_time(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        self.insert_reward(user_id, 1004, reference - timedelta(days=5))
        self.insert_apple_transaction(user_id, reference)

        result = reconciler.reconcile_apple_entitlement(user_id, reference)

        self.assertTrue(result.is_plus)
        self.assertFalse(result.apple_active)
        self.assertEqual(result.expires_at, reference + timedelta(days=7))
        self.assertEqual(self.get_entitlement(user_id)["source"], "referral_reward")

    def test_renewal_changes_only_apple_projection_not_reward_ledger(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        self.insert_reward(user_id, 1005, reference)
        first = self.make_apple_transaction(
            user_id,
            transaction_id=f"initial-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference,
            expires_at=reference + timedelta(days=30),
        )
        self.save_and_reconcile(first, reference)
        rewards_before = self.get_rewards(user_id)
        renewal = self.make_apple_transaction(
            user_id,
            transaction_id=f"renewal-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=1),
            expires_at=reference + timedelta(days=60),
        )

        result = self.save_and_reconcile(renewal, reference)

        entitlement = self.get_entitlement(user_id)
        self.assertEqual(self.get_rewards(user_id), rewards_before)
        self.assertEqual(entitlement["apple_expires_at"], renewal.expires_date)
        self.assertEqual(
            entitlement["referral_expires_at"],
            renewal.expires_date + timedelta(days=7),
        )
        self.assertEqual(result.expires_at, renewal.expires_date + timedelta(days=7))

    def test_referral_added_during_apple_is_preserved_after_apple(self):
        user_id = self.create_user()
        purchase_date = datetime.now(timezone.utc)
        apple_expiry = purchase_date + timedelta(days=30)
        reward_date = purchase_date + timedelta(days=5)
        self.insert_apple_transaction(user_id, apple_expiry)
        reconciler.reconcile_apple_entitlement(user_id, purchase_date)
        self.insert_reward(user_id, 1006, reward_date)

        result = reconciler.reconcile_apple_entitlement(user_id, reward_date)

        entitlement = self.get_entitlement(user_id)
        self.assertEqual(entitlement["apple_expires_at"], apple_expiry)
        self.assertEqual(
            entitlement["referral_expires_at"],
            apple_expiry + timedelta(days=7),
        )
        self.assertEqual(result.expires_at, apple_expiry + timedelta(days=7))

    def test_multiple_referral_rewards_remain_banked_during_apple(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        apple_expiry = reference + timedelta(days=30)
        self.insert_apple_transaction(user_id, apple_expiry)
        self.insert_reward(user_id, 1007, reference)
        self.insert_reward(user_id, 1008, reference + timedelta(days=1))

        result = reconciler.reconcile_apple_entitlement(
            user_id,
            reference + timedelta(days=1),
        )

        self.assertEqual(len(self.get_rewards(user_id)), 2)
        self.assertEqual(result.expires_at, apple_expiry + timedelta(days=14))
        self.assertEqual(
            self.get_entitlement(user_id)["referral_expires_at"],
            apple_expiry + timedelta(days=14),
        )

    def test_refund_of_previous_transaction_keeps_newer_renewal_active(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        refunded = self.make_apple_transaction(
            user_id,
            transaction_id=f"old-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=1),
            expires_at=reference + timedelta(days=30),
            revoked_at=reference + timedelta(seconds=1),
        )
        renewal = self.make_apple_transaction(
            user_id,
            transaction_id=f"new-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=2),
            expires_at=reference + timedelta(days=60),
        )
        self.save_and_reconcile(refunded, reference)

        result = self.save_and_reconcile(renewal, reference)

        self.assertTrue(result.apple_active)
        self.assertEqual(result.expires_at, renewal.expires_date)
        self.assertEqual(
            self.get_entitlement(user_id)["apple_expires_at"],
            renewal.expires_date,
        )

    def test_late_refund_of_t1_cannot_obscure_active_t3_regression(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        t1_purchase = reference - timedelta(days=90)
        t1_expiry = reference - timedelta(days=60)
        t2_purchase = reference - timedelta(days=60)
        t2_expiry = reference - timedelta(days=30)
        t3_purchase = reference - timedelta(days=1)
        t3_expiry = reference + timedelta(days=29)

        t1 = self.make_apple_transaction(
            user_id,
            transaction_id=f"t1-{user_id}",
            original_transaction_id=original_id,
            purchase_date=t1_purchase,
            signed_date=t1_purchase,
            expires_at=t1_expiry,
        )
        t2 = self.make_apple_transaction(
            user_id,
            transaction_id=f"t2-{user_id}",
            original_transaction_id=original_id,
            purchase_date=t2_purchase,
            signed_date=t2_purchase,
            expires_at=t2_expiry,
        )
        t3 = self.make_apple_transaction(
            user_id,
            transaction_id=f"t3-{user_id}",
            original_transaction_id=original_id,
            purchase_date=t3_purchase,
            signed_date=t3_purchase,
            expires_at=t3_expiry,
        )
        for transaction in (t1, t2, t3):
            self.save_and_reconcile(transaction, reference)

        late_refund_t1 = self.make_apple_transaction(
            user_id,
            transaction_id=t1.transaction_id,
            original_transaction_id=original_id,
            purchase_date=t1_purchase,
            signed_date=reference + timedelta(days=1),
            expires_at=t1_expiry,
            revoked_at=reference,
        )
        result = self.save_and_reconcile(late_refund_t1, reference)

        with self.connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT transaction_id, revocation_date
                    FROM apple_transactions
                    WHERE user_id = %s
                    ORDER BY transaction_id;
                    """,
                    (user_id,),
                )
                rows = {row["transaction_id"]: dict(row) for row in cur.fetchall()}

        self.assertIsNotNone(rows[t1.transaction_id]["revocation_date"])
        self.assertIsNone(rows[t2.transaction_id]["revocation_date"])
        self.assertIsNone(rows[t3.transaction_id]["revocation_date"])
        self.assertTrue(result.apple_active)
        self.assertEqual(result.expires_at, t3_expiry)
        self.assertEqual(self.get_entitlement(user_id)["apple_expires_at"], t3_expiry)

    def test_late_refund_of_t2_cannot_obscure_active_t3(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        t2 = self.make_apple_transaction(
            user_id,
            transaction_id=f"t2-{user_id}",
            original_transaction_id=original_id,
            purchase_date=reference - timedelta(days=31),
            signed_date=reference - timedelta(days=31),
            expires_at=reference - timedelta(days=1),
        )
        t3 = self.make_apple_transaction(
            user_id,
            transaction_id=f"t3-{user_id}",
            original_transaction_id=original_id,
            purchase_date=reference - timedelta(days=1),
            signed_date=reference - timedelta(days=1),
            expires_at=reference + timedelta(days=29),
        )
        self.save_and_reconcile(t2, reference)
        self.save_and_reconcile(t3, reference)

        late_refund_t2 = replace(
            t2,
            signed_date=reference + timedelta(days=1),
            revocation_date=reference,
            revocation_reason="1",
        )
        result = self.save_and_reconcile(late_refund_t2, reference)

        self.assertTrue(result.apple_active)
        self.assertEqual(result.expires_at, t3.expires_date)
        self.assertEqual(
            service.get_active_subscription_for_user(user_id, reference)["transaction_id"],
            t3.transaction_id,
        )

    def test_concurrent_t3_renewal_and_late_t1_refund_keep_t3_active(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        t1 = self.make_apple_transaction(
            user_id,
            transaction_id=f"t1-{user_id}",
            original_transaction_id=original_id,
            purchase_date=reference - timedelta(days=60),
            signed_date=reference - timedelta(days=60),
            expires_at=reference - timedelta(days=30),
        )
        self.save_and_reconcile(t1, reference)
        t3 = self.make_apple_transaction(
            user_id,
            transaction_id=f"t3-{user_id}",
            original_transaction_id=original_id,
            purchase_date=reference,
            signed_date=reference,
            expires_at=reference + timedelta(days=30),
        )
        late_refund_t1 = replace(
            t1,
            signed_date=reference + timedelta(days=1),
            revocation_date=reference,
            revocation_reason="1",
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(self.save_and_reconcile, transaction, reference)
                for transaction in (t3, late_refund_t1)
            ]
            [future.result(timeout=10) for future in futures]

        final = reconciler.reconcile_apple_entitlement(user_id, reference)
        active = service.get_active_subscription_for_user(user_id, reference)
        self.assertTrue(final.apple_active)
        self.assertEqual(final.expires_at, t3.expires_date)
        self.assertEqual(active["transaction_id"], t3.transaction_id)
        self.assertEqual(self.count_apple_transactions(user_id), 2)

    def test_repeated_refund_is_idempotent_with_referral(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        refund_at = reference + timedelta(minutes=1)
        self.insert_reward(user_id, 1009, reference)
        self.insert_apple_transaction(
            user_id,
            reference + timedelta(days=30),
            revoked_at=refund_at,
        )

        first = reconciler.reconcile_apple_entitlement(user_id, refund_at)
        second = reconciler.reconcile_apple_entitlement(user_id, refund_at)

        self.assertTrue(first.changed)
        self.assertFalse(second.changed)
        self.assertEqual(first.expires_at, second.expires_at)
        self.assertEqual(len(self.get_rewards(user_id)), 1)

    def test_same_referral_reward_replay_does_not_duplicate_days(self):
        user_id = self.create_user()

        first = self.grant_reward(user_id, 1010)
        second = self.grant_reward(user_id, 1010)

        self.assertFalse(first["already_granted"])
        self.assertTrue(second["already_granted"])
        self.assertEqual(len(self.get_rewards(user_id)), 1)
        self.assertEqual(first["subscription"]["expires_at"], second["subscription"]["expires_at"])

    def test_legacy_mixed_row_backfill_is_deterministic_and_idempotent(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        apple_expiry = reference + timedelta(days=30)
        self.insert_reward(user_id, 1013, reference)
        self.insert_apple_transaction(user_id, apple_expiry)
        self.insert_entitlement(
            user_id,
            "referral_reward",
            apple_expiry,
            original_transaction_id=f"original-{user_id}",
        )

        with self.connect() as conn:
            first_count = plus_entitlements.backfill_plus_entitlement_components(conn)
            second_count = plus_entitlements.backfill_plus_entitlement_components(conn)

        entitlement = self.get_entitlement(user_id)
        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        self.assertEqual(entitlement["source"], "combined")
        self.assertEqual(entitlement["apple_expires_at"], apple_expiry)
        self.assertGreater(
            entitlement["referral_expires_at"],
            apple_expiry + timedelta(days=6, hours=23),
        )
        self.assertLessEqual(
            entitlement["referral_expires_at"],
            apple_expiry + timedelta(days=7),
        )

    def test_apple_active_extends_shorter_referral(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        referral_expiry = reference + timedelta(days=7)
        apple_expiry = reference + timedelta(days=30)
        self.insert_entitlement(user_id, "referral_reward", referral_expiry)
        self.insert_apple_transaction(user_id, apple_expiry)
        result = reconciler.reconcile_apple_entitlement(user_id, reference)
        self.assertEqual(result.expires_at, apple_expiry + timedelta(days=7))
        entitlement = self.get_entitlement(user_id)
        self.assertEqual(entitlement["source"], "combined")
        self.assertEqual(entitlement["apple_expires_at"], apple_expiry)

    def test_apple_active_preserves_longer_referral(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        referral_expiry = reference + timedelta(days=60)
        self.insert_entitlement(user_id, "referral_reward", referral_expiry)
        self.insert_apple_transaction(user_id, reference + timedelta(days=30))
        result = reconciler.reconcile_apple_entitlement(user_id, reference)
        self.assertEqual(result.expires_at, referral_expiry)
        self.assertTrue(result.changed)
        self.assertEqual(self.get_entitlement(user_id)["source"], "combined")

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

    def test_revoked_apple_does_not_infer_referral_without_reward(self):
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
        self.assertFalse(result.is_plus)
        self.assertFalse(result.apple_active)
        self.assertIsNone(result.expires_at)
        self.assertEqual(entitlement["status"], "expired")

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

    def test_concurrent_renewal_and_referral_keep_both_components(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        initial = self.make_apple_transaction(
            user_id,
            transaction_id=f"initial-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference,
            expires_at=reference + timedelta(days=30),
        )
        self.save_and_reconcile(initial, reference)
        renewal = self.make_apple_transaction(
            user_id,
            transaction_id=f"renewal-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=1),
            expires_at=reference + timedelta(days=60),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(self.save_and_reconcile, renewal, None),
                executor.submit(self.grant_reward, user_id, 1011),
            ]
            [future.result(timeout=10) for future in futures]

        final = reconciler.reconcile_apple_entitlement(user_id)
        entitlement = self.get_entitlement(user_id)
        self.assertTrue(final.apple_active)
        self.assertEqual(len(self.get_rewards(user_id)), 1)
        self.assertEqual(self.count_active_entitlements(user_id), 1)
        self.assertEqual(entitlement["source"], "combined")
        self.assertEqual(entitlement["apple_expires_at"], renewal.expires_date)
        self.assertEqual(
            entitlement["referral_expires_at"],
            renewal.expires_date + timedelta(days=7),
        )

    def test_concurrent_refund_and_referral_leave_only_referral(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        transaction_id = f"transaction-{user_id}"
        initial = self.make_apple_transaction(
            user_id,
            transaction_id=transaction_id,
            original_transaction_id=original_id,
            signed_date=reference,
            expires_at=reference + timedelta(days=30),
        )
        self.save_and_reconcile(initial, reference)
        refund_at = datetime.now(timezone.utc)
        refund = self.make_apple_transaction(
            user_id,
            transaction_id=transaction_id,
            original_transaction_id=original_id,
            signed_date=refund_at,
            expires_at=reference + timedelta(days=30),
            revoked_at=refund_at,
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(self.save_and_reconcile, refund, None),
                executor.submit(self.grant_reward, user_id, 1012),
            ]
            [future.result(timeout=10) for future in futures]

        checked_at = datetime.now(timezone.utc)
        final = reconciler.reconcile_apple_entitlement(user_id, checked_at)
        entitlement = self.get_entitlement(user_id)
        self.assertTrue(final.is_plus)
        self.assertFalse(final.apple_active)
        self.assertEqual(len(self.get_rewards(user_id)), 1)
        self.assertEqual(self.count_active_entitlements(user_id), 1)
        self.assertEqual(entitlement["source"], "referral_reward")
        self.assertIsNone(entitlement["apple_expires_at"])
        self.assertGreater(final.expires_at, checked_at + timedelta(days=6, hours=23))
        self.assertLessEqual(final.expires_at, checked_at + timedelta(days=7))

    def test_ledger_is_read_with_same_connection_after_user_lock(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        self.insert_apple_transaction(user_id, reference + timedelta(days=30))
        original_get_active = service.get_active_subscription_for_user
        observed_connection = None

        def assert_locked_then_read(requested_user_id, requested_reference, *, connection=None):
            nonlocal observed_connection
            observed_connection = connection
            self.assertIsNotNone(connection)

            probe = self.connect()
            try:
                with probe.cursor() as cur:
                    with self.assertRaises(psycopg2.errors.LockNotAvailable):
                        cur.execute(
                            "SELECT id FROM users WHERE id = %s FOR UPDATE NOWAIT;",
                            (user_id,),
                        )
                probe.rollback()
            finally:
                probe.close()

            return original_get_active(
                requested_user_id,
                requested_reference,
                connection=connection,
            )

        with patch.object(
            service,
            "get_active_subscription_for_user",
            side_effect=assert_locked_then_read,
        ):
            result = reconciler.reconcile_apple_entitlement(user_id, reference)

        self.assertIsNotNone(observed_connection)
        self.assertTrue(result.apple_active)

    def test_concurrent_did_renew_and_refund_use_latest_ledger_state(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        renewal = self.make_apple_transaction(
            user_id,
            transaction_id=f"transaction-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=1),
            expires_at=reference + timedelta(days=30),
        )
        refund = self.make_apple_transaction(
            user_id,
            transaction_id=f"transaction-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=2),
            expires_at=reference + timedelta(days=30),
            revoked_at=reference + timedelta(seconds=2),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(self.save_and_reconcile, transaction, reference)
                for transaction in (renewal, refund)
            ]
            [future.result(timeout=10) for future in futures]

        self.assertIsNone(service.get_active_subscription_for_user(user_id, reference))
        self.assertEqual(self.count_active_entitlements(user_id), 0)
        self.assertEqual(self.count_apple_transactions(user_id), 1)

    def test_concurrent_refund_and_refund_reversed_restore_latest_state(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        transaction_id = f"transaction-{user_id}"
        base = self.make_apple_transaction(
            user_id,
            transaction_id=transaction_id,
            original_transaction_id=original_id,
            signed_date=reference,
            expires_at=reference + timedelta(days=30),
        )
        self.save_and_reconcile(base, reference)
        refund = self.make_apple_transaction(
            user_id,
            transaction_id=transaction_id,
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=1),
            expires_at=reference + timedelta(days=30),
            revoked_at=reference + timedelta(seconds=1),
        )
        refund_reversed = self.make_apple_transaction(
            user_id,
            transaction_id=transaction_id,
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=2),
            expires_at=reference + timedelta(days=30),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(self.save_and_reconcile, transaction, reference)
                for transaction in (refund, refund_reversed)
            ]
            [future.result(timeout=10) for future in futures]

        active = service.get_active_subscription_for_user(user_id, reference)
        self.assertIsNotNone(active)
        self.assertIsNone(active["revocation_date"])
        self.assertEqual(self.count_active_entitlements(user_id), 1)
        self.assertEqual(self.count_apple_transactions(user_id), 1)

    def test_concurrent_did_renew_events_preserve_latest_expiration(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        first_renewal = self.make_apple_transaction(
            user_id,
            transaction_id=f"renewal-a-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=1),
            expires_at=reference + timedelta(days=30),
        )
        second_renewal = self.make_apple_transaction(
            user_id,
            transaction_id=f"renewal-b-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=2),
            expires_at=reference + timedelta(days=60),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(self.save_and_reconcile, transaction, reference)
                for transaction in (first_renewal, second_renewal)
            ]
            [future.result(timeout=10) for future in futures]

        entitlement = self.get_entitlement(user_id)
        self.assertEqual(entitlement["expires_at"], reference + timedelta(days=60))
        self.assertEqual(self.count_active_entitlements(user_id), 1)
        self.assertEqual(self.count_apple_transactions(user_id), 2)

    def test_out_of_order_notifications_reconcile_from_transaction_chronology(self):
        user_id = self.create_user()
        reference = datetime.now(timezone.utc)
        original_id = f"original-{user_id}"
        newer = self.make_apple_transaction(
            user_id,
            transaction_id=f"newer-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=1),
            expires_at=reference + timedelta(days=60),
        )
        older = self.make_apple_transaction(
            user_id,
            transaction_id=f"older-{user_id}",
            original_transaction_id=original_id,
            signed_date=reference + timedelta(seconds=2),
            expires_at=reference + timedelta(days=30),
        )

        self.save_and_reconcile(newer, reference)
        self.save_and_reconcile(older, reference)

        active = service.get_active_subscription_for_user(user_id, reference)
        entitlement = self.get_entitlement(user_id)
        self.assertEqual(active["transaction_id"], newer.transaction_id)
        self.assertEqual(entitlement["expires_at"], newer.expires_date)
        self.assertEqual(self.count_apple_transactions(user_id), 2)


if __name__ == "__main__":
    unittest.main()
