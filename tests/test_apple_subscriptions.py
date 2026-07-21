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
from uuid import uuid4

import psycopg2

from app.apple_subscriptions import (
    AppleOriginalTransactionOwnershipConflict,
    AppleTransaction,
    AppleTransactionValidationError,
    save_apple_transaction,
    validate_apple_transaction,
)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class AppleSubscriptionsTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-apple-subscriptions-",
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
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE apple_transactions, users RESTART IDENTITY CASCADE;")

    def create_user(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users DEFAULT VALUES RETURNING id;")
                return int(cur.fetchone()[0])

    def transaction(self, base_user_id: int, **changes) -> AppleTransaction:
        base = AppleTransaction(
            user_id=base_user_id,
            product_id="MB.FuelNear.plus.monthly",
            transaction_id="transaction-1",
            original_transaction_id="original-1",
            purchase_date=datetime.now(timezone.utc),
            expires_date=datetime.now(timezone.utc) + timedelta(days=30),
            environment="Sandbox",
        )
        return replace(base, **changes)

    def save(self, transaction: AppleTransaction):
        conn = self.connect()
        try:
            return save_apple_transaction(conn, transaction)
        finally:
            conn.close()

    def row_count(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM apple_transactions;")
                return int(cur.fetchone()[0])

    def transaction_row(self, transaction_id: str) -> tuple:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT signed_date, revocation_date, revocation_reason
                    FROM apple_transactions
                    WHERE transaction_id = %s;
                    """,
                    (transaction_id,),
                )
                return cur.fetchone()

    def test_supported_product_id_is_valid(self):
        transaction = self.transaction(self.create_user())
        self.assertEqual(validate_apple_transaction(transaction), transaction)

    def test_unsupported_product_id_is_rejected(self):
        transaction = self.transaction(self.create_user(), product_id="unsupported.product")
        with self.assertRaises(AppleTransactionValidationError):
            validate_apple_transaction(transaction)

    def test_new_transaction_is_inserted(self):
        result = self.save(self.transaction(self.create_user()))
        self.assertTrue(result.created)
        self.assertEqual(self.row_count(), 1)

    def test_new_transaction_accepts_app_account_token_uuid(self):
        app_account_token = uuid4()
        result = self.save(
            self.transaction(
                self.create_user(),
                app_account_token=app_account_token,
            )
        )
        self.assertEqual(str(result.row["app_account_token"]), str(app_account_token))

    def test_same_transaction_id_is_idempotent(self):
        user_id = self.create_user()
        transaction = self.transaction(user_id)
        first = self.save(transaction)
        second = self.save(transaction)
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.row["id"], second.row["id"])
        self.assertEqual(self.row_count(), 1)

    def test_newer_duplicate_updates_revocation_fields(self):
        user_id = self.create_user()
        signed_date = datetime.now(timezone.utc)
        transaction = self.transaction(user_id, signed_date=signed_date)
        self.save(transaction)

        revocation_date = signed_date + timedelta(hours=1)
        result = self.save(
            replace(
                transaction,
                signed_date=revocation_date,
                revocation_date=revocation_date,
                revocation_reason="1",
            )
        )

        self.assertFalse(result.created)
        self.assertEqual(result.row["revocation_date"], revocation_date)
        self.assertEqual(result.row["revocation_reason"], "1")
        self.assertEqual(self.row_count(), 1)

    def test_older_duplicate_does_not_overwrite_newer_revocation(self):
        user_id = self.create_user()
        signed_date = datetime.now(timezone.utc)
        revocation_date = signed_date + timedelta(hours=1)
        transaction = self.transaction(
            user_id,
            signed_date=revocation_date,
            revocation_date=revocation_date,
            revocation_reason="1",
        )
        self.save(transaction)

        result = self.save(
            replace(
                transaction,
                signed_date=signed_date,
                revocation_date=None,
                revocation_reason=None,
            )
        )

        self.assertEqual(result.row["revocation_date"], revocation_date)
        self.assertEqual(result.row["revocation_reason"], "1")

    def test_newer_refund_reversed_clears_revocation(self):
        user_id = self.create_user()
        signed_date = datetime.now(timezone.utc)
        transaction = self.transaction(
            user_id,
            signed_date=signed_date,
            revocation_date=signed_date,
            revocation_reason="1",
        )
        self.save(transaction)

        result = self.save(
            replace(
                transaction,
                signed_date=signed_date + timedelta(hours=1),
                revocation_date=None,
                revocation_reason=None,
            )
        )

        self.assertIsNone(result.row["revocation_date"])
        self.assertIsNone(result.row["revocation_reason"])

    def test_concurrent_duplicate_transaction_creates_one_row(self):
        transaction = self.transaction(
            self.create_user(),
            signed_date=datetime.now(timezone.utc),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(self.save, (transaction, transaction)))

        self.assertEqual(sorted(result.created for result in results), [False, True])
        self.assertEqual(self.row_count(), 1)

    def test_concurrent_out_of_order_events_preserve_newest_revocation(self):
        user_id = self.create_user()
        initial_signed_date = datetime.now(timezone.utc)
        initial = self.transaction(user_id, signed_date=initial_signed_date)
        self.save(initial)

        older_event = replace(
            initial,
            signed_date=initial_signed_date + timedelta(hours=1),
            revocation_date=None,
            revocation_reason=None,
        )
        revocation_date = initial_signed_date + timedelta(hours=2)
        newer_event = replace(
            initial,
            signed_date=revocation_date,
            revocation_date=revocation_date,
            revocation_reason="1",
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(self.save, (newer_event, older_event)))

        signed_date, stored_revocation_date, revocation_reason = self.transaction_row(
            initial.transaction_id
        )
        self.assertEqual(signed_date, revocation_date)
        self.assertEqual(stored_revocation_date, revocation_date)
        self.assertEqual(revocation_reason, "1")

    def test_multiple_transactions_share_original_for_same_user(self):
        user_id = self.create_user()
        first = self.save(self.transaction(user_id))
        second = self.save(self.transaction(user_id, transaction_id="transaction-2"))
        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertEqual(self.row_count(), 2)

    def test_original_transaction_cannot_move_to_another_user(self):
        first_user_id = self.create_user()
        second_user_id = self.create_user()
        self.save(self.transaction(first_user_id))
        with self.assertRaises(AppleOriginalTransactionOwnershipConflict):
            self.save(self.transaction(second_user_id, transaction_id="transaction-2"))
        self.assertEqual(self.row_count(), 1)

    def test_required_fields_are_validated(self):
        user_id = self.create_user()
        invalid_values = (
            {"user_id": 0},
            {"product_id": ""},
            {"transaction_id": ""},
            {"original_transaction_id": ""},
            {"purchase_date": None},
            {"environment": ""},
        )
        for changes in invalid_values:
            with self.subTest(changes=changes):
                with self.assertRaises(AppleTransactionValidationError):
                    validate_apple_transaction(self.transaction(user_id, **changes))


if __name__ == "__main__":
    unittest.main()
