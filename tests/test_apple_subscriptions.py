from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest

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

    def test_same_transaction_id_is_idempotent(self):
        user_id = self.create_user()
        transaction = self.transaction(user_id)
        first = self.save(transaction)
        second = self.save(transaction)
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.row["id"], second.row["id"])
        self.assertEqual(self.row_count(), 1)

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
