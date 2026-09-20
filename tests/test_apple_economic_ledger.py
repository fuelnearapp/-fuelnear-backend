from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest

import psycopg2
from psycopg2.errors import CheckViolation

from app.apple_subscriptions import (
    AppleEconomicEvidenceStatus,
    MAX_APPLE_PRICE_MILLIUNITS,
    ensure_apple_economic_ledger_schema,
    normalize_apple_economic_evidence,
)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class AppleEconomicEvidenceTestCase(unittest.TestCase):
    def test_positive_price_is_normalized_without_float(self):
        evidence = normalize_apple_economic_evidence(4990, "EUR")
        self.assertEqual(evidence.status, AppleEconomicEvidenceStatus.VALID)
        self.assertEqual(evidence.price_milliunits, 4990)
        self.assertEqual(evidence.currency, "EUR")
        self.assertEqual(evidence.amount, Decimal("4.990"))
        self.assertIsInstance(evidence.amount, Decimal)

    def test_zero_is_valid(self):
        evidence = normalize_apple_economic_evidence(0, "USD")
        self.assertEqual(evidence.status, AppleEconomicEvidenceStatus.VALID)
        self.assertEqual(evidence.amount, Decimal("0"))

    def test_none_pair_is_absent(self):
        evidence = normalize_apple_economic_evidence(None, None)
        self.assertEqual(evidence.status, AppleEconomicEvidenceStatus.ABSENT)
        self.assertIsNone(evidence.amount)

    def test_incomplete_pairs_are_invalid(self):
        for price, currency in ((1000, None), (None, "EUR")):
            with self.subTest(price=price, currency=currency):
                evidence = normalize_apple_economic_evidence(price, currency)
                self.assertEqual(evidence.status, AppleEconomicEvidenceStatus.INVALID)
                self.assertEqual(evidence.invalid_reason, "incomplete_pair")

    def test_invalid_price_values_are_not_coerced(self):
        cases = (
            (-1, "price_out_of_range"),
            (MAX_APPLE_PRICE_MILLIUNITS + 1, "price_out_of_range"),
            (True, "invalid_price_type"),
            (1.5, "invalid_price_type"),
            ("4990", "invalid_price_type"),
        )
        for price, reason in cases:
            with self.subTest(price=price):
                evidence = normalize_apple_economic_evidence(price, "EUR")
                self.assertEqual(evidence.status, AppleEconomicEvidenceStatus.INVALID)
                self.assertEqual(evidence.invalid_reason, reason)

    def test_currency_is_trimmed_and_uppercased(self):
        evidence = normalize_apple_economic_evidence(1000, " eur ")
        self.assertEqual(evidence.status, AppleEconomicEvidenceStatus.VALID)
        self.assertEqual(evidence.currency, "EUR")

    def test_caribbean_guilder_is_recognized_and_normalized(self):
        evidence = normalize_apple_economic_evidence(1000, " xcg ")
        self.assertEqual(evidence.status, AppleEconomicEvidenceStatus.VALID)
        self.assertEqual(evidence.currency, "XCG")

    def test_invalid_and_unknown_currencies_are_rejected(self):
        for currency, reason in (
            ("EU", "invalid_currency_format"),
            ("E1R", "invalid_currency_format"),
            ("EU€", "invalid_currency_format"),
            ("ZZZ", "unknown_currency"),
        ):
            with self.subTest(currency=currency):
                evidence = normalize_apple_economic_evidence(1000, currency)
                self.assertEqual(evidence.status, AppleEconomicEvidenceStatus.INVALID)
                self.assertEqual(evidence.invalid_reason, reason)


class AppleEconomicLedgerSchemaTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-apple-economic-ledger-",
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
                    CREATE TABLE apple_transactions (
                        id BIGSERIAL PRIMARY KEY,
                        transaction_id TEXT NOT NULL UNIQUE
                    );
                    INSERT INTO apple_transactions (transaction_id)
                    VALUES ('historical-transaction');
                    """
                )
            ensure_apple_economic_ledger_schema(conn)
            ensure_apple_economic_ledger_schema(conn)

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

    def test_historical_row_remains_null(self):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT price_milliunits, currency,
                           economic_transaction_signed_at, economic_adjustment,
                           economic_notification_signed_at
                    FROM apple_transactions
                    WHERE transaction_id = 'historical-transaction';
                    """
                )
                self.assertEqual(cur.fetchone(), (None, None, None, None, None))

    def test_valid_pair_and_adjustments_are_accepted(self):
        for index, adjustment in enumerate(
            ("refund", "revoke", "refund_reversed", "revocation_unknown"),
            start=1,
        ):
            with self.subTest(adjustment=adjustment), self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO apple_transactions (
                            transaction_id, price_milliunits, currency,
                            economic_adjustment
                        )
                        VALUES (%s, 4990, 'EUR', %s);
                        """,
                        (f"valid-{index}", adjustment),
                    )

    def test_database_checks_reject_invalid_values(self):
        invalid_rows = (
            ("price-only", 1000, None, None),
            ("currency-only", None, "EUR", None),
            ("negative", -1, "EUR", None),
            ("overflow", MAX_APPLE_PRICE_MILLIUNITS + 1, "EUR", None),
            ("lowercase", 1000, "eur", None),
            ("bad-adjustment", None, None, "chargeback"),
        )
        for transaction_id, price, currency, adjustment in invalid_rows:
            with self.subTest(transaction_id=transaction_id), self.connect() as conn:
                with self.assertRaises(CheckViolation), conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO apple_transactions (
                            transaction_id, price_milliunits, currency,
                            economic_adjustment
                        )
                        VALUES (%s, %s, %s, %s);
                        """,
                        (transaction_id, price, currency, adjustment),
                    )
