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
    AppleBaseEconomicStatus,
    AppleOriginalTransactionOwnershipConflict,
    AppleTransaction,
    AppleTransactionValidationError,
    derive_apple_base_economic_status,
    ensure_apple_economic_ledger_schema,
    reduce_apple_economic_adjustment,
    save_apple_transaction,
    validate_apple_transaction,
)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class AppleEconomicAdjustmentReducerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.t2 = self.t1 + timedelta(minutes=1)

    def reduce(self, current, incoming, *, current_at=None, incoming_at=None):
        return reduce_apple_economic_adjustment(
            current,
            current_at,
            incoming,
            incoming_at,
        )

    def test_new_refund_and_revoke(self):
        refund = self.reduce(None, "refund", incoming_at=self.t1)
        revoke = self.reduce(None, "revoke", incoming_at=self.t1)
        self.assertEqual(refund.economic_adjustment, "refund")
        self.assertEqual(revoke.economic_adjustment, "revoke")

    def test_refund_transitions_to_revoke_or_reversed(self):
        revoke = self.reduce("refund", "revoke", current_at=self.t1, incoming_at=self.t2)
        reversed_result = self.reduce(
            "refund", "refund_reversed", current_at=self.t1, incoming_at=self.t2
        )
        self.assertEqual(revoke.economic_adjustment, "revoke")
        self.assertEqual(reversed_result.economic_adjustment, "refund_reversed")

    def test_revoke_is_terminal(self):
        for incoming in ("refund", "refund_reversed", "revocation_unknown"):
            with self.subTest(incoming=incoming):
                result = self.reduce(
                    "revoke", incoming, current_at=self.t1, incoming_at=self.t2
                )
                self.assertEqual(result.economic_adjustment, "revoke")
                self.assertEqual(result.economic_notification_signed_at, self.t2)

    def test_orphan_reversal_is_unknown(self):
        result = self.reduce(None, "refund_reversed", incoming_at=self.t1)
        self.assertEqual(result.economic_adjustment, "revocation_unknown")

    def test_new_refund_after_reversal_is_refund(self):
        result = self.reduce(
            "refund_reversed", "refund", current_at=self.t1, incoming_at=self.t2
        )
        self.assertEqual(result.economic_adjustment, "refund")

    def test_duplicate_refund_is_idempotent(self):
        result = self.reduce(
            "refund", "refund", current_at=self.t1, incoming_at=self.t1
        )
        self.assertTrue(result.incoming_accepted)
        self.assertFalse(result.changed)

    def test_older_refund_and_reversal_are_ignored(self):
        for incoming in ("refund", "refund_reversed"):
            with self.subTest(incoming=incoming):
                result = self.reduce(
                    "refund_reversed",
                    incoming,
                    current_at=self.t2,
                    incoming_at=self.t1,
                )
                self.assertFalse(result.incoming_accepted)
                self.assertEqual(result.economic_adjustment, "refund_reversed")
                self.assertEqual(result.economic_notification_signed_at, self.t2)

    def test_older_revoke_strengthens_without_reducing_watermark(self):
        result = self.reduce(
            "refund", "revoke", current_at=self.t2, incoming_at=self.t1
        )
        self.assertTrue(result.incoming_accepted)
        self.assertEqual(result.economic_adjustment, "revoke")
        self.assertEqual(result.economic_notification_signed_at, self.t2)

    def test_same_timestamp_priority_is_deterministic(self):
        cases = (
            ("refund", "revoke", "revoke"),
            ("refund_reversed", "refund", "refund"),
            ("refund_reversed", "revocation_unknown", "revocation_unknown"),
        )
        for current, incoming, expected in cases:
            with self.subTest(current=current, incoming=incoming):
                result = self.reduce(
                    current, incoming, current_at=self.t1, incoming_at=self.t1
                )
                self.assertEqual(result.economic_adjustment, expected)

    def test_non_economic_input_is_noop(self):
        result = self.reduce("refund", None, current_at=self.t1)
        self.assertFalse(result.incoming_accepted)
        self.assertFalse(result.changed)
        self.assertEqual(result.economic_notification_signed_at, self.t1)

    def test_base_economic_state_uses_only_persisted_evidence(self):
        base = {
            "environment": "Production",
            "ownership_type": "PURCHASED",
            "transaction_reason": "PURCHASE",
            "price_milliunits": 4990,
            "currency": "EUR",
        }
        self.assertEqual(
            derive_apple_base_economic_status(base),
            AppleBaseEconomicStatus.CONFIRMED_CANDIDATE,
        )
        self.assertEqual(
            derive_apple_base_economic_status({**base, "price_milliunits": 0}),
            AppleBaseEconomicStatus.NON_ECONOMIC,
        )
        self.assertEqual(
            derive_apple_base_economic_status({**base, "environment": "Sandbox"}),
            AppleBaseEconomicStatus.NON_ECONOMIC,
        )
        self.assertEqual(
            derive_apple_base_economic_status(
                {**base, "ownership_type": "FAMILY_SHARED"}
            ),
            AppleBaseEconomicStatus.NON_ECONOMIC,
        )
        self.assertEqual(
            derive_apple_base_economic_status({**base, "price_milliunits": None}),
            AppleBaseEconomicStatus.VERIFIED_UNKNOWN_VALUE,
        )


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
                        grace_period_expires_date TIMESTAMPTZ NULL,
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

    def economic_row(self, transaction_id: str) -> tuple:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT price_milliunits, currency,
                           economic_transaction_signed_at
                    FROM apple_transactions
                    WHERE transaction_id = %s;
                    """,
                    (transaction_id,),
                )
                return cur.fetchone()

    def adjustment_row(self, transaction_id: str) -> tuple:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT economic_adjustment,
                           economic_notification_signed_at,
                           revocation_date,
                           price_milliunits,
                           currency,
                           economic_transaction_signed_at
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

    def test_normal_transaction_leaves_economic_fields_null(self):
        result = self.save(self.transaction(self.create_user()))
        self.assertIsNone(result.row["price_milliunits"])
        self.assertIsNone(result.row["currency"])
        self.assertIsNone(result.row["economic_transaction_signed_at"])
        self.assertIsNone(result.row["economic_adjustment"])
        self.assertIsNone(result.row["economic_notification_signed_at"])

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

    def test_newer_duplicate_updates_grace_period_expiration(self):
        user_id = self.create_user()
        signed_date = datetime.now(timezone.utc)
        transaction = self.transaction(user_id, signed_date=signed_date)
        self.save(transaction)
        grace_expiry = transaction.expires_date + timedelta(days=5)

        result = self.save(
            replace(
                transaction,
                signed_date=signed_date + timedelta(minutes=1),
                grace_period_expires_date=grace_expiry,
            )
        )

        self.assertFalse(result.created)
        self.assertTrue(result.changed)
        self.assertEqual(result.row["grace_period_expires_date"], grace_expiry)

    def test_older_grace_event_cannot_overwrite_newer_expired_state(self):
        user_id = self.create_user()
        signed_date = datetime.now(timezone.utc)
        transaction = self.transaction(
            user_id,
            signed_date=signed_date + timedelta(minutes=2),
            grace_period_expires_date=None,
        )
        self.save(transaction)

        result = self.save(
            replace(
                transaction,
                signed_date=signed_date,
                grace_period_expires_date=transaction.expires_date
                + timedelta(days=5),
            )
        )

        self.assertFalse(result.changed)
        self.assertIsNone(result.row["grace_period_expires_date"])

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

    def test_new_transaction_persists_valid_economic_evidence(self):
        signed_date = datetime.now(timezone.utc)
        result = self.save(
            self.transaction(
                self.create_user(),
                price_milliunits=4990,
                currency="eur",
                economic_transaction_signed_at=signed_date,
            )
        )

        self.assertEqual(result.row["price_milliunits"], 4990)
        self.assertEqual(result.row["currency"], "EUR")
        self.assertEqual(result.row["economic_transaction_signed_at"], signed_date)

    def test_replay_enriches_historical_null_economic_evidence(self):
        user_id = self.create_user()
        transaction = self.transaction(user_id)
        first = self.save(transaction)
        signed_date = datetime.now(timezone.utc)

        second = self.save(
            replace(
                transaction,
                price_milliunits=4990,
                currency="EUR",
                economic_transaction_signed_at=signed_date,
            )
        )

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertTrue(second.changed)
        self.assertEqual(self.economic_row(transaction.transaction_id), (4990, "EUR", signed_date))

    def test_same_economic_pair_is_idempotent(self):
        signed_date = datetime.now(timezone.utc)
        transaction = self.transaction(
            self.create_user(),
            price_milliunits=4990,
            currency="EUR",
            economic_transaction_signed_at=signed_date,
        )
        self.save(transaction)

        result = self.save(transaction)

        self.assertFalse(result.created)
        self.assertFalse(result.changed)
        self.assertEqual(self.economic_row(transaction.transaction_id), (4990, "EUR", signed_date))

    def test_absent_economic_evidence_does_not_clear_persisted_pair(self):
        transaction = self.transaction(
            self.create_user(),
            price_milliunits=4990,
            currency="EUR",
        )
        self.save(transaction)

        result = self.save(
            replace(transaction, price_milliunits=None, currency=None)
        )

        self.assertFalse(result.changed)
        self.assertEqual(self.economic_row(transaction.transaction_id)[:2], (4990, "EUR"))

    def test_invalid_economic_evidence_does_not_clear_persisted_pair(self):
        transaction = self.transaction(
            self.create_user(),
            price_milliunits=4990,
            currency="EUR",
        )
        self.save(transaction)

        result = self.save(
            replace(transaction, price_milliunits=True, currency="EUR")
        )

        self.assertFalse(result.changed)
        self.assertEqual(self.economic_row(transaction.transaction_id)[:2], (4990, "EUR"))

    def test_conflicting_valid_economic_pair_is_preserved(self):
        transaction = self.transaction(
            self.create_user(),
            price_milliunits=4990,
            currency="EUR",
        )
        self.save(transaction)

        with self.assertLogs("app.apple_subscriptions", level="WARNING") as logs:
            result = self.save(
                replace(transaction, price_milliunits=5990, currency="EUR")
            )

        self.assertFalse(result.changed)
        self.assertEqual(self.economic_row(transaction.transaction_id)[:2], (4990, "EUR"))
        self.assertIn("economic evidence conflict ignored", logs.output[0])
        self.assertNotIn(transaction.transaction_id, logs.output[0])

    def test_older_replay_does_not_degrade_economic_watermark(self):
        newer_signed_date = datetime.now(timezone.utc)
        transaction = self.transaction(
            self.create_user(),
            price_milliunits=4990,
            currency="EUR",
            economic_transaction_signed_at=newer_signed_date,
        )
        self.save(transaction)

        result = self.save(
            replace(
                transaction,
                economic_transaction_signed_at=newer_signed_date
                - timedelta(minutes=1),
            )
        )

        self.assertFalse(result.changed)
        self.assertEqual(
            self.economic_row(transaction.transaction_id),
            (4990, "EUR", newer_signed_date),
        )

    def test_refund_revoke_and_reversal_preserve_economic_evidence(self):
        user_id = self.create_user()
        transaction_signed_at = datetime.now(timezone.utc)
        base = self.transaction(
            user_id,
            environment="Production",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            signed_date=transaction_signed_at,
            price_milliunits=4990,
            currency="EUR",
            economic_transaction_signed_at=transaction_signed_at,
        )
        self.save(base)

        refund_at = transaction_signed_at + timedelta(minutes=1)
        refund = self.save(
            replace(
                base,
                signed_date=refund_at,
                revocation_date=refund_at,
                revocation_reason="1",
                economic_adjustment="refund",
                economic_notification_signed_at=refund_at,
            )
        )
        self.assertTrue(refund.economic_adjustment_accepted)
        self.assertEqual(
            self.adjustment_row(base.transaction_id),
            ("refund", refund_at, refund_at, 4990, "EUR", transaction_signed_at),
        )

        reversed_at = refund_at + timedelta(minutes=1)
        reversed_result = self.save(
            replace(
                base,
                signed_date=reversed_at,
                revocation_date=None,
                revocation_reason=None,
                economic_adjustment="refund_reversed",
                economic_notification_signed_at=reversed_at,
            )
        )
        self.assertTrue(reversed_result.economic_adjustment_accepted)
        self.assertEqual(
            self.adjustment_row(base.transaction_id),
            (
                "refund_reversed",
                reversed_at,
                None,
                4990,
                "EUR",
                transaction_signed_at,
            ),
        )

        revoke_at = reversed_at + timedelta(minutes=1)
        self.save(
            replace(
                base,
                signed_date=revoke_at,
                revocation_date=revoke_at,
                revocation_reason="0",
                economic_adjustment="revoke",
                economic_notification_signed_at=revoke_at,
            )
        )
        self.assertEqual(
            self.adjustment_row(base.transaction_id),
            ("revoke", revoke_at, revoke_at, 4990, "EUR", transaction_signed_at),
        )

    def test_stale_reversal_does_not_clear_refund_or_entitlement_state(self):
        user_id = self.create_user()
        refund_at = datetime.now(timezone.utc)
        refunded = self.transaction(
            user_id,
            signed_date=refund_at,
            revocation_date=refund_at,
            revocation_reason="1",
            economic_adjustment="refund",
            economic_notification_signed_at=refund_at,
        )
        self.save(refunded)

        stale = self.save(
            replace(
                refunded,
                signed_date=refund_at - timedelta(minutes=1),
                revocation_date=None,
                revocation_reason=None,
                economic_adjustment="refund_reversed",
                economic_notification_signed_at=refund_at - timedelta(minutes=1),
            )
        )

        self.assertFalse(stale.economic_adjustment_accepted)
        self.assertFalse(stale.changed)
        self.assertEqual(
            self.adjustment_row(refunded.transaction_id)[:3],
            ("refund", refund_at, refund_at),
        )

    def test_direct_restore_enriches_evidence_without_clearing_refund(self):
        user_id = self.create_user()
        refund_at = datetime.now(timezone.utc)
        refunded = self.transaction(
            user_id,
            signed_date=refund_at,
            revocation_date=refund_at,
            revocation_reason="1",
            economic_adjustment="refund",
            economic_notification_signed_at=refund_at,
        )
        self.save(refunded)
        transaction_signed_at = refund_at + timedelta(minutes=1)

        result = self.save(
            replace(
                refunded,
                signed_date=transaction_signed_at,
                revocation_date=None,
                revocation_reason=None,
                price_milliunits=4990,
                currency="EUR",
                economic_transaction_signed_at=transaction_signed_at,
                economic_adjustment=None,
                economic_notification_signed_at=None,
            )
        )

        self.assertTrue(result.changed)
        self.assertEqual(
            self.adjustment_row(refunded.transaction_id),
            ("refund", refund_at, refund_at, 4990, "EUR", transaction_signed_at),
        )

    def test_direct_restore_does_not_clear_revoke(self):
        user_id = self.create_user()
        revoke_at = datetime.now(timezone.utc)
        revoked = self.transaction(
            user_id,
            signed_date=revoke_at,
            revocation_date=revoke_at,
            revocation_reason="0",
            economic_adjustment="revoke",
            economic_notification_signed_at=revoke_at,
        )
        self.save(revoked)

        self.save(
            replace(
                revoked,
                signed_date=revoke_at + timedelta(minutes=1),
                revocation_date=None,
                revocation_reason=None,
                economic_adjustment=None,
                economic_notification_signed_at=None,
            )
        )

        self.assertEqual(
            self.adjustment_row(revoked.transaction_id)[:3],
            ("revoke", revoke_at, revoke_at),
        )

    def test_older_revoke_strengthens_refund_without_reducing_watermark(self):
        user_id = self.create_user()
        refund_at = datetime.now(timezone.utc)
        refunded = self.transaction(
            user_id,
            signed_date=refund_at,
            revocation_date=refund_at,
            revocation_reason="1",
            economic_adjustment="refund",
            economic_notification_signed_at=refund_at,
        )
        self.save(refunded)
        older_revoke_at = refund_at - timedelta(minutes=1)

        result = self.save(
            replace(
                refunded,
                signed_date=older_revoke_at,
                revocation_date=older_revoke_at,
                revocation_reason="0",
                economic_adjustment="revoke",
                economic_notification_signed_at=older_revoke_at,
            )
        )

        self.assertTrue(result.economic_adjustment_accepted)
        self.assertEqual(
            self.adjustment_row(refunded.transaction_id)[:3],
            ("revoke", refund_at, older_revoke_at),
        )

    def test_concurrent_refund_and_revoke_converge_to_revoke(self):
        user_id = self.create_user()
        base_at = datetime.now(timezone.utc)
        base = self.transaction(user_id, signed_date=base_at)
        self.save(base)
        event_at = base_at + timedelta(minutes=1)
        refund = replace(
            base,
            signed_date=event_at,
            revocation_date=event_at,
            revocation_reason="1",
            economic_adjustment="refund",
            economic_notification_signed_at=event_at,
        )
        revoke = replace(
            base,
            signed_date=event_at,
            revocation_date=event_at,
            revocation_reason="0",
            economic_adjustment="revoke",
            economic_notification_signed_at=event_at,
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(self.save, (refund, revoke)))

        self.assertEqual(
            self.adjustment_row(base.transaction_id)[:2],
            ("revoke", event_at),
        )

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
