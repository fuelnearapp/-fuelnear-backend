from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import ANY, patch

import psycopg2

import app.apple_purchase_processor as processor
import app.apple_subscription_reconciler as reconciler
import app.apple_subscription_service as service
import app.apple_subscriptions as repository
import app.creator_attribution as creator_attribution


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
                    """
                )
            creator_attribution.ensure_creator_attribution_schema(conn)
            repository.ensure_apple_economic_ledger_schema(conn)

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
                    "TRUNCATE creator_conversion_events, creator_attributions, "
                    "creator_campaigns, creators, rewards, user_subscriptions, "
                    "apple_transactions, users RESTART IDENTITY CASCADE;"
                )

    def create_user(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users DEFAULT VALUES RETURNING id;")
                return int(cur.fetchone()[0])

    def create_creator_attribution(
        self,
        user_id: int,
        attributed_at: datetime,
    ) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO creators (name, slug, status)
                    VALUES ('Test Creator', %s, 'active')
                    RETURNING id;
                    """,
                    (f"creator-{user_id}",),
                )
                creator_id = int(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO creator_campaigns (
                        creator_id, name, code, status,
                        compensation_type, compensation_value
                    )
                    VALUES (%s, 'Test Campaign', %s, 'active', 'none', NULL)
                    RETURNING id;
                    """,
                    (creator_id, f"CREATOR{user_id:04d}"),
                )
                campaign_id = int(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO creator_attributions (
                        campaign_id, user_id, code_used, source, attributed_at,
                        status, user_deleted
                    )
                    VALUES (%s, %s, %s, 'email_registration', %s, 'active', FALSE)
                    RETURNING id;
                    """,
                    (
                        campaign_id,
                        user_id,
                        f"CREATOR{user_id:04d}",
                        attributed_at,
                    ),
                )
                return int(cur.fetchone()[0])

    def creator_event(self, transaction_id: str):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT conversion_type, occurred_at, economic_status,
                           amount, currency
                    FROM creator_conversion_events
                    WHERE provider = 'apple' AND external_event_key = %s;
                    """,
                    (transaction_id,),
                )
                return cur.fetchone()

    def creator_event_count(self, transaction_id: str) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM creator_conversion_events
                    WHERE provider = 'apple' AND external_event_key = %s;
                    """,
                    (transaction_id,),
                )
                return int(cur.fetchone()[0])

    def creator_milestones(self, attribution_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT plus_converted_at, paid_plus_converted_at
                    FROM creator_attributions
                    WHERE id = %s;
                    """,
                    (attribution_id,),
                )
                return cur.fetchone()

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
        reconcile_mock.assert_called_once_with(user_id, connection=ANY)
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

        reconcile_mock.assert_called_once_with(user_id, connection=ANY)

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

    def test_creator_purchase_is_confirmed_from_persisted_ledger_evidence(self):
        user_id = self.create_user()
        purchase_at = datetime.now(timezone.utc)
        attribution_id = self.create_creator_attribution(
            user_id,
            purchase_at - timedelta(minutes=1),
        )
        transaction = self.transaction(
            user_id,
            purchase_date=purchase_at,
            signed_date=purchase_at,
            environment="Production",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            price_milliunits=4990,
            currency="EUR",
            economic_transaction_signed_at=purchase_at,
        )

        first = processor.process_apple_transaction(transaction)
        second = processor.process_apple_transaction(transaction)

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(
            self.creator_event(transaction.transaction_id),
            ("purchase", purchase_at, "confirmed", Decimal("4.990000"), "EUR"),
        )
        self.assertEqual(self.creator_event_count(transaction.transaction_id), 1)
        self.assertEqual(
            self.creator_milestones(attribution_id),
            (purchase_at, purchase_at),
        )

    def test_restore_enriches_same_unknown_creator_event_using_purchase_time(self):
        user_id = self.create_user()
        purchase_at = datetime.now(timezone.utc) - timedelta(hours=2)
        attribution_id = self.create_creator_attribution(
            user_id,
            purchase_at - timedelta(minutes=1),
        )
        transaction = self.transaction(
            user_id,
            purchase_date=purchase_at,
            signed_date=purchase_at,
            environment="Production",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
        )
        processor.process_apple_transaction(transaction)
        self.assertEqual(
            self.creator_event(transaction.transaction_id)[2:],
            ("verified_unknown_value", None, None),
        )

        restore_at = datetime.now(timezone.utc)
        result = processor.process_apple_transaction(
            replace(
                transaction,
                signed_date=restore_at,
                price_milliunits=4990,
                currency="EUR",
                economic_transaction_signed_at=restore_at,
            )
        )

        self.assertFalse(result.created)
        self.assertEqual(
            self.creator_event(transaction.transaction_id),
            ("purchase", purchase_at, "confirmed", Decimal("4.990000"), "EUR"),
        )
        self.assertEqual(self.creator_event_count(transaction.transaction_id), 1)
        self.assertEqual(
            self.creator_milestones(attribution_id)[1],
            purchase_at,
        )

        processor.process_apple_transaction(
            replace(
                transaction,
                signed_date=restore_at + timedelta(minutes=1),
                price_milliunits=5990,
                currency="USD",
                economic_transaction_signed_at=restore_at,
            )
        )
        self.assertEqual(
            self.creator_event(transaction.transaction_id),
            ("purchase", purchase_at, "confirmed", Decimal("4.990000"), "EUR"),
        )

    def test_pre_attribution_purchase_and_renewal_are_not_creator_conversions(self):
        user_id = self.create_user()
        purchase_at = datetime.now(timezone.utc) - timedelta(days=2)
        purchase = self.transaction(
            user_id,
            purchase_date=purchase_at,
            signed_date=purchase_at,
            environment="Production",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            price_milliunits=0,
            currency="EUR",
            economic_transaction_signed_at=purchase_at,
        )
        processor.process_apple_transaction(purchase)
        attribution_id = self.create_creator_attribution(
            user_id,
            purchase_at + timedelta(days=1),
        )
        renewal_at = purchase_at + timedelta(days=31)
        renewal = replace(
            purchase,
            transaction_id="transaction-renewal-pre-attribution",
            purchase_date=renewal_at,
            signed_date=renewal_at,
            transaction_reason="RENEWAL",
            price_milliunits=4990,
            economic_transaction_signed_at=renewal_at,
        )

        processor.process_apple_transaction(renewal)

        self.assertIsNone(self.creator_event(purchase.transaction_id))
        self.assertIsNone(self.creator_event(renewal.transaction_id))
        self.assertEqual(self.creator_milestones(attribution_id), (None, None))

    def test_post_attribution_trial_can_confirm_first_paid_renewal(self):
        user_id = self.create_user()
        attributed_at = datetime.now(timezone.utc) - timedelta(minutes=2)
        attribution_id = self.create_creator_attribution(user_id, attributed_at)
        purchase_at = attributed_at + timedelta(minutes=1)
        trial = self.transaction(
            user_id,
            purchase_date=purchase_at,
            signed_date=purchase_at,
            environment="Production",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            price_milliunits=0,
            currency="EUR",
            economic_transaction_signed_at=purchase_at,
        )
        processor.process_apple_transaction(trial)
        self.assertIsNone(self.creator_milestones(attribution_id)[1])
        renewal_at = purchase_at + timedelta(days=7)
        renewal = replace(
            trial,
            transaction_id="transaction-paid-renewal",
            purchase_date=renewal_at,
            signed_date=renewal_at,
            transaction_reason="RENEWAL",
            price_milliunits=4990,
            economic_transaction_signed_at=renewal_at,
        )

        processor.process_apple_transaction(renewal)

        self.assertEqual(
            self.creator_event(trial.transaction_id)[2:],
            ("non_economic", None, None),
        )
        self.assertEqual(
            self.creator_event(renewal.transaction_id),
            ("renewal", renewal_at, "confirmed", Decimal("4.990000"), "EUR"),
        )
        self.assertEqual(self.creator_milestones(attribution_id)[1], renewal_at)

    def test_refund_revoke_and_reversal_use_effective_persisted_state(self):
        user_id = self.create_user()
        purchase_at = datetime.now(timezone.utc)
        attribution_id = self.create_creator_attribution(
            user_id,
            purchase_at - timedelta(minutes=1),
        )
        base = self.transaction(
            user_id,
            purchase_date=purchase_at,
            signed_date=purchase_at,
            environment="Production",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            price_milliunits=4990,
            currency="EUR",
            economic_transaction_signed_at=purchase_at,
        )
        processor.process_apple_transaction(base)
        refund_at = purchase_at + timedelta(minutes=1)
        processor.process_apple_transaction(
            replace(
                base,
                signed_date=refund_at,
                revocation_date=refund_at,
                revocation_reason="1",
                economic_adjustment="refund",
                economic_notification_signed_at=refund_at,
            ),
            notification_type="REFUND",
        )
        self.assertEqual(
            self.creator_event(base.transaction_id)[2:],
            ("refunded", Decimal("4.990000"), "EUR"),
        )
        self.assertEqual(self.creator_milestones(attribution_id)[1], purchase_at)

        reversed_at = refund_at + timedelta(minutes=1)
        processor.process_apple_transaction(
            replace(
                base,
                signed_date=reversed_at,
                economic_adjustment="refund_reversed",
                economic_notification_signed_at=reversed_at,
            ),
            notification_type="REFUND_REVERSED",
        )
        self.assertEqual(
            self.creator_event(base.transaction_id)[2:],
            ("confirmed", Decimal("4.990000"), "EUR"),
        )
        self.assertEqual(self.creator_event_count(base.transaction_id), 1)
        self.assertEqual(self.creator_milestones(attribution_id)[1], purchase_at)

        revoke_at = reversed_at + timedelta(minutes=1)
        processor.process_apple_transaction(
            replace(
                base,
                signed_date=revoke_at,
                revocation_date=revoke_at,
                revocation_reason="0",
                economic_adjustment="revoke",
                economic_notification_signed_at=revoke_at,
            ),
            notification_type="REVOKE",
        )
        self.assertEqual(
            self.creator_event(base.transaction_id)[2:],
            ("revoked", Decimal("4.990000"), "EUR"),
        )
        self.assertEqual(self.creator_milestones(attribution_id)[1], purchase_at)

    def test_paid_milestone_converges_to_earliest_confirmed_occurrence(self):
        user_id = self.create_user()
        attributed_at = datetime.now(timezone.utc) - timedelta(days=2)
        attribution_id = self.create_creator_attribution(user_id, attributed_at)
        later_at = attributed_at + timedelta(days=1)
        earlier_at = attributed_at + timedelta(hours=1)
        later = self.transaction(
            user_id,
            transaction_id="transaction-later-paid",
            original_transaction_id="original-later-paid",
            purchase_date=later_at,
            signed_date=later_at,
            environment="Production",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            price_milliunits=6990,
            currency="EUR",
            economic_transaction_signed_at=later_at,
        )
        earlier = replace(
            later,
            transaction_id="transaction-earlier-paid",
            original_transaction_id="original-earlier-paid",
            purchase_date=earlier_at,
            signed_date=earlier_at,
            price_milliunits=4990,
            economic_transaction_signed_at=earlier_at,
        )
        newest_at = later_at + timedelta(hours=1)
        newest = replace(
            later,
            transaction_id="transaction-newest-paid",
            original_transaction_id="original-newest-paid",
            purchase_date=newest_at,
            signed_date=newest_at,
            economic_transaction_signed_at=newest_at,
        )

        processor.process_apple_transaction(later)
        self.assertEqual(self.creator_milestones(attribution_id)[1], later_at)
        processor.process_apple_transaction(earlier)
        self.assertEqual(self.creator_milestones(attribution_id)[1], earlier_at)
        processor.process_apple_transaction(newest)

        plus_converted_at, paid_plus_converted_at = self.creator_milestones(
            attribution_id
        )
        self.assertEqual(plus_converted_at, later_at)
        self.assertEqual(paid_plus_converted_at, earlier_at)

    def test_stale_refund_reversal_has_no_creator_or_entitlement_side_effect(self):
        user_id = self.create_user()
        base = self.transaction(user_id)
        processor.process_apple_transaction(base)
        refund_at = base.signed_date + timedelta(minutes=2)
        refund = replace(
            base,
            signed_date=refund_at,
            revocation_date=refund_at,
            revocation_reason="1",
            economic_adjustment="refund",
            economic_notification_signed_at=refund_at,
        )
        processor.process_apple_transaction(refund, notification_type="REFUND")

        with patch.object(
            processor.creator_attribution,
            "record_creator_apple_conversion",
        ) as creator_mock:
            result = processor.process_apple_transaction(
                replace(
                    base,
                    signed_date=refund_at - timedelta(minutes=1),
                    economic_adjustment="refund_reversed",
                    economic_notification_signed_at=refund_at - timedelta(minutes=1),
                ),
                notification_type="REFUND_REVERSED",
            )

        creator_mock.assert_not_called()
        self.assertFalse(result.is_plus)

    def test_newer_reversal_cannot_degrade_revoke_creator_state(self):
        user_id = self.create_user()
        base = self.transaction(user_id)
        processor.process_apple_transaction(base)
        revoke_at = base.signed_date + timedelta(minutes=1)
        processor.process_apple_transaction(
            replace(
                base,
                signed_date=revoke_at,
                revocation_date=revoke_at,
                revocation_reason="0",
                economic_adjustment="revoke",
                economic_notification_signed_at=revoke_at,
            ),
            notification_type="REVOKE",
        )

        with patch.object(
            processor.creator_attribution,
            "record_creator_apple_conversion",
        ) as creator_mock:
            result = processor.process_apple_transaction(
                replace(
                    base,
                    signed_date=revoke_at + timedelta(minutes=1),
                    revocation_date=None,
                    revocation_reason=None,
                    economic_adjustment="refund_reversed",
                    economic_notification_signed_at=revoke_at
                    + timedelta(minutes=1),
                ),
                notification_type="REFUND_REVERSED",
            )

        self.assertFalse(result.is_plus)
        self.assertEqual(
            creator_mock.call_args.kwargs["economic_state"].status,
            repository.AppleCreatorEconomicStatus.REVOKED,
        )

    def test_accepted_refund_reversal_reaches_creator_and_restores_entitlement(self):
        user_id = self.create_user()
        base = self.transaction(user_id)
        processor.process_apple_transaction(base)
        refund_at = base.signed_date + timedelta(minutes=1)
        processor.process_apple_transaction(
            replace(
                base,
                signed_date=refund_at,
                revocation_date=refund_at,
                revocation_reason="1",
                economic_adjustment="refund",
                economic_notification_signed_at=refund_at,
            ),
            notification_type="REFUND",
        )

        reversed_at = refund_at + timedelta(minutes=1)
        with patch.object(
            processor.creator_attribution,
            "record_creator_apple_conversion",
        ) as creator_mock:
            result = processor.process_apple_transaction(
                replace(
                    base,
                    signed_date=reversed_at,
                    revocation_date=None,
                    revocation_reason=None,
                    economic_adjustment="refund_reversed",
                    economic_notification_signed_at=reversed_at,
                ),
                notification_type="REFUND_REVERSED",
            )

        self.assertTrue(result.is_plus)
        self.assertEqual(
            creator_mock.call_args.kwargs["economic_state"].status,
            repository.AppleCreatorEconomicStatus.NON_ECONOMIC,
        )

    def test_stale_refund_after_reversal_does_not_revoke_entitlement(self):
        user_id = self.create_user()
        base = self.transaction(user_id)
        processor.process_apple_transaction(base)
        refund_at = base.signed_date + timedelta(minutes=1)
        refund = replace(
            base,
            signed_date=refund_at,
            revocation_date=refund_at,
            revocation_reason="1",
            economic_adjustment="refund",
            economic_notification_signed_at=refund_at,
        )
        processor.process_apple_transaction(refund, notification_type="REFUND")
        reversed_at = refund_at + timedelta(minutes=2)
        processor.process_apple_transaction(
            replace(
                base,
                signed_date=reversed_at,
                revocation_date=None,
                revocation_reason=None,
                economic_adjustment="refund_reversed",
                economic_notification_signed_at=reversed_at,
            ),
            notification_type="REFUND_REVERSED",
        )

        with patch.object(
            processor.creator_attribution,
            "record_creator_apple_conversion",
        ) as creator_mock:
            result = processor.process_apple_transaction(
                replace(
                    refund,
                    signed_date=refund_at + timedelta(minutes=1),
                    economic_notification_signed_at=refund_at
                    + timedelta(minutes=1),
                ),
                notification_type="REFUND",
            )

        creator_mock.assert_not_called()
        self.assertTrue(result.is_plus)


if __name__ == "__main__":
    unittest.main()
