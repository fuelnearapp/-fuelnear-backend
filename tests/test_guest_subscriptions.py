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
from uuid import UUID

from fastapi.testclient import TestClient
import psycopg2

from app import (
    apple_notification_processor,
    apple_purchase_processor,
    apple_subscription_reconciler,
    apple_subscription_service,
    apple_subscriptions,
    guest_subscriptions,
    main,
)
from app.apple_jws_verifier import (
    AppleJWSAppAccountTokenMismatchError,
    VerifiedAppleTransaction,
)
from app.apple_notification_verifier import (
    NormalizedAppleNotificationTransaction,
    VerifiedAppStoreNotification,
)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class GuestSubscriptionsTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-guest-subscriptions-",
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
        cls.modules_with_connections = (
            main,
            guest_subscriptions,
            apple_subscriptions,
            apple_subscription_service,
            apple_subscription_reconciler,
        )
        cls.original_connections = {
            module: module.get_connection for module in cls.modules_with_connections
        }
        for module in cls.modules_with_connections:
            module.get_connection = cls.connect

        with cls.connect() as conn:
            main.ensure_auth_schema(conn)
        cls.client = TestClient(main.app)

    @classmethod
    def tearDownClass(cls) -> None:
        for module, original in cls.original_connections.items():
            module.get_connection = original
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
        main.AUTH_GUEST_RATE_LIMIT = 100
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE
                        auth_rate_limits,
                        user_subscriptions,
                        apple_transactions,
                        guest_sessions,
                        guest_identities,
                        user_sessions,
                        email_verification_tokens,
                        rewards,
                        referrals,
                        users
                    RESTART IDENTITY CASCADE;
                    """
                )

    def create_guest(self) -> guest_subscriptions.GuestSessionResult:
        return guest_subscriptions.create_guest_session()

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
                    (f"{suffix}@example.com", f"CODE{suffix.upper()}"[:12]),
                )
                return int(cur.fetchone()[0])

    def transaction(
        self,
        guest: guest_subscriptions.GuestSessionResult,
        *,
        transaction_id: str = "guest-transaction-1",
        original_transaction_id: str = "guest-original-1",
        expires_date: datetime | None = None,
        signed_date: datetime | None = None,
        revocation_date: datetime | None = None,
    ) -> apple_subscriptions.AppleTransaction:
        now = datetime.now(timezone.utc)
        return apple_subscriptions.AppleTransaction(
            user_id=None,
            guest_id=guest.guest_id,
            product_id="MB.FuelNear.plus.monthly",
            transaction_id=transaction_id,
            original_transaction_id=original_transaction_id,
            purchase_date=now,
            expires_date=expires_date or now + timedelta(days=30),
            environment="Sandbox",
            revocation_date=revocation_date,
            app_account_token=guest.app_account_token,
            signed_date=signed_date or now,
        )

    def verified_transaction(
        self,
        guest: guest_subscriptions.GuestSessionResult,
    ) -> VerifiedAppleTransaction:
        transaction = self.transaction(guest)
        return VerifiedAppleTransaction(
            product_id=transaction.product_id,
            transaction_id=transaction.transaction_id,
            original_transaction_id=transaction.original_transaction_id,
            purchase_date=transaction.purchase_date,
            expires_date=transaction.expires_date,
            environment=transaction.environment,
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            revocation_date=None,
            revocation_reason=None,
            app_account_token=guest.app_account_token,
            signed_date=transaction.signed_date,
            storefront="ITA",
            offer_type=None,
        )

    def notification(
        self,
        guest: guest_subscriptions.GuestSessionResult,
        *,
        notification_type: str = "DID_RENEW",
        transaction_id: str = "guest-renewal-2",
        expires_date: datetime | None = None,
        revocation_date: datetime | None = None,
    ) -> VerifiedAppStoreNotification:
        now = datetime.now(timezone.utc)
        return VerifiedAppStoreNotification(
            notification_uuid=f"notification-{transaction_id}",
            notification_type=notification_type,
            subtype=None,
            signed_date=now,
            bundle_id="MB.FuelNear",
            environment="Sandbox",
            app_apple_id=None,
            has_transaction_info=True,
            has_renewal_info=False,
            transaction=NormalizedAppleNotificationTransaction(
                product_id="MB.FuelNear.plus.monthly",
                transaction_id=transaction_id,
                original_transaction_id="guest-original-1",
                purchase_date=now,
                expires_date=expires_date or now + timedelta(days=60),
                revocation_date=revocation_date,
                revocation_reason=1 if revocation_date else None,
                app_account_token=guest.app_account_token,
            ),
            renewal=None,
        )

    def test_01_create_guest_is_anonymous_and_token_is_hashed(self):
        response = self.client.post("/auth/guest")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        UUID(payload["app_account_token"])
        self.assertTrue(payload["created"])
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users;")
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("SELECT token_hash FROM guest_sessions;")
                stored_hash = cur.fetchone()[0]
        self.assertNotEqual(stored_hash, payload["guest_access_token"])

    def test_02_reuse_guest_session_returns_same_identity(self):
        first = self.client.post("/auth/guest").json()
        response = self.client.post(
            "/auth/guest",
            headers={"Authorization": f"Bearer {first['guest_access_token']}"},
        )
        self.assertEqual(response.status_code, 200)
        second = response.json()
        self.assertFalse(second["created"])
        self.assertEqual(second["app_account_token"], first["app_account_token"])
        self.assertEqual(second["guest_access_token"], first["guest_access_token"])

    def test_03_invalid_guest_token_is_rejected(self):
        response = self.client.get(
            "/guest/subscription",
            headers={"Authorization": "Bearer invalid-guest-token"},
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error_code"], "GUEST_SESSION_INVALID")

    def test_04_guest_apple_verify_persists_and_returns_plus(self):
        guest = self.create_guest()
        with patch.object(
            main.apple_jws_verifier,
            "verify_apple_signed_transaction",
            return_value=self.verified_transaction(guest),
        ) as verifier:
            response = self.client.post(
                "/guest/subscription/apple/verify",
                json={"signed_transaction": "signed-guest-transaction"},
                headers={"Authorization": f"Bearer {guest.access_token}"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["is_plus"])
        status_response = self.client.get(
            "/guest/subscription",
            headers={"Authorization": f"Bearer {guest.access_token}"},
        )
        self.assertEqual(status_response.status_code, 200)
        self.assertEqual(status_response.json()["status"], "active")
        verifier.assert_called_once_with(
            signed_transaction="signed-guest-transaction",
            expected_app_account_token=str(guest.app_account_token),
        )

    def test_05_guest_verify_rejects_app_account_token_mismatch(self):
        guest = self.create_guest()
        with patch.object(
            main.apple_jws_verifier,
            "verify_apple_signed_transaction",
            side_effect=AppleJWSAppAccountTokenMismatchError("mismatch"),
        ):
            response = self.client.post(
                "/guest/subscription/apple/verify",
                json={"signed_transaction": "signed-guest-transaction"},
                headers={"Authorization": f"Bearer {guest.access_token}"},
            )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error_code"], "APPLE_APP_ACCOUNT_TOKEN_INVALID")

    def test_06_notification_renews_guest_subscription(self):
        guest = self.create_guest()
        apple_purchase_processor.process_apple_transaction(self.transaction(guest))
        result = apple_notification_processor.process_app_store_notification(
            self.notification(guest)
        )
        self.assertEqual(result.guest_id, guest.guest_id)
        self.assertTrue(result.is_plus)
        self.assertEqual(
            guest_subscriptions.get_guest_subscription_status(guest.guest_id).status,
            "active",
        )

    def test_07_refund_revokes_guest_subscription(self):
        guest = self.create_guest()
        base = self.transaction(guest)
        apple_purchase_processor.process_apple_transaction(base)
        refunded = replace(
            base,
            signed_date=base.signed_date + timedelta(seconds=1),
            revocation_date=datetime.now(timezone.utc),
            revocation_reason="1",
        )
        apple_purchase_processor.process_apple_transaction(refunded)
        status = guest_subscriptions.get_guest_subscription_status(guest.guest_id)
        self.assertFalse(status.is_plus)
        self.assertEqual(status.status, "revoked")

    def test_08_claim_moves_ledger_and_creates_user_entitlement(self):
        guest = self.create_guest()
        user_id = self.create_user()
        apple_purchase_processor.process_apple_transaction(self.transaction(guest))
        with patch.object(
            main,
            "get_current_user_from_token",
            return_value={"id": user_id},
        ):
            response = self.client.post(
                "/user/subscription/claim-guest",
                json={"guest_access_token": guest.access_token},
                headers={"Authorization": "Bearer user-access-token"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["claimed"])
        self.assertTrue(response.json()["is_plus"])
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id, guest_id FROM apple_transactions LIMIT 1;"
                )
                self.assertEqual(cur.fetchone(), (user_id, None))
                cur.execute(
                    "SELECT COUNT(*) FROM user_subscriptions WHERE user_id = %s AND status = 'active';",
                    (user_id,),
                )
                self.assertEqual(cur.fetchone()[0], 1)

    def test_09_repeated_claim_is_idempotent(self):
        guest = self.create_guest()
        user_id = self.create_user()
        apple_purchase_processor.process_apple_transaction(self.transaction(guest))
        first = guest_subscriptions.claim_guest_subscription(user_id, guest.access_token)
        second = guest_subscriptions.claim_guest_subscription(user_id, guest.access_token)
        self.assertTrue(first.claimed)
        self.assertFalse(second.claimed)
        self.assertEqual(second.transferred_transactions, 0)

    def test_09b_claim_keeps_existing_referral_component_separate(self):
        guest = self.create_guest()
        user_id = self.create_user()
        transaction = self.transaction(guest)
        apple_purchase_processor.process_apple_transaction(transaction)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rewards (
                        user_id, referral_id, reward_type, reward_value,
                        status, granted_at
                    )
                    VALUES (%s, NULL, 'plus_days', '7', 'granted', NOW());
                    """,
                    (user_id,),
                )

        first = guest_subscriptions.claim_guest_subscription(user_id, guest.access_token)
        second = guest_subscriptions.claim_guest_subscription(user_id, guest.access_token)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT source, apple_expires_at, referral_expires_at
                    FROM user_subscriptions
                    WHERE user_id = %s AND status = 'active';
                    """,
                    (user_id,),
                )
                source, apple_expiry, referral_expiry = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) FROM rewards WHERE user_id = %s;",
                    (user_id,),
                )
                reward_count = int(cur.fetchone()[0])

        self.assertTrue(first.claimed)
        self.assertFalse(second.claimed)
        self.assertEqual(reward_count, 1)
        self.assertEqual(source, "combined")
        self.assertEqual(apple_expiry, transaction.expires_date)
        self.assertEqual(
            referral_expiry,
            transaction.expires_date + timedelta(days=7),
        )
        self.assertEqual(first.expires_at, referral_expiry)
        self.assertEqual(second.expires_at, referral_expiry)

    def test_10_claim_with_wrong_guest_token_is_rejected(self):
        user_id = self.create_user()
        with self.assertRaises(guest_subscriptions.GuestSessionInvalid):
            guest_subscriptions.claim_guest_subscription(user_id, "invalid-token")

    def test_11_concurrent_verify_and_claim_preserve_ownership(self):
        guest = self.create_guest()
        user_id = self.create_user()
        base = self.transaction(guest)
        apple_purchase_processor.process_apple_transaction(base)
        renewal = replace(
            base,
            transaction_id="guest-concurrent-renewal",
            expires_date=base.expires_date + timedelta(days=30),
            signed_date=base.signed_date + timedelta(seconds=1),
        )

        def verify():
            try:
                apple_purchase_processor.process_apple_transaction(renewal)
            except apple_subscriptions.AppleSubscriptionRepositoryError:
                pass

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(verify),
                executor.submit(
                    guest_subscriptions.claim_guest_subscription,
                    user_id,
                    guest.access_token,
                ),
            ]
            for future in futures:
                future.result(timeout=10)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM apple_transactions WHERE guest_id IS NOT NULL;"
                )
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute(
                    "SELECT COUNT(*) FROM apple_transactions WHERE user_id = %s;",
                    (user_id,),
                )
                self.assertGreaterEqual(cur.fetchone()[0], 1)

    def test_12_concurrent_notification_and_claim_do_not_deadlock(self):
        guest = self.create_guest()
        user_id = self.create_user()
        apple_purchase_processor.process_apple_transaction(self.transaction(guest))
        notification = self.notification(guest, transaction_id="concurrent-notification")

        def process_notification():
            try:
                apple_notification_processor.process_app_store_notification(notification)
            except apple_notification_processor.AppleNotificationProcessorError:
                pass

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(process_notification),
                executor.submit(
                    guest_subscriptions.claim_guest_subscription,
                    user_id,
                    guest.access_token,
                ),
            ]
            for future in futures:
                future.result(timeout=10)
        owner = guest_subscriptions.resolve_owner_by_app_account_token(
            guest.app_account_token
        )
        self.assertEqual(owner, guest_subscriptions.AppleSubscriptionOwner("user", user_id))

    def test_13_claim_preserves_longer_existing_plus(self):
        guest = self.create_guest()
        user_id = self.create_user()
        apple_expiry = datetime.now(timezone.utc) + timedelta(days=30)
        referral_expiry = datetime.now(timezone.utc) + timedelta(days=90)
        apple_purchase_processor.process_apple_transaction(
            self.transaction(guest, expires_date=apple_expiry)
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_subscriptions (
                        user_id, source, status, starts_at, expires_at
                    )
                    VALUES (%s, 'referral_reward', 'active', NOW(), %s);
                    """,
                    (user_id, referral_expiry),
                )
        result = guest_subscriptions.claim_guest_subscription(user_id, guest.access_token)
        self.assertEqual(result.expires_at, referral_expiry)

    def test_14_claim_rejects_original_transaction_owned_elsewhere(self):
        guest = self.create_guest()
        user_id = self.create_user("claiming")
        other_user_id = self.create_user("other")
        transaction = self.transaction(guest)
        apple_purchase_processor.process_apple_transaction(transaction)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO apple_transactions (
                        user_id, guest_id, product_id, transaction_id,
                        original_transaction_id, purchase_date, expires_date,
                        environment, app_account_token, signed_date
                    )
                    VALUES (%s, NULL, %s, 'conflicting-owner-transaction', %s,
                            NOW(), NOW() + INTERVAL '30 days', 'Sandbox', NULL, NOW());
                    """,
                    (
                        other_user_id,
                        transaction.product_id,
                        transaction.original_transaction_id,
                    ),
                )
        with self.assertRaises(guest_subscriptions.GuestOwnershipConflict):
            guest_subscriptions.claim_guest_subscription(user_id, guest.access_token)

    def test_15_claimed_guest_token_is_revoked_and_user_flow_remains_valid(self):
        guest = self.create_guest()
        user_id = self.create_user()
        apple_purchase_processor.process_apple_transaction(self.transaction(guest))
        guest_subscriptions.claim_guest_subscription(user_id, guest.access_token)
        with self.assertRaises(guest_subscriptions.GuestAlreadyClaimed):
            guest_subscriptions.get_guest_by_token(guest.access_token)
        tokens = guest_subscriptions.get_allowed_app_account_tokens_for_user(user_id)
        self.assertIn(guest.app_account_token, tokens)
        self.assertTrue(
            apple_subscription_reconciler.reconcile_apple_entitlement(user_id).is_plus
        )

    def test_16_guest_creation_rate_limit_is_enforced(self):
        main.AUTH_GUEST_RATE_LIMIT = 1
        first = self.client.post("/auth/guest")
        second = self.client.post("/auth/guest")
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)
        self.assertEqual(second.json()["error_code"], "RATE_LIMITED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
