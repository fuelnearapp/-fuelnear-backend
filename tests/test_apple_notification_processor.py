from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import inspect
import unittest
from unittest.mock import patch
from uuid import uuid4

from app import apple_notification_processor as processor
from app.apple_notification_verifier import (
    NormalizedAppleNotificationRenewal,
    NormalizedAppleNotificationTransaction,
    VerifiedAppStoreNotification,
)
from app.apple_purchase_processor import ApplePurchaseProcessingResult
from app.apple_subscriptions import AppleOriginalTransactionOwnershipConflict


class AppleNotificationProcessorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime.now(timezone.utc)
        self.app_account_token = uuid4()

    def notification(
        self,
        notification_type: str = "SUBSCRIBED",
        *,
        transaction: bool = True,
        renewal: bool = False,
    ) -> VerifiedAppStoreNotification:
        transaction_info = (
            NormalizedAppleNotificationTransaction(
                product_id="MB.FuelNear.plus.monthly",
                transaction_id="transaction-1",
                original_transaction_id="original-1",
                purchase_date=self.now,
                expires_date=self.now + timedelta(days=30),
                revocation_date=None,
                revocation_reason=None,
                app_account_token=self.app_account_token,
            )
            if transaction
            else None
        )
        renewal_info = (
            NormalizedAppleNotificationRenewal(
                auto_renew_status=1,
                expiration_intent=None,
                renewal_product_id="MB.FuelNear.plus.monthly",
            )
            if renewal
            else None
        )
        return VerifiedAppStoreNotification(
            notification_uuid="notification-1",
            notification_type=notification_type,
            subtype=None,
            signed_date=self.now,
            bundle_id="MB.FuelNear",
            environment="Sandbox",
            app_apple_id=None,
            has_transaction_info=transaction_info is not None,
            has_renewal_info=renewal_info is not None,
            transaction=transaction_info,
            renewal=renewal_info,
        )

    def processing_result(
        self,
        *,
        created: bool = True,
        changed: bool = True,
        is_plus: bool = True,
        expires_at: datetime | None = None,
    ) -> ApplePurchaseProcessingResult:
        return ApplePurchaseProcessingResult(
            created=created,
            transaction_id="transaction-1",
            original_transaction_id="original-1",
            is_plus=is_plus,
            expires_at=expires_at or self.now + timedelta(days=30),
            changed=changed,
        )

    def process_with_owner(
        self,
        notification: VerifiedAppStoreNotification,
        *,
        result: ApplePurchaseProcessingResult | None = None,
    ):
        with (
            patch.object(
                processor.apple_subscription_service,
                "get_transaction",
                return_value={"user_id": 7},
            ),
            patch.object(
                processor.apple_subscription_service,
                "get_latest_transaction",
                return_value={"user_id": 7},
            ),
            patch.object(
                processor.guest_subscriptions,
                "resolve_owner_by_app_account_token",
                return_value=processor.guest_subscriptions.AppleSubscriptionOwner("user", 7),
            ),
            patch.object(
                processor.apple_purchase_processor,
                "process_apple_transaction",
                return_value=result or self.processing_result(),
            ) as process_mock,
        ):
            return processor.process_app_store_notification(notification), process_mock

    def test_subscribed_is_processed(self):
        result, process_mock = self.process_with_owner(self.notification("SUBSCRIBED"))
        self.assertTrue(result.handled)
        self.assertEqual(result.action, "transaction_processed")
        self.assertEqual(result.user_id, 7)
        process_mock.assert_called_once()

    def test_did_renew_is_processed(self):
        result, _ = self.process_with_owner(self.notification("DID_RENEW"))
        self.assertEqual(result.action, "transaction_processed")

    def test_expired_is_reconciled(self):
        result, _ = self.process_with_owner(
            self.notification("EXPIRED"),
            result=self.processing_result(is_plus=False, expires_at=self.now, changed=True),
        )
        self.assertFalse(result.is_plus)
        self.assertTrue(result.changed)

    def test_refund_passes_revocation_to_purchase_processor(self):
        notification = self.notification("REFUND")
        revoked_transaction = replace(
            notification.transaction,
            revocation_date=self.now,
            revocation_reason=1,
        )
        notification = replace(notification, transaction=revoked_transaction)
        _, process_mock = self.process_with_owner(notification)
        apple_transaction = process_mock.call_args.args[0]
        self.assertEqual(apple_transaction.revocation_date, self.now)
        self.assertEqual(apple_transaction.revocation_reason, "1")
        self.assertEqual(apple_transaction.signed_date, notification.signed_date)

    def test_revoke_is_processed(self):
        result, _ = self.process_with_owner(self.notification("REVOKE"))
        self.assertTrue(result.handled)

    def test_duplicate_transaction_is_reconciled(self):
        result, process_mock = self.process_with_owner(
            self.notification(),
            result=self.processing_result(created=False, changed=False),
        )
        self.assertEqual(result.action, "entitlement_reconciled")
        self.assertFalse(result.created)
        process_mock.assert_called_once()

    def test_ownership_lookup_by_transaction_id(self):
        notification = self.notification()
        with (
            patch.object(
                processor.apple_subscription_service,
                "get_transaction",
                return_value={"user_id": 3},
            ),
            patch.object(
                processor.apple_subscription_service,
                "get_latest_transaction",
                return_value=None,
            ),
            patch.object(
                processor.guest_subscriptions,
                "resolve_owner_by_app_account_token",
                return_value=None,
            ),
            patch.object(
                processor.apple_purchase_processor,
                "process_apple_transaction",
                return_value=self.processing_result(),
            ),
        ):
            result = processor.process_app_store_notification(notification)
        self.assertEqual(result.user_id, 3)

    def test_ownership_lookup_by_original_transaction_id(self):
        notification = self.notification()
        with (
            patch.object(processor.apple_subscription_service, "get_transaction", return_value=None),
            patch.object(
                processor.apple_subscription_service,
                "get_latest_transaction",
                return_value={"user_id": 4},
            ),
            patch.object(
                processor.guest_subscriptions,
                "resolve_owner_by_app_account_token",
                return_value=None,
            ),
            patch.object(
                processor.apple_purchase_processor,
                "process_apple_transaction",
                return_value=self.processing_result(),
            ),
        ):
            result = processor.process_app_store_notification(notification)
        self.assertEqual(result.user_id, 4)

    def test_ownership_lookup_by_app_account_token(self):
        notification = self.notification()
        with (
            patch.object(processor.apple_subscription_service, "get_transaction", return_value=None),
            patch.object(processor.apple_subscription_service, "get_latest_transaction", return_value=None),
            patch.object(
                processor.guest_subscriptions,
                "resolve_owner_by_app_account_token",
                return_value=processor.guest_subscriptions.AppleSubscriptionOwner("user", 5),
            ) as token_lookup,
            patch.object(
                processor.apple_purchase_processor,
                "process_apple_transaction",
                return_value=self.processing_result(),
            ),
        ):
            result = processor.process_app_store_notification(notification)
        self.assertEqual(result.user_id, 5)
        token_lookup.assert_called_once_with(self.app_account_token)

    def test_ownership_not_found_is_rejected(self):
        with (
            patch.object(processor.apple_subscription_service, "get_transaction", return_value=None),
            patch.object(processor.apple_subscription_service, "get_latest_transaction", return_value=None),
            patch.object(
                processor.guest_subscriptions,
                "resolve_owner_by_app_account_token",
                return_value=None,
            ),
        ):
            with self.assertRaises(processor.AppleNotificationOwnershipNotFound):
                processor.process_app_store_notification(self.notification())

    def test_conflicting_ownership_is_rejected(self):
        with (
            patch.object(
                processor.apple_subscription_service,
                "get_transaction",
                return_value={"user_id": 1},
            ),
            patch.object(
                processor.apple_subscription_service,
                "get_latest_transaction",
                return_value={"user_id": 2},
            ),
            patch.object(
                processor.guest_subscriptions,
                "resolve_owner_by_app_account_token",
                return_value=processor.guest_subscriptions.AppleSubscriptionOwner("user", 1),
            ),
        ):
            with self.assertRaises(processor.AppleNotificationOwnershipConflict):
                processor.process_app_store_notification(self.notification())

    def test_repository_ownership_conflict_is_mapped(self):
        notification = self.notification()
        with (
            patch.object(
                processor.apple_subscription_service,
                "get_transaction",
                return_value={"user_id": 7},
            ),
            patch.object(
                processor.apple_subscription_service,
                "get_latest_transaction",
                return_value={"user_id": 7},
            ),
            patch.object(
                processor.guest_subscriptions,
                "resolve_owner_by_app_account_token",
                return_value=processor.guest_subscriptions.AppleSubscriptionOwner("user", 7),
            ),
            patch.object(
                processor.apple_purchase_processor,
                "process_apple_transaction",
                side_effect=AppleOriginalTransactionOwnershipConflict("conflict"),
            ),
        ):
            with self.assertRaises(processor.AppleNotificationOwnershipConflict):
                processor.process_app_store_notification(notification)

    def test_notification_without_transaction_info_has_no_action(self):
        with patch.object(
            processor.apple_purchase_processor,
            "process_apple_transaction",
        ) as process_mock:
            result = processor.process_app_store_notification(
                self.notification("DID_FAIL_TO_RENEW", transaction=False, renewal=True)
            )
        self.assertEqual(result.action, "no_transaction_info")
        process_mock.assert_not_called()

    def test_test_notification_is_ignored(self):
        result = processor.process_app_store_notification(
            self.notification("TEST", transaction=False)
        )
        self.assertTrue(result.handled)
        self.assertEqual(result.action, "ignored_notification")

    def test_unknown_notification_is_safely_unsupported(self):
        result = processor.process_app_store_notification(
            self.notification("FUTURE_APPLE_EVENT", transaction=False)
        )
        self.assertFalse(result.handled)
        self.assertEqual(result.action, "unsupported_notification")

    def test_renewal_info_alone_does_not_change_plus(self):
        with patch.object(
            processor.apple_purchase_processor,
            "process_apple_transaction",
        ) as process_mock:
            result = processor.process_app_store_notification(
                self.notification("PRICE_INCREASE", transaction=False, renewal=True)
            )
        self.assertIsNone(result.is_plus)
        self.assertIsNone(result.changed)
        process_mock.assert_not_called()

    def test_referral_entitlement_tail_is_preserved_by_reconciler_result(self):
        referral_expiry = self.now + timedelta(days=7)
        result, _ = self.process_with_owner(
            self.notification("REFUND"),
            result=self.processing_result(
                is_plus=True,
                expires_at=referral_expiry,
                changed=True,
            ),
        )
        self.assertTrue(result.is_plus)
        self.assertEqual(result.expires_at, referral_expiry)

    def test_processor_contains_no_http_or_jws_verification_logic(self):
        source = inspect.getsource(processor)
        self.assertNotIn("FastAPI", source)
        self.assertNotIn("httpx", source)
        self.assertNotIn("verify_app_store_notification", source)
        self.assertNotIn("SignedDataVerifier", source)


if __name__ == "__main__":
    unittest.main()
