from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from appstoreserverlibrary.models.AutoRenewStatus import AutoRenewStatus
from appstoreserverlibrary.models.Data import Data
from appstoreserverlibrary.models.Environment import Environment
from appstoreserverlibrary.models.ExpirationIntent import ExpirationIntent
from appstoreserverlibrary.models.JWSRenewalInfoDecodedPayload import (
    JWSRenewalInfoDecodedPayload,
)
from appstoreserverlibrary.models.JWSTransactionDecodedPayload import (
    JWSTransactionDecodedPayload,
)
from appstoreserverlibrary.models.NotificationTypeV2 import NotificationTypeV2
from appstoreserverlibrary.models.ResponseBodyV2DecodedPayload import (
    ResponseBodyV2DecodedPayload,
)
from appstoreserverlibrary.models.Subtype import Subtype
from appstoreserverlibrary.signed_data_verifier import (
    VerificationException,
    VerificationStatus,
)

from app.apple_config import AppleSubscriptionsConfig
import app.apple_notification_verifier as notification_verifier_module
from app.apple_notification_verifier import (
    AppleNotificationCertificatesError,
    AppleNotificationConfigurationError,
    AppleNotificationInvalidError,
    AppleNotificationPayloadError,
    AppleNotificationRenewalDataError,
    AppleNotificationTransactionDataError,
    AppleNotificationTypeMissingError,
    AppleNotificationVerificationUnavailableError,
    create_app_store_notification_verifier,
    verify_app_store_notification,
)


class FakeVerifier:
    def __init__(self, notification, transaction=None, renewal=None, error=None):
        self.notification = notification
        self.transaction = transaction
        self.renewal = renewal
        self.error = error
        self.notification_calls: list[str] = []
        self.transaction_calls: list[str] = []
        self.renewal_calls: list[str] = []

    def verify_and_decode_notification(self, value: str):
        self.notification_calls.append(value)
        if self.error is not None:
            raise self.error
        return self.notification

    def verify_and_decode_signed_transaction(self, value: str):
        self.transaction_calls.append(value)
        if isinstance(self.transaction, Exception):
            raise self.transaction
        return self.transaction

    def verify_and_decode_renewal_info(self, value: str):
        self.renewal_calls.append(value)
        if isinstance(self.renewal, Exception):
            raise self.renewal
        return self.renewal


class EnvironmentVerifier(FakeVerifier):
    def __init__(self, environment, notification, transaction=None, renewal=None):
        super().__init__(notification, transaction=transaction, renewal=renewal)
        self.environment = environment

    def verify_and_decode_notification(self, value: str):
        payload = super().verify_and_decode_notification(value)
        if (
            self.environment == Environment.PRODUCTION
            and payload.data is not None
            and payload.data.appAppleId is None
        ):
            raise VerificationException(VerificationStatus.INVALID_APP_IDENTIFIER)
        if payload.data is None or payload.data.environment != self.environment:
            raise VerificationException(VerificationStatus.INVALID_ENVIRONMENT)
        return payload


class AppleNotificationVerifierTestCase(unittest.TestCase):
    def config(
        self,
        *,
        environment: str = "sandbox",
        app_id: int | None = None,
        accepted_environments: tuple[str, ...] = (),
    ):
        return AppleSubscriptionsConfig(
            bundle_id="MB.FuelNear",
            environment=environment,
            app_id=app_id,
            root_certificates_path=Path("/unused/apple-root.cer"),
            enable_online_checks=True,
            accepted_environments=accepted_environments,
        )

    def notification(
        self,
        *,
        environment=Environment.SANDBOX,
        app_id=None,
        notification_type=NotificationTypeV2.DID_RENEW,
        subtype=Subtype.RESUBSCRIBE,
        transaction_info="signed-transaction",
        renewal_info="signed-renewal",
    ):
        return ResponseBodyV2DecodedPayload(
            notificationType=notification_type,
            subtype=subtype,
            notificationUUID="notification-uuid",
            signedDate=1_700_000_000_000,
            data=Data(
                environment=environment,
                appAppleId=app_id,
                bundleId="MB.FuelNear",
                signedTransactionInfo=transaction_info,
                signedRenewalInfo=renewal_info,
            ),
        )

    def transaction(
        self,
        *,
        app_account_token=None,
        environment=Environment.SANDBOX,
    ):
        return JWSTransactionDecodedPayload(
            productId="MB.FuelNear.plus.monthly",
            transactionId="transaction-1",
            originalTransactionId="original-1",
            purchaseDate=1_700_000_000_000,
            expiresDate=1_700_086_400_000,
            revocationDate=None,
            revocationReason=None,
            appAccountToken=str(app_account_token or uuid4()),
            environment=environment,
            bundleId="MB.FuelNear",
        )

    def renewal(
        self,
        *,
        environment=Environment.SANDBOX,
        grace_period_expires_date=None,
    ):
        return JWSRenewalInfoDecodedPayload(
            autoRenewStatus=AutoRenewStatus.ON,
            expirationIntent=ExpirationIntent.BILLING_ERROR,
            autoRenewProductId="MB.FuelNear.plus.monthly",
            gracePeriodExpiresDate=grace_period_expires_date,
            environment=environment,
        )

    def verify(self, notification=None, transaction=None, renewal=None, **kwargs):
        fake = FakeVerifier(
            notification or self.notification(),
            transaction=self.transaction() if transaction is None else transaction,
            renewal=self.renewal() if renewal is None else renewal,
        )
        result = verify_app_store_notification(
            "signed-notification",
            config=kwargs.get("config", self.config()),
            verifier=fake,
        )
        return result, fake

    def test_valid_payload_is_normalized(self):
        token = uuid4()
        result, fake = self.verify(transaction=self.transaction(app_account_token=token))

        self.assertEqual(result.notification_uuid, "notification-uuid")
        self.assertEqual(result.notification_type, "DID_RENEW")
        self.assertEqual(result.subtype, "RESUBSCRIBE")
        self.assertEqual(result.bundle_id, "MB.FuelNear")
        self.assertEqual(result.environment, "Sandbox")
        self.assertEqual(
            result.signed_date,
            datetime.fromtimestamp(1_700_000_000, tz=timezone.utc),
        )
        self.assertTrue(result.has_transaction_info)
        self.assertTrue(result.has_renewal_info)
        self.assertEqual(result.transaction.app_account_token, UUID(str(token)))
        self.assertEqual(result.renewal.auto_renew_status, 1)
        self.assertEqual(result.renewal.expiration_intent, 2)
        self.assertEqual(fake.notification_calls, ["signed-notification"])
        self.assertEqual(fake.transaction_calls, ["signed-transaction"])
        self.assertEqual(fake.renewal_calls, ["signed-renewal"])

    def test_grace_period_expiration_is_normalized(self):
        result, _ = self.verify(
            renewal=self.renewal(grace_period_expires_date=1_700_172_800_000)
        )

        self.assertEqual(
            result.renewal.grace_period_expires_date,
            datetime.fromtimestamp(1_700_172_800, tz=timezone.utc),
        )

    def test_missing_grace_period_expiration_is_allowed(self):
        result, _ = self.verify(renewal=self.renewal())
        self.assertIsNone(result.renewal.grace_period_expires_date)

    def test_invalid_grace_period_expiration_is_rejected(self):
        with self.assertRaises(AppleNotificationRenewalDataError):
            self.verify(renewal=self.renewal(grace_period_expires_date="invalid"))

    def test_empty_payload_is_rejected(self):
        with self.assertRaises(AppleNotificationInvalidError):
            verify_app_store_notification("   ", config=self.config(), verifier=Mock())

    def test_library_verification_error_is_mapped_with_cause(self):
        library_error = VerificationException(VerificationStatus.VERIFICATION_FAILURE)
        verifier = FakeVerifier(self.notification(), error=library_error)

        with self.assertRaises(AppleNotificationInvalidError) as context:
            verify_app_store_notification(
                "signed-notification",
                config=self.config(),
                verifier=verifier,
            )

        self.assertIs(context.exception.__cause__, library_error)

    def test_retryable_library_error_is_reported_as_temporarily_unavailable(self):
        library_error = VerificationException(
            VerificationStatus.RETRYABLE_VERIFICATION_FAILURE
        )
        verifier = FakeVerifier(self.notification(), error=library_error)

        with self.assertRaises(
            AppleNotificationVerificationUnavailableError
        ) as context:
            verify_app_store_notification(
                "signed-notification",
                config=self.config(),
                verifier=verifier,
            )

        self.assertIs(context.exception.__cause__, library_error)

    def test_sandbox_environment_is_normalized(self):
        result, _ = self.verify()
        self.assertEqual(result.environment, "Sandbox")
        self.assertIsNone(result.app_apple_id)

    def test_production_environment_and_app_id_are_validated(self):
        app_id = 123456789
        result, _ = self.verify(
            notification=self.notification(
                environment=Environment.PRODUCTION,
                app_id=app_id,
            ),
            transaction=self.transaction(
                environment=Environment.PRODUCTION,
            ),
            renewal=JWSRenewalInfoDecodedPayload(
                autoRenewStatus=AutoRenewStatus.ON,
                autoRenewProductId="MB.FuelNear.plus.monthly",
                environment=Environment.PRODUCTION,
            ),
            config=self.config(environment="production", app_id=app_id),
        )
        self.assertEqual(result.environment, "Production")
        self.assertEqual(result.app_apple_id, app_id)

    def test_transaction_info_can_be_absent(self):
        result, fake = self.verify(
            notification=self.notification(transaction_info=None),
            transaction=SimpleNamespace(),
        )
        self.assertFalse(result.has_transaction_info)
        self.assertIsNone(result.transaction)
        self.assertEqual(fake.transaction_calls, [])

    def test_renewal_info_can_be_absent(self):
        result, fake = self.verify(
            notification=self.notification(renewal_info=None),
            renewal=SimpleNamespace(),
        )
        self.assertFalse(result.has_renewal_info)
        self.assertIsNone(result.renewal)
        self.assertEqual(fake.renewal_calls, [])

    def test_notification_type_is_required(self):
        payload = self.notification(notification_type=None)
        payload.rawNotificationType = None
        with self.assertRaises(AppleNotificationTypeMissingError):
            self.verify(notification=payload)

    def test_subtype_is_optional(self):
        payload = self.notification(subtype=None)
        payload.rawSubtype = None
        result, _ = self.verify(notification=payload)
        self.assertIsNone(result.subtype)

    def test_incomplete_payload_is_rejected(self):
        payload = self.notification()
        payload.notificationUUID = None
        with self.assertRaises(AppleNotificationPayloadError):
            self.verify(notification=payload)

    def test_invalid_transaction_data_is_mapped(self):
        invalid_transaction = self.transaction()
        invalid_transaction.transactionId = None
        with self.assertRaises(AppleNotificationTransactionDataError):
            self.verify(transaction=invalid_transaction)

    def test_invalid_renewal_data_is_mapped(self):
        invalid_renewal = self.renewal()
        invalid_renewal.autoRenewStatus = None
        invalid_renewal.rawAutoRenewStatus = None
        with self.assertRaises(AppleNotificationRenewalDataError):
            self.verify(renewal=invalid_renewal)

    def test_nested_library_errors_are_mapped(self):
        library_error = VerificationException(VerificationStatus.VERIFICATION_FAILURE)
        with self.assertRaises(AppleNotificationTransactionDataError) as transaction_context:
            self.verify(transaction=library_error)
        self.assertIs(transaction_context.exception.__cause__, library_error)

        with self.assertRaises(AppleNotificationRenewalDataError) as renewal_context:
            self.verify(renewal=library_error)
        self.assertIs(renewal_context.exception.__cause__, library_error)

    def test_nested_retryable_library_errors_remain_retryable(self):
        retryable_error = VerificationException(
            VerificationStatus.RETRYABLE_VERIFICATION_FAILURE
        )
        with self.assertRaises(AppleNotificationVerificationUnavailableError):
            self.verify(transaction=retryable_error)
        with self.assertRaises(AppleNotificationVerificationUnavailableError):
            self.verify(renewal=retryable_error)

    def test_configuration_and_missing_certificates_are_mapped(self):
        with self.assertRaises(AppleNotificationConfigurationError):
            create_app_store_notification_verifier(
                self.config(environment="invalid")
            )

        with self.assertRaises(AppleNotificationCertificatesError):
            create_app_store_notification_verifier(self.config())

    def test_verifier_factory_receives_official_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            certificate = Path(directory) / "AppleRoot.cer"
            certificate.write_bytes(b"certificate")
            factory = Mock(return_value=SimpleNamespace())

            result = create_app_store_notification_verifier(
                AppleSubscriptionsConfig(
                    bundle_id="MB.FuelNear",
                    environment="sandbox",
                    app_id=None,
                    root_certificates_path=certificate,
                    enable_online_checks=False,
                ),
                verifier_factory=factory,
            )

        self.assertIs(result, factory.return_value)
        factory.assert_called_once_with(
            [b"certificate"],
            False,
            Environment.SANDBOX,
            "MB.FuelNear",
            None,
        )

    def test_default_verifier_is_reused_for_same_configuration(self):
        config = self.config()
        verifier = Mock()
        with (
            patch.object(notification_verifier_module, "_default_verifiers", {}),
            patch.object(
                notification_verifier_module,
                "create_app_store_notification_verifier",
                return_value=verifier,
            ) as factory,
        ):
            first = notification_verifier_module._get_default_app_store_notification_verifier(
                config
            )
            second = notification_verifier_module._get_default_app_store_notification_verifier(
                config
            )

        self.assertIs(first, verifier)
        self.assertIs(second, verifier)
        factory.assert_called_once_with(config)

    def test_dual_allowlist_accepts_sandbox_and_production_notifications(self):
        config = self.config(
            environment="production",
            app_id=123456789,
            accepted_environments=("sandbox", "production"),
        )
        sandbox_payload = self.notification(environment=Environment.SANDBOX)
        sandbox_verifier = EnvironmentVerifier(
            Environment.SANDBOX,
            sandbox_payload,
            transaction=self.transaction(environment=Environment.SANDBOX),
            renewal=self.renewal(environment=Environment.SANDBOX),
        )
        production_verifier = EnvironmentVerifier(
            Environment.PRODUCTION,
            sandbox_payload,
        )
        sandbox_result = verify_app_store_notification(
            "sandbox-notification",
            config=config,
            verifiers=[
                ("production", production_verifier),
                ("sandbox", sandbox_verifier),
            ],
        )
        self.assertEqual(sandbox_result.environment, "Sandbox")

        production_payload = self.notification(
            environment=Environment.PRODUCTION,
            app_id=123456789,
        )
        production_verifier = EnvironmentVerifier(
            Environment.PRODUCTION,
            production_payload,
            transaction=self.transaction(environment=Environment.PRODUCTION),
            renewal=self.renewal(environment=Environment.PRODUCTION),
        )
        production_result = verify_app_store_notification(
            "production-notification",
            config=config,
            verifiers=[("production", production_verifier)],
        )
        self.assertEqual(production_result.environment, "Production")

    def test_dual_allowlist_builds_distinct_official_verifiers(self):
        config = self.config(
            environment="production",
            app_id=123456789,
            accepted_environments=("sandbox", "production"),
        )
        production_verifier = Mock(name="production-verifier")
        sandbox_verifier = Mock(name="sandbox-verifier")
        with (
            patch.object(notification_verifier_module, "_default_verifiers", {}),
            patch.object(
                notification_verifier_module,
                "create_app_store_notification_verifier",
                side_effect=[production_verifier, sandbox_verifier],
            ) as factory,
        ):
            candidates = notification_verifier_module._get_configured_notification_verifier_candidates(
                config
            )

        self.assertEqual(
            candidates,
            [
                ("production", production_verifier),
                ("sandbox", sandbox_verifier),
            ],
        )
        self.assertEqual(factory.call_count, 2)
        self.assertEqual(factory.call_args_list[0].args[0].environment, "production")
        self.assertEqual(factory.call_args_list[1].args[0].environment, "sandbox")

    def test_single_environment_allowlist_rejects_the_other_environment(self):
        sandbox_payload = self.notification(environment=Environment.SANDBOX)
        with self.assertRaises(AppleNotificationInvalidError):
            verify_app_store_notification(
                "sandbox-notification",
                config=self.config(environment="production", app_id=123456789),
                verifiers=[
                    (
                        "production",
                        EnvironmentVerifier(Environment.PRODUCTION, sandbox_payload),
                    )
                ],
            )

        production_payload = self.notification(
            environment=Environment.PRODUCTION,
            app_id=123456789,
        )
        with self.assertRaises(AppleNotificationInvalidError):
            verify_app_store_notification(
                "production-notification",
                config=self.config(environment="sandbox"),
                verifiers=[
                    (
                        "sandbox",
                        EnvironmentVerifier(Environment.SANDBOX, production_payload),
                    )
                ],
            )

    def test_transaction_environment_must_match_notification(self):
        with self.assertRaises(AppleNotificationTransactionDataError):
            self.verify(
                transaction=self.transaction(environment=Environment.PRODUCTION)
            )

    def test_renewal_environment_must_match_notification(self):
        with self.assertRaises(AppleNotificationRenewalDataError):
            self.verify(renewal=self.renewal(environment=Environment.PRODUCTION))

    def test_unknown_or_missing_notification_environment_is_rejected(self):
        unknown = self.notification(environment=Environment.XCODE)
        with self.assertRaises(AppleNotificationPayloadError):
            self.verify(notification=unknown)

        missing = self.notification(environment=None)
        with self.assertRaises(AppleNotificationPayloadError):
            self.verify(notification=missing)

        transaction_missing = self.transaction()
        object.__setattr__(transaction_missing, "environment", None)
        with self.assertRaises(AppleNotificationTransactionDataError):
            self.verify(transaction=transaction_missing)

    def test_environment_normalization_is_case_and_whitespace_safe(self):
        notification = self.notification()
        object.__setattr__(notification.data, "environment", " Sandbox ")
        transaction = self.transaction()
        object.__setattr__(transaction, "environment", "SANDBOX")
        renewal = self.renewal()
        object.__setattr__(renewal, "environment", " sandbox ")

        result, _ = self.verify(
            notification=notification,
            transaction=transaction,
            renewal=renewal,
        )
        self.assertEqual(result.environment, "Sandbox")


if __name__ == "__main__":
    unittest.main(verbosity=2)
