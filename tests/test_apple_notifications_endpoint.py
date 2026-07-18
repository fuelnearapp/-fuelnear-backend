from __future__ import annotations

from datetime import datetime, timezone
import inspect
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app import main
from app.apple_notification_processor import (
    AppleNotificationOwnershipConflict,
    AppleNotificationProcessingResult,
    AppleNotificationRepositoryError,
)
from app.apple_notification_verifier import (
    AppleNotificationConfigurationError,
    AppleNotificationInvalidError,
    AppleNotificationVerificationUnavailableError,
    VerifiedAppStoreNotification,
)


class AppleNotificationsEndpointTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)
        self.now = datetime.now(timezone.utc)
        self.verified = VerifiedAppStoreNotification(
            notification_uuid="notification-uuid-1",
            notification_type="SUBSCRIBED",
            subtype="INITIAL_BUY",
            signed_date=self.now,
            bundle_id="MB.FuelNear",
            environment="Sandbox",
            app_apple_id=None,
            has_transaction_info=False,
            has_renewal_info=False,
            transaction=None,
            renewal=None,
        )
        self.processed = AppleNotificationProcessingResult(
            notification_uuid="notification-uuid-1",
            notification_type="SUBSCRIBED",
            subtype="INITIAL_BUY",
            handled=True,
            action="transaction_processed",
            user_id=42,
            transaction_id="transaction-1",
            original_transaction_id="original-1",
            created=True,
            is_plus=True,
            expires_at=self.now,
            changed=True,
        )

    def post(self, payload=None):
        return self.client.post(
            "/apple/notifications",
            json=payload if payload is not None else {"signedPayload": "signed-jws"},
        )

    def test_valid_payload_returns_200(self):
        with (
            patch.object(
                main.apple_notification_verifier,
                "verify_app_store_notification",
                return_value=self.verified,
            ) as verifier_mock,
            patch.object(
                main.apple_notification_processor,
                "process_app_store_notification",
                return_value=self.processed,
            ) as processor_mock,
        ):
            response = self.post()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        verifier_mock.assert_called_once_with("signed-jws")
        processor_mock.assert_called_once_with(self.verified)

    def test_missing_signed_payload_returns_422(self):
        response = self.post({})
        self.assertEqual(response.status_code, 422)

    def test_empty_signed_payload_returns_400(self):
        for value in ("", "   "):
            with self.subTest(value=value):
                response = self.post({"signedPayload": value})
                self.assertEqual(response.status_code, 400)
                self.assertEqual(
                    response.json()["error_code"],
                    "APPLE_NOTIFICATION_INVALID",
                )

    def test_invalid_jws_returns_400_and_skips_processor(self):
        with (
            patch.object(
                main.apple_notification_verifier,
                "verify_app_store_notification",
                side_effect=AppleNotificationInvalidError("cryptographic detail"),
            ) as verifier_mock,
            patch.object(
                main.apple_notification_processor,
                "process_app_store_notification",
            ) as processor_mock,
        ):
            response = self.post()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error_code"], "APPLE_NOTIFICATION_INVALID")
        self.assertNotIn("cryptographic detail", response.text)
        verifier_mock.assert_called_once()
        processor_mock.assert_not_called()

    def test_invalid_apple_configuration_returns_safe_500(self):
        with patch.object(
            main.apple_notification_verifier,
            "verify_app_store_notification",
            side_effect=AppleNotificationConfigurationError("certificate path detail"),
        ):
            response = self.post()

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json()["error_code"],
            "APPLE_NOTIFICATION_UNAVAILABLE",
        )
        self.assertNotIn("certificate path detail", response.text)

    def test_temporary_apple_verification_failure_returns_500(self):
        with patch.object(
            main.apple_notification_verifier,
            "verify_app_store_notification",
            side_effect=AppleNotificationVerificationUnavailableError(
                "temporary OCSP detail"
            ),
        ):
            response = self.post()

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json()["error_code"],
            "APPLE_NOTIFICATION_UNAVAILABLE",
        )
        self.assertNotIn("temporary OCSP detail", response.text)

    def test_processor_failure_returns_safe_500(self):
        with (
            patch.object(
                main.apple_notification_verifier,
                "verify_app_store_notification",
                return_value=self.verified,
            ),
            patch.object(
                main.apple_notification_processor,
                "process_app_store_notification",
                side_effect=AppleNotificationRepositoryError("database detail"),
            ),
        ):
            response = self.post()

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json()["error_code"],
            "APPLE_NOTIFICATION_PROCESSING_FAILED",
        )
        self.assertNotIn("database detail", response.text)

    def test_ownership_conflict_returns_409(self):
        with (
            patch.object(
                main.apple_notification_verifier,
                "verify_app_store_notification",
                return_value=self.verified,
            ),
            patch.object(
                main.apple_notification_processor,
                "process_app_store_notification",
                side_effect=AppleNotificationOwnershipConflict("ownership detail"),
            ),
        ):
            response = self.post()

        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.json()["error_code"],
            "APPLE_NOTIFICATION_OWNERSHIP_CONFLICT",
        )
        self.assertNotIn("ownership detail", response.text)

    def test_unexpected_exception_returns_safe_500(self):
        with patch.object(
            main.apple_notification_verifier,
            "verify_app_store_notification",
            side_effect=RuntimeError("private unexpected detail"),
        ):
            response = self.post()

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json()["error_code"], "SERVER_ERROR")
        self.assertNotIn("private unexpected detail", response.text)

    def test_endpoint_requires_no_user_authentication(self):
        with (
            patch.object(
                main.apple_notification_verifier,
                "verify_app_store_notification",
                return_value=self.verified,
            ),
            patch.object(
                main.apple_notification_processor,
                "process_app_store_notification",
                return_value=self.processed,
            ),
        ):
            response = self.post()

        self.assertEqual(response.status_code, 200)

    def test_request_accepts_only_signed_payload(self):
        response = self.post({"signedPayload": "signed-jws", "extra": "value"})
        self.assertEqual(response.status_code, 422)

    def test_requires_json_content_type(self):
        with patch.object(
            main.apple_notification_verifier,
            "verify_app_store_notification",
        ) as verifier_mock:
            response = self.client.post(
                "/apple/notifications",
                content='{"signedPayload":"signed-jws"}',
                headers={"Content-Type": "text/plain"},
            )

        self.assertEqual(response.status_code, 415)
        self.assertEqual(response.json()["error_code"], "UNSUPPORTED_MEDIA_TYPE")
        verifier_mock.assert_not_called()

    def test_rejects_body_over_configured_limit(self):
        with patch.object(
            main.apple_notification_verifier,
            "verify_app_store_notification",
        ) as verifier_mock:
            response = self.post(
                {
                    "signedPayload": "x"
                    * (main.APPLE_NOTIFICATION_MAX_BODY_BYTES + 1)
                }
            )

        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.json()["error_code"], "REQUEST_TOO_LARGE")
        verifier_mock.assert_not_called()

    def test_signed_payload_is_not_logged(self):
        sensitive_jws = "signed-jws-must-not-appear"
        with (
            patch.object(
                main.apple_notification_verifier,
                "verify_app_store_notification",
                return_value=self.verified,
            ),
            patch.object(
                main.apple_notification_processor,
                "process_app_store_notification",
                return_value=self.processed,
            ),
            patch("builtins.print") as print_mock,
        ):
            response = self.post({"signedPayload": sensitive_jws})

        self.assertEqual(response.status_code, 200)
        logged_output = " ".join(
            " ".join(str(argument) for argument in call.args)
            for call in print_mock.call_args_list
        )
        self.assertNotIn(sensitive_jws, logged_output)

    def test_endpoint_contains_only_http_orchestration(self):
        source = inspect.getsource(main.receive_apple_notification)
        self.assertNotIn("get_connection", source)
        self.assertNotIn("AppleTransaction(", source)
        self.assertNotIn("reconcile_apple_entitlement", source)
        self.assertNotIn("save_apple_transaction", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
