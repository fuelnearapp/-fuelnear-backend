from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

from app import main
from app.apple_jws_verifier import (
    AppleJWSAppAccountTokenMismatchError,
    AppleJWSAppAccountTokenMissingError,
    AppleJWSInvalidError,
    AppleJWSConfigurationError,
    AppleJWSVerificationUnavailableError,
    AppleJWSUnsupportedProductError,
    AppleRootCertificatesError,
    VerifiedAppleTransaction,
)
from app.apple_purchase_processor import ApplePurchaseProcessingResult
from app.apple_subscriptions import (
    AppleOriginalTransactionOwnershipConflict,
    AppleTransaction,
)


class AppleSubscriptionVerifyEndpointTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)
        self.app_account_token = uuid4()
        self.now = datetime.now(timezone.utc)
        self.verified = VerifiedAppleTransaction(
            product_id="MB.FuelNear.plus.monthly",
            transaction_id="transaction-1",
            original_transaction_id="original-1",
            purchase_date=self.now,
            expires_date=self.now + timedelta(days=30),
            environment="Sandbox",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            revocation_date=None,
            revocation_reason=None,
            app_account_token=self.app_account_token,
            signed_date=self.now,
            storefront="ITA",
            offer_type=None,
        )
        self.processed = ApplePurchaseProcessingResult(
            created=True,
            transaction_id="transaction-1",
            original_transaction_id="original-1",
            is_plus=True,
            expires_at=self.now + timedelta(days=30),
            changed=True,
        )
        token_lookup_patcher = patch.object(
            main.guest_subscriptions,
            "get_allowed_app_account_tokens_for_user",
            return_value=[str(self.app_account_token)],
        )
        token_lookup_patcher.start()
        self.addCleanup(token_lookup_patcher.stop)

    def authenticated_user(self) -> dict:
        return {
            "id": 42,
            "app_account_token": str(self.app_account_token),
        }

    def post(self, payload=None):
        return self.client.post(
            "/user/subscription/apple/verify",
            json=payload if payload is not None else {"signed_transaction": "signed-jws"},
            headers={"Authorization": "Bearer access-token"},
        )

    def test_requires_authentication(self):
        response = self.client.post(
            "/user/subscription/apple/verify",
            json={"signed_transaction": "signed-jws"},
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error_code"], "AUTHORIZATION_REQUIRED")

    def test_rejects_missing_or_empty_signed_transaction(self):
        for payload in ({}, {"signed_transaction": ""}, {"signed_transaction": "   "}):
            with self.subTest(payload=payload):
                response = self.post(payload)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.json()["error_code"], "INVALID_REQUEST")

    @patch.object(main.apple_purchase_processor, "process_apple_transaction")
    @patch.object(main.apple_jws_verifier, "verify_apple_signed_transaction")
    @patch.object(main, "get_current_user_from_token")
    def test_success_verifies_processes_and_serializes_response(
        self,
        auth_mock,
        verify_mock,
        process_mock,
    ):
        auth_mock.return_value = self.authenticated_user()
        verify_mock.return_value = self.verified
        process_mock.return_value = self.processed

        response = self.post()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "transaction_id": "transaction-1",
                "original_transaction_id": "original-1",
                "product_id": "MB.FuelNear.plus.monthly",
                "created": True,
                "is_plus": True,
                "expires_at": self.processed.expires_at.isoformat().replace("+00:00", "Z"),
                "changed": True,
            },
        )
        verify_mock.assert_called_once_with(
            signed_transaction="signed-jws",
            expected_app_account_token=str(self.app_account_token),
        )
        process_mock.assert_called_once()
        transaction = process_mock.call_args.args[0]
        self.assertIsInstance(transaction, AppleTransaction)
        self.assertEqual(transaction.user_id, 42)
        self.assertEqual(transaction.transaction_id, self.verified.transaction_id)
        self.assertEqual(transaction.app_account_token, self.app_account_token)

    @patch.object(main.apple_purchase_processor, "process_apple_transaction")
    @patch.object(main.apple_jws_verifier, "verify_apple_signed_transaction")
    @patch.object(main, "get_current_user_from_token")
    def test_duplicate_transaction_is_reconciled_successfully(
        self,
        auth_mock,
        verify_mock,
        process_mock,
    ):
        auth_mock.return_value = self.authenticated_user()
        verify_mock.return_value = self.verified
        process_mock.return_value = ApplePurchaseProcessingResult(
            created=False,
            transaction_id="transaction-1",
            original_transaction_id="original-1",
            is_plus=True,
            expires_at=self.processed.expires_at,
            changed=False,
        )

        response = self.post()

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["created"])
        self.assertFalse(response.json()["changed"])
        process_mock.assert_called_once()

    def assert_verifier_error(self, error, status_code: int, error_code: str):
        with (
            patch.object(main, "get_current_user_from_token", return_value=self.authenticated_user()),
            patch.object(main.apple_jws_verifier, "verify_apple_signed_transaction", side_effect=error),
            patch.object(main.apple_purchase_processor, "process_apple_transaction") as process_mock,
        ):
            response = self.post()

        self.assertEqual(response.status_code, status_code)
        self.assertEqual(response.json()["error_code"], error_code)
        self.assertNotIn("private", response.text.lower())
        process_mock.assert_not_called()

    def test_invalid_jws_returns_400(self):
        self.assert_verifier_error(
            AppleJWSInvalidError("cryptographic details"),
            400,
            "APPLE_TRANSACTION_INVALID",
        )

    def test_temporary_verification_failure_returns_503(self):
        self.assert_verifier_error(
            AppleJWSVerificationUnavailableError("temporary OCSP detail"),
            503,
            "APPLE_VERIFICATION_UNAVAILABLE",
        )

    def test_invalid_server_configuration_returns_safe_500(self):
        self.assert_verifier_error(
            AppleJWSConfigurationError("private configuration detail"),
            500,
            "SERVER_ERROR",
        )

    def test_missing_root_certificates_return_safe_500(self):
        self.assert_verifier_error(
            AppleRootCertificatesError("private certificate path"),
            500,
            "SERVER_ERROR",
        )

    def test_unsupported_product_returns_400(self):
        self.assert_verifier_error(
            AppleJWSUnsupportedProductError("unsupported product"),
            400,
            "APPLE_TRANSACTION_INVALID",
        )

    def test_missing_app_account_token_returns_403(self):
        self.assert_verifier_error(
            AppleJWSAppAccountTokenMissingError("missing token"),
            403,
            "APPLE_APP_ACCOUNT_TOKEN_INVALID",
        )

    def test_different_app_account_token_returns_403(self):
        self.assert_verifier_error(
            AppleJWSAppAccountTokenMismatchError("different token"),
            403,
            "APPLE_APP_ACCOUNT_TOKEN_INVALID",
        )

    @patch.object(main.apple_purchase_processor, "process_apple_transaction")
    @patch.object(main.apple_jws_verifier, "verify_apple_signed_transaction")
    @patch.object(main, "get_current_user_from_token")
    def test_ownership_conflict_returns_409(
        self,
        auth_mock,
        verify_mock,
        process_mock,
    ):
        auth_mock.return_value = self.authenticated_user()
        verify_mock.return_value = self.verified
        process_mock.side_effect = AppleOriginalTransactionOwnershipConflict("database detail")

        response = self.post()

        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.json()["error_code"],
            "APPLE_TRANSACTION_OWNERSHIP_CONFLICT",
        )
        self.assertNotIn("database detail", response.text)

    @patch.object(main.apple_purchase_processor, "process_apple_transaction")
    @patch.object(main.apple_jws_verifier, "verify_apple_signed_transaction")
    @patch.object(main, "get_current_user_from_token")
    def test_internal_error_returns_safe_500(
        self,
        auth_mock,
        verify_mock,
        process_mock,
    ):
        auth_mock.return_value = self.authenticated_user()
        verify_mock.return_value = self.verified
        process_mock.side_effect = RuntimeError("private internal detail")

        response = self.post()

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json()["error_code"], "SERVER_ERROR")
        self.assertNotIn("private internal detail", response.text)

    @patch.object(main.apple_jws_verifier, "verify_apple_signed_transaction")
    @patch.object(main, "get_current_user_from_token")
    def test_missing_user_app_account_token_returns_403(self, auth_mock, verify_mock):
        auth_mock.return_value = {"id": 42, "app_account_token": None}
        with patch.object(
            main.guest_subscriptions,
            "get_allowed_app_account_tokens_for_user",
            return_value=[],
        ):
            response = self.post()

        self.assertEqual(response.status_code, 403)
        self.assertEqual(
            response.json()["error_code"],
            "APPLE_APP_ACCOUNT_TOKEN_INVALID",
        )
        verify_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
