from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from appstoreserverlibrary.models.Environment import Environment
from appstoreserverlibrary.signed_data_verifier import (
    VerificationException,
    VerificationStatus,
)

from app.apple_config import AppleSubscriptionsConfig
import app.apple_jws_verifier as jws_verifier_module
from app.apple_jws_verifier import (
    AppleJWSAppAccountTokenMalformedError,
    AppleJWSAppAccountTokenMismatchError,
    AppleJWSAppAccountTokenMissingError,
    AppleJWSInvalidError,
    AppleJWSVerificationUnavailableError,
    AppleJWSPayloadError,
    AppleJWSUnsupportedProductError,
    AppleRootCertificatesError,
    create_apple_signed_data_verifier,
    load_apple_root_certificates,
    verify_apple_signed_transaction,
)


class FakeVerifier:
    def __init__(self, payload=None, error: Exception | None = None):
        self.payload = payload
        self.error = error
        self.calls: list[str] = []

    def verify_and_decode_signed_transaction(self, value: str):
        self.calls.append(value)
        if self.error is not None:
            raise self.error
        return self.payload


class AppleJWSVerifierTestCase(unittest.TestCase):
    def config(
        self,
        certificate_path: Path,
        *,
        environment: str = "sandbox",
        app_id: int | None = None,
    ) -> AppleSubscriptionsConfig:
        return AppleSubscriptionsConfig(
            bundle_id="MB.FuelNear",
            environment=environment,
            app_id=app_id,
            root_certificates_path=certificate_path,
            enable_online_checks=True,
        )

    def payload(self, **changes):
        token = uuid4()
        values = {
            "productId": "MB.FuelNear.plus.monthly",
            "transactionId": "transaction-1",
            "originalTransactionId": "original-1",
            "purchaseDate": 1_700_000_000_000,
            "expiresDate": 1_700_086_400_000,
            "environment": Environment.SANDBOX,
            "inAppOwnershipType": "PURCHASED",
            "transactionReason": "PURCHASE",
            "revocationDate": None,
            "revocationReason": None,
            "appAccountToken": str(token),
            "signedDate": 1_700_000_001_000,
            "storefront": "ITA",
            "offerType": 1,
        }
        values.update(changes)
        return SimpleNamespace(**values)

    def test_initializes_sandbox_verifier_with_expected_values(self):
        with tempfile.TemporaryDirectory() as directory:
            certificate = Path(directory) / "AppleRoot.cer"
            certificate.write_bytes(b"certificate")
            factory = Mock(return_value=FakeVerifier())

            result = create_apple_signed_data_verifier(
                self.config(certificate),
                verifier_factory=factory,
            )

        self.assertIs(result, factory.return_value)
        factory.assert_called_once_with(
            [b"certificate"],
            True,
            Environment.SANDBOX,
            "MB.FuelNear",
            None,
        )

    def test_maps_production_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            certificate = Path(directory) / "AppleRoot.der"
            certificate.write_bytes(b"certificate")
            factory = Mock(return_value=FakeVerifier())

            create_apple_signed_data_verifier(
                self.config(
                    certificate,
                    environment="production",
                    app_id=123456789,
                ),
                verifier_factory=factory,
            )

        self.assertEqual(factory.call_args.args[2], Environment.PRODUCTION)
        self.assertEqual(factory.call_args.args[4], 123456789)

    def test_loads_multiple_der_certificates_from_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "A.cer").write_bytes(b"first")
            (path / "B.der").write_bytes(b"second")
            (path / "ignored.pem").write_bytes(b"ignored")

            certificates = load_apple_root_certificates(path)

        self.assertEqual(certificates, [b"first", b"second"])

    def test_missing_directory_or_certificates_is_rejected(self):
        with self.assertRaises(AppleRootCertificatesError):
            load_apple_root_certificates(Path("/path/that/does/not/exist"))

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(AppleRootCertificatesError):
                load_apple_root_certificates(Path(directory))

    def test_successfully_normalizes_verified_payload(self):
        payload = self.payload()
        result = verify_apple_signed_transaction(
            "signed-jws",
            verifier=FakeVerifier(payload),
        )

        self.assertEqual(result.product_id, payload.productId)
        self.assertEqual(result.transaction_id, payload.transactionId)
        self.assertEqual(result.original_transaction_id, payload.originalTransactionId)
        self.assertEqual(
            result.purchase_date,
            datetime.fromtimestamp(payload.purchaseDate / 1000, tz=timezone.utc),
        )
        self.assertEqual(result.environment, "Sandbox")
        self.assertEqual(result.app_account_token, UUID(payload.appAccountToken))
        self.assertEqual(result.offer_type, 1)

    def test_empty_jws_is_rejected(self):
        with self.assertRaises(AppleJWSInvalidError):
            verify_apple_signed_transaction("   ", verifier=FakeVerifier())

    def test_verification_exception_is_converted(self):
        verifier = FakeVerifier(
            error=VerificationException(VerificationStatus.VERIFICATION_FAILURE)
        )
        with self.assertRaises(AppleJWSInvalidError) as context:
            verify_apple_signed_transaction("signed-jws", verifier=verifier)

        self.assertIsInstance(context.exception.__cause__, VerificationException)

    def test_retryable_verification_exception_remains_retryable(self):
        verifier = FakeVerifier(
            error=VerificationException(
                VerificationStatus.RETRYABLE_VERIFICATION_FAILURE
            )
        )
        with self.assertRaises(AppleJWSVerificationUnavailableError) as context:
            verify_apple_signed_transaction("signed-jws", verifier=verifier)

        self.assertIsInstance(context.exception.__cause__, VerificationException)

    def test_missing_transaction_id_is_rejected(self):
        with self.assertRaises(AppleJWSPayloadError):
            verify_apple_signed_transaction(
                "signed-jws",
                verifier=FakeVerifier(self.payload(transactionId=None)),
            )

    def test_missing_original_transaction_id_is_rejected(self):
        with self.assertRaises(AppleJWSPayloadError):
            verify_apple_signed_transaction(
                "signed-jws",
                verifier=FakeVerifier(self.payload(originalTransactionId=None)),
            )

    def test_unsupported_product_is_rejected(self):
        with self.assertRaises(AppleJWSUnsupportedProductError):
            verify_apple_signed_transaction(
                "signed-jws",
                verifier=FakeVerifier(self.payload(productId="unsupported.product")),
            )

    def test_matching_app_account_token_is_accepted(self):
        expected = uuid4()
        result = verify_apple_signed_transaction(
            "signed-jws",
            expected,
            verifier=FakeVerifier(self.payload(appAccountToken=str(expected))),
        )
        self.assertEqual(result.app_account_token, expected)

    def test_missing_expected_app_account_token_is_rejected(self):
        with self.assertRaises(AppleJWSAppAccountTokenMissingError):
            verify_apple_signed_transaction(
                "signed-jws",
                uuid4(),
                verifier=FakeVerifier(self.payload(appAccountToken=None)),
            )

    def test_malformed_app_account_token_is_rejected(self):
        with self.assertRaises(AppleJWSAppAccountTokenMalformedError):
            verify_apple_signed_transaction(
                "signed-jws",
                verifier=FakeVerifier(self.payload(appAccountToken="not-a-uuid")),
            )

    def test_different_app_account_token_is_rejected(self):
        with self.assertRaises(AppleJWSAppAccountTokenMismatchError):
            verify_apple_signed_transaction(
                "signed-jws",
                uuid4(),
                verifier=FakeVerifier(self.payload(appAccountToken=str(uuid4()))),
            )

    def test_missing_app_account_token_is_allowed_when_not_expected(self):
        result = verify_apple_signed_transaction(
            "signed-jws",
            verifier=FakeVerifier(self.payload(appAccountToken=None)),
        )
        self.assertIsNone(result.app_account_token)

    def test_default_verifier_is_reused_for_same_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory) / "AppleRoot.cer")
            verifier = FakeVerifier()
            with (
                patch.object(jws_verifier_module, "_default_verifier", None),
                patch.object(jws_verifier_module, "_default_verifier_config", None),
                patch.object(
                    jws_verifier_module,
                    "create_apple_signed_data_verifier",
                    return_value=verifier,
                ) as factory,
            ):
                first = jws_verifier_module._get_default_apple_signed_data_verifier(
                    config
                )
                second = jws_verifier_module._get_default_apple_signed_data_verifier(
                    config
                )

        self.assertIs(first, verifier)
        self.assertIs(second, verifier)
        factory.assert_called_once_with(config)


if __name__ == "__main__":
    unittest.main()
