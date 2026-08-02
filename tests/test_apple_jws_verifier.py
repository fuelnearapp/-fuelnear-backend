from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from appstoreserverlibrary.models.Environment import Environment
from appstoreserverlibrary.signed_data_verifier import (
    SignedDataVerifier,
    VerificationException,
    VerificationStatus,
)
from cryptography import x509
from cryptography.x509.oid import NameOID

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


REPOSITORY_APPLE_CERTIFICATES = (
    Path(__file__).resolve().parents[1] / "certs" / "apple"
)
EXPECTED_APPLE_ROOTS = {
    "AppleIncRootCertificate.cer": (
        "Apple Root CA",
        "B0B1730ECBC7FF4505142C49F1295E6EDA6BCAED7E2C68C5BE91B5A11001F024",
    ),
    "AppleRootCA-G2.cer": (
        "Apple Root CA - G2",
        "C2B9B042DD57830E7D117DAC55AC8AE19407D38E41D88F3215BC3A890444A050",
    ),
    "AppleRootCA-G3.cer": (
        "Apple Root CA - G3",
        "63343ABFB89A6A03EBB57E9B3F5FA7BE7C4F5C756F3017B3A8C488C3653E9179",
    ),
}


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


class EnvironmentVerifier(FakeVerifier):
    def __init__(self, environment: Environment, payload=None, error=None):
        super().__init__(payload=payload, error=error)
        self.environment = environment

    def verify_and_decode_signed_transaction(self, value: str):
        payload = super().verify_and_decode_signed_transaction(value)
        if payload is not None and payload.environment != self.environment:
            raise VerificationException(VerificationStatus.INVALID_ENVIRONMENT)
        return payload


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

    def test_repository_contains_three_valid_official_apple_root_certificates(self):
        certificate_paths = sorted(REPOSITORY_APPLE_CERTIFICATES.glob("*.cer"))

        self.assertEqual(
            [path.name for path in certificate_paths],
            sorted(EXPECTED_APPLE_ROOTS),
        )
        loaded = load_apple_root_certificates(REPOSITORY_APPLE_CERTIFICATES)
        self.assertEqual(len(loaded), 3)

        for path, certificate_bytes in zip(certificate_paths, loaded):
            with self.subTest(certificate=path.name):
                self.assertTrue(certificate_bytes)
                certificate = x509.load_der_x509_certificate(certificate_bytes)
                common_name = certificate.subject.get_attributes_for_oid(
                    NameOID.COMMON_NAME
                )[0].value
                expected_common_name, expected_fingerprint = EXPECTED_APPLE_ROOTS[
                    path.name
                ]
                self.assertEqual(common_name, expected_common_name)
                self.assertEqual(certificate.subject, certificate.issuer)
                self.assertEqual(
                    sha256(certificate_bytes).hexdigest().upper(),
                    expected_fingerprint,
                )

    def test_unreadable_certificate_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            certificate = Path(directory) / "AppleRoot.cer"
            certificate.write_bytes(b"certificate")
            with patch.object(Path, "read_bytes", side_effect=PermissionError):
                with self.assertRaises(AppleRootCertificatesError):
                    load_apple_root_certificates(certificate)

    def test_invalid_certificate_is_rejected_by_official_verifier(self):
        verifier = SignedDataVerifier(
            [b"not-a-der-certificate"],
            False,
            Environment.SANDBOX,
            "MB.FuelNear",
        )

        with self.assertRaises(VerificationException) as context:
            verifier._chain_verifier.verify_chain(
                ["invalid", "invalid", "invalid"],
                perform_online_checks=False,
                effective_date=1_700_000_000,
            )

        self.assertEqual(
            context.exception.status,
            VerificationStatus.INVALID_CERTIFICATE,
        )

    def test_repository_certificates_build_sandbox_verifier(self):
        verifier = create_apple_signed_data_verifier(
            self.config(REPOSITORY_APPLE_CERTIFICATES)
        )

        self.assertEqual(verifier._environment, Environment.SANDBOX)
        self.assertEqual(len(verifier._chain_verifier.root_certificates), 3)

    def test_repository_certificates_build_production_verifier(self):
        verifier = create_apple_signed_data_verifier(
            self.config(
                REPOSITORY_APPLE_CERTIFICATES,
                environment="production",
                app_id=123456789,
            )
        )

        self.assertEqual(verifier._environment, Environment.PRODUCTION)
        self.assertEqual(len(verifier._chain_verifier.root_certificates), 3)

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
                patch.object(jws_verifier_module, "_default_verifiers", {}),
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

    def test_dual_configuration_builds_independent_production_and_sandbox_verifiers(self):
        config = AppleSubscriptionsConfig(
            bundle_id="MB.FuelNear",
            environment="production",
            app_id=123456789,
            root_certificates_path=Path("/app/certs/apple"),
            enable_online_checks=True,
            accepted_environments=("sandbox", "production"),
        )
        production = FakeVerifier()
        sandbox = FakeVerifier()
        with (
            patch.object(jws_verifier_module, "_default_verifiers", {}),
            patch.object(
                jws_verifier_module,
                "create_apple_signed_data_verifier",
                side_effect=[production, sandbox],
            ) as factory,
        ):
            candidates = jws_verifier_module._get_configured_verifier_candidates(
                config
            )

        self.assertEqual(
            [(environment, verifier) for environment, verifier in candidates],
            [("production", production), ("sandbox", sandbox)],
        )
        self.assertIsNot(production, sandbox)
        self.assertEqual(factory.call_args_list[0].args[0].environment, "production")
        self.assertEqual(factory.call_args_list[1].args[0].environment, "sandbox")

    def test_sandbox_transaction_falls_back_after_production_rejects_it(self):
        payload = self.payload(environment=Environment.SANDBOX)
        production = EnvironmentVerifier(Environment.PRODUCTION, payload)
        sandbox = EnvironmentVerifier(Environment.SANDBOX, payload)

        result = verify_apple_signed_transaction(
            "signed-jws",
            verifiers=[("production", production), ("sandbox", sandbox)],
        )

        self.assertEqual(result.environment, "Sandbox")
        self.assertEqual(production.calls, ["signed-jws"])
        self.assertEqual(sandbox.calls, ["signed-jws"])

    def test_production_transaction_is_accepted_without_sandbox_fallback(self):
        payload = self.payload(environment=Environment.PRODUCTION)
        production = EnvironmentVerifier(Environment.PRODUCTION, payload)
        sandbox = EnvironmentVerifier(Environment.SANDBOX, payload)

        result = verify_apple_signed_transaction(
            "signed-jws",
            verifiers=[("production", production), ("sandbox", sandbox)],
        )

        self.assertEqual(result.environment, "Production")
        self.assertEqual(production.calls, ["signed-jws"])
        self.assertEqual(sandbox.calls, [])

    def test_production_falls_back_after_sandbox_rejects_it(self):
        payload = self.payload(environment=Environment.PRODUCTION)
        sandbox = EnvironmentVerifier(Environment.SANDBOX, payload)
        production = EnvironmentVerifier(Environment.PRODUCTION, payload)

        result = verify_apple_signed_transaction(
            "signed-jws",
            verifiers=[("sandbox", sandbox), ("production", production)],
        )

        self.assertEqual(result.environment, "Production")
        self.assertEqual(sandbox.calls, ["signed-jws"])
        self.assertEqual(production.calls, ["signed-jws"])

    def test_testflight_transaction_is_treated_as_sandbox(self):
        payload = self.payload(environment=Environment.SANDBOX)
        result = verify_apple_signed_transaction(
            "testflight-signed-jws",
            verifiers=[
                ("production", EnvironmentVerifier(Environment.PRODUCTION, payload)),
                ("sandbox", EnvironmentVerifier(Environment.SANDBOX, payload)),
            ],
        )

        self.assertEqual(result.environment, "Sandbox")

    def test_invalid_payload_rejected_by_both_verifiers(self):
        error = VerificationException(VerificationStatus.VERIFICATION_FAILURE)
        with self.assertRaises(AppleJWSInvalidError):
            verify_apple_signed_transaction(
                "invalid-jws",
                verifiers=[
                    ("production", FakeVerifier(error=error)),
                    ("sandbox", FakeVerifier(error=error)),
                ],
            )

    def test_xcode_and_local_testing_environments_are_rejected(self):
        for environment in (Environment.XCODE, Environment.LOCAL_TESTING):
            with self.subTest(environment=environment):
                with self.assertRaises(AppleJWSInvalidError):
                    verify_apple_signed_transaction(
                        "local-jws",
                        verifiers=[
                            ("production", FakeVerifier(self.payload(environment=environment))),
                            ("sandbox", FakeVerifier(self.payload(environment=environment))),
                        ],
                    )

    def test_all_supported_products_are_accepted_in_both_environments(self):
        products = (
            "MB.FuelNear.plus.monthly",
            "MB.FuelNear.plus.sixmonths",
            "MB.FuelNear.plus.yearly",
        )
        for environment_name, environment in (
            ("sandbox", Environment.SANDBOX),
            ("production", Environment.PRODUCTION),
        ):
            for product_id in products:
                with self.subTest(environment=environment_name, product_id=product_id):
                    result = verify_apple_signed_transaction(
                        "signed-jws",
                        verifier=FakeVerifier(
                            self.payload(
                                environment=environment,
                                productId=product_id,
                            )
                        ),
                    )
                    self.assertEqual(result.product_id, product_id)
                    self.assertEqual(result.environment.lower(), environment_name)

    def test_bundle_identifier_failure_is_rejected_by_both_verifiers(self):
        error = VerificationException(VerificationStatus.INVALID_APP_IDENTIFIER)
        with self.assertRaises(AppleJWSInvalidError):
            verify_apple_signed_transaction(
                "wrong-bundle-jws",
                verifiers=[
                    ("production", FakeVerifier(error=error)),
                    ("sandbox", FakeVerifier(error=error)),
                ],
            )

    def test_retryable_error_stops_environment_fallback(self):
        sandbox = FakeVerifier(self.payload(environment=Environment.SANDBOX))
        with self.assertRaises(AppleJWSVerificationUnavailableError):
            verify_apple_signed_transaction(
                "signed-jws",
                verifiers=[
                    (
                        "production",
                        FakeVerifier(
                            error=VerificationException(
                                VerificationStatus.RETRYABLE_VERIFICATION_FAILURE
                            )
                        ),
                    ),
                    ("sandbox", sandbox),
                ],
            )
        self.assertEqual(sandbox.calls, [])

    def test_non_environment_verification_error_stops_fallback(self):
        sandbox = FakeVerifier(self.payload(environment=Environment.SANDBOX))
        with self.assertRaises(AppleJWSInvalidError):
            verify_apple_signed_transaction(
                "wrong-bundle-jws",
                verifiers=[
                    (
                        "production",
                        FakeVerifier(
                            error=VerificationException(
                                VerificationStatus.INVALID_APP_IDENTIFIER
                            )
                        ),
                    ),
                    ("sandbox", sandbox),
                ],
            )
        self.assertEqual(sandbox.calls, [])


if __name__ == "__main__":
    unittest.main()
