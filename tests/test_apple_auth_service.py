from __future__ import annotations

from datetime import datetime, timezone
import unittest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import httpx
import jwt

from app.apple_auth_service import (
    APPLE_OAUTH_AUDIENCE,
    AppleAuthorizationCodeInvalid,
    AppleOAuthConfig,
    AppleTokenRevocationTemporaryError,
    decrypt_apple_refresh_token,
    encrypt_apple_refresh_token,
    exchange_apple_authorization_code,
    generate_apple_client_secret,
    revoke_apple_refresh_token,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class AppleAuthServiceTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.private_key = ec.generate_private_key(ec.SECP256R1())
        cls.private_key_pem = cls.private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode("ascii")
        cls.public_key = cls.private_key.public_key()

    def config(self, *, attempts: int = 3) -> AppleOAuthConfig:
        return AppleOAuthConfig(
            client_id="MB.FuelNear",
            team_id="TEAM123456",
            key_id="KEY1234567",
            private_key=self.private_key_pem,
            token_encryption_key=b"x" * 32,
            request_timeout_seconds=2.0,
            revoke_max_attempts=attempts,
            revoke_retry_base_seconds=0.0,
        )

    def test_client_secret_has_expected_claims_and_headers(self):
        reference = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
        secret = generate_apple_client_secret(self.config(), reference)
        claims = jwt.decode(
            secret,
            self.public_key,
            algorithms=["ES256"],
            audience=APPLE_OAUTH_AUDIENCE,
            options={"verify_exp": False},
        )
        headers = jwt.get_unverified_header(secret)

        self.assertEqual(claims["iss"], "TEAM123456")
        self.assertEqual(claims["sub"], "MB.FuelNear")
        self.assertEqual(claims["exp"] - claims["iat"], 300)
        self.assertEqual(headers["kid"], "KEY1234567")
        self.assertEqual(headers["alg"], "ES256")

    def test_refresh_token_encryption_round_trip(self):
        encrypted = encrypt_apple_refresh_token("refresh-token", self.config())
        self.assertNotIn("refresh-token", encrypted)
        self.assertEqual(
            decrypt_apple_refresh_token(encrypted, self.config()),
            "refresh-token",
        )

    def test_authorization_code_exchange_returns_refresh_token(self):
        calls = []

        def post(url, **kwargs):
            calls.append((url, kwargs))
            return FakeResponse(200, {"refresh_token": "apple-refresh-token"})

        result = exchange_apple_authorization_code(
            "authorization-code",
            config=self.config(),
            post=post,
        )

        self.assertEqual(result.refresh_token, "apple-refresh-token")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1]["data"]["grant_type"], "authorization_code")

    def test_reused_authorization_code_is_rejected_cleanly(self):
        with self.assertRaises(AppleAuthorizationCodeInvalid):
            exchange_apple_authorization_code(
                "used-code",
                config=self.config(),
                post=lambda *_args, **_kwargs: FakeResponse(
                    400,
                    {"error": "invalid_grant"},
                ),
            )

    def test_revoke_success(self):
        result = revoke_apple_refresh_token(
            "refresh-token",
            config=self.config(),
            post=lambda *_args, **_kwargs: FakeResponse(200),
            sleep=lambda _seconds: None,
        )
        self.assertEqual(result.status, "revoked")
        self.assertEqual(result.attempts, 1)

    def test_already_invalid_token_is_idempotent_success(self):
        result = revoke_apple_refresh_token(
            "refresh-token",
            config=self.config(),
            post=lambda *_args, **_kwargs: FakeResponse(
                400,
                {"error": "invalid_token"},
            ),
            sleep=lambda _seconds: None,
        )
        self.assertEqual(result.status, "already_invalid")

    def test_apple_5xx_is_retried_then_temporary_failure(self):
        calls = []

        def post(*_args, **_kwargs):
            calls.append(True)
            return FakeResponse(503)

        with self.assertRaises(AppleTokenRevocationTemporaryError):
            revoke_apple_refresh_token(
                "refresh-token",
                config=self.config(attempts=3),
                post=post,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(len(calls), 3)

    def test_timeout_is_retried_then_temporary_failure(self):
        calls = []

        def post(*_args, **_kwargs):
            calls.append(True)
            raise httpx.ReadTimeout("timeout")

        with self.assertRaises(AppleTokenRevocationTemporaryError):
            revoke_apple_refresh_token(
                "refresh-token",
                config=self.config(attempts=2),
                post=post,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
