from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import logging
import os
import secrets
import time
from typing import Any, Callable

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import httpx
import jwt
from psycopg2.extras import RealDictCursor

from app.db import get_connection


APPLE_TOKEN_URL = "https://appleid.apple.com/auth/token"
APPLE_REVOKE_URL = "https://appleid.apple.com/auth/revoke"
APPLE_OAUTH_AUDIENCE = "https://appleid.apple.com"
TOKEN_AAD = b"fuelnear:apple-refresh-token:v1"

logger = logging.getLogger(__name__)


class AppleAuthServiceError(RuntimeError):
    pass


class AppleAuthConfigurationError(AppleAuthServiceError):
    pass


class AppleAuthorizationCodeInvalid(AppleAuthServiceError):
    pass


class AppleTokenExchangeTemporaryError(AppleAuthServiceError):
    pass


class AppleTokenExchangeError(AppleAuthServiceError):
    pass


class AppleTokenEncryptionError(AppleAuthServiceError):
    pass


class AppleTokenRevocationTemporaryError(AppleAuthServiceError):
    pass


class AppleTokenRevocationError(AppleAuthServiceError):
    pass


@dataclass(frozen=True, slots=True)
class AppleOAuthConfig:
    client_id: str
    team_id: str
    key_id: str
    private_key: str
    token_encryption_key: bytes
    request_timeout_seconds: float
    revoke_max_attempts: int
    revoke_retry_base_seconds: float


@dataclass(frozen=True, slots=True)
class AppleTokenExchangeResult:
    refresh_token: str


@dataclass(frozen=True, slots=True)
class AppleTokenRevocationResult:
    status: str
    attempts: int


def _required_env(name: str, *fallback_names: str) -> str:
    for candidate in (name, *fallback_names):
        value = os.getenv(candidate)
        if value and value.strip():
            return value.strip()
    raise AppleAuthConfigurationError(f"{name} is not configured")


def _load_encryption_key() -> bytes:
    encoded = _required_env("APPLE_TOKEN_ENCRYPTION_KEY")
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        key = base64.urlsafe_b64decode(padded.encode("ascii"))
    except Exception as exc:
        raise AppleAuthConfigurationError(
            "APPLE_TOKEN_ENCRYPTION_KEY is invalid"
        ) from exc
    if len(key) != 32:
        raise AppleAuthConfigurationError(
            "APPLE_TOKEN_ENCRYPTION_KEY must encode exactly 32 bytes"
        )
    return key


def load_apple_oauth_config() -> AppleOAuthConfig:
    private_key = _required_env("APPLE_AUTH_PRIVATE_KEY").replace("\\n", "\n")
    return AppleOAuthConfig(
        client_id=_required_env("APPLE_AUTH_CLIENT_ID", "APPLE_CLIENT_ID", "APPLE_BUNDLE_ID"),
        team_id=_required_env("APPLE_AUTH_TEAM_ID"),
        key_id=_required_env("APPLE_AUTH_KEY_ID"),
        private_key=private_key,
        token_encryption_key=_load_encryption_key(),
        request_timeout_seconds=max(
            1.0,
            float(os.getenv("APPLE_AUTH_REQUEST_TIMEOUT_SECONDS", "10")),
        ),
        revoke_max_attempts=max(
            1,
            int(os.getenv("APPLE_REVOKE_MAX_ATTEMPTS", "3")),
        ),
        revoke_retry_base_seconds=max(
            0.0,
            float(os.getenv("APPLE_REVOKE_RETRY_BASE_SECONDS", "0.5")),
        ),
    )


def generate_apple_client_secret(
    config: AppleOAuthConfig | None = None,
    reference_date: datetime | None = None,
) -> str:
    selected = config or load_apple_oauth_config()
    now = reference_date or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    expires_at = now + timedelta(minutes=5)
    try:
        return jwt.encode(
            {
                "iss": selected.team_id,
                "iat": int(now.timestamp()),
                "exp": int(expires_at.timestamp()),
                "aud": APPLE_OAUTH_AUDIENCE,
                "sub": selected.client_id,
            },
            selected.private_key,
            algorithm="ES256",
            headers={"kid": selected.key_id},
        )
    except Exception as exc:
        raise AppleAuthConfigurationError(
            "Apple client secret could not be generated"
        ) from exc


def encrypt_apple_refresh_token(
    refresh_token: str,
    config: AppleOAuthConfig | None = None,
) -> str:
    if not isinstance(refresh_token, str) or not refresh_token.strip():
        raise AppleTokenEncryptionError("Apple refresh token is required")
    selected = config or load_apple_oauth_config()
    nonce = secrets.token_bytes(12)
    try:
        encrypted = AESGCM(selected.token_encryption_key).encrypt(
            nonce,
            refresh_token.strip().encode("utf-8"),
            TOKEN_AAD,
        )
    except Exception as exc:
        raise AppleTokenEncryptionError("Apple refresh token encryption failed") from exc
    return base64.urlsafe_b64encode(nonce + encrypted).decode("ascii")


def decrypt_apple_refresh_token(
    ciphertext: str,
    config: AppleOAuthConfig | None = None,
) -> str:
    if not isinstance(ciphertext, str) or not ciphertext.strip():
        raise AppleTokenEncryptionError("Encrypted Apple refresh token is required")
    selected = config or load_apple_oauth_config()
    try:
        payload = base64.urlsafe_b64decode(ciphertext.strip().encode("ascii"))
        if len(payload) <= 12:
            raise ValueError("Encrypted payload is too short")
        plaintext = AESGCM(selected.token_encryption_key).decrypt(
            payload[:12],
            payload[12:],
            TOKEN_AAD,
        )
        token = plaintext.decode("utf-8")
    except Exception as exc:
        raise AppleTokenEncryptionError("Apple refresh token decryption failed") from exc
    if not token:
        raise AppleTokenEncryptionError("Apple refresh token decryption failed")
    return token


def exchange_apple_authorization_code(
    authorization_code: str,
    *,
    config: AppleOAuthConfig | None = None,
    post: Callable[..., Any] = httpx.post,
) -> AppleTokenExchangeResult:
    if not isinstance(authorization_code, str) or not authorization_code.strip():
        raise AppleAuthorizationCodeInvalid("Apple authorization code is required")
    selected = config or load_apple_oauth_config()
    client_secret = generate_apple_client_secret(selected)
    try:
        response = post(
            APPLE_TOKEN_URL,
            data={
                "client_id": selected.client_id,
                "client_secret": client_secret,
                "code": authorization_code.strip(),
                "grant_type": "authorization_code",
            },
            timeout=selected.request_timeout_seconds,
        )
    except (httpx.TimeoutException, httpx.NetworkError) as exc:
        raise AppleTokenExchangeTemporaryError(
            "Apple authorization code exchange is temporarily unavailable"
        ) from exc
    except Exception as exc:
        raise AppleTokenExchangeTemporaryError(
            "Apple authorization code exchange failed"
        ) from exc

    if response.status_code == 200:
        try:
            refresh_token = response.json().get("refresh_token")
        except Exception as exc:
            raise AppleTokenExchangeError("Apple token response is invalid") from exc
        if not isinstance(refresh_token, str) or not refresh_token.strip():
            raise AppleTokenExchangeError("Apple token response has no refresh token")
        return AppleTokenExchangeResult(refresh_token=refresh_token.strip())

    if response.status_code in {429, 500, 502, 503, 504}:
        raise AppleTokenExchangeTemporaryError(
            "Apple authorization code exchange is temporarily unavailable"
        )
    error_code = None
    try:
        error_code = response.json().get("error")
    except Exception:
        pass
    if error_code in {"invalid_grant", "invalid_request"}:
        raise AppleAuthorizationCodeInvalid("Apple authorization code is invalid")
    raise AppleTokenExchangeError("Apple authorization code exchange was rejected")


def revoke_apple_refresh_token(
    refresh_token: str,
    *,
    config: AppleOAuthConfig | None = None,
    post: Callable[..., Any] = httpx.post,
    sleep: Callable[[float], None] = time.sleep,
) -> AppleTokenRevocationResult:
    if not isinstance(refresh_token, str) or not refresh_token.strip():
        raise AppleTokenRevocationError("Apple refresh token is required")
    selected = config or load_apple_oauth_config()
    client_secret = generate_apple_client_secret(selected)

    for attempt in range(1, selected.revoke_max_attempts + 1):
        try:
            response = post(
                APPLE_REVOKE_URL,
                data={
                    "client_id": selected.client_id,
                    "client_secret": client_secret,
                    "token": refresh_token.strip(),
                    "token_type_hint": "refresh_token",
                },
                timeout=selected.request_timeout_seconds,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            if attempt >= selected.revoke_max_attempts:
                raise AppleTokenRevocationTemporaryError(
                    "Apple token revocation is temporarily unavailable"
                ) from exc
        except Exception as exc:
            if attempt >= selected.revoke_max_attempts:
                raise AppleTokenRevocationTemporaryError(
                    "Apple token revocation failed"
                ) from exc
        else:
            if response.status_code == 200:
                return AppleTokenRevocationResult("revoked", attempt)
            error_code = None
            try:
                error_code = response.json().get("error")
            except Exception:
                pass
            if response.status_code == 400 and error_code in {
                "invalid_grant",
                "invalid_token",
            }:
                return AppleTokenRevocationResult("already_invalid", attempt)
            if response.status_code in {400, 401} and error_code == "invalid_client":
                raise AppleAuthConfigurationError(
                    "Apple token revocation client configuration is invalid"
                )
            if response.status_code not in {429, 500, 502, 503, 504}:
                raise AppleTokenRevocationError("Apple token revocation was rejected")
            if attempt >= selected.revoke_max_attempts:
                raise AppleTokenRevocationTemporaryError(
                    "Apple token revocation is temporarily unavailable"
                )

        sleep(selected.revoke_retry_base_seconds * attempt)

    raise AppleTokenRevocationTemporaryError(
        "Apple token revocation is temporarily unavailable"
    )


def ensure_apple_auth_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "ALTER TABLE user_auth_providers "
            "ADD COLUMN IF NOT EXISTS apple_refresh_token_ciphertext TEXT NULL;"
        )
        cur.execute(
            "ALTER TABLE user_auth_providers "
            "ADD COLUMN IF NOT EXISTS apple_token_updated_at TIMESTAMPTZ NULL;"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS apple_token_revocations (
                id BIGSERIAL PRIMARY KEY,
                token_fingerprint TEXT NOT NULL UNIQUE,
                token_ciphertext TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'failed')),
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_error_code TEXT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_apple_token_revocations_due
            ON apple_token_revocations(status, next_attempt_at);
            """
        )


def queue_apple_revocation(conn: Any, token_ciphertext: str) -> None:
    if not token_ciphertext:
        raise ValueError("Encrypted Apple token is required")
    retention_days = max(1, int(os.getenv("APPLE_REVOCATION_RETENTION_DAYS", "30")))
    fingerprint = hashlib.sha256(token_ciphertext.encode("utf-8")).hexdigest()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO apple_token_revocations (
                token_fingerprint, token_ciphertext, status, attempts,
                next_attempt_at, expires_at, created_at, updated_at
            )
            VALUES (%s, %s, 'pending', 0, NOW(), NOW() + (%s * INTERVAL '1 day'), NOW(), NOW())
            ON CONFLICT (token_fingerprint) DO UPDATE SET
                token_ciphertext = EXCLUDED.token_ciphertext,
                status = 'pending',
                next_attempt_at = NOW(),
                updated_at = NOW();
            """,
            (fingerprint, token_ciphertext, retention_days),
        )


def process_pending_apple_revocations(limit: int = 25) -> dict[str, int]:
    summary = {"processed": 0, "completed": 0, "temporary_failed": 0, "expired": 0}
    conn = get_connection()
    try:
        with conn:
            ensure_apple_auth_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    DELETE FROM apple_token_revocations
                    WHERE expires_at <= NOW()
                    RETURNING id;
                    """
                )
                summary["expired"] = len(cur.fetchall())
                cur.execute(
                    """
                    SELECT id, token_ciphertext, attempts
                    FROM apple_token_revocations
                    WHERE status = 'pending'
                      AND next_attempt_at <= NOW()
                    ORDER BY id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED;
                    """,
                    (max(1, limit),),
                )
                rows = [dict(row) for row in cur.fetchall()]

            for row in rows:
                summary["processed"] += 1
                try:
                    token = decrypt_apple_refresh_token(row["token_ciphertext"])
                    result = revoke_apple_refresh_token(token)
                except (AppleTokenRevocationTemporaryError, AppleAuthConfigurationError):
                    summary["temporary_failed"] += 1
                    delay_minutes = min(24 * 60, 2 ** min(int(row["attempts"]), 10))
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE apple_token_revocations
                            SET attempts = attempts + 1,
                                next_attempt_at = NOW() + (%s * INTERVAL '1 minute'),
                                last_error_code = 'temporary_failure',
                                updated_at = NOW()
                            WHERE id = %s;
                            """,
                            (delay_minutes, row["id"]),
                        )
                    continue
                except (AppleTokenEncryptionError, AppleTokenRevocationError):
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE apple_token_revocations
                            SET status = 'failed',
                                attempts = attempts + 1,
                                last_error_code = 'permanent_failure',
                                updated_at = NOW()
                            WHERE id = %s;
                            """,
                            (row["id"],),
                        )
                    continue

                if result.status in {"revoked", "already_invalid"}:
                    with conn.cursor() as cur:
                        cur.execute(
                            "DELETE FROM apple_token_revocations WHERE id = %s;",
                            (row["id"],),
                        )
                    summary["completed"] += 1
    finally:
        conn.close()

    logger.info(
        "apple pending revocations processed=%s completed=%s temporary_failed=%s expired=%s",
        summary["processed"],
        summary["completed"],
        summary["temporary_failed"],
        summary["expired"],
    )
    return summary
