from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hmac
from pathlib import Path
import threading
from typing import Any, Callable
from uuid import UUID

from appstoreserverlibrary.models.Environment import Environment
from appstoreserverlibrary.signed_data_verifier import (
    SignedDataVerifier,
    VerificationException,
    VerificationStatus,
)

from app.apple_config import (
    AppleSubscriptionsConfig,
    AppleSubscriptionsConfigurationError,
    load_apple_subscriptions_config,
    validate_apple_subscriptions_config,
)
from app.apple_subscriptions import SUPPORTED_APPLE_PRODUCT_IDS


class AppleJWSVerificationError(RuntimeError):
    pass


class AppleJWSConfigurationError(AppleJWSVerificationError):
    pass


class AppleRootCertificatesError(AppleJWSVerificationError):
    pass


class AppleJWSInvalidError(AppleJWSVerificationError):
    pass


class AppleJWSVerificationUnavailableError(AppleJWSVerificationError):
    pass


class AppleJWSPayloadError(AppleJWSVerificationError):
    pass


class AppleJWSUnsupportedProductError(AppleJWSPayloadError):
    pass


class AppleJWSAppAccountTokenError(AppleJWSPayloadError):
    pass


class AppleJWSAppAccountTokenMissingError(AppleJWSAppAccountTokenError):
    pass


class AppleJWSAppAccountTokenMalformedError(AppleJWSAppAccountTokenError):
    pass


class AppleJWSAppAccountTokenMismatchError(AppleJWSAppAccountTokenError):
    pass


@dataclass(frozen=True, slots=True)
class VerifiedAppleTransaction:
    product_id: str
    transaction_id: str
    original_transaction_id: str
    purchase_date: datetime
    expires_date: datetime | None
    environment: str
    ownership_type: str | None
    transaction_reason: str | None
    revocation_date: datetime | None
    revocation_reason: str | None
    app_account_token: UUID | None
    signed_date: datetime | None
    storefront: str | None
    offer_type: int | None


VerifierFactory = Callable[..., SignedDataVerifier]
_default_verifier: SignedDataVerifier | None = None
_default_verifier_config: AppleSubscriptionsConfig | None = None
_default_verifier_lock = threading.Lock()


def load_apple_root_certificates(path: Path) -> list[bytes]:
    if not isinstance(path, Path):
        raise AppleRootCertificatesError(
            "Apple root certificates path is invalid"
        )
    if not path.exists():
        raise AppleRootCertificatesError(
            "Apple root certificates path does not exist"
        )

    if path.is_file():
        certificate_paths = [path]
    elif path.is_dir():
        certificate_paths = sorted(
            candidate
            for candidate in path.iterdir()
            if candidate.is_file() and candidate.suffix.lower() in {".cer", ".der"}
        )
    else:
        raise AppleRootCertificatesError(
            "Apple root certificates path is not a file or directory"
        )

    if not certificate_paths:
        raise AppleRootCertificatesError(
            "No Apple root certificates were found"
        )

    certificates: list[bytes] = []
    try:
        for certificate_path in certificate_paths:
            certificate = certificate_path.read_bytes()
            if not certificate:
                raise AppleRootCertificatesError(
                    "An Apple root certificate is empty"
                )
            certificates.append(certificate)
    except AppleRootCertificatesError:
        raise
    except OSError as exc:
        raise AppleRootCertificatesError(
            "Apple root certificates could not be read"
        ) from exc

    return certificates


def create_apple_signed_data_verifier(
    config: AppleSubscriptionsConfig | None = None,
    *,
    verifier_factory: VerifierFactory = SignedDataVerifier,
) -> SignedDataVerifier:
    try:
        validated_config = validate_apple_subscriptions_config(
            config if config is not None else load_apple_subscriptions_config()
        )
    except AppleSubscriptionsConfigurationError as exc:
        raise AppleJWSConfigurationError(
            "Apple subscriptions configuration is invalid"
        ) from exc

    environment = {
        "sandbox": Environment.SANDBOX,
        "production": Environment.PRODUCTION,
    }[validated_config.environment]
    root_certificates = load_apple_root_certificates(
        validated_config.root_certificates_path
    )

    try:
        return verifier_factory(
            root_certificates,
            validated_config.enable_online_checks,
            environment,
            validated_config.bundle_id,
            validated_config.app_id,
        )
    except Exception as exc:
        raise AppleJWSConfigurationError(
            "Apple signed data verifier could not be initialized"
        ) from exc


def _get_default_apple_signed_data_verifier(
    config: AppleSubscriptionsConfig,
) -> SignedDataVerifier:
    global _default_verifier, _default_verifier_config
    if _default_verifier is None or _default_verifier_config != config:
        with _default_verifier_lock:
            if _default_verifier is None or _default_verifier_config != config:
                _default_verifier = create_apple_signed_data_verifier(config)
                _default_verifier_config = config
    return _default_verifier


def _required_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AppleJWSPayloadError(f"Apple transaction is missing {field_name}")
    return value.strip()


def _optional_enum_value(value: Any) -> Any:
    if value is None:
        return None
    return getattr(value, "value", value)


def _optional_text_value(value: Any) -> str | None:
    normalized = _optional_enum_value(value)
    if normalized is None:
        return None
    return str(normalized)


def _optional_integer_value(value: Any, field_name: str) -> int | None:
    normalized = _optional_enum_value(value)
    if normalized is None:
        return None
    if isinstance(normalized, bool) or not isinstance(normalized, int):
        raise AppleJWSPayloadError(
            f"Apple transaction has invalid {field_name}"
        )
    return normalized


def _timestamp_milliseconds(value: Any, field_name: str, *, required: bool) -> datetime | None:
    if value is None:
        if required:
            raise AppleJWSPayloadError(
                f"Apple transaction is missing {field_name}"
            )
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AppleJWSPayloadError(
            f"Apple transaction has invalid {field_name}"
        )
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise AppleJWSPayloadError(
            f"Apple transaction has invalid {field_name}"
        ) from exc


def _parse_app_account_token(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str) or not value.strip():
        raise AppleJWSAppAccountTokenMalformedError(
            "Apple transaction appAccountToken is malformed"
        )
    try:
        return UUID(value.strip())
    except ValueError as exc:
        raise AppleJWSAppAccountTokenMalformedError(
            "Apple transaction appAccountToken is malformed"
        ) from exc


def _expected_app_account_token(value: UUID | str | None) -> UUID | None:
    if value is None:
        return None
    try:
        return value if isinstance(value, UUID) else UUID(str(value).strip())
    except (AttributeError, ValueError) as exc:
        raise AppleJWSAppAccountTokenMalformedError(
            "Expected appAccountToken is malformed"
        ) from exc


def verify_apple_signed_transaction(
    signed_transaction: str,
    expected_app_account_token: UUID | str | None = None,
    *,
    config: AppleSubscriptionsConfig | None = None,
    verifier: SignedDataVerifier | None = None,
) -> VerifiedAppleTransaction:
    if not isinstance(signed_transaction, str) or not signed_transaction.strip():
        raise AppleJWSInvalidError("Apple signed transaction is required")

    if verifier is not None:
        selected_verifier = verifier
    else:
        try:
            validated_config = validate_apple_subscriptions_config(
                config if config is not None else load_apple_subscriptions_config()
            )
        except AppleSubscriptionsConfigurationError as exc:
            raise AppleJWSConfigurationError(
                "Apple subscriptions configuration is invalid"
            ) from exc
        selected_verifier = _get_default_apple_signed_data_verifier(
            validated_config
        )
    try:
        payload = selected_verifier.verify_and_decode_signed_transaction(
            signed_transaction.strip()
        )
    except VerificationException as exc:
        if exc.status == VerificationStatus.RETRYABLE_VERIFICATION_FAILURE:
            raise AppleJWSVerificationUnavailableError(
                "Apple transaction verification is temporarily unavailable"
            ) from exc
        raise AppleJWSInvalidError("Apple signed transaction is invalid") from exc
    except Exception as exc:
        raise AppleJWSInvalidError("Apple signed transaction could not be verified") from exc

    product_id = _required_text(payload.productId, "productId")
    if product_id not in SUPPORTED_APPLE_PRODUCT_IDS:
        raise AppleJWSUnsupportedProductError(
            "Apple transaction product is not supported"
        )

    transaction_id = _required_text(payload.transactionId, "transactionId")
    original_transaction_id = _required_text(
        payload.originalTransactionId,
        "originalTransactionId",
    )
    payload_app_account_token = _parse_app_account_token(payload.appAccountToken)
    expected_token = _expected_app_account_token(expected_app_account_token)
    if expected_token is not None:
        if payload_app_account_token is None:
            raise AppleJWSAppAccountTokenMissingError(
                "Apple transaction appAccountToken is required"
            )
        if not hmac.compare_digest(
            payload_app_account_token.bytes,
            expected_token.bytes,
        ):
            raise AppleJWSAppAccountTokenMismatchError(
                "Apple transaction does not belong to the expected account"
            )

    environment = _required_text(
        _optional_text_value(payload.environment),
        "environment",
    )

    return VerifiedAppleTransaction(
        product_id=product_id,
        transaction_id=transaction_id,
        original_transaction_id=original_transaction_id,
        purchase_date=_timestamp_milliseconds(
            payload.purchaseDate,
            "purchaseDate",
            required=True,
        ),
        expires_date=_timestamp_milliseconds(
            payload.expiresDate,
            "expiresDate",
            required=False,
        ),
        environment=environment,
        ownership_type=_optional_text_value(payload.inAppOwnershipType),
        transaction_reason=_optional_text_value(payload.transactionReason),
        revocation_date=_timestamp_milliseconds(
            payload.revocationDate,
            "revocationDate",
            required=False,
        ),
        revocation_reason=_optional_text_value(payload.revocationReason),
        app_account_token=payload_app_account_token,
        signed_date=_timestamp_milliseconds(
            payload.signedDate,
            "signedDate",
            required=False,
        ),
        storefront=_optional_text_value(payload.storefront),
        offer_type=_optional_integer_value(payload.offerType, "offerType"),
    )
