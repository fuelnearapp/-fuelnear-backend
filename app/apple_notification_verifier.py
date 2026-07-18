from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hmac
from pathlib import Path
import threading
from typing import Any, Callable
from uuid import UUID

from appstoreserverlibrary.models.Environment import Environment
from appstoreserverlibrary.models.JWSRenewalInfoDecodedPayload import (
    JWSRenewalInfoDecodedPayload,
)
from appstoreserverlibrary.models.JWSTransactionDecodedPayload import (
    JWSTransactionDecodedPayload,
)
from appstoreserverlibrary.models.ResponseBodyV2DecodedPayload import (
    ResponseBodyV2DecodedPayload,
)
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


class AppleNotificationVerificationError(RuntimeError):
    pass


class AppleNotificationConfigurationError(AppleNotificationVerificationError):
    pass


class AppleNotificationCertificatesError(AppleNotificationVerificationError):
    pass


class AppleNotificationInvalidError(AppleNotificationVerificationError):
    pass


class AppleNotificationVerificationUnavailableError(AppleNotificationVerificationError):
    pass


class AppleNotificationPayloadError(AppleNotificationVerificationError):
    pass


class AppleNotificationTypeMissingError(AppleNotificationPayloadError):
    pass


class AppleNotificationTransactionDataError(AppleNotificationPayloadError):
    pass


class AppleNotificationRenewalDataError(AppleNotificationPayloadError):
    pass


@dataclass(frozen=True, slots=True)
class NormalizedAppleNotificationTransaction:
    product_id: str
    transaction_id: str
    original_transaction_id: str
    purchase_date: datetime
    expires_date: datetime | None
    revocation_date: datetime | None
    revocation_reason: int | None
    app_account_token: UUID | None


@dataclass(frozen=True, slots=True)
class NormalizedAppleNotificationRenewal:
    auto_renew_status: int
    expiration_intent: int | None
    renewal_product_id: str


@dataclass(frozen=True, slots=True)
class VerifiedAppStoreNotification:
    notification_uuid: str
    notification_type: str
    subtype: str | None
    signed_date: datetime
    bundle_id: str
    environment: str
    app_apple_id: int | None
    has_transaction_info: bool
    has_renewal_info: bool
    transaction: NormalizedAppleNotificationTransaction | None
    renewal: NormalizedAppleNotificationRenewal | None


VerifierFactory = Callable[..., SignedDataVerifier]
_default_verifier: SignedDataVerifier | None = None
_default_verifier_config: AppleSubscriptionsConfig | None = None
_default_verifier_lock = threading.Lock()


def _load_root_certificates(path: Path | None) -> list[bytes]:
    if path is None or not isinstance(path, Path):
        raise AppleNotificationCertificatesError(
            "Apple root certificates path is invalid"
        )
    if not path.exists():
        raise AppleNotificationCertificatesError(
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
        raise AppleNotificationCertificatesError(
            "Apple root certificates path is not a file or directory"
        )

    if not certificate_paths:
        raise AppleNotificationCertificatesError(
            "No Apple root certificates were found"
        )

    try:
        certificates = [certificate_path.read_bytes() for certificate_path in certificate_paths]
    except OSError as exc:
        raise AppleNotificationCertificatesError(
            "Apple root certificates could not be read"
        ) from exc

    if any(not certificate for certificate in certificates):
        raise AppleNotificationCertificatesError(
            "An Apple root certificate is empty"
        )
    return certificates


def create_app_store_notification_verifier(
    config: AppleSubscriptionsConfig | None = None,
    *,
    verifier_factory: VerifierFactory = SignedDataVerifier,
) -> SignedDataVerifier:
    try:
        validated_config = validate_apple_subscriptions_config(
            config if config is not None else load_apple_subscriptions_config()
        )
    except AppleSubscriptionsConfigurationError as exc:
        raise AppleNotificationConfigurationError(
            "Apple subscriptions configuration is invalid"
        ) from exc

    environment = {
        "sandbox": Environment.SANDBOX,
        "production": Environment.PRODUCTION,
    }[validated_config.environment]
    certificates = _load_root_certificates(
        validated_config.root_certificates_path
    )

    try:
        return verifier_factory(
            certificates,
            validated_config.enable_online_checks,
            environment,
            validated_config.bundle_id,
            validated_config.app_id,
        )
    except Exception as exc:
        raise AppleNotificationConfigurationError(
            "Apple notification verifier could not be initialized"
        ) from exc


def _get_default_app_store_notification_verifier(
    config: AppleSubscriptionsConfig,
) -> SignedDataVerifier:
    global _default_verifier, _default_verifier_config
    if _default_verifier is None or _default_verifier_config != config:
        with _default_verifier_lock:
            if _default_verifier is None or _default_verifier_config != config:
                _default_verifier = create_app_store_notification_verifier(config)
                _default_verifier_config = config
    return _default_verifier


def _is_retryable_verification_error(exc: VerificationException) -> bool:
    return exc.status == VerificationStatus.RETRYABLE_VERIFICATION_FAILURE


def _required_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AppleNotificationPayloadError(
            f"Apple notification is missing {field_name}"
        )
    return value.strip()


def _enum_value(value: Any) -> Any:
    if value is None:
        return None
    return getattr(value, "value", value)


def _enum_or_raw(value: Any, raw_value: Any) -> Any:
    normalized = _enum_value(value)
    return normalized if normalized is not None else raw_value


def _milliseconds_datetime(
    value: Any,
    field_name: str,
    *,
    required: bool,
    error_type: type[AppleNotificationPayloadError] = AppleNotificationPayloadError,
) -> datetime | None:
    if value is None:
        if required:
            raise error_type(f"Apple notification is missing {field_name}")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise error_type(f"Apple notification has invalid {field_name}")
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise error_type(f"Apple notification has invalid {field_name}") from exc


def _notification_metadata(
    payload: ResponseBodyV2DecodedPayload,
) -> tuple[str | None, int | None, Any]:
    if payload.data is not None:
        return (
            payload.data.bundleId,
            payload.data.appAppleId,
            payload.data.environment,
        )
    if payload.summary is not None:
        return (
            payload.summary.bundleId,
            payload.summary.appAppleId,
            payload.summary.environment,
        )
    if payload.externalPurchaseToken is not None:
        external_purchase_id = payload.externalPurchaseToken.externalPurchaseId
        environment = (
            Environment.SANDBOX
            if isinstance(external_purchase_id, str)
            and external_purchase_id.startswith("SANDBOX")
            else Environment.PRODUCTION
        )
        return (
            payload.externalPurchaseToken.bundleId,
            payload.externalPurchaseToken.appAppleId,
            environment,
        )
    if payload.appData is not None:
        return (
            payload.appData.bundleId,
            payload.appData.appAppleId,
            payload.appData.environment,
        )
    return None, None, None


def _validate_notification_metadata(
    payload: ResponseBodyV2DecodedPayload,
    config: AppleSubscriptionsConfig,
) -> tuple[str, int | None, str]:
    bundle_id_value, app_apple_id, environment_value = _notification_metadata(payload)
    bundle_id = _required_text(bundle_id_value, "bundleId")
    environment = _required_text(_enum_value(environment_value), "environment")
    expected_environment = {
        "sandbox": Environment.SANDBOX.value,
        "production": Environment.PRODUCTION.value,
    }[config.environment]

    if not hmac.compare_digest(bundle_id, config.bundle_id):
        raise AppleNotificationPayloadError(
            "Apple notification bundle identifier is invalid"
        )
    if not hmac.compare_digest(environment, expected_environment):
        raise AppleNotificationPayloadError(
            "Apple notification environment is invalid"
        )
    if config.environment == "production" and app_apple_id != config.app_id:
        raise AppleNotificationPayloadError(
            "Apple notification app identifier is invalid"
        )
    return bundle_id, app_apple_id, environment


def _parse_app_account_token(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return value if isinstance(value, UUID) else UUID(str(value).strip())
    except (AttributeError, ValueError) as exc:
        raise AppleNotificationTransactionDataError(
            "Apple notification transaction appAccountToken is invalid"
        ) from exc


def _normalize_transaction(
    payload: JWSTransactionDecodedPayload,
) -> NormalizedAppleNotificationTransaction:
    try:
        product_id = _required_text(payload.productId, "transaction productId")
        transaction_id = _required_text(payload.transactionId, "transactionId")
        original_transaction_id = _required_text(
            payload.originalTransactionId,
            "originalTransactionId",
        )
    except AppleNotificationPayloadError as exc:
        raise AppleNotificationTransactionDataError(str(exc)) from exc

    revocation_reason = _enum_or_raw(
        payload.revocationReason,
        getattr(payload, "rawRevocationReason", None),
    )
    if revocation_reason is not None and (
        isinstance(revocation_reason, bool) or not isinstance(revocation_reason, int)
    ):
        raise AppleNotificationTransactionDataError(
            "Apple notification transaction revocationReason is invalid"
        )

    return NormalizedAppleNotificationTransaction(
        product_id=product_id,
        transaction_id=transaction_id,
        original_transaction_id=original_transaction_id,
        purchase_date=_milliseconds_datetime(
            payload.purchaseDate,
            "purchaseDate",
            required=True,
            error_type=AppleNotificationTransactionDataError,
        ),
        expires_date=_milliseconds_datetime(
            payload.expiresDate,
            "expiresDate",
            required=False,
            error_type=AppleNotificationTransactionDataError,
        ),
        revocation_date=_milliseconds_datetime(
            payload.revocationDate,
            "revocationDate",
            required=False,
            error_type=AppleNotificationTransactionDataError,
        ),
        revocation_reason=revocation_reason,
        app_account_token=_parse_app_account_token(payload.appAccountToken),
    )


def _normalize_renewal(
    payload: JWSRenewalInfoDecodedPayload,
) -> NormalizedAppleNotificationRenewal:
    auto_renew_status = _enum_or_raw(
        payload.autoRenewStatus,
        getattr(payload, "rawAutoRenewStatus", None),
    )
    expiration_intent = _enum_or_raw(
        payload.expirationIntent,
        getattr(payload, "rawExpirationIntent", None),
    )
    renewal_product_id = payload.autoRenewProductId or payload.productId

    if isinstance(auto_renew_status, bool) or not isinstance(auto_renew_status, int):
        raise AppleNotificationRenewalDataError(
            "Apple notification renewal autoRenewStatus is invalid"
        )
    if expiration_intent is not None and (
        isinstance(expiration_intent, bool) or not isinstance(expiration_intent, int)
    ):
        raise AppleNotificationRenewalDataError(
            "Apple notification renewal expirationIntent is invalid"
        )
    if not isinstance(renewal_product_id, str) or not renewal_product_id.strip():
        raise AppleNotificationRenewalDataError(
            "Apple notification renewal productId is missing"
        )

    return NormalizedAppleNotificationRenewal(
        auto_renew_status=auto_renew_status,
        expiration_intent=expiration_intent,
        renewal_product_id=renewal_product_id.strip(),
    )


def verify_app_store_notification(
    signed_payload: str,
    *,
    config: AppleSubscriptionsConfig | None = None,
    verifier: SignedDataVerifier | None = None,
) -> VerifiedAppStoreNotification:
    if not isinstance(signed_payload, str) or not signed_payload.strip():
        raise AppleNotificationInvalidError(
            "Apple signed notification is required"
        )

    try:
        validated_config = validate_apple_subscriptions_config(
            config if config is not None else load_apple_subscriptions_config()
        )
    except AppleSubscriptionsConfigurationError as exc:
        raise AppleNotificationConfigurationError(
            "Apple subscriptions configuration is invalid"
        ) from exc

    selected_verifier = (
        verifier
        if verifier is not None
        else _get_default_app_store_notification_verifier(validated_config)
    )
    try:
        payload = selected_verifier.verify_and_decode_notification(
            signed_payload.strip()
        )
    except VerificationException as exc:
        if _is_retryable_verification_error(exc):
            raise AppleNotificationVerificationUnavailableError(
                "Apple notification verification is temporarily unavailable"
            ) from exc
        raise AppleNotificationInvalidError(
            "Apple signed notification is invalid"
        ) from exc
    except Exception as exc:
        raise AppleNotificationInvalidError(
            "Apple signed notification could not be verified"
        ) from exc

    notification_type = _enum_or_raw(
        payload.notificationType,
        getattr(payload, "rawNotificationType", None),
    )
    if not isinstance(notification_type, str) or not notification_type.strip():
        raise AppleNotificationTypeMissingError(
            "Apple notificationType is required"
        )
    subtype_value = _enum_or_raw(
        payload.subtype,
        getattr(payload, "rawSubtype", None),
    )
    subtype = (
        subtype_value.strip()
        if isinstance(subtype_value, str) and subtype_value.strip()
        else None
    )

    notification_uuid = _required_text(
        payload.notificationUUID,
        "notificationUUID",
    )
    signed_date = _milliseconds_datetime(
        payload.signedDate,
        "signedDate",
        required=True,
    )
    bundle_id, app_apple_id, environment = _validate_notification_metadata(
        payload,
        validated_config,
    )

    signed_transaction_info = (
        payload.data.signedTransactionInfo
        if payload.data is not None
        else None
    )
    signed_renewal_info = (
        payload.data.signedRenewalInfo
        if payload.data is not None
        else None
    )

    transaction = None
    if signed_transaction_info:
        try:
            transaction_payload = selected_verifier.verify_and_decode_signed_transaction(
                signed_transaction_info
            )
        except VerificationException as exc:
            if _is_retryable_verification_error(exc):
                raise AppleNotificationVerificationUnavailableError(
                    "Apple transaction verification is temporarily unavailable"
                ) from exc
            raise AppleNotificationTransactionDataError(
                "Apple notification transaction data is invalid"
            ) from exc
        except Exception as exc:
            raise AppleNotificationTransactionDataError(
                "Apple notification transaction data is invalid"
            ) from exc
        transaction = _normalize_transaction(transaction_payload)

    renewal = None
    if signed_renewal_info:
        try:
            renewal_payload = selected_verifier.verify_and_decode_renewal_info(
                signed_renewal_info
            )
        except VerificationException as exc:
            if _is_retryable_verification_error(exc):
                raise AppleNotificationVerificationUnavailableError(
                    "Apple renewal verification is temporarily unavailable"
                ) from exc
            raise AppleNotificationRenewalDataError(
                "Apple notification renewal data is invalid"
            ) from exc
        except Exception as exc:
            raise AppleNotificationRenewalDataError(
                "Apple notification renewal data is invalid"
            ) from exc
        renewal = _normalize_renewal(renewal_payload)

    return VerifiedAppStoreNotification(
        notification_uuid=notification_uuid,
        notification_type=notification_type.strip(),
        subtype=subtype,
        signed_date=signed_date,
        bundle_id=bundle_id,
        environment=environment,
        app_apple_id=app_apple_id,
        has_transaction_info=transaction is not None,
        has_renewal_info=renewal is not None,
        transaction=transaction,
        renewal=renewal,
    )
