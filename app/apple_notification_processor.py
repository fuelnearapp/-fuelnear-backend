from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Final

from app import (
    apple_purchase_processor,
    apple_subscription_reconciler,
    apple_subscription_service,
    apple_subscriptions,
)
from app.apple_notification_verifier import VerifiedAppStoreNotification


PROCESSABLE_NOTIFICATION_TYPES: Final[frozenset[str]] = frozenset(
    {
        "SUBSCRIBED",
        "DID_RENEW",
        "DID_FAIL_TO_RENEW",
        "EXPIRED",
        "GRACE_PERIOD_EXPIRED",
        "REFUND",
        "REFUND_REVERSED",
        "REVOKE",
        "OFFER_REDEEMED",
        "PRICE_INCREASE",
        "RENEWAL_EXTENDED",
    }
)

IGNORED_NOTIFICATION_TYPES: Final[frozenset[str]] = frozenset(
    {
        "TEST",
        "REFUND_DECLINED",
        "RENEWAL_EXTENSION",
        "CONSUMPTION_REQUEST",
        "EXTERNAL_PURCHASE_TOKEN",
        "ONE_TIME_CHARGE",
    }
)


class AppleNotificationProcessorError(RuntimeError):
    pass


class AppleNotificationOwnershipNotFound(AppleNotificationProcessorError):
    pass


class AppleNotificationTransactionInsufficient(AppleNotificationProcessorError):
    pass


class AppleNotificationOwnershipConflict(AppleNotificationProcessorError):
    pass


class AppleNotificationRepositoryError(AppleNotificationProcessorError):
    pass


class AppleNotificationReconciliationError(AppleNotificationProcessorError):
    pass


class AppleNotificationTypeNotProcessable(AppleNotificationProcessorError):
    pass


@dataclass(frozen=True, slots=True)
class AppleNotificationProcessingResult:
    notification_uuid: str
    notification_type: str
    subtype: str | None
    handled: bool
    action: str
    user_id: int | None = None
    transaction_id: str | None = None
    original_transaction_id: str | None = None
    created: bool | None = None
    is_plus: bool | None = None
    expires_at: datetime | None = None
    changed: bool | None = None
    reason: str | None = None


def _lookup_owners(notification: VerifiedAppStoreNotification) -> set[int]:
    transaction = notification.transaction
    if transaction is None:
        raise AppleNotificationTransactionInsufficient(
            "Apple notification transaction data is required"
        )

    owners: set[int] = set()
    existing_transaction = apple_subscription_service.get_transaction(
        transaction.transaction_id
    )
    if existing_transaction is not None:
        owners.add(int(existing_transaction["user_id"]))

    existing_original = apple_subscription_service.get_latest_transaction(
        transaction.original_transaction_id
    )
    if existing_original is not None:
        owners.add(int(existing_original["user_id"]))

    if transaction.app_account_token is not None:
        token_owner = apple_subscription_service.get_user_id_by_app_account_token(
            transaction.app_account_token
        )
        if token_owner is not None:
            owners.add(token_owner)

    return owners


def _resolve_user_id(notification: VerifiedAppStoreNotification) -> int:
    try:
        owners = _lookup_owners(notification)
    except AppleNotificationTransactionInsufficient:
        raise
    except (TypeError, ValueError, KeyError) as exc:
        raise AppleNotificationTransactionInsufficient(
            "Apple notification ownership data is invalid"
        ) from exc
    except Exception as exc:
        raise AppleNotificationRepositoryError(
            "Apple notification ownership lookup failed"
        ) from exc

    if not owners:
        raise AppleNotificationOwnershipNotFound(
            "Apple notification ownership could not be resolved"
        )
    if len(owners) != 1:
        raise AppleNotificationOwnershipConflict(
            "Apple notification ownership is inconsistent"
        )
    return next(iter(owners))


def _to_apple_transaction(
    notification: VerifiedAppStoreNotification,
    user_id: int,
) -> apple_subscriptions.AppleTransaction:
    transaction = notification.transaction
    if transaction is None:
        raise AppleNotificationTransactionInsufficient(
            "Apple notification transaction data is required"
        )

    try:
        normalized = apple_subscriptions.AppleTransaction(
            user_id=user_id,
            product_id=transaction.product_id,
            transaction_id=transaction.transaction_id,
            original_transaction_id=transaction.original_transaction_id,
            purchase_date=transaction.purchase_date,
            expires_date=transaction.expires_date,
            environment=notification.environment,
            revocation_date=transaction.revocation_date,
            revocation_reason=(
                str(transaction.revocation_reason)
                if transaction.revocation_reason is not None
                else None
            ),
            app_account_token=transaction.app_account_token,
            signed_date=notification.signed_date,
        )
        return apple_subscriptions.validate_apple_transaction(normalized)
    except apple_subscriptions.AppleTransactionValidationError as exc:
        raise AppleNotificationTransactionInsufficient(
            "Apple notification transaction data is invalid"
        ) from exc


def _no_action_result(
    notification: VerifiedAppStoreNotification,
    *,
    handled: bool,
    action: str,
    reason: str,
) -> AppleNotificationProcessingResult:
    return AppleNotificationProcessingResult(
        notification_uuid=notification.notification_uuid,
        notification_type=notification.notification_type,
        subtype=notification.subtype,
        handled=handled,
        action=action,
        reason=reason,
    )


def process_app_store_notification(
    notification: VerifiedAppStoreNotification,
) -> AppleNotificationProcessingResult:
    if not isinstance(notification, VerifiedAppStoreNotification):
        raise AppleNotificationTypeNotProcessable(
            "A verified Apple notification is required"
        )

    notification_type = notification.notification_type.strip().upper()
    if notification_type in IGNORED_NOTIFICATION_TYPES:
        return _no_action_result(
            notification,
            handled=True,
            action="ignored_notification",
            reason="notification_type_ignored",
        )
    if notification_type not in PROCESSABLE_NOTIFICATION_TYPES:
        return _no_action_result(
            notification,
            handled=False,
            action="unsupported_notification",
            reason="notification_type_unsupported",
        )
    if notification.transaction is None:
        return _no_action_result(
            notification,
            handled=True,
            action="no_transaction_info",
            reason="transaction_info_missing",
        )

    user_id = _resolve_user_id(notification)
    transaction = _to_apple_transaction(notification, user_id)
    try:
        processing_result = apple_purchase_processor.process_apple_transaction(
            transaction
        )
    except (
        apple_subscriptions.AppleOriginalTransactionOwnershipConflict,
        apple_subscriptions.AppleTransactionIdentityConflict,
    ) as exc:
        raise AppleNotificationOwnershipConflict(
            "Apple notification transaction ownership conflicts with existing data"
        ) from exc
    except apple_subscriptions.AppleTransactionValidationError as exc:
        raise AppleNotificationTransactionInsufficient(
            "Apple notification transaction data is invalid"
        ) from exc
    except apple_subscriptions.AppleSubscriptionRepositoryError as exc:
        raise AppleNotificationRepositoryError(
            "Apple notification transaction could not be persisted"
        ) from exc
    except apple_subscription_reconciler.AppleEntitlementReconciliationError as exc:
        raise AppleNotificationReconciliationError(
            "Apple notification entitlement could not be reconciled"
        ) from exc

    return AppleNotificationProcessingResult(
        notification_uuid=notification.notification_uuid,
        notification_type=notification.notification_type,
        subtype=notification.subtype,
        handled=True,
        action=(
            "transaction_processed"
            if processing_result.created
            else "entitlement_reconciled"
        ),
        user_id=user_id,
        transaction_id=processing_result.transaction_id,
        original_transaction_id=processing_result.original_transaction_id,
        created=processing_result.created,
        is_plus=processing_result.is_plus,
        expires_at=processing_result.expires_at,
        changed=processing_result.changed,
    )
