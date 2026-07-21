from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app import apple_subscription_reconciler, apple_subscriptions, guest_subscriptions


@dataclass(frozen=True, slots=True)
class ApplePurchaseProcessingResult:
    created: bool
    transaction_id: str
    original_transaction_id: str
    is_plus: bool
    expires_at: datetime | None
    changed: bool


def process_apple_transaction(
    transaction: apple_subscriptions.AppleTransaction,
) -> ApplePurchaseProcessingResult:
    normalized_transaction = apple_subscriptions.validate_apple_transaction(transaction)
    saved_transaction = apple_subscriptions.save_apple_transaction_with_managed_connection(
        normalized_transaction
    )
    if normalized_transaction.user_id is not None:
        entitlement = apple_subscription_reconciler.reconcile_apple_entitlement(
            normalized_transaction.user_id
        )
        is_plus = entitlement.is_plus
        expires_at = entitlement.expires_at
        changed = entitlement.changed
    else:
        guest_status = guest_subscriptions.get_guest_subscription_status(
            normalized_transaction.guest_id
        )
        is_plus = guest_status.is_plus
        expires_at = guest_status.expires_at
        changed = saved_transaction.changed

    return ApplePurchaseProcessingResult(
        created=saved_transaction.created,
        transaction_id=str(saved_transaction.row["transaction_id"]),
        original_transaction_id=str(saved_transaction.row["original_transaction_id"]),
        is_plus=is_plus,
        expires_at=expires_at,
        changed=changed,
    )
