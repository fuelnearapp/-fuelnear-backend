from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app import (
    apple_subscription_reconciler,
    apple_subscriptions,
    creator_attribution,
    guest_subscriptions,
)


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
    *,
    notification_type: str | None = None,
    notification_subtype: str | None = None,
) -> ApplePurchaseProcessingResult:
    normalized_transaction = apple_subscriptions.validate_apple_transaction(transaction)
    conn = apple_subscriptions.get_connection()
    try:
        with conn:
            if normalized_transaction.user_id is not None:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM users WHERE id = %s FOR UPDATE;",
                        (normalized_transaction.user_id,),
                    )
                    if cur.fetchone() is None:
                        raise apple_subscription_reconciler.AppleEntitlementUserNotFound(
                            "User not found"
                        )

            saved_transaction = apple_subscriptions.save_apple_transaction_in_transaction(
                conn,
                normalized_transaction,
            )

            if normalized_transaction.user_id is not None:
                persisted_signed_date = saved_transaction.row.get("signed_date")
                incoming_signed_date = normalized_transaction.signed_date
                context_is_current = not (
                    notification_type is not None
                    and incoming_signed_date is not None
                    and persisted_signed_date is not None
                    and incoming_signed_date < persisted_signed_date
                )
                creator_attribution.record_creator_apple_conversion(
                    conn,
                    user_id=normalized_transaction.user_id,
                    transaction_id=str(saved_transaction.row["transaction_id"]),
                    original_transaction_id=str(
                        saved_transaction.row["original_transaction_id"]
                    ),
                    purchase_date=saved_transaction.row["purchase_date"],
                    product_id=str(saved_transaction.row["product_id"]),
                    transaction_reason=saved_transaction.row.get("transaction_reason"),
                    ownership_type=saved_transaction.row.get("ownership_type"),
                    revocation_date=saved_transaction.row.get("revocation_date"),
                    notification_type=notification_type if context_is_current else None,
                    notification_subtype=(
                        notification_subtype if context_is_current else None
                    ),
                )
                entitlement = apple_subscription_reconciler.reconcile_apple_entitlement(
                    normalized_transaction.user_id,
                    connection=conn,
                )
                is_plus = entitlement.is_plus
                expires_at = entitlement.expires_at
                changed = entitlement.changed
            else:
                guest_status = guest_subscriptions.get_guest_subscription_status(
                    normalized_transaction.guest_id,
                    connection=conn,
                )
                is_plus = guest_status.is_plus
                expires_at = guest_status.expires_at
                changed = saved_transaction.changed
    finally:
        conn.close()

    return ApplePurchaseProcessingResult(
        created=saved_transaction.created,
        transaction_id=str(saved_transaction.row["transaction_id"]),
        original_transaction_id=str(saved_transaction.row["original_transaction_id"]),
        is_plus=is_plus,
        expires_at=expires_at,
        changed=changed,
    )
