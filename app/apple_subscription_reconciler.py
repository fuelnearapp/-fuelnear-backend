from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import RealDictCursor

from app import apple_subscription_service
from app.db import get_connection


APPLE_SUBSCRIPTION_SOURCE = "apple_subscription"


class AppleEntitlementReconciliationError(RuntimeError):
    pass


class AppleEntitlementUserNotFound(AppleEntitlementReconciliationError):
    pass


class AppleEntitlementMissingExpiration(AppleEntitlementReconciliationError):
    pass


@dataclass(frozen=True, slots=True)
class AppleEntitlementReconciliationResult:
    is_plus: bool
    expires_at: datetime | None
    apple_active: bool
    changed: bool


def _reference_date(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise ValueError("reference_date must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def reconcile_apple_entitlement(
    user_id: int,
    reference_date: datetime | None = None,
    *,
    connection: Any | None = None,
) -> AppleEntitlementReconciliationResult:
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")

    normalized_reference_date = _reference_date(reference_date)
    conn = connection if connection is not None else get_connection()
    owns_connection = connection is None
    try:
        with (conn if owns_connection else nullcontext(conn)):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id FROM users WHERE id = %s FOR UPDATE;", (user_id,))
                if cur.fetchone() is None:
                    raise AppleEntitlementUserNotFound("User not found")

                apple_subscription = apple_subscription_service.get_active_subscription_for_user(
                    user_id,
                    normalized_reference_date,
                    connection=conn,
                )
                apple_active = apple_subscription is not None
                apple_expires_at = (
                    apple_subscription["expires_date"]
                    if apple_subscription
                    else None
                )
                if apple_active and apple_expires_at is None:
                    raise AppleEntitlementMissingExpiration(
                        "An active Apple subscription requires expires_date for entitlement reconciliation"
                    )

                cur.execute(
                    """
                    UPDATE user_subscriptions
                    SET status = 'expired',
                        updated_at = NOW()
                    WHERE user_id = %s
                      AND status = 'active'
                      AND expires_at <= %s;
                    """,
                    (user_id, normalized_reference_date),
                )
                changed = cur.rowcount > 0

                cur.execute(
                    """
                    SELECT id, source, status, starts_at, expires_at,
                           original_transaction_id, created_at, updated_at
                    FROM user_subscriptions
                    WHERE user_id = %s
                      AND status = 'active'
                      AND expires_at > %s
                    ORDER BY expires_at DESC, id DESC
                    LIMIT 1
                    FOR UPDATE;
                    """,
                    (user_id, normalized_reference_date),
                )
                active_entitlement = cur.fetchone()

                if apple_active:
                    if active_entitlement is None:
                        starts_at = min(
                            apple_subscription["purchase_date"],
                            normalized_reference_date,
                        )
                        cur.execute(
                            """
                            INSERT INTO user_subscriptions (
                                user_id,
                                source,
                                status,
                                starts_at,
                                expires_at,
                                original_transaction_id,
                                created_at,
                                updated_at
                            )
                            VALUES (%s, %s, 'active', %s, %s, %s, NOW(), NOW())
                            RETURNING expires_at;
                            """,
                            (
                                user_id,
                                APPLE_SUBSCRIPTION_SOURCE,
                                starts_at,
                                apple_expires_at,
                                apple_subscription["original_transaction_id"],
                            ),
                        )
                        entitlement_expires_at = cur.fetchone()["expires_at"]
                        changed = True
                    else:
                        entitlement_expires_at = active_entitlement["expires_at"]
                        if apple_expires_at > entitlement_expires_at:
                            cur.execute(
                                """
                                UPDATE user_subscriptions
                                SET expires_at = %s,
                                    updated_at = NOW()
                                WHERE id = %s
                                RETURNING expires_at;
                                """,
                                (apple_expires_at, active_entitlement["id"]),
                            )
                            entitlement_expires_at = cur.fetchone()["expires_at"]
                            changed = True

                    return AppleEntitlementReconciliationResult(
                        is_plus=True,
                        expires_at=entitlement_expires_at,
                        apple_active=True,
                        changed=changed,
                    )

                if active_entitlement is not None and active_entitlement["source"] == APPLE_SUBSCRIPTION_SOURCE:
                    referral_tail_expires_at = None
                    original_transaction_id = active_entitlement["original_transaction_id"]
                    if original_transaction_id:
                        cur.execute(
                            """
                            SELECT expires_date
                            FROM apple_transactions
                            WHERE original_transaction_id = %s
                            ORDER BY COALESCE(signed_date, purchase_date) DESC,
                                     purchase_date DESC,
                                     id DESC
                            LIMIT 1;
                            """,
                            (original_transaction_id,),
                        )
                        latest_apple_transaction = cur.fetchone()
                        apple_component_expires_at = (
                            latest_apple_transaction["expires_date"]
                            if latest_apple_transaction
                            else None
                        )
                        if apple_component_expires_at is not None:
                            referral_tail_start = max(
                                apple_component_expires_at,
                                normalized_reference_date,
                            )
                            if active_entitlement["expires_at"] > referral_tail_start:
                                referral_tail_expires_at = (
                                    normalized_reference_date
                                    + (
                                        active_entitlement["expires_at"]
                                        - referral_tail_start
                                    )
                                )

                    if referral_tail_expires_at is not None:
                        cur.execute(
                            """
                            UPDATE user_subscriptions
                            SET source = 'referral_reward',
                                starts_at = %s,
                                expires_at = %s,
                                original_transaction_id = NULL,
                                updated_at = NOW()
                            WHERE id = %s;
                            """,
                            (
                                normalized_reference_date,
                                referral_tail_expires_at,
                                active_entitlement["id"],
                            ),
                        )
                        changed = True
                        return AppleEntitlementReconciliationResult(
                            is_plus=True,
                            expires_at=referral_tail_expires_at,
                            apple_active=False,
                            changed=changed,
                        )

                    cur.execute(
                        """
                        UPDATE user_subscriptions
                        SET status = 'expired',
                            expires_at = LEAST(expires_at, %s),
                            updated_at = NOW()
                        WHERE id = %s;
                        """,
                        (normalized_reference_date, active_entitlement["id"]),
                    )
                    changed = True
                    active_entitlement = None

                return AppleEntitlementReconciliationResult(
                    is_plus=active_entitlement is not None,
                    expires_at=active_entitlement["expires_at"] if active_entitlement else None,
                    apple_active=False,
                    changed=changed,
                )
    finally:
        if owns_connection:
            conn.close()
