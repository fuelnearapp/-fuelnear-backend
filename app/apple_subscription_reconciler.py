from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import RealDictCursor

from app import apple_subscription_service, plus_entitlements
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
                    apple_subscription_service.effective_subscription_expiration(
                        apple_subscription
                    )
                    if apple_subscription
                    else None
                )
                if apple_active and apple_expires_at is None:
                    raise AppleEntitlementMissingExpiration(
                        "An active Apple subscription requires expires_date for entitlement reconciliation"
                    )
                effective_apple_subscription = (
                    {
                        **dict(apple_subscription),
                        "expires_date": apple_expires_at,
                    }
                    if apple_subscription
                    else None
                )
                components = plus_entitlements.reconcile_user_plus_entitlement(
                    conn,
                    user_id,
                    normalized_reference_date,
                    apple_subscription=effective_apple_subscription,
                    user_locked=True,
                )
                return AppleEntitlementReconciliationResult(
                    is_plus=components.is_plus,
                    expires_at=components.expires_at,
                    apple_active=components.apple_active,
                    changed=components.changed,
                )
    finally:
        if owns_connection:
            conn.close()
