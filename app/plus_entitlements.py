from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from psycopg2.extras import RealDictCursor


APPLE_SOURCE = "apple_subscription"
REFERRAL_SOURCE = "referral_reward"
COMBINED_SOURCE = "combined"


@dataclass(frozen=True, slots=True)
class PlusEntitlementComponents:
    is_plus: bool
    expires_at: datetime | None
    apple_active: bool
    apple_expires_at: datetime | None
    referral_active: bool
    referral_expires_at: datetime | None
    source: str | None
    changed: bool
    subscription: dict[str, Any] | None


def normalize_reference_date(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise ValueError("reference_date must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def ensure_plus_entitlement_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "ALTER TABLE user_subscriptions "
            "ADD COLUMN IF NOT EXISTS apple_expires_at TIMESTAMPTZ NULL;"
        )
        cur.execute(
            "ALTER TABLE user_subscriptions "
            "ADD COLUMN IF NOT EXISTS referral_expires_at TIMESTAMPTZ NULL;"
        )


def _load_active_subscription(conn: Any, user_id: int) -> dict[str, Any] | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, user_id, source, status, starts_at, expires_at,
                   original_transaction_id, apple_expires_at,
                   referral_expires_at, created_at, updated_at
            FROM user_subscriptions
            WHERE user_id = %s
              AND status = 'active'
            ORDER BY expires_at DESC, id DESC
            LIMIT 1
            FOR UPDATE;
            """,
            (user_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def _load_active_apple_subscription(
    conn: Any,
    user_id: int,
    reference_date: datetime,
) -> dict[str, Any] | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH latest_transactions AS (
                SELECT DISTINCT ON (original_transaction_id) *
                FROM apple_transactions
                WHERE user_id = %s
                ORDER BY original_transaction_id,
                         COALESCE(signed_date, purchase_date) DESC,
                         purchase_date DESC,
                         id DESC
            )
            SELECT *
            FROM latest_transactions
            WHERE revocation_date IS NULL
              AND expires_date IS NOT NULL
              AND expires_date > %s
            ORDER BY expires_date DESC,
                     COALESCE(signed_date, purchase_date) DESC,
                     id DESC
            LIMIT 1;
            """,
            (user_id, reference_date),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def _load_apple_intervals(
    conn: Any,
    user_id: int,
) -> list[tuple[datetime, datetime]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, original_transaction_id, purchase_date, expires_date,
                   revocation_date, signed_date
            FROM apple_transactions
            WHERE user_id = %s
              AND expires_date IS NOT NULL
            ORDER BY original_transaction_id,
                     COALESCE(signed_date, purchase_date) DESC,
                     purchase_date DESC,
                     id DESC;
            """,
            (user_id,),
        )
        rows = [dict(row) for row in cur.fetchall()]

    latest_by_original: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest_by_original.setdefault(str(row["original_transaction_id"]), row)

    intervals: list[tuple[datetime, datetime]] = []
    for row in rows:
        start = row["purchase_date"]
        end = row["expires_date"]
        if row["revocation_date"] is not None:
            end = min(end, row["revocation_date"])
        latest = latest_by_original[str(row["original_transaction_id"])]
        if latest["revocation_date"] is not None:
            end = min(end, latest["revocation_date"])
        if end > start:
            intervals.append((start, end))

    intervals.sort(key=lambda item: (item[0], item[1]))
    merged: list[tuple[datetime, datetime]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
            continue
        merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def _load_referral_rewards(conn: Any, user_id: int) -> list[tuple[datetime, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT granted_at, reward_value
            FROM rewards
            WHERE user_id = %s
              AND reward_type = 'plus_days'
              AND status = 'granted'
              AND granted_at IS NOT NULL
            ORDER BY granted_at, id;
            """,
            (user_id,),
        )
        rewards: list[tuple[datetime, int]] = []
        for granted_at, reward_value in cur.fetchall():
            try:
                days = int(str(reward_value))
            except (TypeError, ValueError):
                continue
            if days > 0:
                rewards.append((granted_at, days))
        return rewards


def _covered_seconds(
    start: datetime,
    end: datetime,
    intervals: list[tuple[datetime, datetime]],
) -> float:
    if end <= start:
        return 0.0
    covered = 0.0
    for interval_start, interval_end in intervals:
        overlap_start = max(start, interval_start)
        overlap_end = min(end, interval_end)
        if overlap_end > overlap_start:
            covered += (overlap_end - overlap_start).total_seconds()
    return covered


def _project_referral_expiry(
    reference_date: datetime,
    remaining_seconds: float,
    intervals: list[tuple[datetime, datetime]],
) -> datetime | None:
    if remaining_seconds <= 0:
        return None

    cursor = reference_date
    for interval_start, interval_end in intervals:
        if interval_end <= cursor:
            continue
        if interval_start > cursor:
            available_seconds = (interval_start - cursor).total_seconds()
            if remaining_seconds <= available_seconds:
                return cursor + timedelta(seconds=remaining_seconds)
            remaining_seconds -= available_seconds
        cursor = max(cursor, interval_end)
    return cursor + timedelta(seconds=remaining_seconds)


def _calculate_referral_expiry(
    rewards: list[tuple[datetime, int]],
    intervals: list[tuple[datetime, datetime]],
    reference_date: datetime,
) -> datetime | None:
    balance_seconds = 0.0
    cursor: datetime | None = None
    for granted_at, days in rewards:
        if granted_at > reference_date:
            break
        if cursor is not None and balance_seconds > 0:
            elapsed = (granted_at - cursor).total_seconds()
            elapsed -= _covered_seconds(cursor, granted_at, intervals)
            balance_seconds = max(0.0, balance_seconds - max(0.0, elapsed))
        cursor = granted_at
        balance_seconds += timedelta(days=days).total_seconds()

    if cursor is None:
        return None
    if balance_seconds > 0 and reference_date > cursor:
        elapsed = (reference_date - cursor).total_seconds()
        elapsed -= _covered_seconds(cursor, reference_date, intervals)
        balance_seconds = max(0.0, balance_seconds - max(0.0, elapsed))
    return _project_referral_expiry(reference_date, balance_seconds, intervals)


def _legacy_referral_expiry(
    active_subscription: dict[str, Any] | None,
    apple_subscription: dict[str, Any] | None,
    apple_intervals: list[tuple[datetime, datetime]],
    reference_date: datetime,
) -> datetime | None:
    if active_subscription is None:
        return None
    stored_referral_expiry = active_subscription.get("referral_expires_at")
    if stored_referral_expiry and stored_referral_expiry > reference_date:
        return stored_referral_expiry

    source = active_subscription.get("source")
    aggregate_expiry = active_subscription.get("expires_at")
    if aggregate_expiry is None or aggregate_expiry <= reference_date:
        return None

    if source in {REFERRAL_SOURCE, COMBINED_SOURCE}:
        apple_expiry = apple_subscription.get("expires_date") if apple_subscription else None
        if apple_expiry is not None and aggregate_expiry <= apple_expiry:
            remaining = (aggregate_expiry - reference_date).total_seconds()
            return _project_referral_expiry(reference_date, remaining, apple_intervals)
        return aggregate_expiry

    return None


def reconcile_user_plus_entitlement(
    conn: Any,
    user_id: int,
    reference_date: datetime | None = None,
    *,
    apple_subscription: dict[str, Any] | None = None,
    user_locked: bool = False,
) -> PlusEntitlementComponents:
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    reference = normalize_reference_date(reference_date)

    if not user_locked:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE id = %s FOR UPDATE;", (user_id,))
            if cur.fetchone() is None:
                raise ValueError("User not found")

    active_subscription = _load_active_subscription(conn, user_id)
    if apple_subscription is None:
        apple_subscription = _load_active_apple_subscription(conn, user_id, reference)
    apple_expires_at = (
        apple_subscription.get("expires_date") if apple_subscription else None
    )
    apple_active = bool(apple_expires_at and apple_expires_at > reference)

    apple_intervals = _load_apple_intervals(conn, user_id)
    referral_rewards = _load_referral_rewards(conn, user_id)
    referral_expires_at = _calculate_referral_expiry(
        referral_rewards,
        apple_intervals,
        reference,
    )
    if not referral_rewards:
        referral_expires_at = _legacy_referral_expiry(
            active_subscription,
            apple_subscription,
            apple_intervals,
            reference,
        )
    referral_active = bool(
        referral_expires_at and referral_expires_at > reference
    )

    if apple_active and referral_active:
        source = COMBINED_SOURCE
    elif apple_active:
        source = APPLE_SOURCE
    elif referral_active:
        source = REFERRAL_SOURCE
    else:
        source = None

    component_expiries = [
        expiry
        for expiry in (apple_expires_at, referral_expires_at)
        if expiry is not None and expiry > reference
    ]
    expires_at = max(component_expiries) if component_expiries else None
    original_transaction_id = (
        apple_subscription.get("original_transaction_id")
        if apple_subscription and apple_active
        else None
    )

    changed = False
    subscription_row: dict[str, Any] | None = None
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if expires_at is None:
            if active_subscription is not None:
                cur.execute(
                    """
                    UPDATE user_subscriptions
                    SET status = 'expired',
                        expires_at = LEAST(expires_at, %s),
                        updated_at = NOW()
                    WHERE id = %s;
                    """,
                    (reference, active_subscription["id"]),
                )
                changed = cur.rowcount > 0
        elif active_subscription is None:
            starts_at_candidates = [reference]
            if apple_subscription and apple_subscription.get("purchase_date"):
                starts_at_candidates.append(apple_subscription["purchase_date"])
            if referral_rewards:
                starts_at_candidates.append(referral_rewards[0][0])
            cur.execute(
                """
                INSERT INTO user_subscriptions (
                    user_id, source, status, starts_at, expires_at,
                    original_transaction_id, apple_expires_at,
                    referral_expires_at, created_at, updated_at
                )
                VALUES (%s, %s, 'active', %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING *;
                """,
                (
                    user_id,
                    source,
                    min(starts_at_candidates),
                    expires_at,
                    original_transaction_id,
                    apple_expires_at if apple_active else None,
                    referral_expires_at if referral_active else None,
                ),
            )
            subscription_row = dict(cur.fetchone())
            changed = True
        else:
            cur.execute(
                """
                UPDATE user_subscriptions
                SET source = %s,
                    expires_at = %s,
                    original_transaction_id = %s,
                    apple_expires_at = %s,
                    referral_expires_at = %s,
                    updated_at = NOW()
                WHERE id = %s
                  AND (
                      source IS DISTINCT FROM %s
                      OR expires_at IS DISTINCT FROM %s
                      OR original_transaction_id IS DISTINCT FROM %s
                      OR apple_expires_at IS DISTINCT FROM %s
                      OR referral_expires_at IS DISTINCT FROM %s
                  )
                RETURNING *;
                """,
                (
                    source,
                    expires_at,
                    original_transaction_id,
                    apple_expires_at if apple_active else None,
                    referral_expires_at if referral_active else None,
                    active_subscription["id"],
                    source,
                    expires_at,
                    original_transaction_id,
                    apple_expires_at if apple_active else None,
                    referral_expires_at if referral_active else None,
                ),
            )
            updated = cur.fetchone()
            changed = updated is not None
            subscription_row = dict(updated) if updated else active_subscription

    return PlusEntitlementComponents(
        is_plus=expires_at is not None,
        expires_at=expires_at,
        apple_active=apple_active,
        apple_expires_at=apple_expires_at if apple_active else None,
        referral_active=referral_active,
        referral_expires_at=referral_expires_at if referral_active else None,
        source=source,
        changed=changed,
        subscription=subscription_row,
    )


def backfill_plus_entitlement_components(conn: Any) -> int:
    ensure_plus_entitlement_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT user_id
            FROM user_subscriptions
            WHERE status = 'active'
              AND apple_expires_at IS NULL
              AND referral_expires_at IS NULL
            ORDER BY user_id;
            """
        )
        user_ids = [int(row[0]) for row in cur.fetchall()]

    for user_id in user_ids:
        reconcile_user_plus_entitlement(conn, user_id)
    return len(user_ids)
