from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from app.db import get_connection


APPLE_TRANSACTION_COLUMNS = """
    id,
    user_id,
    guest_id,
    product_id,
    transaction_id,
    original_transaction_id,
    purchase_date,
    expires_date,
    grace_period_expires_date,
    environment,
    ownership_type,
    transaction_reason,
    revocation_date,
    revocation_reason,
    app_account_token,
    signed_date,
    storefront,
    offer_type,
    created_at,
    updated_at
"""


def _required_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _required_user_id(user_id: int) -> int:
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    return user_id


def _required_app_account_token(app_account_token: UUID) -> UUID:
    if not isinstance(app_account_token, UUID):
        raise ValueError("app_account_token must be a UUID")
    return app_account_token


def _reference_date(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise ValueError("reference_date must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def effective_subscription_expiration(transaction: dict[str, Any]) -> datetime | None:
    expires_date = transaction.get("expires_date")
    grace_period_expires_date = transaction.get("grace_period_expires_date")
    if expires_date is None:
        return None
    if grace_period_expires_date is None:
        return expires_date
    return max(expires_date, grace_period_expires_date)


def get_transaction(transaction_id: str) -> dict[str, Any] | None:
    normalized_transaction_id = _required_identifier(transaction_id, "transaction_id")
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT {APPLE_TRANSACTION_COLUMNS}
                FROM apple_transactions
                WHERE transaction_id = %s
                LIMIT 1;
                """,
                (normalized_transaction_id,),
            )
            row = cur.fetchone()
            return dict(row) if row is not None else None
    finally:
        conn.close()


def get_user_id_by_app_account_token(app_account_token: UUID) -> int | None:
    normalized_app_account_token = _required_app_account_token(app_account_token)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM users
                WHERE app_account_token = %s
                LIMIT 1;
                """,
                (str(normalized_app_account_token),),
            )
            row = cur.fetchone()
            return int(row[0]) if row is not None else None
    finally:
        conn.close()


def get_latest_transaction(original_transaction_id: str) -> dict[str, Any] | None:
    normalized_original_transaction_id = _required_identifier(
        original_transaction_id,
        "original_transaction_id",
    )
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT {APPLE_TRANSACTION_COLUMNS}
                FROM apple_transactions
                WHERE original_transaction_id = %s
                ORDER BY (expires_date IS NULL) DESC,
                         expires_date DESC NULLS FIRST,
                         purchase_date DESC,
                         id DESC
                LIMIT 1;
                """,
                (normalized_original_transaction_id,),
            )
            row = cur.fetchone()
            return dict(row) if row is not None else None
    finally:
        conn.close()


def get_transactions_for_user(user_id: int) -> list[dict[str, Any]]:
    normalized_user_id = _required_user_id(user_id)
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT {APPLE_TRANSACTION_COLUMNS}
                FROM apple_transactions
                WHERE user_id = %s
                ORDER BY (expires_date IS NULL) DESC,
                         expires_date DESC NULLS FIRST,
                         purchase_date DESC,
                         id DESC;
                """,
                (normalized_user_id,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def is_subscription_active(
    original_transaction_id: str,
    reference_date: datetime | None = None,
) -> bool:
    normalized_original_transaction_id = _required_identifier(
        original_transaction_id,
        "original_transaction_id",
    )
    normalized_reference_date = _reference_date(reference_date)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM apple_transactions
                WHERE original_transaction_id = %s
                  AND revocation_date IS NULL
                  AND (
                      expires_date IS NULL
                      OR expires_date > %s
                      OR grace_period_expires_date > %s
                  )
                LIMIT 1;
                """,
                (
                    normalized_original_transaction_id,
                    normalized_reference_date,
                    normalized_reference_date,
                ),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def get_active_subscription_for_user(
    user_id: int,
    reference_date: datetime | None = None,
    *,
    connection: Any | None = None,
) -> dict[str, Any] | None:
    normalized_user_id = _required_user_id(user_id)
    normalized_reference_date = _reference_date(reference_date)
    conn = connection if connection is not None else get_connection()
    owns_connection = connection is None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT {APPLE_TRANSACTION_COLUMNS}
                FROM apple_transactions
                WHERE user_id = %s
                  AND revocation_date IS NULL
                  AND (
                      expires_date IS NULL
                      OR expires_date > %s
                      OR grace_period_expires_date > %s
                  )
                ORDER BY (expires_date IS NULL) DESC,
                         GREATEST(expires_date, grace_period_expires_date) DESC NULLS FIRST,
                         purchase_date DESC,
                         id DESC
                LIMIT 1;
                """,
                (
                    normalized_user_id,
                    normalized_reference_date,
                    normalized_reference_date,
                ),
            )
            row = cur.fetchone()
            return dict(row) if row is not None else None
    finally:
        if owns_connection:
            conn.close()
