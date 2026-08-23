from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from app.db import get_connection


SUPPORTED_APPLE_PRODUCT_IDS = frozenset(
    {
        "MB.FuelNear.plus.monthly",
        "MB.FuelNear.plus.sixmonths",
        "MB.FuelNear.plus.yearly",
    }
)


class AppleSubscriptionRepositoryError(RuntimeError):
    pass


class AppleTransactionValidationError(AppleSubscriptionRepositoryError):
    pass


class AppleOriginalTransactionOwnershipConflict(AppleSubscriptionRepositoryError):
    pass


class AppleTransactionIdentityConflict(AppleSubscriptionRepositoryError):
    pass


@dataclass(frozen=True, slots=True)
class AppleTransaction:
    user_id: int | None
    product_id: str
    transaction_id: str
    original_transaction_id: str
    purchase_date: datetime
    environment: str
    guest_id: int | None = None
    expires_date: datetime | None = None
    grace_period_expires_date: datetime | None = None
    ownership_type: str | None = None
    transaction_reason: str | None = None
    revocation_date: datetime | None = None
    revocation_reason: str | None = None
    app_account_token: UUID | None = None
    signed_date: datetime | None = None
    storefront: str | None = None
    offer_type: int | None = None


@dataclass(frozen=True, slots=True)
class AppleTransactionSaveResult:
    created: bool
    changed: bool
    row: dict[str, Any]


def _normalize_required_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AppleTransactionValidationError(f"{field_name} is required")
    return value.strip()


def _normalize_optional_text(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise AppleTransactionValidationError(f"{field_name} must be a string")
    return value.strip() or None


def _validate_optional_datetime(value: datetime | None, field_name: str) -> None:
    if value is not None and not isinstance(value, datetime):
        raise AppleTransactionValidationError(f"{field_name} must be a datetime")


def validate_apple_transaction(transaction: AppleTransaction) -> AppleTransaction:
    if not isinstance(transaction, AppleTransaction):
        raise AppleTransactionValidationError("transaction must be an AppleTransaction")
    if transaction.user_id is not None and (
        isinstance(transaction.user_id, bool)
        or not isinstance(transaction.user_id, int)
        or transaction.user_id <= 0
    ):
        raise AppleTransactionValidationError("user_id must be a positive integer")
    if transaction.guest_id is not None and (
        isinstance(transaction.guest_id, bool)
        or not isinstance(transaction.guest_id, int)
        or transaction.guest_id <= 0
    ):
        raise AppleTransactionValidationError("guest_id must be a positive integer")
    if (transaction.user_id is None) == (transaction.guest_id is None):
        raise AppleTransactionValidationError(
            "Apple transaction must have exactly one owner"
        )

    product_id = _normalize_required_text(transaction.product_id, "product_id")
    if product_id not in SUPPORTED_APPLE_PRODUCT_IDS:
        raise AppleTransactionValidationError("Unsupported Apple product_id")

    transaction_id = _normalize_required_text(transaction.transaction_id, "transaction_id")
    original_transaction_id = _normalize_required_text(
        transaction.original_transaction_id,
        "original_transaction_id",
    )
    environment = _normalize_required_text(transaction.environment, "environment")

    if not isinstance(transaction.purchase_date, datetime):
        raise AppleTransactionValidationError("purchase_date is required")

    _validate_optional_datetime(transaction.expires_date, "expires_date")
    _validate_optional_datetime(
        transaction.grace_period_expires_date,
        "grace_period_expires_date",
    )
    _validate_optional_datetime(transaction.revocation_date, "revocation_date")
    _validate_optional_datetime(transaction.signed_date, "signed_date")

    if transaction.app_account_token is not None and not isinstance(transaction.app_account_token, UUID):
        raise AppleTransactionValidationError("app_account_token must be a UUID")
    if transaction.offer_type is not None and (
        isinstance(transaction.offer_type, bool) or not isinstance(transaction.offer_type, int)
    ):
        raise AppleTransactionValidationError("offer_type must be an integer")

    return replace(
        transaction,
        product_id=product_id,
        transaction_id=transaction_id,
        original_transaction_id=original_transaction_id,
        environment=environment,
        ownership_type=_normalize_optional_text(transaction.ownership_type, "ownership_type"),
        transaction_reason=_normalize_optional_text(transaction.transaction_reason, "transaction_reason"),
        revocation_reason=_normalize_optional_text(transaction.revocation_reason, "revocation_reason"),
        storefront=_normalize_optional_text(transaction.storefront, "storefront"),
    )


def save_apple_transaction(conn: Any, transaction: AppleTransaction) -> AppleTransactionSaveResult:
    normalized = validate_apple_transaction(transaction)

    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if normalized.guest_id is not None:
                cur.execute(
                    """
                    SELECT claimed_user_id
                    FROM guest_identities
                    WHERE id = %s
                    FOR UPDATE;
                    """,
                    (normalized.guest_id,),
                )
                guest_owner = cur.fetchone()
                if guest_owner is None or guest_owner["claimed_user_id"] is not None:
                    raise AppleTransactionIdentityConflict(
                        "Guest Apple subscription ownership is no longer active"
                    )
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0));",
                (normalized.original_transaction_id,),
            )
            cur.execute(
                """
                SELECT *
                FROM apple_transactions
                WHERE transaction_id = %s
                LIMIT 1
                FOR UPDATE;
                """,
                (normalized.transaction_id,),
            )
            existing_transaction = cur.fetchone()
            if existing_transaction is not None:
                if (
                    existing_transaction["user_id"] != normalized.user_id
                    or existing_transaction.get("guest_id") != normalized.guest_id
                    or existing_transaction["original_transaction_id"] != normalized.original_transaction_id
                    or (
                        normalized.app_account_token is not None
                        and existing_transaction["app_account_token"] is not None
                        and str(existing_transaction["app_account_token"])
                        != str(normalized.app_account_token)
                    )
                ):
                    raise AppleTransactionIdentityConflict(
                        "transaction_id is already associated with a different Apple subscription"
                    )

                existing_signed_date = existing_transaction["signed_date"]
                if normalized.signed_date is not None and (
                    existing_signed_date is None
                    or normalized.signed_date > existing_signed_date
                ):
                    cur.execute(
                        """
                        UPDATE apple_transactions
                        SET product_id = %s,
                            purchase_date = %s,
                            expires_date = COALESCE(%s, expires_date),
                            grace_period_expires_date = %s,
                            environment = %s,
                            ownership_type = COALESCE(%s, ownership_type),
                            transaction_reason = COALESCE(%s, transaction_reason),
                            revocation_date = %s,
                            revocation_reason = %s,
                            app_account_token = COALESCE(%s, app_account_token),
                            signed_date = %s,
                            storefront = COALESCE(%s, storefront),
                            offer_type = COALESCE(%s, offer_type),
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING *;
                        """,
                        (
                            normalized.product_id,
                            normalized.purchase_date,
                            normalized.expires_date,
                            normalized.grace_period_expires_date,
                            normalized.environment,
                            normalized.ownership_type,
                            normalized.transaction_reason,
                            normalized.revocation_date,
                            normalized.revocation_reason,
                            (
                                str(normalized.app_account_token)
                                if normalized.app_account_token is not None
                                else None
                            ),
                            normalized.signed_date,
                            normalized.storefront,
                            normalized.offer_type,
                            existing_transaction["id"],
                        ),
                    )
                    updated_transaction = cur.fetchone()
                    return AppleTransactionSaveResult(
                        created=False,
                        changed=True,
                        row=dict(updated_transaction),
                    )
                return AppleTransactionSaveResult(
                    created=False,
                    changed=False,
                    row=dict(existing_transaction),
                )

            cur.execute(
                """
                SELECT user_id, guest_id
                FROM apple_transactions
                WHERE original_transaction_id = %s
                  AND NOT (
                      user_id IS NOT DISTINCT FROM %s
                      AND guest_id IS NOT DISTINCT FROM %s
                  )
                LIMIT 1
                FOR UPDATE;
                """,
                (
                    normalized.original_transaction_id,
                    normalized.user_id,
                    normalized.guest_id,
                ),
            )
            existing_owner = cur.fetchone()
            if existing_owner is not None:
                raise AppleOriginalTransactionOwnershipConflict(
                    "original_transaction_id is already associated with another user"
                )

            cur.execute(
                """
                INSERT INTO apple_transactions (
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
                    offer_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *;
                """,
                (
                    normalized.user_id,
                    normalized.guest_id,
                    normalized.product_id,
                    normalized.transaction_id,
                    normalized.original_transaction_id,
                    normalized.purchase_date,
                    normalized.expires_date,
                    normalized.grace_period_expires_date,
                    normalized.environment,
                    normalized.ownership_type,
                    normalized.transaction_reason,
                    normalized.revocation_date,
                    normalized.revocation_reason,
                    (
                        str(normalized.app_account_token)
                        if normalized.app_account_token is not None
                        else None
                    ),
                    normalized.signed_date,
                    normalized.storefront,
                    normalized.offer_type,
                ),
            )
            inserted_transaction = cur.fetchone()

    if inserted_transaction is None:
        raise AppleSubscriptionRepositoryError("Apple transaction insert returned no row")

    return AppleTransactionSaveResult(
        created=True,
        changed=True,
        row=dict(inserted_transaction),
    )


def save_apple_transaction_with_managed_connection(
    transaction: AppleTransaction,
) -> AppleTransactionSaveResult:
    conn = get_connection()
    try:
        return save_apple_transaction(conn, transaction)
    finally:
        conn.close()
