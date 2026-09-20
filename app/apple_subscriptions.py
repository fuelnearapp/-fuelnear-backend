from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from enum import Enum
import hashlib
import logging
from typing import Any, Final, Mapping
from uuid import UUID

from psycopg2.extras import RealDictCursor

from app.db import get_connection


logger = logging.getLogger(__name__)


SUPPORTED_APPLE_PRODUCT_IDS = frozenset(
    {
        "MB.FuelNear.plus.monthly",
        "MB.FuelNear.plus.sixmonths",
        "MB.FuelNear.plus.yearly",
    }
)

MAX_APPLE_PRICE_MILLIUNITS: Final[int] = 999_999_999_999_999
APPLE_ECONOMIC_ADJUSTMENTS: Final[frozenset[str]] = frozenset(
    {"refund", "revoke", "refund_reversed", "revocation_unknown"}
)
APPLE_ECONOMIC_ADJUSTMENT_PRIORITY: Final[dict[str | None, int]] = {
    None: 0,
    "refund_reversed": 1,
    "revocation_unknown": 2,
    "refund": 3,
    "revoke": 4,
}

# ISO 4217 alphabetic codes accepted for Apple transaction prices. This local
# set keeps validation deterministic without adding a runtime dependency.
ISO_4217_CURRENCY_CODES: Final[frozenset[str]] = frozenset(
    """
    AED AFN ALL AMD ANG AOA ARS AUD AWG AZN BAM BBD BDT BGN BHD BIF BMD BND
    BOB BOV BRL BSD BTN BWP BYN BZD CAD CDF CHE CHF CHW CLF CLP CNY COP COU
    CRC CUC CUP CVE CZK DJF DKK DOP DZD EGP ERN ETB EUR FJD FKP GBP GEL GHS
    GIP GMD GNF GTQ GYD HKD HNL HTG HUF IDR ILS INR IQD IRR ISK JMD JOD JPY
    KES KGS KHR KMF KPW KRW KWD KYD KZT LAK LBP LKR LRD LSL LYD MAD MDL MGA
    MKD MMK MNT MOP MRU MUR MVR MWK MXN MXV MYR MZN NAD NGN NIO NOK NPR NZD
    OMR PAB PEN PGK PHP PKR PLN PYG QAR RON RSD RUB RWF SAR SBD SCR SDG SEK
    SGD SHP SLE SOS SRD SSP STN SVC SYP SZL THB TJS TMT TND TOP TRY TTD TWD
    TZS UAH UGX USD USN UYI UYU UYW UZS VED VES VND VUV WST XAF XCD XCG XDR
    XOF
    XPF XSU XUA YER ZAR ZMW ZWG
    """.split()
)


class AppleSubscriptionRepositoryError(RuntimeError):
    pass


class AppleTransactionValidationError(AppleSubscriptionRepositoryError):
    pass


class AppleOriginalTransactionOwnershipConflict(AppleSubscriptionRepositoryError):
    pass


class AppleTransactionIdentityConflict(AppleSubscriptionRepositoryError):
    pass


class AppleEconomicEvidenceStatus(str, Enum):
    ABSENT = "absent"
    VALID = "valid"
    INVALID = "invalid"


class AppleBaseEconomicStatus(str, Enum):
    CONFIRMED_CANDIDATE = "confirmed_candidate"
    NON_ECONOMIC = "non_economic"
    VERIFIED_UNKNOWN_VALUE = "verified_unknown_value"


@dataclass(frozen=True, slots=True)
class AppleEconomicEvidence:
    status: AppleEconomicEvidenceStatus
    price_milliunits: int | None = None
    currency: str | None = None
    invalid_reason: str | None = None

    @property
    def amount(self) -> Decimal | None:
        if self.status is not AppleEconomicEvidenceStatus.VALID:
            return None
        if self.price_milliunits is None:
            return None
        return Decimal(self.price_milliunits) / Decimal(1000)


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
    price_milliunits: int | None = None
    currency: str | None = None
    economic_transaction_signed_at: datetime | None = None
    economic_adjustment: str | None = None
    economic_notification_signed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AppleTransactionSaveResult:
    created: bool
    changed: bool
    row: dict[str, Any]
    economic_adjustment_accepted: bool = False


@dataclass(frozen=True, slots=True)
class AppleEconomicAdjustmentReduction:
    economic_adjustment: str | None
    economic_notification_signed_at: datetime | None
    incoming_accepted: bool
    changed: bool


def derive_apple_base_economic_status(
    transaction: Mapping[str, Any],
) -> AppleBaseEconomicStatus:
    environment = str(transaction.get("environment") or "").strip().upper()
    ownership_type = str(transaction.get("ownership_type") or "").strip().upper()
    transaction_reason = str(
        transaction.get("transaction_reason") or ""
    ).strip().upper()

    if environment == "SANDBOX":
        return AppleBaseEconomicStatus.NON_ECONOMIC
    if environment != "PRODUCTION":
        return AppleBaseEconomicStatus.VERIFIED_UNKNOWN_VALUE
    if ownership_type == "FAMILY_SHARED":
        return AppleBaseEconomicStatus.NON_ECONOMIC

    evidence = normalize_apple_economic_evidence(
        transaction.get("price_milliunits"),
        transaction.get("currency"),
    )
    if evidence.status is not AppleEconomicEvidenceStatus.VALID:
        return AppleBaseEconomicStatus.VERIFIED_UNKNOWN_VALUE
    if evidence.price_milliunits == 0:
        return AppleBaseEconomicStatus.NON_ECONOMIC
    if ownership_type != "PURCHASED":
        return AppleBaseEconomicStatus.VERIFIED_UNKNOWN_VALUE
    if transaction_reason not in {"PURCHASE", "RENEWAL"}:
        return AppleBaseEconomicStatus.VERIFIED_UNKNOWN_VALUE
    return AppleBaseEconomicStatus.CONFIRMED_CANDIDATE


def _next_apple_economic_adjustment(
    current_adjustment: str | None,
    incoming_adjustment: str,
) -> str:
    if current_adjustment == "revoke" or incoming_adjustment == "revoke":
        return "revoke"
    if incoming_adjustment == "refund":
        return "refund"
    if incoming_adjustment == "refund_reversed":
        return (
            "refund_reversed"
            if current_adjustment == "refund"
            else "revocation_unknown"
        )
    if current_adjustment == "refund":
        return "refund"
    return "revocation_unknown"


def reduce_apple_economic_adjustment(
    current_adjustment: str | None,
    current_notification_signed_at: datetime | None,
    incoming_adjustment: str | None,
    incoming_notification_signed_at: datetime | None,
) -> AppleEconomicAdjustmentReduction:
    if current_adjustment not in APPLE_ECONOMIC_ADJUSTMENT_PRIORITY:
        raise AppleTransactionValidationError(
            "current economic_adjustment is invalid"
        )
    if current_notification_signed_at is not None and not isinstance(
        current_notification_signed_at, datetime
    ):
        raise AppleTransactionValidationError(
            "current economic_notification_signed_at must be a datetime"
        )
    if incoming_adjustment is None:
        if incoming_notification_signed_at is not None:
            raise AppleTransactionValidationError(
                "economic_notification_signed_at requires economic_adjustment"
            )
        return AppleEconomicAdjustmentReduction(
            current_adjustment,
            current_notification_signed_at,
            False,
            False,
        )
    if incoming_adjustment not in APPLE_ECONOMIC_ADJUSTMENTS:
        raise AppleTransactionValidationError("economic_adjustment is invalid")
    if not isinstance(incoming_notification_signed_at, datetime):
        raise AppleTransactionValidationError(
            "economic_notification_signed_at is required for economic_adjustment"
        )

    if (
        current_notification_signed_at is not None
        and incoming_notification_signed_at < current_notification_signed_at
    ):
        if incoming_adjustment == "revoke" and current_adjustment != "revoke":
            return AppleEconomicAdjustmentReduction(
                "revoke",
                current_notification_signed_at,
                True,
                True,
            )
        return AppleEconomicAdjustmentReduction(
            current_adjustment,
            current_notification_signed_at,
            False,
            False,
        )

    if (
        current_notification_signed_at is not None
        and incoming_notification_signed_at == current_notification_signed_at
    ):
        if incoming_adjustment == current_adjustment:
            return AppleEconomicAdjustmentReduction(
                current_adjustment,
                current_notification_signed_at,
                True,
                False,
            )
        candidate = _next_apple_economic_adjustment(
            current_adjustment,
            incoming_adjustment,
        )
        if (
            APPLE_ECONOMIC_ADJUSTMENT_PRIORITY[candidate]
            > APPLE_ECONOMIC_ADJUSTMENT_PRIORITY[current_adjustment]
        ):
            return AppleEconomicAdjustmentReduction(
                candidate,
                current_notification_signed_at,
                True,
                True,
            )
        return AppleEconomicAdjustmentReduction(
            current_adjustment,
            current_notification_signed_at,
            False,
            False,
        )

    next_adjustment = _next_apple_economic_adjustment(
        current_adjustment,
        incoming_adjustment,
    )
    return AppleEconomicAdjustmentReduction(
        next_adjustment,
        incoming_notification_signed_at,
        True,
        (
            next_adjustment != current_adjustment
            or incoming_notification_signed_at != current_notification_signed_at
        ),
    )


def normalize_apple_economic_evidence(
    price: object | None,
    currency: object | None,
) -> AppleEconomicEvidence:
    if price is None and currency is None:
        return AppleEconomicEvidence(AppleEconomicEvidenceStatus.ABSENT)
    if price is None or currency is None:
        return AppleEconomicEvidence(
            AppleEconomicEvidenceStatus.INVALID,
            invalid_reason="incomplete_pair",
        )
    if type(price) is not int:
        return AppleEconomicEvidence(
            AppleEconomicEvidenceStatus.INVALID,
            invalid_reason="invalid_price_type",
        )
    if price < 0 or price > MAX_APPLE_PRICE_MILLIUNITS:
        return AppleEconomicEvidence(
            AppleEconomicEvidenceStatus.INVALID,
            invalid_reason="price_out_of_range",
        )
    if type(currency) is not str:
        return AppleEconomicEvidence(
            AppleEconomicEvidenceStatus.INVALID,
            invalid_reason="invalid_currency_type",
        )

    normalized_currency = currency.strip().upper()
    if (
        len(normalized_currency) != 3
        or not normalized_currency.isascii()
        or not normalized_currency.isalpha()
    ):
        return AppleEconomicEvidence(
            AppleEconomicEvidenceStatus.INVALID,
            invalid_reason="invalid_currency_format",
        )
    if normalized_currency not in ISO_4217_CURRENCY_CODES:
        return AppleEconomicEvidence(
            AppleEconomicEvidenceStatus.INVALID,
            invalid_reason="unknown_currency",
        )
    return AppleEconomicEvidence(
        AppleEconomicEvidenceStatus.VALID,
        price_milliunits=price,
        currency=normalized_currency,
    )


def ensure_apple_economic_ledger_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE apple_transactions
                ADD COLUMN IF NOT EXISTS price_milliunits BIGINT NULL,
                ADD COLUMN IF NOT EXISTS currency VARCHAR(3) NULL,
                ADD COLUMN IF NOT EXISTS economic_transaction_signed_at TIMESTAMPTZ NULL,
                ADD COLUMN IF NOT EXISTS economic_adjustment TEXT NULL,
                ADD COLUMN IF NOT EXISTS economic_notification_signed_at TIMESTAMPTZ NULL;
            """
        )
        constraints = (
            (
                "apple_transactions_price_milliunits_check",
                "price_milliunits IS NULL OR "
                f"(price_milliunits >= 0 AND price_milliunits <= {MAX_APPLE_PRICE_MILLIUNITS})",
            ),
            (
                "apple_transactions_economic_pair_check",
                "((price_milliunits IS NULL AND currency IS NULL) OR "
                "(price_milliunits IS NOT NULL AND currency IS NOT NULL))",
            ),
            (
                "apple_transactions_currency_format_check",
                "currency IS NULL OR currency ~ '^[A-Z]{3}$'",
            ),
            (
                "apple_transactions_economic_adjustment_check",
                "economic_adjustment IS NULL OR economic_adjustment IN "
                "('refund', 'revoke', 'refund_reversed', 'revocation_unknown')",
            ),
        )
        for constraint_name, expression in constraints:
            cur.execute(
                """
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'apple_transactions'::regclass
                  AND conname = %s;
                """,
                (constraint_name,),
            )
            if cur.fetchone() is None:
                cur.execute(
                    f"""
                    ALTER TABLE apple_transactions
                    ADD CONSTRAINT {constraint_name}
                    CHECK ({expression}) NOT VALID;
                    """
                )
            cur.execute(
                f"""
                ALTER TABLE apple_transactions
                VALIDATE CONSTRAINT {constraint_name};
                """
            )


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
    _validate_optional_datetime(
        transaction.economic_transaction_signed_at,
        "economic_transaction_signed_at",
    )
    _validate_optional_datetime(
        transaction.economic_notification_signed_at,
        "economic_notification_signed_at",
    )

    economic_adjustment = _normalize_optional_text(
        transaction.economic_adjustment,
        "economic_adjustment",
    )
    if economic_adjustment is not None:
        economic_adjustment = economic_adjustment.lower()
        if economic_adjustment not in APPLE_ECONOMIC_ADJUSTMENTS:
            raise AppleTransactionValidationError("economic_adjustment is invalid")
        if transaction.economic_notification_signed_at is None:
            raise AppleTransactionValidationError(
                "economic_notification_signed_at is required for economic_adjustment"
            )
    elif transaction.economic_notification_signed_at is not None:
        raise AppleTransactionValidationError(
            "economic_notification_signed_at requires economic_adjustment"
        )

    if transaction.app_account_token is not None and not isinstance(transaction.app_account_token, UUID):
        raise AppleTransactionValidationError("app_account_token must be a UUID")
    if transaction.offer_type is not None and (
        isinstance(transaction.offer_type, bool) or not isinstance(transaction.offer_type, int)
    ):
        raise AppleTransactionValidationError("offer_type must be an integer")

    economic_evidence = normalize_apple_economic_evidence(
        transaction.price_milliunits,
        transaction.currency,
    )
    economic_evidence_is_valid = (
        economic_evidence.status is AppleEconomicEvidenceStatus.VALID
    )

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
        price_milliunits=(
            economic_evidence.price_milliunits if economic_evidence_is_valid else None
        ),
        currency=economic_evidence.currency if economic_evidence_is_valid else None,
        economic_transaction_signed_at=(
            transaction.economic_transaction_signed_at
            if economic_evidence_is_valid
            else None
        ),
        economic_adjustment=economic_adjustment,
    )


def _save_apple_transaction(
    conn: Any,
    transaction: AppleTransaction,
    *,
    manage_transaction: bool,
) -> AppleTransactionSaveResult:
    normalized = validate_apple_transaction(transaction)

    with (conn if manage_transaction else nullcontext(conn)):
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

                current_transaction = dict(existing_transaction)
                changed = False
                current_adjustment = current_transaction.get("economic_adjustment")
                adjustment_reduction = reduce_apple_economic_adjustment(
                    current_adjustment,
                    current_transaction.get("economic_notification_signed_at"),
                    normalized.economic_adjustment,
                    normalized.economic_notification_signed_at,
                )
                incoming_adjustment = normalized.economic_adjustment
                apply_incoming_revocation = (
                    adjustment_reduction.incoming_accepted
                    and (
                        (
                            incoming_adjustment == "refund"
                            and adjustment_reduction.economic_adjustment == "refund"
                        )
                        or (
                            incoming_adjustment == "revoke"
                            and adjustment_reduction.economic_adjustment == "revoke"
                        )
                        or (
                            incoming_adjustment == "refund_reversed"
                            and current_adjustment == "refund"
                            and adjustment_reduction.economic_adjustment
                            == "refund_reversed"
                        )
                    )
                )
                preserve_persisted_revocation = (
                    not apply_incoming_revocation
                    and (
                        incoming_adjustment is not None
                        or current_adjustment
                        in {"refund", "revoke", "revocation_unknown"}
                    )
                )
                effective_revocation_date = (
                    current_transaction.get("revocation_date")
                    if preserve_persisted_revocation
                    else normalized.revocation_date
                )
                effective_revocation_reason = (
                    current_transaction.get("revocation_reason")
                    if preserve_persisted_revocation
                    else normalized.revocation_reason
                )
                existing_signed_date = current_transaction["signed_date"]
                general_context_accepted = (
                    incoming_adjustment is None
                    or adjustment_reduction.incoming_accepted
                )
                if (
                    general_context_accepted
                    and normalized.signed_date is not None
                    and (
                        existing_signed_date is None
                        or normalized.signed_date > existing_signed_date
                    )
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
                            effective_revocation_date,
                            effective_revocation_reason,
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
                    current_transaction = dict(updated_transaction)
                    changed = True

                incoming_price = normalized.price_milliunits
                incoming_currency = normalized.currency
                if incoming_price is not None and incoming_currency is not None:
                    existing_price = current_transaction.get("price_milliunits")
                    existing_currency = current_transaction.get("currency")
                    existing_economic_signed_at = current_transaction.get(
                        "economic_transaction_signed_at"
                    )
                    incoming_economic_signed_at = (
                        normalized.economic_transaction_signed_at
                    )

                    if existing_price is None and existing_currency is None:
                        cur.execute(
                            """
                            UPDATE apple_transactions
                            SET price_milliunits = %s,
                                currency = %s,
                                economic_transaction_signed_at = %s,
                                updated_at = NOW()
                            WHERE id = %s
                            RETURNING *;
                            """,
                            (
                                incoming_price,
                                incoming_currency,
                                incoming_economic_signed_at,
                                current_transaction["id"],
                            ),
                        )
                        current_transaction = dict(cur.fetchone())
                        changed = True
                    elif (
                        existing_price == incoming_price
                        and existing_currency == incoming_currency
                    ):
                        if incoming_economic_signed_at is not None and (
                            existing_economic_signed_at is None
                            or incoming_economic_signed_at
                            > existing_economic_signed_at
                        ):
                            cur.execute(
                                """
                                UPDATE apple_transactions
                                SET economic_transaction_signed_at = %s,
                                    updated_at = NOW()
                                WHERE id = %s
                                RETURNING *;
                                """,
                                (
                                    incoming_economic_signed_at,
                                    current_transaction["id"],
                                ),
                            )
                            current_transaction = dict(cur.fetchone())
                            changed = True
                    else:
                        transaction_reference = hashlib.sha256(
                            normalized.transaction_id.encode("utf-8")
                        ).hexdigest()[:12]
                        logger.warning(
                            "Apple economic evidence conflict ignored "
                            "transaction_ref=%s",
                            transaction_reference,
                        )

                if adjustment_reduction.changed:
                    cur.execute(
                        """
                        UPDATE apple_transactions
                        SET economic_adjustment = %s,
                            economic_notification_signed_at = %s,
                            revocation_date = CASE
                                WHEN %s THEN %s
                                ELSE revocation_date
                            END,
                            revocation_reason = CASE
                                WHEN %s THEN %s
                                ELSE revocation_reason
                            END,
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING *;
                        """,
                        (
                            adjustment_reduction.economic_adjustment,
                            adjustment_reduction.economic_notification_signed_at,
                            apply_incoming_revocation,
                            normalized.revocation_date,
                            apply_incoming_revocation,
                            normalized.revocation_reason,
                            current_transaction["id"],
                        ),
                    )
                    current_transaction = dict(cur.fetchone())
                    changed = True

                return AppleTransactionSaveResult(
                    created=False,
                    changed=changed,
                    row=current_transaction,
                    economic_adjustment_accepted=(
                        adjustment_reduction.incoming_accepted
                    ),
                )

            adjustment_reduction = reduce_apple_economic_adjustment(
                None,
                None,
                normalized.economic_adjustment,
                normalized.economic_notification_signed_at,
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
                    offer_type,
                    price_milliunits,
                    currency,
                    economic_transaction_signed_at,
                    economic_adjustment,
                    economic_notification_signed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    normalized.price_milliunits,
                    normalized.currency,
                    normalized.economic_transaction_signed_at,
                    adjustment_reduction.economic_adjustment,
                    adjustment_reduction.economic_notification_signed_at,
                ),
            )
            inserted_transaction = cur.fetchone()

    if inserted_transaction is None:
        raise AppleSubscriptionRepositoryError("Apple transaction insert returned no row")

    return AppleTransactionSaveResult(
        created=True,
        changed=True,
        row=dict(inserted_transaction),
        economic_adjustment_accepted=adjustment_reduction.incoming_accepted,
    )


def save_apple_transaction(conn: Any, transaction: AppleTransaction) -> AppleTransactionSaveResult:
    return _save_apple_transaction(conn, transaction, manage_transaction=True)


def save_apple_transaction_in_transaction(
    conn: Any,
    transaction: AppleTransaction,
) -> AppleTransactionSaveResult:
    """Persist using the transaction already owned by the caller."""
    return _save_apple_transaction(conn, transaction, manage_transaction=False)


def save_apple_transaction_with_managed_connection(
    transaction: AppleTransaction,
) -> AppleTransactionSaveResult:
    conn = get_connection()
    try:
        return save_apple_transaction(conn, transaction)
    finally:
        conn.close()
