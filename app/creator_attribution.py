from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Any, Final

from psycopg2.extras import RealDictCursor


CREATOR_CODE_PATTERN: Final[str] = r"^[A-Z0-9]{9,32}$"
CREATOR_ATTRIBUTION_SOURCES: Final[frozenset[str]] = frozenset(
    {
        "email_registration",
        "google_registration",
        "apple_registration",
        "post_registration",
    }
)
_CREATOR_CODE_RE: Final[re.Pattern[str]] = re.compile(CREATOR_CODE_PATTERN)
_APPLE_NOTIFICATION_REFUND: Final[str] = "REFUND"
_APPLE_NOTIFICATION_REFUND_REVERSED: Final[str] = "REFUND_REVERSED"
_APPLE_NOTIFICATION_REVOKE: Final[str] = "REVOKE"
_APPLE_OWNERSHIP_FAMILY_SHARED: Final[str] = "FAMILY_SHARED"
_APPLE_OWNERSHIP_PURCHASED: Final[str] = "PURCHASED"
_APPLE_REASON_PURCHASE: Final[str] = "PURCHASE"
_APPLE_REASON_RENEWAL: Final[str] = "RENEWAL"


class CreatorAttributionError(RuntimeError):
    error_code = "CREATOR_ATTRIBUTION_ERROR"
    default_message = "Creator attribution failed"

    def __init__(self, message: str | None = None):
        super().__init__(message or self.default_message)


class CreatorCodeFormatInvalidError(CreatorAttributionError):
    error_code = "CREATOR_CODE_FORMAT_INVALID"
    default_message = "Creator code format is invalid"


class CreatorCodeNotFoundError(CreatorAttributionError):
    error_code = "CREATOR_CODE_NOT_FOUND"
    default_message = "Creator code was not found"


class CreatorCampaignNotActiveError(CreatorAttributionError):
    error_code = "CREATOR_CAMPAIGN_NOT_ACTIVE"
    default_message = "Creator campaign is not active"


class CreatorNotActiveError(CreatorAttributionError):
    error_code = "CREATOR_NOT_ACTIVE"
    default_message = "Creator is not active"


class CreatorCampaignNotStartedError(CreatorAttributionError):
    error_code = "CREATOR_CAMPAIGN_NOT_STARTED"
    default_message = "Creator campaign has not started"


class CreatorCampaignEndedError(CreatorAttributionError):
    error_code = "CREATOR_CAMPAIGN_ENDED"
    default_message = "Creator campaign has ended"


class CreatorAttributionAlreadySetError(CreatorAttributionError):
    error_code = "CREATOR_ATTRIBUTION_ALREADY_SET"
    default_message = "Creator attribution is already set"


class CreatorAttributionSourceInvalidError(CreatorAttributionError):
    error_code = "CREATOR_ATTRIBUTION_SOURCE_INVALID"
    default_message = "Creator attribution source is invalid"


class CreatorAttributionWindowExpiredError(CreatorAttributionError):
    error_code = "CREATOR_ATTRIBUTION_WINDOW_EXPIRED"
    default_message = "Creator attribution window has expired"


class CreatorAttributionUserNotFoundError(CreatorAttributionError):
    error_code = "CREATOR_ATTRIBUTION_USER_NOT_FOUND"
    default_message = "Creator attribution user was not found"


class CreatorAttributionConcurrencyError(CreatorAttributionError):
    error_code = "CREATOR_ATTRIBUTION_CONCURRENCY_ERROR"
    default_message = "Creator attribution could not be resolved atomically"


class CreatorAppleConversionConflictError(CreatorAttributionError):
    error_code = "CREATOR_APPLE_CONVERSION_CONFLICT"
    default_message = "Apple creator conversion identity conflicts with existing data"


@dataclass(frozen=True)
class CreatorCampaign:
    id: int
    creator_id: int
    code: str
    status: str
    starts_at: datetime | None
    ends_at: datetime | None
    post_registration_window_hours: int


@dataclass(frozen=True)
class CreatorAttributionResult:
    attribution_id: int
    campaign_id: int
    user_id: int
    code_used: str
    source: str
    attributed_at: datetime
    created: bool
    idempotent: bool


@dataclass(frozen=True)
class CreatorAppleConversionResult:
    event_id: int
    attribution_id: int
    conversion_type: str
    economic_status: str
    occurred_at: datetime
    created: bool
    changed: bool
    plus_milestone_set: bool


def ensure_creator_attribution_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS creators (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT creators_status_check
                    CHECK (status IN ('active', 'paused', 'archived'))
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS creator_campaigns (
                id BIGSERIAL PRIMARY KEY,
                creator_id BIGINT NOT NULL REFERENCES creators(id) ON DELETE RESTRICT,
                name TEXT NOT NULL,
                code TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'draft',
                starts_at TIMESTAMPTZ NULL,
                ends_at TIMESTAMPTZ NULL,
                post_registration_window_hours INTEGER NOT NULL DEFAULT 24,
                compensation_type TEXT NOT NULL DEFAULT 'none',
                compensation_value NUMERIC(12,4) NULL,
                compensation_currency TEXT NOT NULL DEFAULT 'EUR',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT creator_campaigns_code_check
                    CHECK (code ~ '^[A-Z0-9]{9,32}$'),
                CONSTRAINT creator_campaigns_status_check
                    CHECK (status IN ('draft', 'active', 'paused', 'ended')),
                CONSTRAINT creator_campaigns_date_range_check
                    CHECK (starts_at IS NULL OR ends_at IS NULL OR ends_at > starts_at),
                CONSTRAINT creator_campaigns_registration_window_check
                    CHECK (post_registration_window_hours > 0),
                CONSTRAINT creator_campaigns_compensation_type_check
                    CHECK (compensation_type IN ('none', 'per_qualified_user')),
                CONSTRAINT creator_campaigns_compensation_value_check
                    CHECK (
                        (compensation_type = 'none' AND compensation_value IS NULL)
                        OR (
                            compensation_type = 'per_qualified_user'
                            AND compensation_value IS NOT NULL
                            AND compensation_value <> 'NaN'::numeric
                            AND compensation_value > 0
                        )
                    ),
                CONSTRAINT creator_campaigns_compensation_currency_check
                    CHECK (compensation_currency ~ '^[A-Z]{3}$')
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS creator_attributions (
                id BIGSERIAL PRIMARY KEY,
                campaign_id BIGINT NOT NULL
                    REFERENCES creator_campaigns(id) ON DELETE RESTRICT,
                user_id BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
                code_used TEXT NOT NULL,
                source TEXT NOT NULL,
                attributed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                verified_at TIMESTAMPTZ NULL,
                qualified_at TIMESTAMPTZ NULL,
                plus_converted_at TIMESTAMPTZ NULL,
                paid_plus_converted_at TIMESTAMPTZ NULL,
                status TEXT NOT NULL DEFAULT 'active',
                user_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                anonymized_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT creator_attributions_code_used_check
                    CHECK (code_used ~ '^[A-Z0-9]{9,32}$'),
                CONSTRAINT creator_attributions_source_check
                    CHECK (
                        source IN (
                            'email_registration',
                            'google_registration',
                            'apple_registration',
                            'post_registration'
                        )
                    ),
                CONSTRAINT creator_attributions_status_check
                    CHECK (status IN ('active', 'invalid', 'anonymized'))
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS creator_conversion_events (
                id BIGSERIAL PRIMARY KEY,
                attribution_id BIGINT NOT NULL
                    REFERENCES creator_attributions(id) ON DELETE RESTRICT,
                conversion_type TEXT NOT NULL,
                provider TEXT NOT NULL,
                external_event_key TEXT NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                product_id TEXT NULL,
                economic_status TEXT NOT NULL,
                amount NUMERIC(18,6) NULL,
                currency TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT creator_conversion_events_type_check
                    CHECK (conversion_type IN ('plus_granted', 'purchase', 'renewal')),
                CONSTRAINT creator_conversion_events_provider_check
                    CHECK (
                        provider IN (
                            'apple',
                            'internal_referral',
                            'internal_promo',
                            'google_play'
                        )
                    ),
                CONSTRAINT creator_conversion_events_external_key_check
                    CHECK (
                        external_event_key = BTRIM(external_event_key)
                        AND CHAR_LENGTH(external_event_key) BETWEEN 1 AND 255
                    ),
                CONSTRAINT creator_conversion_events_product_id_check
                    CHECK (
                        product_id IS NULL
                        OR (
                            product_id = BTRIM(product_id)
                            AND CHAR_LENGTH(product_id) BETWEEN 1 AND 255
                        )
                    ),
                CONSTRAINT creator_conversion_events_economic_status_check
                    CHECK (
                        economic_status IN (
                            'non_economic',
                            'verified_unknown_value',
                            'confirmed',
                            'refunded',
                            'revoked'
                        )
                    ),
                CONSTRAINT creator_conversion_events_amount_currency_check
                    CHECK (
                        (amount IS NULL AND currency IS NULL)
                        OR (
                            amount IS NOT NULL
                            AND amount <> 'NaN'::numeric
                            AND amount >= 0
                            AND currency IS NOT NULL
                            AND currency ~ '^[A-Z]{3}$'
                        )
                    ),
                CONSTRAINT creator_conversion_events_economic_value_check
                    CHECK (
                        (economic_status = 'non_economic'
                            AND amount IS NULL
                            AND currency IS NULL)
                        OR (economic_status = 'verified_unknown_value'
                            AND amount IS NULL
                            AND currency IS NULL)
                        OR (economic_status = 'confirmed'
                            AND amount IS NOT NULL
                            AND amount > 0
                            AND currency IS NOT NULL)
                        OR economic_status IN ('refunded', 'revoked')
                    ),
                CONSTRAINT ux_creator_conversion_events_provider_external_key
                    UNIQUE (provider, external_event_key)
            );
            """
        )

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_creator_attributions_user_id
            ON creator_attributions(user_id)
            WHERE user_id IS NOT NULL;
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_creator_campaigns_creator_id
            ON creator_campaigns(creator_id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_creator_attributions_campaign_id
            ON creator_attributions(campaign_id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_creator_attributions_attributed_at
            ON creator_attributions(attributed_at);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_creator_attributions_verified_at
            ON creator_attributions(verified_at);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_creator_attributions_qualified_at
            ON creator_attributions(qualified_at);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_creator_conversion_events_attribution_occurred
            ON creator_conversion_events(attribution_id, occurred_at);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_creator_conversion_events_occurred_at
            ON creator_conversion_events(occurred_at);
            """
        )

        cur.execute(
            """
            CREATE OR REPLACE FUNCTION enforce_creator_attribution_snapshot_immutability()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.campaign_id IS DISTINCT FROM OLD.campaign_id
                   OR NEW.code_used IS DISTINCT FROM OLD.code_used
                   OR NEW.attributed_at IS DISTINCT FROM OLD.attributed_at THEN
                    RAISE EXCEPTION 'Creator attribution snapshot fields are immutable'
                        USING ERRCODE = '23514';
                END IF;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgrelid = 'creator_attributions'::regclass
                      AND tgname = 'creator_attributions_snapshot_immutable'
                      AND NOT tgisinternal
                ) THEN
                    CREATE TRIGGER creator_attributions_snapshot_immutable
                    BEFORE UPDATE OF campaign_id, code_used, attributed_at
                    ON creator_attributions
                    FOR EACH ROW
                    EXECUTE FUNCTION enforce_creator_attribution_snapshot_immutability();
                END IF;
            END
            $$;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION enforce_creator_conversion_event_identity_immutability()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.attribution_id IS DISTINCT FROM OLD.attribution_id
                   OR NEW.conversion_type IS DISTINCT FROM OLD.conversion_type
                   OR NEW.provider IS DISTINCT FROM OLD.provider
                   OR NEW.external_event_key IS DISTINCT FROM OLD.external_event_key
                   OR NEW.occurred_at IS DISTINCT FROM OLD.occurred_at
                   OR NEW.product_id IS DISTINCT FROM OLD.product_id THEN
                    RAISE EXCEPTION 'Creator conversion event identity fields are immutable'
                        USING ERRCODE = '23514';
                END IF;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgrelid = 'creator_conversion_events'::regclass
                      AND tgname = 'creator_conversion_events_identity_immutable'
                      AND NOT tgisinternal
                ) THEN
                    CREATE TRIGGER creator_conversion_events_identity_immutable
                    BEFORE UPDATE OF
                        attribution_id,
                        conversion_type,
                        provider,
                        external_event_key,
                        occurred_at,
                        product_id
                    ON creator_conversion_events
                    FOR EACH ROW
                    EXECUTE FUNCTION enforce_creator_conversion_event_identity_immutability();
                END IF;
            END
            $$;
            """
        )


def normalize_creator_code(creator_code: str) -> str:
    if not isinstance(creator_code, str):
        raise CreatorCodeFormatInvalidError

    normalized = creator_code.strip().upper()
    if _CREATOR_CODE_RE.fullmatch(normalized) is None:
        raise CreatorCodeFormatInvalidError

    return normalized


def _normalize_reference_date(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _fetch_creator_campaign(
    cur: Any,
    normalized_code: str,
    *,
    lock: bool,
) -> tuple[CreatorCampaign, str, datetime]:
    lock_clause = "FOR SHARE OF campaign, creator" if lock else ""
    cur.execute(
        f"""
        SELECT
            campaign.id,
            campaign.creator_id,
            campaign.code,
            campaign.status,
            campaign.starts_at,
            campaign.ends_at,
            campaign.post_registration_window_hours,
            creator.status AS creator_status,
            CURRENT_TIMESTAMP AS database_now
        FROM creator_campaigns AS campaign
        INNER JOIN creators AS creator ON creator.id = campaign.creator_id
        WHERE campaign.code = %s
        LIMIT 1
        {lock_clause};
        """,
        (normalized_code,),
    )
    row = cur.fetchone()
    if row is None:
        raise CreatorCodeNotFoundError

    campaign = CreatorCampaign(
        id=int(row["id"]),
        creator_id=int(row["creator_id"]),
        code=row["code"],
        status=row["status"],
        starts_at=row["starts_at"],
        ends_at=row["ends_at"],
        post_registration_window_hours=int(row["post_registration_window_hours"]),
    )
    database_now = _normalize_reference_date(row["database_now"], "database_now")
    return campaign, row["creator_status"], database_now


def _validate_creator_campaign(
    campaign: CreatorCampaign,
    creator_status: str,
    reference_date: datetime,
) -> None:
    if campaign.status != "active":
        raise CreatorCampaignNotActiveError
    if creator_status != "active":
        raise CreatorNotActiveError

    if campaign.starts_at is not None:
        starts_at = _normalize_reference_date(campaign.starts_at, "starts_at")
        if reference_date < starts_at:
            raise CreatorCampaignNotStartedError

    if campaign.ends_at is not None:
        ends_at = _normalize_reference_date(campaign.ends_at, "ends_at")
        if reference_date >= ends_at:
            raise CreatorCampaignEndedError


def resolve_creator_campaign(
    conn: Any,
    creator_code: str,
    *,
    reference_date: datetime | None = None,
) -> CreatorCampaign:
    normalized_code = normalize_creator_code(creator_code)
    if reference_date is not None:
        reference_date = _normalize_reference_date(reference_date, "reference_date")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        campaign, creator_status, database_now = _fetch_creator_campaign(
            cur,
            normalized_code,
            lock=False,
        )

    _validate_creator_campaign(
        campaign,
        creator_status,
        reference_date or database_now,
    )
    return campaign


def validate_post_registration_window(
    user_created_at: datetime,
    campaign: CreatorCampaign,
    *,
    reference_date: datetime | None = None,
) -> None:
    """Validate the half-open window [user_created_at, attribution_deadline)."""
    normalized_user_created_at = _normalize_reference_date(user_created_at, "user_created_at")
    normalized_reference_date = _normalize_reference_date(
        reference_date or datetime.now(timezone.utc),
        "reference_date",
    )
    attribution_deadline = normalized_user_created_at + timedelta(
        hours=campaign.post_registration_window_hours
    )
    if normalized_reference_date < normalized_user_created_at:
        raise ValueError("reference_date must not precede user_created_at")
    if normalized_reference_date >= attribution_deadline:
        raise CreatorAttributionWindowExpiredError


def _attribution_result(
    row: dict[str, Any],
    *,
    created: bool,
    idempotent: bool,
) -> CreatorAttributionResult:
    return CreatorAttributionResult(
        attribution_id=int(row["id"]),
        campaign_id=int(row["campaign_id"]),
        user_id=int(row["user_id"]),
        code_used=row["code_used"],
        source=row["source"],
        attributed_at=row["attributed_at"],
        created=created,
        idempotent=idempotent,
    )


def _resolve_existing_attribution(
    existing_attribution: dict[str, Any],
    campaign_id: int,
) -> CreatorAttributionResult:
    if int(existing_attribution["campaign_id"]) != campaign_id:
        raise CreatorAttributionAlreadySetError
    return _attribution_result(existing_attribution, created=False, idempotent=True)


def apply_creator_attribution(
    conn: Any,
    user_id: int,
    creator_code: str,
    source: str,
    *,
    reference_date: datetime | None = None,
) -> CreatorAttributionResult:
    """Apply first-touch attribution inside the caller's current transaction."""
    normalized_code = normalize_creator_code(creator_code)
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise CreatorAttributionUserNotFoundError
    if source not in CREATOR_ATTRIBUTION_SOURCES:
        raise CreatorAttributionSourceInvalidError
    if reference_date is not None:
        reference_date = _normalize_reference_date(reference_date, "reference_date")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, created_at
            FROM users
            WHERE id = %s
            FOR UPDATE;
            """,
            (user_id,),
        )
        user = cur.fetchone()
        if user is None:
            raise CreatorAttributionUserNotFoundError

        campaign, creator_status, database_now = _fetch_creator_campaign(
            cur,
            normalized_code,
            lock=True,
        )
        effective_reference_date = reference_date or database_now

        cur.execute(
            """
            SELECT id, campaign_id, user_id, code_used, source, attributed_at
            FROM creator_attributions
            WHERE user_id = %s
            LIMIT 1
            FOR UPDATE;
            """,
            (user_id,),
        )
        existing_attribution = cur.fetchone()
        if existing_attribution is not None:
            return _resolve_existing_attribution(existing_attribution, campaign.id)

        _validate_creator_campaign(campaign, creator_status, effective_reference_date)
        if source == "post_registration":
            validate_post_registration_window(
                user["created_at"],
                campaign,
                reference_date=effective_reference_date,
            )

        cur.execute(
            """
            INSERT INTO creator_attributions (
                campaign_id,
                user_id,
                code_used,
                source,
                attributed_at,
                status
            )
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, 'active')
            ON CONFLICT (user_id) WHERE user_id IS NOT NULL DO NOTHING
            RETURNING id, campaign_id, user_id, code_used, source, attributed_at;
            """,
            (campaign.id, user_id, normalized_code, source),
        )
        created_attribution = cur.fetchone()
        if created_attribution is not None:
            return _attribution_result(created_attribution, created=True, idempotent=False)

        cur.execute(
            """
            SELECT id, campaign_id, user_id, code_used, source, attributed_at
            FROM creator_attributions
            WHERE user_id = %s
            LIMIT 1
            FOR UPDATE;
            """,
            (user_id,),
        )
        concurrent_attribution = cur.fetchone()
        if concurrent_attribution is None:
            raise CreatorAttributionConcurrencyError
        return _resolve_existing_attribution(concurrent_attribution, campaign.id)


def _normalize_apple_conversion_value(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized or None


def _apple_conversion_identity(
    transaction_reason: str | None,
    ownership_type: str | None,
) -> tuple[str, str] | None:
    if transaction_reason not in {_APPLE_REASON_PURCHASE, _APPLE_REASON_RENEWAL}:
        return None
    if ownership_type == _APPLE_OWNERSHIP_PURCHASED:
        return (
            "purchase" if transaction_reason == _APPLE_REASON_PURCHASE else "renewal",
            "verified_unknown_value",
        )
    if ownership_type == _APPLE_OWNERSHIP_FAMILY_SHARED:
        return "plus_granted", "non_economic"
    return None


def _next_apple_economic_status(
    current_status: str,
    base_status: str,
    notification_type: str | None,
) -> str:
    if notification_type == _APPLE_NOTIFICATION_REVOKE:
        return "revoked"
    if notification_type == _APPLE_NOTIFICATION_REFUND:
        return "revoked" if current_status == "revoked" else "refunded"
    if notification_type == _APPLE_NOTIFICATION_REFUND_REVERSED:
        return base_status if current_status == "refunded" else current_status
    return current_status


def _set_plus_conversion_milestone(
    cur: Any,
    attribution_id: int,
    occurred_at: datetime,
    conversion_type: str,
    transaction_reason: str | None,
    economic_status: str,
) -> bool:
    if conversion_type not in {"purchase", "plus_granted"}:
        return False
    if transaction_reason != _APPLE_REASON_PURCHASE:
        return False
    if economic_status in {"refunded", "revoked"}:
        return False

    cur.execute(
        """
        UPDATE creator_attributions
        SET plus_converted_at = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
          AND status = 'active'
          AND user_deleted = FALSE
          AND user_id IS NOT NULL
          AND plus_converted_at IS NULL
        RETURNING id;
        """,
        (occurred_at, attribution_id),
    )
    return cur.fetchone() is not None


def record_creator_apple_conversion(
    conn: Any,
    *,
    user_id: int,
    transaction_id: str,
    original_transaction_id: str,
    purchase_date: datetime,
    product_id: str,
    transaction_reason: str | None,
    ownership_type: str | None,
    revocation_date: datetime | None = None,
    notification_type: str | None = None,
    notification_subtype: str | None = None,
) -> CreatorAppleConversionResult | None:
    """Record one server-verified Apple event in the caller's transaction."""
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise ValueError("transaction_id is required")
    if not isinstance(original_transaction_id, str) or not original_transaction_id.strip():
        raise ValueError("original_transaction_id is required")
    if not isinstance(product_id, str) or not product_id.strip():
        raise ValueError("product_id is required")

    normalized_transaction_id = transaction_id.strip()
    normalized_original_transaction_id = original_transaction_id.strip()
    normalized_product_id = product_id.strip()
    normalized_purchase_date = _normalize_reference_date(purchase_date, "purchase_date")
    if revocation_date is not None:
        _normalize_reference_date(revocation_date, "revocation_date")
    normalized_reason = _normalize_apple_conversion_value(transaction_reason)
    normalized_ownership = _normalize_apple_conversion_value(ownership_type)
    normalized_notification_type = _normalize_apple_conversion_value(notification_type)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, attributed_at, plus_converted_at
            FROM creator_attributions
            WHERE user_id = %s
              AND status = 'active'
              AND user_deleted = FALSE
            LIMIT 1
            FOR UPDATE;
            """,
            (user_id,),
        )
        attribution = cur.fetchone()
        if attribution is None:
            return None

        attributed_at = _normalize_reference_date(
            attribution["attributed_at"],
            "attributed_at",
        )
        if normalized_purchase_date < attributed_at:
            return None

        cur.execute(
            """
            SELECT
                id,
                attribution_id,
                conversion_type,
                occurred_at,
                product_id,
                economic_status
            FROM creator_conversion_events
            WHERE provider = 'apple'
              AND external_event_key = %s
            LIMIT 1
            FOR UPDATE;
            """,
            (normalized_transaction_id,),
        )
        existing_event = cur.fetchone()
        if existing_event is not None:
            if int(existing_event["attribution_id"]) != int(attribution["id"]):
                raise CreatorAppleConversionConflictError

            base_status = (
                "non_economic"
                if existing_event["conversion_type"] == "plus_granted"
                else "verified_unknown_value"
            )
            next_status = _next_apple_economic_status(
                existing_event["economic_status"],
                base_status,
                normalized_notification_type,
            )
            status_changed = next_status != existing_event["economic_status"]
            if status_changed:
                cur.execute(
                    """
                    UPDATE creator_conversion_events
                    SET economic_status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                    """,
                    (next_status, existing_event["id"]),
                )

            plus_milestone_set = _set_plus_conversion_milestone(
                cur,
                int(attribution["id"]),
                existing_event["occurred_at"],
                existing_event["conversion_type"],
                normalized_reason,
                next_status,
            )
            return CreatorAppleConversionResult(
                event_id=int(existing_event["id"]),
                attribution_id=int(attribution["id"]),
                conversion_type=existing_event["conversion_type"],
                economic_status=next_status,
                occurred_at=existing_event["occurred_at"],
                created=False,
                changed=status_changed or plus_milestone_set,
                plus_milestone_set=plus_milestone_set,
            )

        identity = _apple_conversion_identity(normalized_reason, normalized_ownership)
        if identity is None:
            return None
        conversion_type, base_status = identity

        if revocation_date is not None and normalized_notification_type not in {
            _APPLE_NOTIFICATION_REFUND,
            _APPLE_NOTIFICATION_REFUND_REVERSED,
            _APPLE_NOTIFICATION_REVOKE,
        }:
            return None

        if normalized_reason == _APPLE_REASON_RENEWAL:
            cur.execute(
                """
                SELECT purchase_date, transaction_reason
                FROM apple_transactions
                WHERE original_transaction_id = %s
                ORDER BY purchase_date, id;
                """,
                (normalized_original_transaction_id,),
            )
            purchase_dates = [
                _normalize_reference_date(row["purchase_date"], "purchase_date")
                for row in cur.fetchall()
                if _normalize_apple_conversion_value(row["transaction_reason"])
                == _APPLE_REASON_PURCHASE
            ]
            if any(value < attributed_at for value in purchase_dates):
                return None
            if not any(value >= attributed_at for value in purchase_dates):
                return None

        economic_status = base_status
        if normalized_notification_type == _APPLE_NOTIFICATION_REVOKE:
            economic_status = "revoked"
        elif normalized_notification_type == _APPLE_NOTIFICATION_REFUND:
            economic_status = "refunded"

        cur.execute(
            """
            INSERT INTO creator_conversion_events (
                attribution_id,
                conversion_type,
                provider,
                external_event_key,
                occurred_at,
                product_id,
                economic_status,
                amount,
                currency
            )
            VALUES (%s, %s, 'apple', %s, %s, %s, %s, NULL, NULL)
            ON CONFLICT (provider, external_event_key) DO NOTHING
            RETURNING id;
            """,
            (
                attribution["id"],
                conversion_type,
                normalized_transaction_id,
                normalized_purchase_date,
                normalized_product_id,
                economic_status,
            ),
        )
        inserted_event = cur.fetchone()
        if inserted_event is None:
            cur.execute(
                """
                SELECT id, attribution_id, conversion_type, occurred_at,
                       product_id, economic_status
                FROM creator_conversion_events
                WHERE provider = 'apple'
                  AND external_event_key = %s
                LIMIT 1
                FOR UPDATE;
                """,
                (normalized_transaction_id,),
            )
            concurrent_event = cur.fetchone()
            if (
                concurrent_event is None
                or int(concurrent_event["attribution_id"]) != int(attribution["id"])
            ):
                raise CreatorAppleConversionConflictError
            return CreatorAppleConversionResult(
                event_id=int(concurrent_event["id"]),
                attribution_id=int(attribution["id"]),
                conversion_type=concurrent_event["conversion_type"],
                economic_status=concurrent_event["economic_status"],
                occurred_at=concurrent_event["occurred_at"],
                created=False,
                changed=False,
                plus_milestone_set=False,
            )

        plus_milestone_set = _set_plus_conversion_milestone(
            cur,
            int(attribution["id"]),
            normalized_purchase_date,
            conversion_type,
            normalized_reason,
            economic_status,
        )
        return CreatorAppleConversionResult(
            event_id=int(inserted_event["id"]),
            attribution_id=int(attribution["id"]),
            conversion_type=conversion_type,
            economic_status=economic_status,
            occurred_at=normalized_purchase_date,
            created=True,
            changed=True,
            plus_milestone_set=plus_milestone_set,
        )
