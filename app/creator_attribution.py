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
