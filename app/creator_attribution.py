from __future__ import annotations

from typing import Any


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
