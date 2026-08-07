BEGIN;

ALTER TABLE user_auth_providers
ADD COLUMN IF NOT EXISTS apple_refresh_token_ciphertext TEXT NULL;

ALTER TABLE user_auth_providers
ADD COLUMN IF NOT EXISTS apple_token_updated_at TIMESTAMPTZ NULL;

CREATE TABLE IF NOT EXISTS apple_token_revocations (
    id BIGSERIAL PRIMARY KEY,
    token_fingerprint TEXT NOT NULL UNIQUE,
    token_ciphertext TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error_code TEXT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_apple_token_revocations_due
ON apple_token_revocations(status, next_attempt_at);

COMMIT;
