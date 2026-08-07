BEGIN;

ALTER TABLE user_subscriptions
ADD COLUMN IF NOT EXISTS apple_expires_at TIMESTAMPTZ NULL;

ALTER TABLE user_subscriptions
ADD COLUMN IF NOT EXISTS referral_expires_at TIMESTAMPTZ NULL;

COMMIT;
