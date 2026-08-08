BEGIN;

ALTER TABLE sent_price_notifications
ADD COLUMN IF NOT EXISTS processing_attempts INTEGER NOT NULL DEFAULT 0;

UPDATE sent_price_notifications
SET processing_attempts = 1
WHERE status = 'processing'
  AND processing_attempts = 0;

CREATE INDEX IF NOT EXISTS idx_sent_price_notifications_stale_processing
ON sent_price_notifications(mimit_run_id, status, processing_started_at);

COMMIT;
