ALTER TABLE durable.followups ADD COLUMN attempts integer NOT NULL DEFAULT 0;
ALTER TABLE durable.followups ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp();
ALTER TABLE durable.followups ADD COLUMN last_error text;
CREATE INDEX followups_due ON durable.followups (tenant_id,next_attempt_at,created_at) WHERE completed_at IS NULL;
