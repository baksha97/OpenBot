ALTER TABLE durable.events ADD COLUMN source_sequence bigint;
CREATE UNIQUE INDEX events_run_sequence ON durable.events (tenant_id,thread_id,run_id,source_sequence) WHERE source_sequence IS NOT NULL;
ALTER TABLE durable.messages ADD COLUMN run_id text;
