-- Completed retry results are immutable even when a later approval updates canonical messages.
ALTER TABLE durable.runs ADD COLUMN output_messages jsonb;
