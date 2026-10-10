ALTER TABLE durable.runs ADD COLUMN editable_message_ids jsonb NOT NULL DEFAULT '[]';
