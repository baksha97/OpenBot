CREATE SCHEMA IF NOT EXISTS durable;
CREATE TABLE durable.threads (
  tenant_id text NOT NULL,
  id text NOT NULL,
  owner_id text NOT NULL,
  agent_id text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}',
  state jsonb NOT NULL DEFAULT '{}',
  cursor bigint NOT NULL DEFAULT 0,
  fence bigint NOT NULL DEFAULT 0,
  active_run_id text,
  lease_until timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, id)
);
CREATE TABLE durable.runs (
  tenant_id text NOT NULL,
  thread_id text NOT NULL,
  id text NOT NULL,
  principal_id text NOT NULL,
  agent_id text NOT NULL,
  idempotency_key text NOT NULL,
  input_hash text NOT NULL,
  input jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('accepted','running','succeeded','failed','cancelled','interrupted')),
  fence bigint,
  cancel_requested boolean NOT NULL DEFAULT false,
  error text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  PRIMARY KEY (tenant_id, thread_id, id),
  UNIQUE (tenant_id, thread_id, idempotency_key),
  FOREIGN KEY (tenant_id, thread_id) REFERENCES durable.threads ON DELETE CASCADE
);
CREATE TABLE durable.events (
  tenant_id text NOT NULL,
  thread_id text NOT NULL,
  cursor bigint NOT NULL,
  run_id text NOT NULL,
  event jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, thread_id, cursor),
  FOREIGN KEY (tenant_id, thread_id, run_id) REFERENCES durable.runs ON DELETE CASCADE
);
CREATE TABLE durable.messages (
  tenant_id text NOT NULL,
  thread_id text NOT NULL,
  id text NOT NULL,
  position bigint NOT NULL,
  message jsonb NOT NULL,
  PRIMARY KEY (tenant_id, thread_id, id),
  FOREIGN KEY (tenant_id, thread_id) REFERENCES durable.threads ON DELETE CASCADE
);
CREATE TABLE durable.followups (
  tenant_id text NOT NULL,
  thread_id text NOT NULL,
  id text NOT NULL,
  run_id text NOT NULL,
  payload jsonb NOT NULL,
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, thread_id, run_id) REFERENCES durable.runs ON DELETE CASCADE
);
CREATE INDEX runs_recovery ON durable.runs (status) WHERE status IN ('running','accepted');
CREATE INDEX followups_pending ON durable.followups (created_at) WHERE completed_at IS NULL;
