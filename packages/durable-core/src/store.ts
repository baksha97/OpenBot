import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import postgres from "postgres";
import { patchState, project } from "./projection";
import {
  DurableError,
  type Action,
  type Authorize,
  type Event,
  type Followup,
  type Lease,
  type Message,
  type Run,
  type Scope,
  type StoredEvent,
  type Thread,
} from "./types";

type Tx = postgres.TransactionSql;
type Row = postgres.Row;
function threadOf(row: Row): Thread {
  return {
    id: row.id,
    ownerId: row.owner_id,
    agentId: row.agent_id,
    metadata: row.metadata,
    state: row.state,
    cursor: Number(row.cursor),
  };
}
function runOf(row: Row): Run {
  return {
    id: row.id,
    threadId: row.thread_id,
    principalId: row.principal_id,
    agentId: row.agent_id,
    input: row.input,
    startCursor: Number(row.accepted_cursor),
    status: row.status,
    fence: row.fence == null ? undefined : Number(row.fence),
    cancelRequested: row.cancel_requested,
    error: row.error ?? undefined,
  };
}
function canonical(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  return `{${Object.keys(value)
    .sort()
    .filter((key) => (value as Row)[key] !== undefined)
    .map((key) => `${JSON.stringify(key)}:${canonical((value as Row)[key])}`)
    .join(",")}}`;
}

export class PostgresStore {
  readonly sql: postgres.Sql;
  constructor(
    url: string,
    private readonly authorize: Authorize,
    options: { max?: number } = {},
  ) {
    this.sql = postgres(url, { max: options.max ?? 10, onnotice: () => {} });
  }
  async close() {
    await this.sql.end({ timeout: 5 });
  }
  /** Authorize an application follow-up before it writes to another durable queue. */
  async assertAccess(
    scope: Scope,
    threadId: string,
    action: Action,
  ): Promise<void> {
    await this.boundary(scope, threadId, action);
  }
  async migrate() {
    // Session-independent transaction advisory lock also serializes migration across replicas.
    const migrations = await Promise.all(
      [1, 2, 3, 4, 5, 6].map((version) =>
        readFile(
          new URL(
            `../migrations/${String(version).padStart(4, "0")}.sql`,
            import.meta.url,
          ),
          "utf8",
        ),
      ),
    );
    await this.sql.begin(async (tx) => {
      await tx`SELECT pg_advisory_xact_lock(742349862)`;
      await tx`CREATE SCHEMA IF NOT EXISTS durable`;
      await tx`CREATE TABLE IF NOT EXISTS durable.migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`;
      for (const [index, migration] of migrations.entries()) {
        const version = index + 1;
        const applied =
          await tx`SELECT version FROM durable.migrations WHERE version=${version}`;
        if (!applied.length) {
          await tx.unsafe(migration);
          await tx`INSERT INTO durable.migrations (version) VALUES (${version})`;
        }
      }
    });
  }

  private async boundary(
    scope: Scope,
    id: string,
    action: Action,
    tx: postgres.Sql | Tx = this.sql,
    lock = false,
  ): Promise<Thread> {
    if (!scope.tenantId || !scope.principalId || !id)
      throw new DurableError(
        "invalid",
        "A trusted scope and thread are required",
      );
    const rows = lock
      ? await tx`SELECT * FROM durable.threads WHERE tenant_id=${scope.tenantId} AND id=${id} FOR UPDATE`
      : await tx`SELECT * FROM durable.threads WHERE tenant_id=${scope.tenantId} AND id=${id}`;
    if (!rows.length) throw new DurableError("not_found", "Thread not found");
    const thread = threadOf(rows[0]);
    await this.authorize(scope, action, thread);
    return thread;
  }
  async create(
    scope: Scope,
    input: {
      id: string;
      ownerId: string;
      agentId: string;
      metadata?: Record<string, unknown>;
    },
  ): Promise<Thread> {
    if (
      !scope.tenantId ||
      !scope.principalId ||
      !input.id ||
      !input.ownerId ||
      !input.agentId
    )
      throw new DurableError(
        "invalid",
        "Trusted scope and thread identity required",
      );
    const proposed: Thread = {
      ...input,
      metadata: input.metadata ?? {},
      state: {},
      cursor: 0,
    };
    await this.authorize(scope, "create", proposed);
    return this.sql.begin(async (tx) => {
      await tx`INSERT INTO durable.threads (tenant_id,id,owner_id,agent_id,metadata) VALUES (${scope.tenantId},${input.id},${input.ownerId},${input.agentId},${tx.json(proposed.metadata as postgres.JSONValue)}) ON CONFLICT DO NOTHING`;
      const thread = await this.boundary(scope, input.id, "read", tx);
      if (thread.ownerId !== input.ownerId || thread.agentId !== input.agentId)
        throw new DurableError(
          "conflict",
          "Thread already belongs to another owner or agent",
        );
      return thread;
    });
  }
  async get(scope: Scope, threadId: string) {
    return this.boundary(scope, threadId, "read");
  }
  async bind(scope: Scope, threadId: string, agentId: string) {
    await this.sql.begin(async (tx) => {
      const thread = await this.boundary(scope, threadId, "update", tx, true);
      if (thread.agentId !== "unassigned") {
        if (thread.agentId !== agentId)
          throw new DurableError("conflict", "Thread agent does not match");
        return;
      }
      await this.authorize(scope, "run", { ...thread, agentId });
      await tx`UPDATE durable.threads SET agent_id=${agentId} WHERE tenant_id=${scope.tenantId} AND id=${threadId}`;
    });
  }
  async list(scope: Scope): Promise<Thread[]> {
    const rows = await this
      .sql`SELECT * FROM durable.threads WHERE tenant_id=${scope.tenantId} ORDER BY updated_at DESC`;
    const allowed: Thread[] = [];
    for (const row of rows) {
      const thread = threadOf(row);
      try {
        await this.authorize(scope, "read", thread);
        allowed.push(thread);
      } catch (error) {
        if (!(error instanceof DurableError) || error.code !== "forbidden")
          throw error;
      }
    }
    return allowed;
  }
  async update(scope: Scope, threadId: string, patch: Record<string, unknown>) {
    await this.sql.begin(async (tx) => {
      await this.boundary(scope, threadId, "update", tx, true);
      await tx`UPDATE durable.threads SET metadata=metadata || ${tx.json(patch as postgres.JSONValue)},updated_at=clock_timestamp() WHERE tenant_id=${scope.tenantId} AND id=${threadId}`;
    });
  }
  async delete(scope: Scope, threadId: string) {
    await this.sql.begin(async (tx) => {
      await this.boundary(scope, threadId, "delete", tx, true);
      const [thread] =
        await tx`SELECT active_run_id FROM durable.threads WHERE tenant_id=${scope.tenantId} AND id=${threadId}`;
      if (thread.active_run_id)
        throw new DurableError(
          "conflict",
          "Stop and settle the active run before deleting",
        );
      await tx`DELETE FROM durable.threads WHERE tenant_id=${scope.tenantId} AND id=${threadId}`;
    });
  }
  async snapshot(scope: Scope, threadId: string) {
    return this.sql.begin(async (tx) => {
      const thread = await this.boundary(scope, threadId, "read", tx, true);
      const messages =
        await tx`SELECT message FROM durable.messages WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} ORDER BY position,id`;
      return {
        thread,
        messages: messages.map((row) => row.message as Message),
        cursor: thread.cursor,
      };
    });
  }
  async submit(
    scope: Scope,
    input: {
      threadId: string;
      runId: string;
      key: string;
      agentId: string;
      input: unknown;
      messages: readonly Message[];
      fingerprint?: unknown;
      mutableMessageIds?: readonly string[];
    },
  ): Promise<{ run: Run; created: boolean }> {
    if (!input.key || !input.runId)
      throw new DurableError(
        "invalid",
        "Run identity and idempotency key are required",
      );
    const hash = createHash("sha256")
      .update(
        canonical({
          source: input.fingerprint ?? {
            agentId: input.agentId,
            input: input.input,
            messages: input.messages,
          },
          mutableMessageIds: input.mutableMessageIds ?? [],
        }),
      )
      .digest("hex");
    return this.sql.begin(async (tx) => {
      const target = await this.boundary(
        scope,
        input.threadId,
        "run",
        tx,
        true,
      );
      if (target.agentId !== input.agentId)
        throw new DurableError("conflict", "Thread agent does not match");
      const [prior] =
        await tx`SELECT * FROM durable.runs WHERE tenant_id=${scope.tenantId} AND thread_id=${input.threadId} AND (idempotency_key=${input.key} OR id=${input.runId})`;
      if (prior) {
        if (
          prior.input_hash !== hash ||
          prior.principal_id !== scope.principalId
        )
          throw new DurableError(
            "conflict",
            "Idempotency key reused with different input",
          );
        return { run: runOf(prior), created: false };
      }
      const [thread] =
        await tx`SELECT active_run_id FROM durable.threads WHERE tenant_id=${scope.tenantId} AND id=${input.threadId}`;
      if (thread.active_run_id)
        throw new DurableError("conflict", "Thread is already running");
      const [row] =
        await tx`INSERT INTO durable.runs (tenant_id,thread_id,id,principal_id,agent_id,idempotency_key,input_hash,input,status,accepted_cursor,editable_message_ids) VALUES (${scope.tenantId},${input.threadId},${input.runId},${scope.principalId},${input.agentId},${input.key},${hash},${tx.json(input.input as postgres.JSONValue)},'accepted',${target.cursor},${tx.json((input.mutableMessageIds ?? []) as string[])}) RETURNING *`;
      // Reserve this thread before execution; queued accepted work can safely be interrupted after a crash.
      await tx`UPDATE durable.threads SET active_run_id=${input.runId},lease_until=clock_timestamp()+interval '60 seconds',updated_at=clock_timestamp() WHERE tenant_id=${scope.tenantId} AND id=${input.threadId}`;
      for (const message of input.messages)
        await this.message(
          tx,
          scope,
          input.threadId,
          message,
          false,
          input.runId,
        );
      return { run: runOf(row), created: true };
    });
  }
  async claim(scope: Scope, threadId: string, runId: string): Promise<Lease> {
    return this.sql.begin(async (tx) => {
      await this.boundary(scope, threadId, "run", tx, true);
      const [accepted] =
        await tx`SELECT principal_id FROM durable.runs WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${runId}`;
      if (accepted && accepted.principal_id !== scope.principalId)
        throw new DurableError("forbidden", "Run belongs to another principal");
      const [thread] =
        await tx`UPDATE durable.threads SET fence=fence+1,lease_until=clock_timestamp()+interval '60 seconds' WHERE tenant_id=${scope.tenantId} AND id=${threadId} AND active_run_id=${runId} AND lease_until>clock_timestamp() RETURNING fence`;
      if (!thread)
        throw new DurableError("lease_lost", "Run reservation expired");
      const [run] =
        await tx`UPDATE durable.runs SET status='running',fence=${thread.fence} WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${runId} AND principal_id=${scope.principalId} AND status='accepted' AND cancel_requested=false RETURNING id`;
      if (!run)
        throw new DurableError("conflict", "Run already claimed or cancelled");
      return { scope, threadId, runId, fence: Number(thread.fence) };
    });
  }
  private async guarded(tx: Tx, lease: Lease, allowCancelled = false) {
    await this.boundary(lease.scope, lease.threadId, "run", tx, true);
    const [valid] =
      await tx`SELECT r.cancel_requested FROM durable.threads t JOIN durable.runs r ON r.tenant_id=t.tenant_id AND r.thread_id=t.id AND r.id=t.active_run_id WHERE t.tenant_id=${lease.scope.tenantId} AND t.id=${lease.threadId} AND t.active_run_id=${lease.runId} AND t.fence=${lease.fence} AND r.fence=${lease.fence} AND r.principal_id=${lease.scope.principalId} AND r.status='running' AND t.lease_until>clock_timestamp()`;
    if (!valid) throw new DurableError("lease_lost", "Writer lease lost");
    if (valid.cancel_requested && !allowCancelled)
      throw new DurableError("cancelled", "Run cancelled");
  }
  async renew(lease: Lease) {
    await this.sql.begin(async (tx) => {
      await this.guarded(tx, lease);
      await tx`UPDATE durable.threads SET lease_until=clock_timestamp()+interval '60 seconds' WHERE tenant_id=${lease.scope.tenantId} AND id=${lease.threadId}`;
    });
  }
  private async message(
    tx: Tx,
    scope: Scope,
    threadId: string,
    message: Message,
    replace: boolean,
    runId: string,
  ) {
    if (!message.id || !message.role)
      throw new DurableError("invalid", "Messages need stable IDs and roles");
    const [existing] =
      await tx`SELECT message,run_id FROM durable.messages WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${message.id}`;
    if (existing && !replace) {
      if (canonical(existing.message) !== canonical(message))
        throw new DurableError("conflict", "Message ID has different content");
      return;
    }
    if (existing && replace && existing.run_id !== runId) {
      const [editable] =
        await tx`SELECT id FROM durable.runs WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${runId} AND editable_message_ids @> ${tx.json([message.id])}`;
      if (!editable) return;
    }
    await tx`INSERT INTO durable.messages (tenant_id,thread_id,id,position,message,run_id) SELECT ${scope.tenantId},${threadId},${message.id},COALESCE(MAX(position),0)+1,${tx.json(message as postgres.JSONValue)},${runId} FROM durable.messages WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} ON CONFLICT (tenant_id,thread_id,id) DO UPDATE SET message=EXCLUDED.message,run_id=EXCLUDED.run_id`;
  }
  private async event(tx: Tx, lease: Lease, event: Event, sequence?: number) {
    const { scope, threadId, runId } = lease;
    const [thread] =
      await tx`UPDATE durable.threads SET cursor=cursor+1,updated_at=clock_timestamp() WHERE tenant_id=${scope.tenantId} AND id=${threadId} RETURNING cursor,state`;
    await tx`INSERT INTO durable.events (tenant_id,thread_id,cursor,run_id,event,source_sequence) VALUES (${scope.tenantId},${threadId},${thread.cursor},${runId},${tx.json(event as postgres.JSONValue)},${sequence ?? null})`;
    let id =
      event.type === "TOOL_CALL_START"
        ? (event.parentMessageId ?? event.toolCallId)
        : (event.messageId ?? event.toolCallId);
    if (event.type === "TOOL_CALL_RESULT")
      id = event.messageId ?? `result:${event.toolCallId}`;
    const [prior] =
      event.type === "TOOL_CALL_ARGS"
        ? await tx`SELECT message FROM durable.messages WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND message->'toolCalls' @> ${tx.json([{ id: event.toolCallId }] as postgres.JSONValue)} LIMIT 1`
        : typeof id === "string"
          ? await tx`SELECT message FROM durable.messages WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${id}`
          : [];
    const projected = project(event, prior?.message);
    if (projected)
      await this.message(tx, scope, threadId, projected, true, runId);
    if (event.type === "MESSAGES_SNAPSHOT") {
      if (!Array.isArray(event.messages))
        throw new DurableError("invalid", "Invalid message snapshot");
      for (const message of event.messages as Message[])
        await this.message(tx, scope, threadId, message, true, runId);
    }
    if (event.type === "STATE_SNAPSHOT" || event.type === "STATE_DELTA") {
      const state =
        event.type === "STATE_SNAPSHOT"
          ? event.snapshot
          : patchState(thread.state, event.delta);
      await tx`UPDATE durable.threads SET state=${tx.json(state as postgres.JSONValue)} WHERE tenant_id=${scope.tenantId} AND id=${threadId}`;
    }
    return { cursor: Number(thread.cursor), runId, event };
  }
  async append(
    lease: Lease,
    event: Event,
    sequence: number,
  ): Promise<StoredEvent> {
    if (["RUN_FINISHED", "RUN_ERROR"].includes(event.type))
      throw new DurableError("invalid", "Terminal events require finalization");
    if (!Number.isSafeInteger(sequence) || sequence < 0)
      throw new DurableError("invalid", "Stable event sequence required");
    return this.sql.begin(async (tx) => {
      await this.guarded(tx, lease);
      const [prior] =
        await tx`SELECT cursor,event FROM durable.events WHERE tenant_id=${lease.scope.tenantId} AND thread_id=${lease.threadId} AND run_id=${lease.runId} AND source_sequence=${sequence}`;
      if (prior) {
        if (canonical(prior.event) !== canonical(event))
          throw new DurableError(
            "conflict",
            "Event sequence reused with different payload",
          );
        return {
          cursor: Number(prior.cursor),
          runId: lease.runId,
          event: prior.event as Event,
        };
      }
      return this.event(tx, lease, event, sequence);
    });
  }
  async finish(
    lease: Lease,
    input: {
      status: "succeeded" | "failed" | "cancelled";
      event: Event;
      messages?: readonly Message[];
      state?: unknown;
      error?: string;
      followups?: Followup[];
    },
  ) {
    if (
      (input.status === "succeeded") !==
      (input.event.type === "RUN_FINISHED")
    )
      throw new DurableError("invalid", "Terminal event must match run status");
    return this.sql.begin(async (tx) => {
      // Cancellation is allowed to close its own lease, but success can never pass cancellation.
      await this.guarded(tx, lease, input.status !== "succeeded");
      for (const message of input.messages ?? [])
        await this.message(
          tx,
          lease.scope,
          lease.threadId,
          message,
          true,
          lease.runId,
        );
      if (input.state !== undefined)
        await tx`UPDATE durable.threads SET state=${tx.json(input.state as postgres.JSONValue)} WHERE tenant_id=${lease.scope.tenantId} AND id=${lease.threadId}`;
      const event = await this.event(tx, lease, input.event);
      for (const followup of input.followups ?? []) {
        const [saved] =
          await tx`INSERT INTO durable.followups (tenant_id,thread_id,id,run_id,payload) VALUES (${lease.scope.tenantId},${lease.threadId},${followup.id},${lease.runId},${tx.json(followup.payload as postgres.JSONValue)}) ON CONFLICT (tenant_id,id) DO UPDATE SET id=EXCLUDED.id WHERE durable.followups.thread_id=EXCLUDED.thread_id AND durable.followups.run_id=EXCLUDED.run_id AND durable.followups.payload=EXCLUDED.payload RETURNING id`;
        if (!saved)
          throw new DurableError(
            "conflict",
            "Followup identity has different content",
          );
      }
      if (input.status === "succeeded") {
        const outputs =
          input.messages ??
          (
            await tx`SELECT message FROM durable.messages WHERE tenant_id=${lease.scope.tenantId} AND thread_id=${lease.threadId} AND run_id=${lease.runId} ORDER BY position,id`
          ).map((row) => row.message);
        await tx`UPDATE durable.runs SET output_messages=${tx.json(outputs as postgres.JSONValue)} WHERE tenant_id=${lease.scope.tenantId} AND thread_id=${lease.threadId} AND id=${lease.runId}`;
      }
      await tx`UPDATE durable.runs SET status=${input.status},error=${input.error ?? null},finished_at=clock_timestamp() WHERE tenant_id=${lease.scope.tenantId} AND thread_id=${lease.threadId} AND id=${lease.runId} AND status='running' AND fence=${lease.fence}`;
      await tx`UPDATE durable.threads SET active_run_id=NULL,lease_until=NULL WHERE tenant_id=${lease.scope.tenantId} AND id=${lease.threadId}`;
      return event;
    });
  }
  async run(
    scope: Scope,
    threadId: string,
    runId?: string,
  ): Promise<Run | undefined> {
    await this.boundary(scope, threadId, "read");
    const rows = runId
      ? await this
          .sql`SELECT * FROM durable.runs WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${runId}`
      : await this
          .sql`SELECT * FROM durable.runs WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} ORDER BY created_at DESC LIMIT 1`;
    return rows[0] ? runOf(rows[0]) : undefined;
  }
  async runMessages(
    scope: Scope,
    threadId: string,
    runId: string,
  ): Promise<Message[]> {
    await this.boundary(scope, threadId, "read");
    const [run] = await this
      .sql`SELECT output_messages FROM durable.runs WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${runId}`;
    if (run?.output_messages !== null && run?.output_messages !== undefined)
      return run.output_messages as Message[];
    const rows = await this
      .sql`SELECT message FROM durable.messages WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND run_id=${runId} ORDER BY position,id`;
    return rows.map((row) => row.message as Message);
  }
  async cancel(scope: Scope, threadId: string, runId?: string) {
    return this.sql.begin(async (tx) => {
      await this.boundary(scope, threadId, "stop", tx, true);
      const rows =
        await tx`UPDATE durable.runs SET cancel_requested=true WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND (${runId ?? null}::text IS NULL OR id=${runId ?? null}) AND status IN ('accepted','running') RETURNING id,status`;
      for (const run of rows)
        if (run.status === "accepted") {
          const [thread] =
            await tx`SELECT fence FROM durable.threads WHERE tenant_id=${scope.tenantId} AND id=${threadId}`;
          await this.event(
            tx,
            { scope, threadId, runId: run.id, fence: Number(thread.fence) },
            {
              type: "RUN_ERROR",
              code: "cancelled",
              message: "Run cancelled before execution",
            },
          );
          await tx`UPDATE durable.runs SET status='cancelled',finished_at=clock_timestamp() WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${run.id}`;
          await tx`UPDATE durable.threads SET active_run_id=NULL,lease_until=NULL,fence=fence+1 WHERE tenant_id=${scope.tenantId} AND id=${threadId} AND active_run_id=${run.id}`;
        }
      return rows.length > 0;
    });
  }
  async events(
    scope: Scope,
    threadId: string,
    after: number,
    limit = 128,
  ): Promise<StoredEvent[]> {
    await this.boundary(scope, threadId, "read");
    if (!Number.isSafeInteger(after) || after < 0)
      throw new DurableError("invalid", "Invalid event cursor");
    const rows = await this
      .sql`SELECT cursor,run_id,event FROM durable.events WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND cursor>${after} ORDER BY cursor LIMIT ${Math.max(1, Math.min(limit, 128))}`;
    return rows.map((row) => ({
      cursor: Number(row.cursor),
      runId: row.run_id,
      event: row.event,
    }));
  }
  /** Only expired reservations are interrupted. Tools in an uncertain run are never replayed. */
  async recover(scope: Scope, threadId: string): Promise<boolean> {
    return this.sql.begin(async (tx) => {
      await this.boundary(scope, threadId, "read", tx, true);
      const [thread] =
        await tx`SELECT * FROM durable.threads WHERE tenant_id=${scope.tenantId} AND id=${threadId} AND active_run_id IS NOT NULL AND lease_until<=clock_timestamp()`;
      if (!thread) return false;
      const lease = {
        scope,
        threadId,
        runId: thread.active_run_id,
        fence: Number(thread.fence),
      };
      await this.event(tx, lease, {
        type: "RUN_ERROR",
        code: "interrupted",
        message:
          "The worker was interrupted. Committed output is preserved; uncertain tools will not be retried.",
      });
      await tx`UPDATE durable.runs SET status=CASE WHEN cancel_requested THEN 'cancelled' ELSE 'interrupted' END,error='worker_interrupted',finished_at=clock_timestamp() WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${thread.active_run_id} AND status IN ('accepted','running')`;
      await tx`UPDATE durable.threads SET active_run_id=NULL,lease_until=NULL,fence=fence+1 WHERE tenant_id=${scope.tenantId} AND id=${threadId}`;
      return true;
    });
  }
  async pending(scope: Scope, threadId: string) {
    await this.boundary(scope, threadId, "read");
    return this
      .sql`SELECT id,payload FROM durable.followups WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND completed_at IS NULL ORDER BY created_at`;
  }
  async acknowledge(scope: Scope, threadId: string, id: string) {
    await this.boundary(scope, threadId, "update");
    await this
      .sql`UPDATE durable.followups SET completed_at=clock_timestamp() WHERE tenant_id=${scope.tenantId} AND thread_id=${threadId} AND id=${id}`;
  }
}
