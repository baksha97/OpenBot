# Local durable threads

Set `THREADS_BACKEND=local` to keep conversation history and run execution in the application's PostgreSQL database. The unset default remains `intelligence`. Local mode needs `DATABASE_URL`, `KEY_ENCRYPTION_KEY`, the existing authentication configuration, and an agent endpoint or built-in agent configuration. It needs no Intelligence credentials and does not construct Intelligence clients. Invalid backend names and incomplete Intelligence configurations fail startup; neither backend silently falls back.

```sh
THREADS_BACKEND=local
DATABASE_URL=postgres://openbot:password@localhost:5432/openbot
```

Keep the encryption key and your identity provider configuration in the existing secret mechanism. Local mode preserves Bot access checks, tool grants, governance, approvals, history sanitization and signed handoffs. The single-user flag is still restricted to a local trial; it is not a substitute for authentication on a shared installation.

For Helm, set `config.threadsBackend: local`. Intelligence URLs and the `intelligence-api-key` Secret entry are then omitted from server/worker environment templates. The normal PostgreSQL, vault key and identity provider requirements still apply. Compose passes `THREADS_BACKEND` from the existing `.env` file. Use the same backend and stable `DEPLOYMENT_ID` on every replica. The deployment ID is the tenant partition; changing it makes existing local histories inaccessible in that process.

## Migrations and backups

Run the existing migration entry point before serving traffic:

```sh
cd server
bun --env-file=../.env scripts/migrate.ts
```

It applies the application's Drizzle migrations and the versioned migrations in `packages/durable-core/migrations`. Local server startup also applies the durable migrations under a PostgreSQL transaction advisory lock, so concurrent replicas cannot partially apply a migration. The migration account needs schema/table/index creation rights. The execution account needs read/write access to the `durable` schema. The migrations add a separate schema; they do not convert Intelligence histories.

Back up the whole application database, including both `public` and `durable`, using your normal PostgreSQL backup/WAL policy. Keep the original encryption key in your secret manager. A messages-only backup loses run identity, cancellation, cursors and pending relays. Test restoration into a separate database, run migrations there, then verify history and a new follow-up turn through the application before calling the backup usable. Retain `durable.migrations` with the backup.

## Execution, replay and workers

`@openbot/durable-core` is an internal MIT workspace package. It has no dependency on OpenBot, Hono or CopilotKit. OpenBot supplies a trusted tenant/principal and rechecks current ownership, channel membership and Bot grants at each boundary. Direct channel histories remain private to the mapped member; group Bot histories require current group membership. Deleting a channel conversation goes through the existing channel route, not the generic thread-delete route.

A run's accepted input and stable identity commit before the agent starts. Reusing the same run/idempotency key returns the recorded run; different input for that key returns conflict. Only the accepted principal may claim or use its writer lease, even in a shared group. Events commit with ordered cursors and message/state projections before SSE publication. A successful terminal transaction commits final output, state, follow-up intents and successful status together before a client can observe `RUN_FINISHED`. Per-run output is immutable, so a later approval continuation can update canonical history without changing an earlier retry result. Conflicting follow-up identities reject the complete terminal transaction. Store failures abort the agent and cannot emit successful completion.

One writer holds a thread's database-clock lease for 60 seconds, renewed every 20 seconds. Monotonic fencing tokens are checked on each append/finalization. A second writer receives conflict. Cancellation is stored in PostgreSQL and polled independently of agent output; stale leases and cancellation stop execution. Closing a browser connection only detaches its subscription. Foreground and headless execution use the same durable service.

Replay reads a consistent snapshot and then ordered database events. `Last-Event-ID` can replay after a committed cursor. A terminal-status read is drained through its committed watermark, including when completion races the event query. The server stream pulls one frame per read and fetches at most 128 events per database batch. The persistence middleware allows at most 128 pending events; an agent that outruns storage fails visibly rather than accumulating arbitrary buffered output. An individual event/history snapshot can still be large; configure proxy/application request limits and provision PostgreSQL storage accordingly. There is no automatic history/event retention or compaction in this version.

The transport adapter targets the pinned public `AgentRunner` and middleware APIs in runtime 1.77.0 / AG-UI 1.0.1. SDK setup, agent middleware and generative UI setup run before the application supplies the pull-based SSE response. A public `AbstractAgent` invocation wraps the complete configured `runAgent` pipeline so synthesized tool results and approval interrupts persist too; the original agent's initialization/finalization hooks still run. This adapter avoids the SDK's unbounded asynchronous SSE subscriber. Existing private Intelligence connection/lock calls are contained in `server/src/intelligence-compat.ts`; local mode does not call them.

Routine runs and handoff queue deliveries retain stable run keys. Successful retries return committed output without a second model turn. Handoff results and relay intent commit with terminal success before the queue acknowledges delivery. The reconciler offers the stable relay queue key before acknowledging the intent, making a crash between those operations idempotent. It uses the accepted run's principal, not the thread creator, and validates the signed work actor. Failed/revoked intents are deferred for 30 seconds with attempts/error recorded, allowing later intents to proceed. Unsupported intent kinds are quarantined with `next_attempt_at='infinity'` and `last_error='unsupported_kind'`. Operators can inspect these records; requeue only after resolving the underlying access or implementation problem.

## Recovery and operational limits

After process death or a lost lease, the next authorized stream/status/submission interrupts the expired run and preserves committed output. Recovery never reruns uncertain external tools. A retry of an interrupted run returns that recorded interruption; it does not resume it. There is no checkpoint/resume adapter in this version. Inspect an uncertain tool's external outcome before requesting a separate new turn. Graceful process shutdown also relies on this safe interruption for unfinished runs; allow up to the lease duration for stale status to settle after restart.

Automatic Learning, Intelligence Inspector/trajectory features, and hosted Intelligence history import are unavailable locally. `/api/capabilities` advertises `learning:false` and `threadTransport:'sse'`; the app hides Learning controls and its route explains the limitation. Existing Intelligence mode remains available and its configured behavior is unchanged. PGlite, a separately published package, OpenMuse integration and other runtime versions are outside this change.

To roll back the application, stop traffic/workers and retain a full backup. Switching `THREADS_BACKEND=intelligence` requires its complete credentials and switches to its own history store; it does not move local conversations. Roll forward again with the same database and deployment ID to reopen local histories. Do not delete the `durable` schema as part of an application rollback. Older binaries ignore it; destructive schema rollback is a separate, explicitly reviewed operation after backup.

## Validation

Use a dedicated migrated PostgreSQL test database, never the running application database:

```sh
TEST_DATABASE_URL=postgres://.../openbot_test \
OPENBOT_DURABLE_TEST_DATABASE_URL=postgres://.../openbot_test \
COPILOTKIT_TELEMETRY_DISABLED=true bun run test:durable
```

The durable suite exercises multiprocess contention, stale fencing, real worker kill after a tool effect, event/finalization database faults, authenticated HTTP/SSE run/reload/follow-up, cancellation, bounded slow-store behavior, production membership policy, approval updates, routine/handoff retries and relay reconciliation. The full project gates remain `bun run test:ci`, `bun run typecheck`, `bun run format:check`, `bun run lint`, and the chart's existing render checks. Live vendor/model/Intelligence smoke requires separately available credentials; passing local tests does not establish that live integration.

For the actual browser composer/reload/follow-up path, build the app first, install the repository-pinned Chromium (`bunx --bun playwright-core install chromium`), and run the user-path test with `OPENBOT_DURABLE_BROWSER=1` and the same dedicated test database variables. `PLAYWRIGHT_BROWSERS_PATH` can keep the browser download in a disposable directory. This test uses a scripted external AG-UI endpoint and single-user trial authentication; it does not claim live model or identity-provider coverage.
