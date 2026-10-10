import { DurableError } from "@openbot/durable-core";
import type { LocalThreads } from "./local-threads";
import { handoffRelayOffer, type HandoffWork } from "./agents/handoff-runner";
import type { WorkQueue } from "./work/queue";

/** Offer first, acknowledge second. Stable queue keys make crashes between them harmless. */
export function createDurableFollowupReconciler(
  backend: LocalThreads,
  queue: Pick<WorkQueue, "offer">,
) {
  let running = false;
  return async () => {
    if (running) return;
    running = true;
    const store = backend.service.store;
    try {
      // This internal worker scan chooses trusted scope from the accepted run, never from payload.
      const rows =
        await store.sql`SELECT f.id,f.thread_id,r.principal_id,f.payload FROM durable.followups f JOIN durable.runs r ON r.tenant_id=f.tenant_id AND r.thread_id=f.thread_id AND r.id=f.run_id WHERE f.tenant_id=${backend.tenantId} AND f.completed_at IS NULL AND f.next_attempt_at<=clock_timestamp() ORDER BY f.created_at LIMIT 100`;
      for (const row of rows) {
        const payload = row.payload as {
          kind: string;
          work: HandoffWork;
          key: string;
          answer: string | null;
        };
        if (
          !payload ||
          typeof payload !== "object" ||
          payload.kind !== "handoff-result"
        ) {
          await store.sql`UPDATE durable.followups SET next_attempt_at='infinity',last_error='unsupported_kind' WHERE tenant_id=${backend.tenantId} AND thread_id=${row.thread_id} AND id=${row.id}`;
          continue;
        }
        try {
          const scope = backend.scope(row.principal_id);
          await store.assertAccess(scope, row.thread_id, "run");
          await store.assertAccess(scope, row.thread_id, "update");
          if (payload.work?.actorId !== scope.principalId)
            throw new DurableError("forbidden", "Followup actor mismatch");
          if (!payload.work.answerIn && payload.answer)
            await queue.offer(
              handoffRelayOffer(payload.work, payload.key, payload.answer),
            );
          await store.acknowledge(scope, row.thread_id, row.id);
        } catch {
          // A revoked actor or failed queue/ack cannot prevent later intents from being serviced.
          // Only retry bookkeeping changes here; no unauthorized work is offered or acknowledged.
          await store.sql`UPDATE durable.followups SET attempts=attempts+1,next_attempt_at=clock_timestamp()+interval '30 seconds',last_error='delivery_deferred' WHERE tenant_id=${backend.tenantId} AND thread_id=${row.thread_id} AND id=${row.id}`;
        }
      }
    } finally {
      running = false;
    }
  };
}
