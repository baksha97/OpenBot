import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  DurableError,
  DurableService,
  PostgresStore,
  type Scope,
} from "../src";

const url = process.env.OPENBOT_DURABLE_TEST_DATABASE_URL;
const integration = url ? describe : describe.skip;
integration("durable PostgreSQL contracts", () => {
  const tenantId = `test-${crypto.randomUUID()}`;
  const scope: Scope = { tenantId, principalId: "alice" };
  const authorize = async (
    scope: Scope,
    _action: unknown,
    thread: { ownerId: string },
  ) => {
    if (scope.principalId !== thread.ownerId)
      throw new DurableError("forbidden", "Not owner");
  };
  const store = new PostgresStore(url!, authorize, { max: 2 });
  const other = new PostgresStore(url!, authorize, { max: 2 });
  const service = new DurableService(store, 10);
  beforeAll(async () => {
    await Promise.all([store.migrate(), other.migrate()]);
  });
  afterAll(async () => {
    await service.drain();
    await store.sql`DELETE FROM durable.threads WHERE tenant_id=${tenantId}`;
    await Promise.all([store.close(), other.close()]);
  });
  async function thread() {
    const id = crypto.randomUUID();
    await store.create(scope, { id, ownerId: "alice", agentId: "bot" });
    return id;
  }
  function submission(threadId: string, id = crypto.randomUUID()) {
    return {
      threadId,
      runId: id,
      key: id,
      agentId: "bot",
      input: { text: "hello" },
      messages: [{ id: `input-${id}`, role: "user", content: "hello" }],
    };
  }
  async function active(threadId: string) {
    const input = submission(threadId);
    await store.submit(scope, input);
    return store.claim(scope, threadId, input.runId);
  }
  test("migration is versioned and safe to run concurrently", async () => {
    await Promise.all([store.migrate(), other.migrate()]);
    expect(
      (
        await store.sql`SELECT count(*) AS n FROM durable.migrations WHERE version=1`
      )[0].n,
    ).toBe("1");
  });
  test("acceptance persists input and deduplicates across connections; changed payload conflicts", async () => {
    const id = await thread();
    const input = submission(id);
    const results = await Promise.all([
      store.submit(scope, input),
      other.submit(scope, input),
    ]);
    expect(results.filter((result) => result.created)).toHaveLength(1);
    expect((await store.snapshot(scope, id)).messages).toEqual(input.messages);
    await expect(
      other.submit(scope, { ...input, input: { text: "changed" } }),
    ).rejects.toMatchObject({ code: "conflict" });
    await expect(other.submit(scope, submission(id))).rejects.toMatchObject({
      code: "conflict",
    });
  });
  test("one fenced writer, stale writer cannot append or finalize after expiry and recovery", async () => {
    const id = await thread();
    const lease = await active(id);
    await store.append(
      lease,
      { type: "TEXT_MESSAGE_START", messageId: "reply", role: "assistant" },
      0,
    );
    await store.append(
      lease,
      { type: "TEXT_MESSAGE_CONTENT", messageId: "reply", delta: "committed" },
      1,
    );
    await store.sql`UPDATE durable.threads SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=${tenantId} AND id=${id}`;
    await expect(
      store.append(lease, { type: "CUSTOM", name: "stale" }, 2),
    ).rejects.toMatchObject({ code: "lease_lost" });
    expect(await other.recover(scope, id)).toBe(true);
    expect((await store.run(scope, id))?.status).toBe("interrupted");
    const fresh = await active(id);
    expect(fresh.fence).toBeGreaterThan(lease.fence);
    await expect(
      store.finish(lease, {
        status: "succeeded",
        event: { type: "RUN_FINISHED" },
      }),
    ).rejects.toMatchObject({ code: "lease_lost" });
    expect(
      (await store.snapshot(scope, id)).messages.some(
        (message) => message.content === "committed",
      ),
    ).toBe(true);
  });
  test("shared run access cannot claim or forge a lease for another accepted principal", async () => {
    const shared = new PostgresStore(url!, async () => {}, { max: 2 });
    try {
      const id = await thread();
      const input = submission(id);
      await shared.submit(scope, input);
      const member = { ...scope, principalId: "member" };
      await expect(shared.claim(member, id, input.runId)).rejects.toMatchObject(
        { code: "forbidden" },
      );
      const lease = await shared.claim(scope, id, input.runId);
      const forged = { ...lease, scope: member };
      await expect(shared.renew(forged)).rejects.toMatchObject({
        code: "lease_lost",
      });
      await expect(
        shared.append(forged, { type: "CUSTOM", name: "forged" }, 0),
      ).rejects.toMatchObject({ code: "lease_lost" });
      for (const status of ["succeeded", "failed", "cancelled"] as const)
        await expect(
          shared.finish(forged, {
            status,
            event: {
              type: status === "succeeded" ? "RUN_FINISHED" : "RUN_ERROR",
            },
          }),
        ).rejects.toMatchObject({ code: "lease_lost" });
      expect((await shared.run(scope, id))?.status).toBe("running");
      await shared.finish(lease, {
        status: "succeeded",
        event: { type: "RUN_FINISHED" },
      });
    } finally {
      await shared.close();
    }
  });
  test("conflicting followup identity rolls back terminal success and output", async () => {
    const id = await thread();
    const lease = await active(id);
    await expect(
      store.finish(lease, {
        status: "succeeded",
        event: { type: "RUN_FINISHED" },
        messages: [
          { id: "result", role: "assistant", content: "must roll back" },
        ],
        followups: [
          { id: `${id}:intent`, payload: { value: 1 } },
          { id: `${id}:intent`, payload: { value: 2 } },
        ],
      }),
    ).rejects.toMatchObject({ code: "conflict" });
    expect((await store.run(scope, id))?.status).toBe("running");
    expect(
      (await store.events(scope, id, 0)).some(
        (entry) => entry.event.type === "RUN_FINISHED",
      ),
    ).toBe(false);
    expect(
      (await store.snapshot(scope, id)).messages.some(
        (message) => message.id === "result",
      ),
    ).toBe(false);
    expect(await store.pending(scope, id)).toHaveLength(0);
    await store.finish(lease, {
      status: "succeeded",
      event: { type: "RUN_FINISHED" },
    });
  });
  test("persistent cancellation prevents success and works through another connection", async () => {
    const id = await thread();
    const lease = await active(id);
    expect(await other.cancel(scope, id, lease.runId)).toBe(true);
    await expect(store.renew(lease)).rejects.toMatchObject({
      code: "cancelled",
    });
    await expect(
      store.finish(lease, {
        status: "succeeded",
        event: { type: "RUN_FINISHED" },
      }),
    ).rejects.toMatchObject({ code: "cancelled" });
    await store.finish(lease, {
      status: "cancelled",
      event: { type: "RUN_ERROR", code: "cancelled" },
    });
    expect((await store.run(scope, id))?.status).toBe("cancelled");
  });
  test("all read/write/run/stop/replay/destructive boundaries reject another principal and tenant", async () => {
    const id = await thread();
    const stranger = { ...scope, principalId: "bob" };
    for (const action of [
      () => store.get(stranger, id),
      () => store.snapshot(stranger, id),
      () => store.submit(stranger, submission(id)),
      () => store.cancel(stranger, id),
      () => store.events(stranger, id, 0),
      () => store.update(stranger, id, { name: "hijack" }),
      () => store.delete(stranger, id),
      () => store.run(stranger, id),
      () => store.recover(stranger, id),
      () => store.pending(stranger, id),
      () => store.acknowledge(stranger, id, "x"),
    ])
      await expect(action()).rejects.toMatchObject({ code: "forbidden" });
    expect(await store.list(stranger)).toEqual([]);
    await expect(
      store.get({ ...scope, tenantId: "other-tenant" }, id),
    ).rejects.toMatchObject({ code: "not_found" });
  });
  test("metadata merge cannot overwrite message history and state patches persist", async () => {
    const id = await thread();
    const lease = await active(id);
    await store.append(
      lease,
      { type: "STATE_SNAPSHOT", snapshot: { count: 1 } },
      0,
    );
    await store.append(
      lease,
      {
        type: "STATE_DELTA",
        delta: [{ op: "replace", path: "/count", value: 2 }],
      },
      1,
    );
    await Promise.all([
      store.update(scope, id, { name: "hello" }),
      other.update(scope, id, { archived: true }),
    ]);
    const snapshot = await store.snapshot(scope, id);
    expect(snapshot.messages).toHaveLength(1);
    expect(snapshot.thread.metadata).toEqual({ name: "hello", archived: true });
    expect(snapshot.thread.state).toEqual({ count: 2 });
  });
  test("final output, success and followup intent are committed before terminal is observable", async () => {
    const id = await thread();
    const input = submission(id);
    await service.submit(scope, input, async ({ emit }) => {
      await emit({ type: "RUN_STARTED", threadId: id, runId: input.runId });
      return {
        terminalEvent: { type: "RUN_FINISHED" },
        messages: [{ id: "output", role: "assistant", content: "answer" }],
        followups: [{ id: `intent-${id}`, payload: { relay: "answer" } }],
      };
    });
    const events = [];
    for await (const event of service.stream(scope, id, { runId: input.runId }))
      events.push(event);
    expect(events.at(-1)?.event.type).toBe("RUN_FINISHED");
    expect((await store.run(scope, id))?.status).toBe("succeeded");
    expect((await store.snapshot(scope, id)).messages.at(-1)?.content).toBe(
      "answer",
    );
    expect(await store.pending(scope, id)).toHaveLength(1);
  });
  test("replay drains finalization committed between event read and status read", async () => {
    const id = await thread();
    const lease = await active(id);
    const original = store.events.bind(store);
    let first = true;
    store.events = async (...args) => {
      const events = await original(...args);
      if (first) {
        first = false;
        await store.finish(lease, {
          status: "succeeded",
          event: { type: "RUN_FINISHED" },
          messages: [{ id: "last", role: "assistant", content: "last" }],
        });
      }
      return events;
    };
    try {
      const events = [];
      for await (const event of service.stream(scope, id)) events.push(event);
      expect(events.at(-1)?.event.type).toBe("RUN_FINISHED");
    } finally {
      store.events = original;
    }
  });
  test("event and finalization persistence faults never create successful status or terminal", async () => {
    for (const final of [false, true]) {
      const id = await thread();
      const input = submission(id);
      const original = final
        ? store.finish.bind(store)
        : store.append.bind(store);
      if (final)
        store.finish = async () => {
          throw new Error("injected finalization failure");
        };
      else
        store.append = async () => {
          throw new Error("injected event failure");
        };
      try {
        await service.submit(scope, input, async ({ emit }) => {
          await emit({ type: "RUN_STARTED" });
          return { terminalEvent: { type: "RUN_FINISHED" } };
        });
        await service.drain();
        expect((await store.run(scope, id))?.status).not.toBe("succeeded");
        expect(
          (await store.events(scope, id, 0)).some(
            (row) => row.event.type === "RUN_FINISHED",
          ),
        ).toBe(false);
      } finally {
        if (final) store.finish = original as typeof store.finish;
        else store.append = original as typeof store.append;
      }
    }
  });
});

(url ? describe : describe.skip)(
  "protocol projection and append retries",
  () => {
    const tenantId = `projection-${crypto.randomUUID()}`;
    const scope = { tenantId, principalId: "alice" };
    const store = new PostgresStore(url!, async () => {}, { max: 2 });
    beforeAll(async () => {
      await store.migrate();
    });
    afterAll(async () => {
      await store.sql`DELETE FROM durable.threads WHERE tenant_id=${tenantId}`;
      await store.close();
    });
    test("multi-tool parent message, activity JSON patches and stable event sequence survive replay", async () => {
      const id = crypto.randomUUID();
      await store.create(scope, { id, ownerId: "alice", agentId: "bot" });
      const runId = crypto.randomUUID();
      await store.submit(scope, {
        threadId: id,
        runId,
        key: runId,
        agentId: "bot",
        input: {},
        messages: [],
      });
      const lease = await store.claim(scope, id, runId);
      const events = [
        {
          type: "TEXT_MESSAGE_START",
          messageId: "assistant",
          role: "assistant",
        },
        { type: "TEXT_MESSAGE_CONTENT", messageId: "assistant", delta: "same" },
        { type: "TEXT_MESSAGE_CONTENT", messageId: "assistant", delta: "same" },
        {
          type: "TOOL_CALL_START",
          parentMessageId: "assistant",
          toolCallId: "one",
          toolCallName: "first",
        },
        { type: "TOOL_CALL_ARGS", toolCallId: "one", delta: "{}" },
        {
          type: "TOOL_CALL_START",
          parentMessageId: "assistant",
          toolCallId: "two",
          toolCallName: "second",
        },
        { type: "TOOL_CALL_ARGS", toolCallId: "two", delta: '{"n":2}' },
        {
          type: "ACTIVITY_SNAPSHOT",
          messageId: "card",
          activityType: "a2ui",
          content: { title: "old", items: [1, 2] },
        },
        {
          type: "ACTIVITY_DELTA",
          messageId: "card",
          patch: [
            { op: "replace", path: "/title", value: "new" },
            { op: "copy", from: "/items/0", path: "/items/-" },
            { op: "move", from: "/items/1", path: "/items/0" },
            { op: "test", path: "/title", value: "new" },
          ],
        },
      ];
      for (const [sequence, event] of events.entries()) {
        await store.append(lease, event, sequence);
        await store.append(lease, event, sequence);
      }
      await expect(
        store.append(lease, { type: "CUSTOM", name: "conflict" }, 1),
      ).rejects.toMatchObject({ code: "conflict" });
      const snapshot = await store.snapshot(scope, id);
      expect(snapshot.cursor).toBe(events.length);
      expect(snapshot.messages[0]).toMatchObject({
        content: "samesame",
        toolCalls: [
          { id: "one", function: { name: "first", arguments: "{}" } },
          { id: "two", function: { name: "second", arguments: '{"n":2}' } },
        ],
      });
      expect(snapshot.messages[1]).toMatchObject({
        role: "activity",
        content: { title: "new", items: [2, 1, 1] },
      });
      await store.finish(lease, {
        status: "succeeded",
        event: { type: "RUN_FINISHED" },
      });
      const replay = [];
      for await (const event of new DurableService(store, 10).stream(
        scope,
        id,
        { snapshot: true },
      ))
        replay.push(event);
      expect(replay[0].event.messages).toEqual(snapshot.messages);
    });
    test("actual PostgreSQL rejects event/finalization writes without false successful status", async () => {
      const triggerName = `fault_${crypto.randomUUID().replaceAll("-", "")}`;
      await store.sql.unsafe(
        `CREATE FUNCTION durable.${triggerName}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.tenant_id = '${tenantId}' AND NEW.event->>'type' IN ('CUSTOM','RUN_FINISHED') THEN RAISE EXCEPTION 'injected durability failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER ${triggerName} BEFORE INSERT ON durable.events FOR EACH ROW EXECUTE FUNCTION durable.${triggerName}()`,
      );
      try {
        for (const fault of ["append", "finish"]) {
          const id = crypto.randomUUID();
          await store.create(scope, { id, ownerId: "alice", agentId: "bot" });
          const runId = crypto.randomUUID();
          const service = new DurableService(store, 10);
          await service.submit(
            scope,
            {
              threadId: id,
              runId,
              key: runId,
              agentId: "bot",
              input: {},
              messages: [],
            },
            async ({ emit }) => {
              await emit({
                type: fault === "append" ? "CUSTOM" : "RUN_STARTED",
                name: "fault",
              });
              return {
                terminalEvent: { type: "RUN_FINISHED" },
                messages: [
                  {
                    id: "final",
                    role: "assistant",
                    content: "must-not-commit",
                  },
                ],
              };
            },
          );
          await service.drain();
          expect((await store.run(scope, id))?.status).toBe("failed");
          expect(
            (await store.events(scope, id, 0)).some(
              (row) => row.event.type === "RUN_FINISHED",
            ),
          ).toBe(false);
          expect((await store.snapshot(scope, id)).messages).toHaveLength(0);
        }
      } finally {
        await store.sql.unsafe(
          `DROP TRIGGER ${triggerName} ON durable.events; DROP FUNCTION durable.${triggerName}()`,
        );
      }
    });
  },
);
