import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { DurableError, DurableService, PostgresStore } from "../src";
const url = process.env.OPENBOT_DURABLE_TEST_DATABASE_URL;
(url ? describe : describe.skip)("real worker processes", () => {
  const tenantId = `process-${crypto.randomUUID()}`;
  const scope = { tenantId, principalId: "alice" };
  const store = new PostgresStore(
    url!,
    async (scope, _action, thread) => {
      if (scope.principalId !== thread.ownerId)
        throw new DurableError("forbidden", "Not owner");
    },
    { max: 2 },
  );
  beforeAll(async () => {
    await store.migrate();
    await store.sql`CREATE TABLE IF NOT EXISTS durable.test_effects (tenant_id text NOT NULL)`;
  });
  afterAll(async () => {
    await store.sql`DELETE FROM durable.test_effects WHERE tenant_id=${tenantId}`;
    await store.sql`DELETE FROM durable.threads WHERE tenant_id=${tenantId}`;
    await store.close();
  });
  async function thread() {
    const id = crypto.randomUUID();
    await store.create(scope, { id, ownerId: "alice", agentId: "bot" });
    return id;
  }
  function spawn(mode: string, id: string, run: string, fence?: number) {
    return Bun.spawn(
      [
        process.execPath,
        new URL("./fixtures/process.ts", import.meta.url).pathname,
        mode,
        tenantId,
        id,
        run,
        String(fence ?? ""),
      ],
      {
        env: { ...process.env, OPENBOT_DURABLE_TEST_DATABASE_URL: url },
        stdout: "pipe",
        stderr: "pipe",
      },
    );
  }
  test("four processes contend on one identity and accept only once", async () => {
    const id = await thread();
    const run = crypto.randomUUID();
    const children = Array.from({ length: 4 }, () => spawn("submit", id, run));
    const outputs = await Promise.all(
      children.map(async (child) => {
        const text = await new Response(child.stdout).text();
        expect(await child.exited).toBe(0);
        return JSON.parse(text);
      }),
    );
    expect(outputs.filter((output) => output.created)).toHaveLength(1);
    expect((await store.snapshot(scope, id)).messages).toHaveLength(1);
    const lease = await store.claim(scope, id, run);
    await store.sql`UPDATE durable.threads SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=${tenantId} AND id=${id}`;
    await store.recover(scope, id);
    const stale = spawn("stale", id, run, lease.fence);
    expect((await new Response(stale.stdout).text()).trim()).toBe("lease_lost");
    expect(await stale.exited).toBe(0);
  });
  test("SIGKILL after external effect preserves output and interrupts without replaying the tool", async () => {
    const id = await thread();
    const run = crypto.randomUUID();
    const child = spawn("crash", id, run);
    const reader = child.stdout.getReader();
    const ready = await reader.read();
    expect(new TextDecoder().decode(ready.value)).toContain("effect-committed");
    child.kill("SIGKILL");
    await child.exited;
    await store.sql`UPDATE durable.threads SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=${tenantId} AND id=${id}`;
    const fresh = new DurableService(store, 10);
    let executed = 0;
    const input = {
      threadId: id,
      runId: run,
      key: run,
      agentId: "bot",
      input: { text: "process" },
      messages: [{ id: `input-${run}`, role: "user", content: "process" }],
    };
    await fresh.submit(scope, input, async () => {
      executed++;
      return { terminalEvent: { type: "RUN_FINISHED" } };
    });
    const events = [];
    for await (const event of fresh.stream(scope, id, { runId: run }))
      events.push(event);
    expect(executed).toBe(0);
    expect((await store.run(scope, id, run))?.status).toBe("interrupted");
    expect(events.at(-1)?.event.type).toBe("RUN_ERROR");
    expect(
      (
        await store.sql`SELECT count(*) AS n FROM durable.test_effects WHERE tenant_id=${tenantId}`
      )[0].n,
    ).toBe("1");
    expect(
      (await store.snapshot(scope, id)).messages.some((message) =>
        Array.isArray(message.toolCalls),
      ),
    ).toBe(true);
  }, 10_000);
});
