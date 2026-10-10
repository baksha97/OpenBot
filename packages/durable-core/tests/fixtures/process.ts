import { DurableError, DurableService, PostgresStore } from "../../src";
const [mode, tenantId, threadId, runId, fence] = process.argv.slice(2);
const scope = { tenantId: tenantId!, principalId: "alice" };
const store = new PostgresStore(
  process.env.OPENBOT_DURABLE_TEST_DATABASE_URL!,
  async (scope, _action, thread) => {
    if (scope.principalId !== thread.ownerId)
      throw new DurableError("forbidden", "Not owner");
  },
);
const input = {
  threadId: threadId!,
  runId: runId!,
  key: runId!,
  agentId: "bot",
  input: { text: "process" },
  messages: [{ id: `input-${runId}`, role: "user", content: "process" }],
};
if (mode === "submit") {
  const result = await store.submit(scope, input);
  console.log(JSON.stringify({ created: result.created }));
  await store.close();
} else if (mode === "stale") {
  try {
    await store.append(
      { scope, threadId: threadId!, runId: runId!, fence: Number(fence) },
      { type: "CUSTOM", name: "stale" },
      0,
    );
    console.log("unexpected-success");
  } catch (error) {
    console.log(
      error instanceof DurableError ? error.code : "unexpected-error",
    );
  }
  await store.close();
} else if (mode === "crash") {
  const service = new DurableService(store, 10);
  await service.submit(scope, input, async ({ emit }) => {
    await emit({ type: "RUN_STARTED", threadId, runId });
    await emit({
      type: "TOOL_CALL_START",
      toolCallId: "uncertain-tool",
      toolCallName: "external_write",
    });
    await emit({
      type: "TOOL_CALL_ARGS",
      toolCallId: "uncertain-tool",
      delta: "{}",
    });
    await emit({ type: "TOOL_CALL_END", toolCallId: "uncertain-tool" });
    await store.sql`INSERT INTO durable.test_effects (tenant_id) VALUES (${tenantId!})`;
    console.log("effect-committed");
    await new Promise(() => {});
    return { terminalEvent: { type: "RUN_FINISHED" } };
  });
  await service.drain();
}
