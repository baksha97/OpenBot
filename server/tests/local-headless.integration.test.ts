import {
  createGroupConversations,
  createGroupStore,
} from "../src/channels/group";
import { HeadlessToolSuspension } from "../src/computer/headless-tools";
import { afterAll, describe, expect, test } from "bun:test";
import {
  AbstractAgent,
  type BaseEvent,
  type RunAgentInput,
} from "@ag-ui/client";
import {
  DurableError,
  DurableService,
  PostgresStore,
} from "@openbot/durable-core";
import { Observable } from "rxjs";
import { createHandoffDelivery } from "../src/agents/handoff-delivery";
import { createDurableFollowupReconciler } from "../src/durable-followups";
import { createTurnRunner } from "../src/routines/run-turn";
import { LocalThreads } from "../src/local-threads";
import { durableRunId } from "../src/run-identity";
import { createDatabase } from "../src/db/client";
import { createWorkQueue } from "../src/work/queue";
import {
  workItems,
  users,
  channels,
  channelMemberships,
} from "../src/db/schema";
import { like, eq, or } from "drizzle-orm";
import { TEST_POOL, testDatabaseUrl } from "./support/database";

const tenantId = `headless-${crypto.randomUUID()}`;
const scope = { tenantId, principalId: "owner" };
const revokedThreads = new Set<string>();
const readOnlyThreads = new Set<string>();
const store = new PostgresStore(
  testDatabaseUrl(),
  async (scope, action, thread) => {
    if (
      revokedThreads.has(thread.id) ||
      (readOnlyThreads.has(thread.id) && action !== "read") ||
      (scope.principalId !== thread.ownerId &&
        !(scope.principalId === "member" && thread.metadata.shared === true))
    )
      throw new DurableError("forbidden", "Owner or current member required");
  },
  { max: 2 },
);
const service = new DurableService(store, 5);
const backend = new LocalThreads(service, tenantId);
const database = createDatabase(testDatabaseUrl(), TEST_POOL);
let calls = 0;
const groupOwner = `${tenantId}:group-owner`;
const groupChannel = `${tenantId}:group-channel`;
class ScriptAgent extends AbstractAgent {
  constructor(
    private readonly events?: (
      input: RunAgentInput,
    ) => Record<string, unknown>[],
    agentId = "bot",
  ) {
    super({ agentId });
  }
  run(input: RunAgentInput) {
    calls++;
    return new Observable<BaseEvent>((subscriber) => {
      const id = `answer:${input.runId}`;
      const events = this.events?.(input) ?? [
        { type: "RUN_STARTED", threadId: input.threadId, runId: input.runId },
        { type: "TEXT_MESSAGE_START", messageId: id, role: "assistant" },
        {
          type: "TEXT_MESSAGE_CONTENT",
          messageId: id,
          delta: "Durable headless answer",
        },
        { type: "TEXT_MESSAGE_END", messageId: id },
        { type: "RUN_FINISHED", threadId: input.threadId, runId: input.runId },
      ];
      for (const event of events) subscriber.next(event as BaseEvent);
      subscriber.complete();
    });
  }
}
afterAll(async () => {
  await service.drain();
  await store.sql`DELETE FROM durable.threads WHERE tenant_id=${tenantId}`;
  await database
    .delete(workItems)
    .where(
      or(
        like(workItems.key, `relay:${tenantId}%`),
        like(workItems.key, `${tenantId}%`),
      ),
    );
  await database.delete(channels).where(eq(channels.id, groupChannel));
  await database.delete(users).where(eq(users.id, groupOwner));
  await store.close();
  await database.$client.close();
});
describe("local foreground and headless share durable execution", () => {
  test("successful routine retry returns persisted answer without duplicate execution or turn", async () => {
    await store.migrate();
    const threadId = crypto.randomUUID();
    const runId = durableRunId(`${tenantId}:routine`);
    let initialized = 0;
    let finalized = 0;
    const turn = createTurnRunner({
      backendForOwner: (owner) => backend.headless(owner),
      buildAgentFor: async () => {
        const agent = new ScriptAgent();
        agent.subscribe({
          onRunInitialized: () => {
            initialized++;
          },
          onRunFinalized: () => {
            finalized++;
          },
        });
        return agent;
      },
    });
    const input = {
      ownerUserId: "owner",
      routineId: "routine",
      agentId: "bot",
      threadId,
      instruction: "Report once",
      runId,
    };
    const before = calls;
    expect((await turn(input)).replyText).toBe("Durable headless answer");
    expect((await turn(input)).replyText).toBe("Durable headless answer");
    expect(calls - before).toBe(1);
    expect(initialized).toBe(1);
    expect(finalized).toBe(1);
    expect((await store.snapshot(scope, threadId)).messages).toHaveLength(2);
  });
  test("handoff retry retains result and reconciles relay once after acknowledgement fault", async () => {
    const key = `${tenantId}:handoff`;
    const threadId = crypto.randomUUID();
    await backend.ensure(scope, threadId, "bot");
    const scratch = durableRunId(`${key}:scratch`);
    const runId = durableRunId(`${key}:run`);
    const delivery = createHandoffDelivery({
      agentFor: async () => new ScriptAgent(),
      history: async () => [],
      runnerForActor: (owner) => backend.runner(backend.scope(owner)) as never,
      mintThreadId: () => crypto.randomUUID(),
      newRunId: () => crypto.randomUUID(),
      threadForKey: () => scratch,
      runForKey: () => runId,
      lock: {
        acquire: async (input) => {
          await backend.ensure(scope, input.threadId, input.agentId);
          return { runId: input.runId };
        },
        renew: async () => {},
        release: async () => {},
      },
    });
    const input = {
      deliveryKey: key,
      work: {
        fromBotId: "from",
        toBotId: "bot",
        actorId: "owner",
        threadId,
        runId: "asking",
        depth: 1,
        task: "Find the answer",
      },
      message: "Internal prompt",
      shown: "From asked Bot for help",
      assertion: "signed-first",
    };
    const before = calls;
    expect(await delivery.deliver(input)).toEqual({
      answer: "Durable headless answer",
      durable: true,
    });
    expect(
      await delivery.deliver({
        ...input,
        assertion: "new-signed-token-on-retry",
      }),
    ).toEqual({ answer: "Durable headless answer", durable: true });
    expect(calls - before).toBe(1);
    const saved = await store.snapshot(scope, scratch);
    expect(saved.messages).toHaveLength(2);
    expect(saved.messages[0]?.content).toBe(input.shown);
    expect(await store.pending(scope, scratch)).toHaveLength(1);
    const queue = createWorkQueue(database);
    const reconcile = createDurableFollowupReconciler(backend, queue);
    const original = store.acknowledge.bind(store);
    store.acknowledge = async () => {
      throw new Error("ack crash");
    };
    try {
      await reconcile();
    } finally {
      store.acknowledge = original;
    }
    // A new reconciler models restart after offer committed but acknowledgement did not.
    await store.sql`UPDATE durable.followups SET next_attempt_at=clock_timestamp() WHERE tenant_id=${tenantId}`;
    await createDurableFollowupReconciler(backend, queue)();
    await reconcile();
    expect(await store.pending(scope, scratch)).toHaveLength(0);
    const relays = await database
      .select()
      .from(workItems)
      .where(like(workItems.key, `relay:${key}`));
    expect(relays).toHaveLength(1);
    expect(JSON.stringify(relays[0]?.payload)).toContain(
      "Durable headless answer",
    );
  });
  test("approval resume updates existing assistant and preserves tool result in canonical history", async () => {
    const threadId = crypto.randomUUID();
    await backend.ensure(scope, threadId, "bot");
    const assistant = {
      id: "approval-parent",
      role: "assistant" as const,
      content: "Awaiting approval",
      toolCalls: [
        {
          id: "approval-call",
          type: "function" as const,
          function: { name: "approve", arguments: "{}" },
        },
      ],
    };
    const first = await store.submit(scope, {
      threadId,
      runId: crypto.randomUUID(),
      key: "seed",
      agentId: "bot",
      input: {},
      messages: [assistant],
    });
    await store.finish(await store.claim(scope, threadId, first.run.id), {
      status: "succeeded",
      event: { type: "RUN_FINISHED" },
    });
    const tool = {
      id: "approval-result",
      role: "tool" as const,
      toolCallId: "approval-call",
      content: "Approved",
    };
    const agent = new ScriptAgent((input) => [
      { type: "RUN_STARTED", threadId, runId: input.runId },
      {
        type: "MESSAGES_SNAPSHOT",
        messages: [{ ...assistant, content: "Approval completed" }, tool],
      },
      { type: "RUN_FINISHED", threadId, runId: input.runId },
    ]);
    const runner = backend.runner(scope);
    await new Promise<void>((resolve, reject) =>
      runner
        .run({
          threadId,
          agent,
          input: {
            threadId,
            runId: crypto.randomUUID(),
            messages: [assistant, tool],
            tools: [],
            context: [],
            state: {},
            forwardedProps: {},
            resume: [
              {
                interruptId: "approval-call",
                status: "resolved",
                payload: "Approved",
              },
            ],
          },
          persistedInputMessages: [tool],
        })
        .subscribe({ error: reject, complete: resolve }),
    );
    const saved = await store.snapshot(scope, threadId);
    expect(saved.messages).toHaveLength(2);
    expect(saved.messages[0]?.content).toBe("Approval completed");
    expect(saved.messages[1]).toMatchObject({
      id: "approval-result",
      toolCallId: "approval-call",
      content: "Approved",
    });
    expect(await store.runMessages(scope, threadId, first.run.id)).toEqual([
      assistant,
    ]);
  });
  test("revoked actor and unknown intent ahead of a valid intent cannot starve reconciliation", async () => {
    const poisonThread = crypto.randomUUID();
    await backend.ensure(scope, poisonThread, "bot");
    const poisonRun = crypto.randomUUID();
    await store.submit(scope, {
      threadId: poisonThread,
      runId: poisonRun,
      key: poisonRun,
      agentId: "bot",
      input: {},
      messages: [],
    });
    const poisonWork = {
      fromBotId: "from",
      toBotId: "bot",
      actorId: "owner",
      threadId: poisonThread,
      runId: poisonRun,
      depth: 1,
      task: "Revoked",
    };
    await store.finish(await store.claim(scope, poisonThread, poisonRun), {
      status: "succeeded",
      event: { type: "RUN_FINISHED" },
      followups: [
        {
          id: "revoked",
          payload: {
            kind: "handoff-result",
            work: poisonWork,
            key: `${tenantId}:revoked`,
            answer: "secret",
          },
        },
      ],
    });
    revokedThreads.add(poisonThread);
    const readOnlyThread = crypto.randomUUID();
    await backend.ensure(scope, readOnlyThread, "bot");
    const readOnlyRun = crypto.randomUUID();
    await store.submit(scope, {
      threadId: readOnlyThread,
      runId: readOnlyRun,
      key: readOnlyRun,
      agentId: "bot",
      input: {},
      messages: [],
    });
    await store.finish(await store.claim(scope, readOnlyThread, readOnlyRun), {
      status: "succeeded",
      event: { type: "RUN_FINISHED" },
      followups: [
        {
          id: "read-only",
          payload: {
            kind: "handoff-result",
            work: {
              ...poisonWork,
              threadId: readOnlyThread,
              runId: readOnlyRun,
            },
            key: `${tenantId}:read-only`,
            answer: "must not relay",
          },
        },
      ],
    });
    readOnlyThreads.add(readOnlyThread);
    expect((await store.get(scope, readOnlyThread)).id).toBe(readOnlyThread);
    const threadId = crypto.randomUUID();
    await store.create(scope, {
      id: threadId,
      ownerId: "owner",
      agentId: "bot",
      metadata: { shared: true },
    });
    const memberScope = backend.scope("member");
    const runId = crypto.randomUUID();
    await store.submit(memberScope, {
      threadId,
      runId,
      key: runId,
      agentId: "bot",
      input: {},
      messages: [],
    });
    const work = {
      fromBotId: "from",
      toBotId: "bot",
      actorId: "member",
      threadId,
      runId,
      depth: 1,
      task: "Relay",
    };
    await store.finish(await store.claim(memberScope, threadId, runId), {
      status: "succeeded",
      event: { type: "RUN_FINISHED" },
      followups: [
        {
          id: "poison",
          payload: {
            kind: "handoff-result",
            work: { ...work, actorId: "revoked" },
            key: `${tenantId}:poison`,
            answer: "private",
          },
        },
        { id: "unknown", payload: { kind: "unsupported" } },
        {
          id: "valid",
          payload: {
            kind: "handoff-result",
            work,
            key: `${tenantId}:valid`,
            answer: "visible",
          },
        },
      ],
    });
    await createDurableFollowupReconciler(backend, createWorkQueue(database))();
    const rows =
      await store.sql`SELECT id,attempts,last_error,completed_at FROM durable.followups WHERE tenant_id=${tenantId} AND thread_id=${threadId}`;
    expect(rows.find((row) => row.id === "poison")).toMatchObject({
      attempts: 1,
      last_error: "delivery_deferred",
      completed_at: null,
    });
    expect(rows.find((row) => row.id === "unknown")).toMatchObject({
      last_error: "unsupported_kind",
      completed_at: null,
    });
    expect(rows.find((row) => row.id === "valid")?.completed_at).not.toBeNull();
    expect(
      await database
        .select()
        .from(workItems)
        .where(like(workItems.key, `relay:${tenantId}:read-only`)),
    ).toHaveLength(0);
    expect(await store.pending(scope, readOnlyThread)).toHaveLength(1);
    const relays = await database
      .select()
      .from(workItems)
      .where(like(workItems.key, `relay:${tenantId}:valid`));
    expect(relays).toHaveLength(1);
    expect(
      await database
        .select()
        .from(workItems)
        .where(like(workItems.key, `relay:${tenantId}:poison`)),
    ).toHaveLength(0);
    expect(
      await database
        .select()
        .from(workItems)
        .where(like(workItems.key, `relay:${tenantId}:revoked`)),
    ).toHaveLength(0);
    expect(
      (
        await store.sql`SELECT attempts FROM durable.followups WHERE tenant_id=${tenantId} AND id='revoked'`
      )[0]?.attempts,
    ).toBe(1);
  });
  test("headless approval suspension and successful retry retain waiting without repeating the tool", async () => {
    const threadId = crypto.randomUUID();
    const runId = crypto.randomUUID();
    let executions = 0;
    const before = calls;
    const turn = createTurnRunner({
      backendForOwner: (owner) => backend.headless(owner),
      buildAgentFor: async () =>
        new ScriptAgent((input) => [
          { type: "RUN_STARTED", threadId, runId: input.runId },
          {
            type: "TOOL_CALL_START",
            toolCallId: "waiting-call",
            toolCallName: "approve",
            parentMessageId: "waiting-parent",
          },
          { type: "TOOL_CALL_ARGS", toolCallId: "waiting-call", delta: "{}" },
          { type: "TOOL_CALL_END", toolCallId: "waiting-call" },
          { type: "RUN_FINISHED", threadId, runId: input.runId },
        ]),
      toolsForTurn: async () => [
        {
          definition: {
            name: "approve",
            description: "Needs approval",
            parameters: { type: "object" },
          },
          execute: async () => {
            executions++;
            throw new HeadlessToolSuspension("Awaiting approval", {
              kind: "approval",
              requestId: "approval-request",
            });
          },
        },
      ],
    });
    const input = {
      ownerUserId: "owner",
      routineId: "approval",
      agentId: "bot",
      threadId,
      instruction: "Wait for approval",
      runId,
    };
    await expect(turn(input)).rejects.toBeInstanceOf(HeadlessToolSuspension);
    await expect(turn(input)).rejects.toBeInstanceOf(HeadlessToolSuspension);
    expect(executions).toBe(1);
    expect(calls - before).toBe(1);
    expect(
      (await store.snapshot(scope, threadId)).messages.some(
        (message) => message.id === "waiting-parent",
      ),
    ).toBe(true);
    expect(
      (await store.events(scope, threadId, 0)).some(
        (entry) =>
          entry.event.type === "CUSTOM" &&
          entry.event.name === "openbot.headless.waiting",
      ),
    ).toBe(true);
  });
  test("group queue runs two Bots through local durability and duplicate send creates no new turns", async () => {
    await database.insert(users).values({
      id: groupOwner,
      email: `${groupOwner}@example.test`,
      name: "Local Group",
    });
    await database.insert(channels).values({
      id: groupChannel,
      name: "Local Group",
      description: "Durable local group",
    });
    await database
      .insert(channelMemberships)
      .values({ channelId: groupChannel, userId: groupOwner });
    const runTurn = createTurnRunner({
      backendForOwner: (owner) => backend.headless(owner),
      buildAgentFor: async ({ agentId }) => new ScriptAgent(undefined, agentId),
    });
    const group = createGroupConversations({
      store: createGroupStore(database, () => crypto.randomUUID()),
      queue: createWorkQueue(database),
      owner: `${tenantId}:worker`,
      roster: async () => [
        { id: "bot", name: "Ada" },
        { id: "bot-two", name: "Grace" },
      ],
      runTurn,
      caps: { maxDepth: 1, maxPerRun: 2 },
      mayAddress: async () => false,
      auditStore: { insert: async () => {} },
      progressEveryMs: 0,
    });
    const message = {
      id: `${tenantId}:group-message`,
      text: "Both answer once",
    };
    const before = calls;
    await group.send(groupOwner, groupChannel, message);
    await group.send(groupOwner, groupChannel, message);
    for (let i = 0; i < 5 && (await group.sweep()); i++) {}
    const saved = await group.list(groupOwner, groupChannel);
    expect(saved.messages).toHaveLength(3);
    expect(
      saved.messages
        .filter((message) => message.agentId !== null)
        .map((message) => message.status),
    ).toEqual(["completed", "completed"]);
    expect(calls - before).toBe(2);
    const runs =
      await store.sql`SELECT id,status FROM durable.runs WHERE tenant_id=${tenantId} AND principal_id=${groupOwner}`;
    expect(runs).toHaveLength(2);
    expect(runs.every((run) => run.status === "succeeded")).toBe(true);
  });
});
