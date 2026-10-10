import { afterAll, describe, expect, test } from "bun:test";
import { DurableError, PostgresStore } from "@openbot/durable-core";
import { and, eq, inArray } from "drizzle-orm";
import { createDatabase } from "../src/db/client";
import {
  agentProfiles,
  agents,
  channelMemberships,
  channels,
  intelligenceChannelMappings,
  users,
} from "../src/db/schema";
import { groupBotThreads } from "../src/db/schema/group";
import { createAgentProfileStore } from "../src/agents/profile-store";
import { createChannelStore } from "../src/channels/routes";
import { createThreadIdentity } from "../src/channels/thread-identity";
import { localThreadPolicy } from "../src/local-thread-policy";
import { TEST_POOL, testDatabaseUrl } from "./support/database";

const database = createDatabase(testDatabaseUrl(), TEST_POOL);
const tenantId = `policy-${crypto.randomUUID()}`;
const profiles = createAgentProfileStore(
  database,
  new URL("https://agent.example.test"),
);
const channelStore = createChannelStore(
  database,
  profiles,
  createThreadIdentity(tenantId),
);
const ids = ["owner", "member", "stranger"].map((id) => `${tenantId}-${id}`);
const [ownerId, memberId, strangerId] = ids as [string, string, string];
const scope = (principalId: string) => ({ tenantId, principalId });
const store = new PostgresStore(
  testDatabaseUrl(),
  localThreadPolicy({
    tenantId,
    database,
    profiles,
    channels: channelStore,
    actorFor: async (id) => {
      if (!ids.includes(id))
        throw new DurableError("forbidden", "Unknown session actor");
      return { id, role: "user" };
    },
  }),
  { max: 2 },
);
let botId: string;
let channelId: string;
afterAll(async () => {
  await store.sql`DELETE FROM durable.threads WHERE tenant_id=${tenantId}`;
  if (channelId)
    await database.delete(channels).where(eq(channels.id, channelId));
  if (botId) {
    await database
      .delete(agentProfiles)
      .where(eq(agentProfiles.agentId, botId));
    await database.delete(agents).where(eq(agents.id, botId));
  }
  await database.delete(users).where(inArray(users.id, ids));
  await store.close();
  await database.$client.close();
});
describe("production durable authorization with real memberships", () => {
  test("direct histories stay private; group reads/writes require current membership and bot grants", async () => {
    await store.migrate();
    await database
      .insert(users)
      .values(ids.map((id) => ({ id, email: `${id}@example.test`, name: id })));
    botId = (
      await profiles.create(
        { id: ownerId, role: "user" },
        {
          name: "Policy Bot",
          title: "Tester",
          roleDescription: "Test access.",
          visibility: "public",
        },
      )
    ).id;
    const channel = await channelStore.create({ id: ownerId, role: "user" }, [
      botId,
    ]);
    channelId = channel.id;
    await database
      .insert(channelMemberships)
      .values({ channelId, userId: memberId });
    await database
      .insert(intelligenceChannelMappings)
      .values({ channelId, userId: memberId, threadId: crypto.randomUUID() });
    await store.create(scope(ownerId), {
      id: channel.threadId,
      ownerId,
      agentId: botId,
    });
    expect((await store.get(scope(ownerId), channel.threadId)).ownerId).toBe(
      ownerId,
    );
    await expect(
      store.get(scope(memberId), channel.threadId),
    ).rejects.toMatchObject({ code: "forbidden" });
    const shared = crypto.randomUUID();
    await store.create(scope(ownerId), { id: shared, ownerId, agentId: botId });
    await database.insert(groupBotThreads).values({
      ownerUserId: ownerId,
      channelId,
      agentId: botId,
      threadId: shared,
    });
    expect((await store.get(scope(memberId), shared)).id).toBe(shared);
    await store.update(scope(memberId), shared, { title: "Shared" });
    const runId = crypto.randomUUID();
    await store.submit(scope(memberId), {
      threadId: shared,
      runId,
      key: runId,
      agentId: botId,
      input: {},
      messages: [],
    });
    await store.cancel(scope(memberId), shared, runId);
    for (const action of [
      () => store.get(scope(strangerId), shared),
      () => store.snapshot(scope(strangerId), shared),
      () => store.events(scope(strangerId), shared, 0),
      () => store.run(scope(strangerId), shared),
      () => store.update(scope(strangerId), shared, {}),
      () => store.cancel(scope(strangerId), shared),
      () => store.delete(scope(strangerId), shared),
      () => store.get({ tenantId: "foreign", principalId: ownerId }, shared),
    ])
      await expect(action()).rejects.toMatchObject({
        code: expect.stringMatching(/forbidden|not_found/),
      });
    await expect(store.delete(scope(memberId), shared)).rejects.toMatchObject({
      code: "forbidden",
    });
    await database
      .delete(channelMemberships)
      .where(
        and(
          eq(channelMemberships.channelId, channelId),
          eq(channelMemberships.userId, memberId),
        ),
      );
    await expect(store.get(scope(memberId), shared)).rejects.toMatchObject({
      code: "forbidden",
    });
    // The owner is also subject to the channel's deletion boundary, not exempt by ownership.
    await database
      .update(channels)
      .set({ deletedAt: new Date() })
      .where(eq(channels.id, channelId));
    await expect(store.get(scope(ownerId), shared)).rejects.toMatchObject({
      code: "forbidden",
    });
  });
});
