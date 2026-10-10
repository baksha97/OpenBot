import { expect, test } from "bun:test";
import { fileURLToPath } from "node:url";
import { eq } from "drizzle-orm";
import { PostgresStore } from "@openbot/durable-core";
import { createDatabase } from "../src/db/client";
import { agentProfiles, agents, channels } from "../src/db/schema";
import { TEST_POOL, testDatabaseUrl } from "./support/database";

/** The real application/auth/resolver/governance path, with an external AG-UI endpoint. */
test(
  "local application saves tools/activity, survives process kill, reconnects and seeds a followup",
  async () => {
    const tenantId = `user-path-${crypto.randomUUID()}`;
    const threadId = crypto.randomUUID();
    const root = fileURLToPath(new URL("../..", import.meta.url));
    const prompts: Record<string, unknown>[][] = [];
    const remote = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch: async (request) => {
        const input = await request.json();
        prompts.push(input.messages);
        const id = `answer:${input.runId}`;
        const events = [
          { type: "RUN_STARTED", threadId: input.threadId, runId: input.runId },
          { type: "TEXT_MESSAGE_START", messageId: id, role: "assistant" },
          {
            type: "TOOL_CALL_START",
            toolCallId: `tool:${input.runId}`,
            toolCallName: "inspect",
            parentMessageId: id,
          },
          {
            type: "TOOL_CALL_ARGS",
            toolCallId: `tool:${input.runId}`,
            delta: "{}",
          },
          { type: "TOOL_CALL_END", toolCallId: `tool:${input.runId}` },
          {
            type: "TOOL_CALL_RESULT",
            messageId: `result:${input.runId}`,
            toolCallId: `tool:${input.runId}`,
            content: "tool outcome",
          },
          {
            type: "ACTIVITY_SNAPSHOT",
            messageId: `card:${input.runId}`,
            activityType: "test-card",
            content: { value: 1 },
          },
          {
            type: "ACTIVITY_DELTA",
            messageId: `card:${input.runId}`,
            activityType: "test-card",
            patch: [{ op: "replace", path: "/value", value: 2 }],
          },
          {
            type: "TEXT_MESSAGE_CONTENT",
            messageId: id,
            delta: "Saved tool-backed reply",
          },
          { type: "TEXT_MESSAGE_END", messageId: id },
          {
            type: "RUN_FINISHED",
            threadId: input.threadId,
            runId: input.runId,
          },
        ];
        return new Response(
          events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""),
          { headers: { "content-type": "text/event-stream" } },
        );
      },
    });
    let child: ReturnType<typeof Bun.spawn> | undefined;
    const output: Promise<string[]>[] = [];
    const portReservation = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch: () => new Response(),
    });
    const port = portReservation.port;
    portReservation.stop(true);
    const endpoint = `http://127.0.0.1:${port}`;
    async function start() {
      child = Bun.spawn(
        [process.execPath, "server/tests/fixtures/local-startup.ts"],
        {
          cwd: root,
          env: {
            PATH: process.env.PATH ?? "/usr/bin:/bin",
            HOME: "/tmp",
            THREADS_BACKEND: "local",
            DATABASE_URL: testDatabaseUrl(),
            TENANT_PACKAGE_DIR: `${root}/examples/fintech`,
            KEY_ENCRYPTION_KEY: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            OPENBOT_SINGLE_USER: "true",
            PORT: String(port),
            ...(process.env.OPENBOT_DURABLE_BROWSER
              ? { APP_DIST_DIR: `${root}/app/dist` }
              : {}),
            COPILOTKIT_TELEMETRY_DISABLED: "true",
            DO_NOT_TRACK: "1",
            OPENBOT_SELF_HOST_BANNER: "false",
            DEPLOYMENT_ID: tenantId,
            AGENT_ENDPOINT_ALLOWED_HOSTS: `127.0.0.1:${remote.port}`,
          },
          stdout: "pipe",
          stderr: "pipe",
        },
      );
      output.push(
        Promise.all([
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ]),
      );
      for (let i = 0; i < 100; i++) {
        await Bun.sleep(100);
        try {
          if ((await fetch(`${endpoint}/health`)).ok) return;
        } catch {}
      }
      throw new Error("Local application did not start");
    }
    const database = createDatabase(testDatabaseUrl(), TEST_POOL);
    const store = new PostgresStore(testDatabaseUrl(), async () => {}, {
      max: 2,
    });
    let botId: string | undefined;
    let browserChannelId: string | undefined;
    async function request(path: string, body?: unknown) {
      return fetch(`${endpoint}${path}`, {
        method: body ? "POST" : "GET",
        headers: { "content-type": "application/json" },
        body: body ? JSON.stringify(body) : undefined,
      });
    }
    function input(runId: string, content: string) {
      return {
        threadId,
        runId,
        messages: [{ id: `user:${runId}`, role: "user", content }],
        state: {},
        tools: [],
        context: [],
        forwardedProps: {},
      };
    }
    try {
      await start();
      const created = await request("/api/agents", {
        name: "Local User Path",
        title: "Tester",
        roleDescription: "Verify persistence.",
        visibility: "private",
        endpoint: `http://127.0.0.1:${remote.port}/agent`,
      });
      expect(created.status).toBe(201);
      botId = (await created.json()).agent.id;
      const first = input(crypto.randomUUID(), "first");
      const run = await request(`/api/copilotkit/agent/${botId}/run`, first);
      expect(run.status).toBe(200);
      expect(await run.text()).toContain("RUN_FINISHED");
      child!.kill("SIGKILL");
      await child!.exited;
      await start();
      const saved = await (
        await request(`/api/copilotkit/threads/${threadId}/messages`)
      ).json();
      expect(
        saved.messages.find(
          (message: Record<string, unknown>) => message.role === "activity",
        ),
      ).toMatchObject({ activityType: "test-card", content: { value: 2 } });
      expect(
        saved.messages.find(
          (message: Record<string, unknown>) => message.role === "tool",
        ),
      ).toMatchObject({ content: "tool outcome" });
      const replay = await (
        await request(
          `/api/copilotkit/agent/${botId}/connect`,
          input(crypto.randomUUID(), ""),
        )
      ).text();
      expect(replay).toContain("MESSAGES_SNAPSHOT");
      expect(replay).toContain("test-card");
      expect(replay).toContain('"value":2');
      expect(replay).toContain("tool outcome");
      expect(
        await (
          await request(
            `/api/copilotkit/agent/${botId}/run`,
            input(crypto.randomUUID(), "followup"),
          )
        ).text(),
      ).toContain("RUN_FINISHED");
      expect(prompts).toHaveLength(2);
      expect(
        prompts[1]?.some(
          (message) => message.content === "Saved tool-backed reply",
        ),
      ).toBe(true);
      if (process.env.OPENBOT_DURABLE_BROWSER) {
        const { chromium } = await import("playwright-core");
        const browser = await chromium.launch({
          headless: true,
          args: ["--no-sandbox"],
        });
        try {
          const page = await browser.newPage();
          const errors: string[] = [];
          page.on("pageerror", (error) => errors.push(error.message));
          const response = await request("/api/channels", {
            agentIds: [botId],
          });
          expect(response.status).toBe(201);
          const { channel } = await response.json();
          browserChannelId = channel.id;
          await request("/api/me/onboarding", { completed: true });
          await page.goto(`${endpoint}/channel/${channel.id}`);
          const composer = page.getByRole("textbox", {
            name: "Message",
            exact: true,
          });
          const messages = page.getByLabel("Messages", { exact: true });
          await composer.fill("Browser first turn");
          await composer.press("Enter");
          await messages
            .getByText("Saved tool-backed reply", { exact: true })
            .waitFor();
          await page
            .getByRole("button", { name: "Stop the Bot", exact: true })
            .waitFor({ state: "hidden" });
          await page.reload();
          await messages
            .getByText("Saved tool-backed reply", { exact: true })
            .waitFor();
          await composer.fill("Browser followup");
          await composer.press("Enter");
          await messages
            .getByText("Saved tool-backed reply", { exact: true })
            .nth(1)
            .waitFor();
          await page
            .getByRole("button", { name: "Stop the Bot", exact: true })
            .waitFor({ state: "hidden" });
          expect(errors).toEqual([]);
          expect(prompts).toHaveLength(4);
          expect(
            prompts[3]?.some(
              (message) => message.content === "Saved tool-backed reply",
            ),
          ).toBe(true);
        } finally {
          await browser.close();
        }
      }
      expect(
        prompts[1]?.some(
          (message) =>
            message.role === "tool" && message.content === "tool outcome",
        ),
      ).toBe(true);
    } finally {
      if (child) {
        child.kill("SIGKILL");
        await child.exited;
      }
      remote.stop(true);
      const logs = (await Promise.all(output)).flat().join("\n");
      await store.sql`DELETE FROM durable.threads WHERE tenant_id=${tenantId}`;
      if (browserChannelId)
        await database
          .delete(channels)
          .where(eq(channels.id, browserChannelId));
      if (botId) {
        await database
          .delete(agentProfiles)
          .where(eq(agentProfiles.agentId, botId));
        await database.delete(agents).where(eq(agents.id, botId));
      }
      await store.close();
      await database.$client.close();
      expect(logs).not.toContain("FORBIDDEN_");
    }
  },
  process.env.OPENBOT_DURABLE_BROWSER ? 90_000 : 30_000,
);
