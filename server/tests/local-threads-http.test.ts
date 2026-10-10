import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { HttpAgent } from "@ag-ui/client";
import {
  DurableError,
  DurableService,
  PostgresStore,
} from "@openbot/durable-core";
import { Hono } from "hono";
import { loadConfig } from "../src/config";
import { mountCopilotRuntime } from "../src/copilot";
import { holdLocalAgentStream, LocalThreads } from "../src/local-threads";

const url = process.env.OPENBOT_DURABLE_TEST_DATABASE_URL;
(url ? describe : describe.skip)(
  "authenticated local HTTP and external AG-UI",
  () => {
    const tenantId = `http-${crypto.randomUUID()}`;
    const store = new PostgresStore(
      url!,
      async (scope, _action, thread) => {
        if (
          thread.ownerId !== scope.principalId &&
          !(thread.metadata.shared === true && scope.principalId === "member")
        )
          throw new DurableError("forbidden", "Owner or member required");
      },
      { max: 2 },
    );
    const service = new DurableService(store, 10);
    let backend = new LocalThreads(service, tenantId);
    let calls = 0;
    const prompts: unknown[][] = [];
    let remote: ReturnType<typeof Bun.serve>;
    let server: ReturnType<typeof Bun.serve>;
    const threadId = crypto.randomUUID();
    const config = loadConfig({
      THREADS_BACKEND: "local",
      DATABASE_URL: url!,
      KEY_ENCRYPTION_KEY: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
      OPENBOT_SINGLE_USER: "true",
    });
    function app() {
      const resolver = {
        resolveAgentsForActor: async () => ({
          bot: new HttpAgent({
            url: `http://127.0.0.1:${remote.port}/agent`,
            agentId: "bot",
          }),
        }),
        resolveAgentForActor: async () =>
          new HttpAgent({
            url: `http://127.0.0.1:${remote.port}/agent`,
            agentId: "bot",
          }),
      };
      const actor = async (request: Request) => ({
        id: request.headers.get("x-test-session")!,
        role: "user" as const,
      });
      const runtime = mountCopilotRuntime(
        config,
        resolver as never,
        async (request) => ({ id: (await actor(request)).id, name: "test" }),
        actor,
        { localThreads: backend },
      );
      const app = new Hono();
      app.use("*", async (context, next) => {
        if (!context.req.header("x-test-session"))
          return context.json({ error: "Sign in" }, 401);
        await next();
      });
      app.route("/", runtime.handler);
      return app;
    }
    beforeAll(async () => {
      await store.migrate();
      remote = Bun.serve({
        port: 0,
        hostname: "127.0.0.1",
        fetch: async (request) => {
          const input = await request.json();
          prompts.push(input.messages);
          calls++;
          const messageId = `answer-${input.runId}`;
          const content = input.messages.at(-1)?.content;
          if (content === "flood") {
            const events = [
              {
                type: "RUN_STARTED",
                threadId: input.threadId,
                runId: input.runId,
              },
              { type: "TEXT_MESSAGE_START", messageId, role: "assistant" },
              ...Array.from({ length: 200 }, () => ({
                type: "TEXT_MESSAGE_CONTENT",
                messageId,
                delta: "x",
              })),
              { type: "TEXT_MESSAGE_END", messageId },
              {
                type: "RUN_FINISHED",
                threadId: input.threadId,
                runId: input.runId,
              },
            ];
            return new Response(
              events
                .map((event) => `data: ${JSON.stringify(event)}\n\n`)
                .join(""),
              { headers: { "content-type": "text/event-stream" } },
            );
          }
          if (content === "unfinished")
            return new Response(
              `data: ${JSON.stringify({ type: "RUN_STARTED", threadId: input.threadId, runId: input.runId })}\n\n`,
              { headers: { "content-type": "text/event-stream" } },
            );
          if (content === "quiet") {
            // Says nothing for longer than the API server's idle timeout, as a slow model does.
            return new Response(
              new ReadableStream({
                async start(controller) {
                  const emit = (event: unknown) =>
                    controller.enqueue(
                      new TextEncoder().encode(
                        `data: ${JSON.stringify(event)}\n\n`,
                      ),
                    );
                  emit({
                    type: "RUN_STARTED",
                    threadId: input.threadId,
                    runId: input.runId,
                  });
                  await Bun.sleep(3_500);
                  emit({
                    type: "TEXT_MESSAGE_START",
                    messageId,
                    role: "assistant",
                  });
                  emit({
                    type: "TEXT_MESSAGE_CONTENT",
                    messageId,
                    delta: "quiet answer",
                  });
                  emit({ type: "TEXT_MESSAGE_END", messageId });
                  emit({
                    type: "RUN_FINISHED",
                    threadId: input.threadId,
                    runId: input.runId,
                  });
                  controller.close();
                },
              }),
              { headers: { "content-type": "text/event-stream" } },
            );
          }
          if (content === "slow") {
            let timer: ReturnType<typeof setTimeout>;
            return new Response(
              new ReadableStream({
                start(controller) {
                  const emit = (event: unknown) =>
                    controller.enqueue(
                      new TextEncoder().encode(
                        `data: ${JSON.stringify(event)}\n\n`,
                      ),
                    );
                  emit({
                    type: "RUN_STARTED",
                    threadId: input.threadId,
                    runId: input.runId,
                  });
                  timer = setTimeout(() => {
                    emit({
                      type: "TEXT_MESSAGE_START",
                      messageId,
                      role: "assistant",
                    });
                    emit({
                      type: "TEXT_MESSAGE_CONTENT",
                      messageId,
                      delta: "detached answer",
                    });
                    emit({ type: "TEXT_MESSAGE_END", messageId });
                    emit({
                      type: "RUN_FINISHED",
                      threadId: input.threadId,
                      runId: input.runId,
                    });
                    controller.close();
                  }, 400);
                },
                cancel() {
                  clearTimeout(timer);
                },
              }),
              { headers: { "content-type": "text/event-stream" } },
            );
          }
          const events = [
            {
              type: "RUN_STARTED",
              threadId: input.threadId,
              runId: input.runId,
            },
            { type: "TEXT_MESSAGE_START", messageId, role: "assistant" },
            {
              type: "TEXT_MESSAGE_CONTENT",
              messageId,
              delta: `answer ${calls}`,
            },
            { type: "TEXT_MESSAGE_END", messageId },
            {
              type: "RUN_FINISHED",
              threadId: input.threadId,
              runId: input.runId,
            },
          ];
          return new Response(
            events
              .map((event) => `data: ${JSON.stringify(event)}\n\n`)
              .join(""),
            { headers: { "content-type": "text/event-stream" } },
          );
        },
      });
      server = Bun.serve({
        port: 0,
        hostname: "127.0.0.1",
        fetch: app().fetch,
      });
    });
    afterAll(async () => {
      server.stop(true);
      remote.stop(true);
      await service.drain();
      await store.sql`DELETE FROM durable.threads WHERE tenant_id=${tenantId}`;
      await store.close();
    });
    async function request(
      path: string,
      body?: unknown,
      user = "alice",
      method = body ? "POST" : "GET",
    ) {
      return fetch(`http://127.0.0.1:${server.port}/api/copilotkit${path}`, {
        method,
        headers: { "x-test-session": user, "content-type": "application/json" },
        body: body ? JSON.stringify(body) : undefined,
      });
    }
    function input(runId: string, message: string) {
      return {
        threadId,
        runId,
        messages: [{ id: `user-${runId}`, role: "user", content: message }],
        state: {},
        tools: [],
        context: [],
        forwardedProps: {},
      };
    }
    test("real HTTP run commits, restart restores history, followup seeds canonical history, duplicate runs replay", async () => {
      const first = input(crypto.randomUUID(), "first");
      const response = await request("/agent/bot/run", first);
      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toContain(
        "text/event-stream",
      );
      const stream = await response.text();
      expect(stream).toContain("RUN_FINISHED");
      expect(stream).toContain("id: ");
      expect(calls).toBe(1);
      const history = await (
        await request(`/threads/${threadId}/messages?agentId=bot`)
      ).json();
      expect(history.messages).toHaveLength(2);
      expect(history.messages[1].content).toBe("answer 1");
      // Rebuild the application facade/runtime against the durable database with no process history.
      server.stop(true);
      backend = new LocalThreads(new DurableService(store, 10), tenantId);
      server = Bun.serve({
        port: 0,
        hostname: "127.0.0.1",
        fetch: app().fetch,
      });
      const replay = await (
        await request("/agent/bot/connect", input(crypto.randomUUID(), ""))
      ).text();
      expect(replay).toContain("MESSAGES_SNAPSHOT");
      expect(replay).toContain("answer 1");
      const duplicate = await (await request("/agent/bot/run", first)).text();
      expect(duplicate).toContain("RUN_FINISHED");
      expect(calls).toBe(1);
      const follow = input(crypto.randomUUID(), "followup");
      expect(await (await request("/agent/bot/run", follow)).text()).toContain(
        "RUN_FINISHED",
      );
      expect(calls).toBe(2);
      expect(prompts[1]).toHaveLength(3);
      expect(prompts[1][1]).toMatchObject({ content: "answer 1" });
      const saved = await (
        await request(`/threads/${threadId}/messages`)
      ).json();
      expect(saved.messages).toHaveLength(4);
      expect(
        (
          await request("/agent/bot/run", {
            ...first,
            messages: [{ ...first.messages[0], content: "conflict" }],
          })
        ).status,
      ).toBe(409);
    }, 30_000);
    test("history/state/events/metadata/delete/run/stream/stop/is-running reject another owner; shared member can read", async () => {
      for (const path of [
        `/threads/${threadId}/messages`,
        `/threads/${threadId}/events`,
        `/threads/${threadId}/state`,
        `/threads/${threadId}/runs`,
      ])
        expect((await request(path, undefined, "bob")).status).toBe(403);
      expect(
        (
          await request(
            `/threads/${threadId}`,
            { name: "hijack" },
            "bob",
            "PATCH",
          )
        ).status,
      ).toBe(403);
      expect(
        (await request(`/threads/${threadId}`, undefined, "bob", "DELETE"))
          .status,
      ).toBe(403);
      expect(
        (
          await request(
            "/agent/bot/run",
            input(crypto.randomUUID(), "stolen"),
            "bob",
          )
        ).status,
      ).toBe(403);
      expect(
        (
          await request(
            "/agent/bot/connect",
            input(crypto.randomUUID(), ""),
            "bob",
          )
        ).status,
      ).toBe(403);
      expect(
        (await request(`/agent/bot/stop/${threadId}`, {}, "bob")).status,
      ).toBe(403);
      await store.update(backend.scope("alice"), threadId, { shared: true });
      expect(
        (await request(`/threads/${threadId}/messages`, undefined, "member"))
          .status,
      ).toBe(200);
      expect(
        (await fetch(`http://127.0.0.1:${server.port}/api/copilotkit/info`))
          .status,
      ).toBe(401);
      expect((await request("/info")).status).toBe(200);
    });
    test("actual public persistence middleware aborts on DB event failure and cannot emit success", async () => {
      const failing = input(crypto.randomUUID(), "fault");
      const original = store.append.bind(store);
      store.append = async () => {
        throw new Error("injected persistence failure");
      };
      try {
        const response = await request("/agent/bot/run", failing);
        const body = await response.text();
        expect(body).not.toContain("RUN_FINISHED");
        expect(body).toContain("RUN_ERROR");
        expect(
          (await store.run(backend.scope("alice"), threadId, failing.runId))
            ?.status,
        ).toBe("failed");
      } finally {
        store.append = original;
      }
    });
    test("disconnect detaches, stop persists cancellation, and malformed completion is never success", async () => {
      const detached = input(crypto.randomUUID(), "slow");
      const response = await request("/agent/bot/run", detached);
      await response.body!.cancel();
      await backend.service.drain();
      expect(
        (await store.run(backend.scope("alice"), threadId, detached.runId))
          ?.status,
      ).toBe("succeeded");
      expect(
        (await store.snapshot(backend.scope("alice"), threadId)).messages.some(
          (message) => message.content === "detached answer",
        ),
      ).toBe(true);
      const cancelled = input(crypto.randomUUID(), "slow");
      const running = await request("/agent/bot/run", cancelled);
      expect(
        (
          await request(
            `/agent/bot/stop/${threadId}`,
            { runId: cancelled.runId },
            "bob",
          )
        ).status,
      ).toBe(403);
      expect(
        await (
          await request(`/agent/bot/stop/${threadId}`, {
            runId: cancelled.runId,
          })
        ).json(),
      ).toEqual({ stopped: true });
      const text = await running.text();
      expect(text).toContain("RUN_ERROR");
      expect(text).not.toContain("RUN_FINISHED");
      expect(
        (await store.run(backend.scope("alice"), threadId, cancelled.runId))
          ?.status,
      ).toBe("cancelled");
      const unfinished = input(crypto.randomUUID(), "unfinished");
      const eof = await (await request("/agent/bot/run", unfinished)).text();
      expect(eof).not.toContain("RUN_FINISHED");
      expect(eof).toContain("RUN_ERROR");
      expect(
        (
          await request(
            `/agent/bot/stop/${threadId}`,
            undefined,
            "alice",
            "POST",
          )
        ).status,
      ).toBe(200);
    });
    test("slow storage bounds pending middleware events and fails rather than queueing arbitrary output", async () => {
      const original = store.append.bind(store);
      let appended = 0;
      store.append = async (...args) => {
        appended++;
        await new Promise((resolve) => setTimeout(resolve, 25));
        return original(...args);
      };
      const flood = input(crypto.randomUUID(), "flood");
      try {
        const text = await (await request("/agent/bot/run", flood)).text();
        expect(text).toContain("RUN_ERROR");
        expect(text).not.toContain("RUN_FINISHED");
        expect(appended).toBeLessThan(130);
        expect(
          (await store.run(backend.scope("alice"), threadId, flood.runId))
            ?.status,
        ).toBe("failed");
      } finally {
        store.append = original;
      }
    });
    test("a channel's thread opens empty before its first run", async () => {
      const fresh = crypto.randomUUID();
      const opened = await request("/agent/bot/connect", {
        ...input(crypto.randomUUID(), ""),
        threadId: fresh,
        messages: [],
      });
      expect(opened.status).toBe(200);
      expect(opened.headers.get("content-type")).toContain("text/event-stream");
      expect(await opened.text()).not.toContain("RUN_ERROR");
      const empty = await request(`/threads/${fresh}/messages?agentId=bot`);
      expect(empty.status).toBe(200);
      expect((await empty.json()).messages).toEqual([]);
    });
    test("a quiet Bot's reply outlasts the server's idle timeout", async () => {
      // The Bot is silent for longer than this server's idle timeout after RUN_STARTED.
      const handler = app();
      const strict = Bun.serve({
        port: 0,
        hostname: "127.0.0.1",
        idleTimeout: 2,
        fetch: (request, server) => {
          holdLocalAgentStream(request, server);
          return handler.fetch(request);
        },
      });
      try {
        const stream = await (
          await fetch(
            `http://127.0.0.1:${strict.port}/api/copilotkit/agent/bot/run`,
            {
              method: "POST",
              headers: {
                "x-test-session": "alice",
                "content-type": "application/json",
              },
              body: JSON.stringify({
                ...input(crypto.randomUUID(), "quiet"),
                threadId: crypto.randomUUID(),
              }),
            },
          )
        ).text();
        expect(stream).toContain("quiet answer");
        expect(stream).toContain("RUN_FINISHED");
      } finally {
        strict.stop(true);
      }
    }, 15_000);
  },
);
