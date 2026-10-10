import {
  AbstractAgent,
  type BaseEvent,
  type Message,
  Middleware,
  type RunAgentInput,
} from "@ag-ui/client";
import {
  AgentRunner,
  type AgentRunnerConnectRequest,
  type AgentRunnerIsRunningRequest,
  type AgentRunnerRunRequest,
  type AgentRunnerStopRequest,
} from "@copilotkit/runtime/v2";
import {
  DurableError,
  type DurableService,
  type Event,
  type Scope,
  type Thread,
  terminal,
} from "@openbot/durable-core";
import { Observable, concatMap, from, map, tap, finalize } from "rxjs";
import { sanitizeSeededHistory } from "./agents/history-sanitize";

/** The AG-UI routes answered with a durable SSE stream that lasts as long as its run. */
export const LOCAL_AGENT_STREAM_PATH = /\/agent\/[^/]+\/(run|connect)$/;

/**
 * Exempt a local run or connect stream from `Bun.serve`'s idle timeout.
 *
 * Bun closes a connection that has received nothing for its idle timeout (10 seconds by default),
 * and writing a response does not reset that clock. A model that takes longer than that to answer
 * lost the browser's stream after RUN_STARTED: the run still committed, but the reply only appeared
 * after a reload. The stream ends with its run, or when the client disconnects.
 */
export function holdLocalAgentStream(
  request: Request,
  server: { timeout(request: Request, seconds: number): void },
  basePath = "/api/copilotkit",
) {
  const { pathname } = new URL(request.url);
  if (
    pathname.startsWith(`${basePath}/`) &&
    LOCAL_AGENT_STREAM_PATH.test(pathname)
  )
    server.timeout(request, 0);
}

function missingThread(error: unknown) {
  return error instanceof DurableError && error.code === "not_found";
}

/** App facade: authentication supplies scope; the core's policy checks ownership/membership. */
export class LocalThreads {
  constructor(
    readonly service: DurableService,
    readonly tenantId: string,
  ) {}
  scope(principalId: string): Scope {
    return { tenantId: this.tenantId, principalId };
  }
  runner(scope: Scope, after?: number, http = false): LocalAgentRunner {
    return new LocalAgentRunner(this, scope, after, http);
  }
  async ensure(scope: Scope, threadId: string, agentId: string) {
    try {
      const thread = await this.service.store.get(scope, threadId);
      await this.service.store.bind(scope, threadId, agentId);
      return thread;
    } catch (error) {
      if (!(error instanceof DurableError) || error.code !== "not_found")
        throw error;
      return this.service.store.create(scope, {
        id: threadId,
        ownerId: scope.principalId,
        agentId,
      });
    }
  }
  /** Database history in the dialect the existing headless integration consumes. */
  async history(
    scope: Scope,
    threadId: string,
  ): Promise<{
    messages: {
      id: string;
      role: string;
      content?: unknown;
      toolCalls?: { id: string; name: string; args: string }[];
      toolCallId?: string;
    }[];
  }> {
    const { messages } = await this.service.store.snapshot(scope, threadId);
    return {
      messages: messages.map((message) => ({
        ...message,
        toolCallId: message.toolCallId as string | undefined,
        toolCalls: Array.isArray(message.toolCalls)
          ? (message.toolCalls as MessageToolCall[]).map((call) => ({
              id: call.id,
              name: call.function.name,
              args: call.function.arguments,
            }))
          : undefined,
      })),
    };
  }
  /** A scoped backend for the existing headless orchestration, with no platform locks. */
  headless(principalId: string) {
    const scope = this.scope(principalId);
    const runner = this.runner(scope);
    return {
      runner,
      completedMessages: async (threadId: string, runId: string) =>
        (await this.service.store.runMessages(
          scope,
          threadId,
          runId,
        )) as Message[],
      intelligence: {
        getOrCreateThread: async (input: {
          threadId: string;
          userId: string;
          agentId: string;
        }) => {
          if (input.userId !== principalId)
            throw new DurableError("forbidden", "Owner scope mismatch");
          return this.ensure(scope, input.threadId, input.agentId);
        },
        getThreadMessages: async (input: {
          threadId: string;
          userId: string;
        }) => {
          if (input.userId !== principalId)
            throw new DurableError("forbidden", "Owner scope mismatch");
          return this.history(scope, input.threadId);
        },
        // Compatibility with existing orchestration only. The durable service owns all DB leases.
        ɵacquireThreadLock: async (input: { threadId: string }) => {
          await this.service.store.get(scope, input.threadId);
        },
        ɵrenewThreadLock: async (input: { threadId: string }) => {
          await this.service.store.get(scope, input.threadId);
        },
        ɵcleanupThreadLock: async (input: { threadId: string }) => {
          await this.service.store.get(scope, input.threadId);
        },
      },
    };
  }
  async http(
    scope: Scope,
    request: Request,
    basePath: string,
  ): Promise<Response | undefined> {
    const url = new URL(request.url);
    const suffix = url.pathname.slice(basePath.length);
    const stop = suffix.match(/^\/agent\/([^/]+)\/stop\/([^/]+)$/);
    if (stop && request.method === "POST") {
      try {
        const threadId = decodeURIComponent(stop[2]);
        const thread = await this.service.store.get(scope, threadId);
        if (thread.agentId !== decodeURIComponent(stop[1]))
          throw new DurableError("forbidden", "Thread Bot mismatch");
        const raw = await request.text();
        let body: { runId?: string } | null;
        try {
          body = raw.trim() ? JSON.parse(raw) : {};
        } catch {
          throw new DurableError("invalid", "Invalid stop scope");
        }
        if (
          !body ||
          typeof body !== "object" ||
          Array.isArray(body) ||
          Object.keys(body).some((key) => key !== "runId") ||
          (body.runId !== undefined &&
            (typeof body.runId !== "string" || !body.runId.trim()))
        )
          throw new DurableError("invalid", "Invalid stop scope");
        return Response.json({
          stopped: await this.service.store.cancel(scope, threadId, body.runId),
        });
      } catch (error) {
        return localErrorResponse(error);
      }
    }
    if (suffix.startsWith("/inspector/") || suffix.startsWith("/trajectory/"))
      return Response.json(
        { error: "This capability requires Intelligence" },
        { status: 501 },
      );
    const match = suffix.match(
      /^\/threads(?:\/([^/]+))?(?:\/(messages|events|state|runs))?$/,
    );
    if (!match) return;
    const threadId = match[1] ? decodeURIComponent(match[1]) : undefined;
    try {
      const store = this.service.store;
      if (request.method === "GET") {
        if (!threadId)
          return Response.json({
            threads: (await store.list(scope)).map((thread) =>
              this.record(thread),
            ),
            hasMore: false,
          });
        /*
         * A channel's thread id exists before its first run creates the durable thread, and the app
         * reads its history as soon as the channel opens. Intelligence mode answers that read with
         * an empty history (`historyOrEmpty`), and so does this; only a missing thread qualifies,
         * so an unauthorized read of an existing one is still refused.
         */
        if (match[2] === "messages") {
          try {
            return Response.json({
              messages: (await store.snapshot(scope, threadId)).messages,
            });
          } catch (error) {
            if (missingThread(error)) return Response.json({ messages: [] });
            throw error;
          }
        }
        const thread = await store.get(scope, threadId);
        if (match[2] === "state")
          return Response.json({
            state: (await store.snapshot(scope, threadId)).thread.state,
          });
        if (match[2] === "events")
          return Response.json({
            events: await store.events(
              scope,
              threadId,
              Number(url.searchParams.get("after") ?? 0),
            ),
          });
        if (match[2] === "runs") {
          await store.recover(scope, threadId);
          return Response.json({ run: await store.run(scope, threadId) });
        }
        return Response.json({ thread: this.record(thread) });
      }
      if (request.method === "POST" && !threadId) {
        const body = (await request.json()) as {
          threadId?: string;
          agentId: string;
          name?: string;
        };
        const thread = await store.create(scope, {
          id: body.threadId ?? crypto.randomUUID(),
          ownerId: scope.principalId,
          agentId: body.agentId,
          metadata: { name: body.name ?? null },
        });
        return Response.json({ thread: this.record(thread) }, { status: 201 });
      }
      if (request.method === "PATCH" && threadId) {
        const patch = (await request.json()) as Record<string, unknown>;
        await store.update(scope, threadId, patch);
        return Response.json({
          thread: this.record(await store.get(scope, threadId)),
        });
      }
      if (request.method === "DELETE" && threadId) {
        await store.delete(scope, threadId);
        return Response.json({ deleted: true });
      }
      return Response.json(
        { error: "Unsupported local thread operation" },
        { status: 405 },
      );
    } catch (error) {
      return localErrorResponse(error);
    }
  }
  private record(thread: Thread) {
    return {
      id: thread.id,
      agentId: thread.agentId,
      createdById: thread.ownerId,
      organizationId: this.tenantId,
      name: thread.metadata.name ?? null,
      archived: thread.metadata.archived ?? false,
    };
  }
}
type MessageToolCall = {
  id: string;
  function: { name: string; arguments: string };
};
export function localErrorResponse(error: unknown) {
  const status =
    error instanceof DurableError
      ? (
          {
            forbidden: 403,
            not_found: 404,
            conflict: 409,
            invalid: 400,
            lease_lost: 409,
            cancelled: 409,
          } as const
        )[error.code]
      : 500;
  return Response.json(
    {
      error:
        error instanceof DurableError
          ? error.message
          : "Local thread operation failed",
    },
    { status },
  );
}

/** Public SDK adapter. Trusted scope is captured explicitly at each app boundary. */
export class LocalAgentRunner extends AgentRunner {
  private captureResolve!: (input: {
    threadId: string;
    runId?: string;
    after?: number;
    snapshot?: boolean;
    empty?: boolean;
  }) => void;
  private captureReject!: (error: unknown) => void;
  private readonly captured = new Promise<{
    threadId: string;
    runId?: string;
    after?: number;
    snapshot?: boolean;
    empty?: boolean;
  }>((resolve, reject) => {
    this.captureResolve = resolve;
    this.captureReject = reject;
  });
  constructor(
    private readonly backend: LocalThreads,
    private readonly scope: Scope,
    private readonly after?: number,
    private readonly http = false,
  ) {
    super();
    void this.captured.catch(() => {});
  }
  /** Pull one durable event per read; SDK setup runs first, with an empty transport subscription. */
  async response(signal: AbortSignal): Promise<Response> {
    try {
      const captured = await this.captured;
      // Connecting to a thread whose first run has not started: nothing to replay, as the SDK's
      // own runner answers for a thread it has never seen.
      if (captured.empty)
        return new Response("", {
          headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
          },
        });
      const detach = new AbortController();
      const abort = () => detach.abort();
      signal.addEventListener("abort", abort, { once: true });
      const iterator = this.backend.service.stream(
        this.scope,
        captured.threadId,
        { ...captured, signal: detach.signal },
      );
      const encoder = new TextEncoder();
      return new Response(
        new ReadableStream<Uint8Array>({
          pull: async (controller) => {
            try {
              const next = await iterator.next();
              if (next.done) {
                signal.removeEventListener("abort", abort);
                controller.close();
              } else
                controller.enqueue(
                  encoder.encode(
                    `id: ${next.value.cursor}\ndata: ${JSON.stringify(next.value.event)}\n\n`,
                  ),
                );
            } catch (error) {
              signal.removeEventListener("abort", abort);
              controller.error(error);
            }
          },
          cancel: async () => {
            detach.abort();
            signal.removeEventListener("abort", abort);
            await iterator.return(undefined);
          },
        }),
        {
          headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
          },
        },
      );
    } catch (error) {
      return localErrorResponse(error);
    }
  }
  run(
    request: AgentRunnerRunRequest & {
      completionIntents?: (
        messages: readonly Message[],
      ) => import("@openbot/durable-core").Followup[];
    },
  ): Observable<BaseEvent> {
    return new Observable((subscriber) => {
      const detach = new AbortController();
      void (async () => {
        const { service } = this.backend;
        await this.backend.ensure(
          this.scope,
          request.threadId,
          request.agent.agentId ?? "default",
        );
        const snapshot = await service.store.snapshot(
          this.scope,
          request.threadId,
        );
        const oldIds = new Set(snapshot.messages.map((message) => message.id));
        const accepted = (request.persistedInputMessages ??
          request.input.messages.filter(
            (message) => !oldIds.has(message.id),
          )) as import("@openbot/durable-core").Message[];
        const input = request.input;
        const resumeIds = new Set(
          (input.resume ?? []).map((entry) => entry.interruptId),
        );
        const mutableMessageIds = snapshot.messages
          .filter(
            (message) =>
              Array.isArray(message.toolCalls) &&
              (message.toolCalls as MessageToolCall[]).some((call) =>
                resumeIds.has(call.id),
              ),
          )
          .map((message) => message.id);
        const run = await service.submit(
          this.scope,
          {
            threadId: request.threadId,
            runId: input.runId,
            key: input.runId,
            agentId: request.agent.agentId ?? "default",
            input,
            messages: accepted,
            mutableMessageIds,
            fingerprint: {
              agentId: request.agent.agentId,
              input: request.persistedInputMessages
                ? {
                    ...input,
                    messages: request.persistedInputMessages,
                    state: undefined,
                    forwardedProps: input.forwardedProps?.openbotRun
                      ? { ...input.forwardedProps, openbotRun: undefined }
                      : input.forwardedProps,
                  }
                : input,
            },
          },
          async ({ signal, emit }) => {
            const agent = request.agent;
            const canonical = await service.store.snapshot(
              this.scope,
              request.threadId,
            );
            const modelMessages = request.persistedInputMessages
              ? request.input.messages
              : (canonical.messages as Message[]);
            agent.threadId = request.threadId;
            agent.setMessages(
              sanitizeSeededHistory(
                modelMessages,
                new Set(
                  (request.input.resume ?? []).map(
                    (entry) => entry.interruptId,
                  ),
                ),
              ),
            );
            agent.setState(canonical.thread.state as Record<string, unknown>);
            let end: BaseEvent | undefined;
            let persistenceError: unknown;
            const before = new Set(agent.messages.map((message) => message.id));
            const abort = () => agent.abortRun();
            signal.addEventListener("abort", abort, { once: true });
            const invocation = new DurableInvocation(agent);
            invocation.use(
              new PersistEvents(
                async (event) => {
                  if (signal.aborted) throw signal.reason;
                  if (
                    event.type === "RUN_FINISHED" ||
                    event.type === "RUN_ERROR"
                  ) {
                    end = event;
                    return;
                  }
                  try {
                    await emit(event as Event);
                  } catch (error) {
                    persistenceError = error;
                    agent.abortRun();
                    throw error;
                  }
                },
                () => agent.abortRun(),
              ),
            );
            let rejectAbort: (() => void) | undefined;
            try {
              const abortPromise = new Promise<never>((_, reject) => {
                rejectAbort = () => reject(signal.reason);
                signal.addEventListener("abort", rejectAbort, { once: true });
                if (signal.aborted) rejectAbort();
              });
              await Promise.race([
                invocation.runAgent(request.input),
                abortPromise,
              ]);
              if (persistenceError) throw persistenceError;
              if (end?.type !== "RUN_FINISHED")
                throw new Error(
                  "Agent ended without a successful terminal event",
                );
              if (
                "outcome" in end &&
                (end.outcome as { type?: string })?.type === "cancelled"
              )
                throw new DurableError("cancelled", "Agent cancelled");
              const outputs = agent.messages.filter(
                (message) =>
                  !before.has(message.id) ||
                  mutableMessageIds.includes(message.id),
              );
              return {
                terminalEvent: end as Event,
                followups: request.completionIntents?.(outputs),
                messages: outputs as import("@openbot/durable-core").Message[],
                state: agent.state,
              };
            } finally {
              signal.removeEventListener("abort", abort);
              if (rejectAbort) signal.removeEventListener("abort", rejectAbort);
            }
          },
        );
        if (this.http) {
          this.captureResolve({
            threadId: request.threadId,
            runId: run.id,
            after: run.startCursor,
          });
          subscriber.complete();
          return;
        }
        for await (const entry of service.stream(this.scope, request.threadId, {
          after: run.startCursor,
          runId: run.id,
          signal: detach.signal,
        }))
          subscriber.next(entry.event as BaseEvent);
        if (!detach.signal.aborted) subscriber.complete();
      })().catch((error) => {
        if (this.http) this.captureReject(error);
        subscriber.error(error);
      });
      // Detach only. Stopping is a separate persisted operation.
      return () => detach.abort();
    });
  }
  connect(request: AgentRunnerConnectRequest): Observable<BaseEvent> {
    return new Observable((subscriber) => {
      const detach = new AbortController();
      void (async () => {
        if (this.http) {
          try {
            await this.backend.service.store.get(this.scope, request.threadId);
          } catch (error) {
            if (!missingThread(error)) throw error;
            this.captureResolve({ threadId: request.threadId, empty: true });
            subscriber.complete();
            return;
          }
          this.captureResolve({
            threadId: request.threadId,
            snapshot: true,
            after: this.after,
          });
          subscriber.complete();
          return;
        }
        for await (const entry of this.backend.service.stream(
          this.scope,
          request.threadId,
          { snapshot: true, after: this.after, signal: detach.signal },
        ))
          subscriber.next(entry.event as BaseEvent);
        if (!detach.signal.aborted) subscriber.complete();
      })().catch((error) => {
        if (this.http) this.captureReject(error);
        subscriber.error(error);
      });
      return () => detach.abort();
    });
  }
  async result(threadId: string, runId: string) {
    return (await this.backend.service.store.runMessages(
      this.scope,
      threadId,
      runId,
    )) as Message[];
  }
  async isRunning(request: AgentRunnerIsRunningRequest) {
    await this.backend.service.store.recover(this.scope, request.threadId);
    const run = await this.backend.service.store.run(
      this.scope,
      request.threadId,
    );
    return Boolean(run && !terminal(run.status));
  }
  async stop(request: AgentRunnerStopRequest) {
    return this.backend.service.store.cancel(
      this.scope,
      request.threadId,
      request.runId,
    );
  }
}
/** Wrap the complete public pipeline, including middleware-synthesized events.
 * use() appends inside existing middleware in the pinned SDK, so adding persistence
 * directly to the transport would omit generated tool results and interrupts.
 */
class DurableInvocation extends AbstractAgent {
  constructor(private readonly delegate: AbstractAgent) {
    super({
      agentId: delegate.agentId,
      threadId: delegate.threadId,
      initialMessages: delegate.messages,
      initialState: delegate.state,
    });
  }
  run(input: RunAgentInput): Observable<BaseEvent> {
    return new Observable((subscriber) => {
      let settled = false;
      void this.delegate
        .runAgent(input, {
          onEvent: ({ event }) => {
            subscriber.next(event);
          },
        })
        .then(
          () => {
            settled = true;
            subscriber.complete();
          },
          (error) => {
            settled = true;
            subscriber.error(error);
          },
        );
      return () => {
        if (!settled) this.delegate.abortRun();
      };
    });
  }
}
class PersistEvents extends Middleware {
  constructor(
    private readonly persist: (event: BaseEvent) => Promise<void>,
    private readonly abort: () => void,
  ) {
    super();
  }
  run(input: RunAgentInput, next: AbstractAgent): Observable<BaseEvent> {
    let pending = 0;
    return this.runNext(input, next).pipe(
      tap(() => {
        if (++pending > 128) {
          this.abort();
          throw new Error("Agent exceeded durable event buffer limit");
        }
      }),
      concatMap((event) =>
        from(this.persist(event)).pipe(
          map(() => event),
          finalize(() => {
            pending--;
          }),
        ),
      ),
    );
  }
}
