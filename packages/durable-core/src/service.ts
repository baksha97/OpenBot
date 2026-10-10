import type { PostgresStore } from "./store";
import {
  DurableError,
  type Event,
  type Followup,
  type Message,
  type Run,
  type Scope,
  type StoredEvent,
  terminal,
} from "./types";

export type Executor = (context: {
  run: Run;
  signal: AbortSignal;
  emit: (event: Event) => Promise<void>;
}) => Promise<{
  terminalEvent: Event;
  messages?: readonly Message[];
  state?: unknown;
  followups?: Followup[];
}>;
export class DurableService {
  private readonly tasks = new Set<Promise<void>>();
  constructor(
    readonly store: PostgresStore,
    readonly pollMs = 250,
  ) {}
  async submit(
    scope: Scope,
    input: Parameters<PostgresStore["submit"]>[1],
    execute: Executor,
  ): Promise<Run> {
    await this.store.recover(scope, input.threadId);
    const accepted = await this.store.submit(scope, input);
    if (accepted.created) {
      const task = this.execute(scope, accepted.run, execute).finally(() =>
        this.tasks.delete(task),
      );
      this.tasks.add(task);
      // Execution is detached from HTTP. Failures are recorded or remain recoverable after lease expiry.
      void task.catch(() => {});
    }
    return accepted.run;
  }
  private async execute(scope: Scope, run: Run, execute: Executor) {
    const lease = await this.store.claim(scope, run.threadId, run.id);
    const controller = new AbortController();
    let failure: unknown;
    let renewing = false;
    const heartbeat = setInterval(() => {
      if (renewing) return;
      renewing = true;
      void this.store
        .renew(lease)
        .catch((error) => {
          failure = error;
          controller.abort(error);
        })
        .finally(() => {
          renewing = false;
        });
    }, 20_000);
    // Cancellation must not depend on an agent producing another event.
    let polling = false;
    const cancelPoll = setInterval(() => {
      if (polling) return;
      polling = true;
      void this.store
        .run(scope, run.threadId, run.id)
        .then((current) => {
          if (current?.cancelRequested)
            controller.abort(new DurableError("cancelled", "Run cancelled"));
        })
        .catch((error) => {
          failure = error;
          controller.abort(error);
        })
        .finally(() => {
          polling = false;
        });
    }, this.pollMs);
    let sequence = 0;
    try {
      const result = await execute({
        run,
        signal: controller.signal,
        emit: async (event) => {
          if (controller.signal.aborted) throw controller.signal.reason;
          try {
            await this.store.append(lease, event, sequence);
            sequence++;
          } catch (error) {
            failure = error;
            controller.abort(error);
            throw error;
          }
        },
      });
      if (result.terminalEvent.type !== "RUN_FINISHED")
        throw new Error("Executor did not confirm successful completion");
      if (failure) throw failure;
      if (controller.signal.aborted) throw controller.signal.reason;
      await this.store.finish(lease, {
        ...result,
        status: "succeeded",
        event: {
          ...result.terminalEvent,
          threadId: run.threadId,
          runId: run.id,
        },
      });
    } catch (error) {
      controller.abort(error);
      // No terminal success can be published when persistence failed. If this write also fails,
      // the lease expires and recovery records interruption; the original error remains visible.
      await this.store
        .finish(lease, {
          status:
            error instanceof DurableError && error.code === "cancelled"
              ? "cancelled"
              : "failed",
          error: "run_failed",
          event: {
            type: "RUN_ERROR",
            code: error instanceof DurableError ? error.code : "run_failed",
            message: "The run did not complete.",
          },
        })
        .catch(() => {});
      throw error;
    } finally {
      clearInterval(heartbeat);
      clearInterval(cancelPoll);
    }
  }
  /** DB-backed replay then polling: one cursor, no history/live gap or process event buffer. */
  async *stream(
    scope: Scope,
    threadId: string,
    options: {
      after?: number;
      runId?: string;
      signal?: AbortSignal;
      snapshot?: boolean;
    } = {},
  ): AsyncGenerator<StoredEvent> {
    if (
      options.after !== undefined &&
      (!Number.isSafeInteger(options.after) || options.after < 0)
    )
      throw new DurableError("invalid", "Invalid replay cursor");
    let cursor = options.after ?? 0;
    if (options.snapshot && options.after === undefined) {
      const snapshot = await this.store.snapshot(scope, threadId);
      cursor = snapshot.cursor;
      yield {
        cursor,
        runId: options.runId ?? "",
        event: { type: "MESSAGES_SNAPSHOT", messages: snapshot.messages },
      };
      yield {
        cursor,
        runId: options.runId ?? "",
        event: { type: "STATE_SNAPSHOT", snapshot: snapshot.thread.state },
      };
    }
    while (!options.signal?.aborted) {
      await this.store.recover(scope, threadId);
      const events = await this.store.events(scope, threadId, cursor);
      for (const event of events) {
        cursor = event.cursor;
        if (!options.runId || event.runId === options.runId) yield event;
      }
      const run = await this.store.run(scope, threadId, options.runId);
      if (events.length < 128 && (!run || terminal(run.status))) {
        // Finalization may commit after the event query. Drain its committed watermark first.
        const watermark = (await this.store.get(scope, threadId)).cursor;
        if (cursor >= watermark) return;
        continue;
      }
      if (!events.length)
        await new Promise<void>((resolve) => {
          const done = () => {
            clearTimeout(timer);
            options.signal?.removeEventListener("abort", done);
            resolve();
          };
          const timer = setTimeout(done, this.pollMs);
          options.signal?.addEventListener("abort", done, { once: true });
        });
    }
  }
  async drain() {
    await Promise.allSettled([...this.tasks]);
  }
}
