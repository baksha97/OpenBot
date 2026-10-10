import type { CopilotKitIntelligence } from "@copilotkit/runtime/v2";

/** Pinned runtime 1.77.0 private Intelligence seam. Keep upgrades and contract checks here. */
export type LegacyThreadClient = {
  getOrCreateThread(params: {
    threadId: string;
    userId: string;
    agentId: string;
    learningContainerId?: string;
  }): Promise<unknown>;
  getThreadMessages(params: { threadId: string; userId: string }): Promise<{
    messages: {
      id: string;
      role: string;
      content?: unknown;
      activityType?: string;
      toolCalls?: { id: string; name: string; args: string }[];
      toolCallId?: string;
    }[];
  }>;
  ɵacquireThreadLock(params: {
    threadId: string;
    runId: string;
    userId: string;
    agentId: string;
    ttlSeconds?: number;
    learningContainerId?: string;
  }): Promise<unknown>;
  /** NOTE: no `userId` and no `agentId` — renew is identified by the thread and the run alone. */
  ɵrenewThreadLock(params: {
    threadId: string;
    runId: string;
    ttlSeconds: number;
  }): Promise<unknown>;
  ɵcleanupThreadLock(params: {
    threadId: string;
    runId: string;
  }): Promise<void>;
};
export function intelligenceLocks<
  T extends Pick<
    LegacyThreadClient,
    "ɵacquireThreadLock" | "ɵrenewThreadLock" | "ɵcleanupThreadLock"
  >,
>(client: T) {
  return {
    acquire: (input: Parameters<T["ɵacquireThreadLock"]>[0]) =>
      client.ɵacquireThreadLock(input) as ReturnType<T["ɵacquireThreadLock"]>,
    renew: (input: Parameters<T["ɵrenewThreadLock"]>[0]) =>
      client.ɵrenewThreadLock(input) as ReturnType<T["ɵrenewThreadLock"]>,
    release: (input: Parameters<T["ɵcleanupThreadLock"]>[0]) =>
      client.ɵcleanupThreadLock(input),
  };
}
export function intelligenceRunnerConnection(client: CopilotKitIntelligence) {
  return {
    url: client.ɵgetRunnerWsUrl(),
    authToken: client.ɵgetRunnerAuthToken(),
  };
}
