import { mock } from "bun:test";
import * as runtime from "@copilotkit/runtime/v2";
// Loading the real application must never construct even an idle Intelligence client in local mode.
mock.module("@copilotkit/runtime/v2", () => ({
  ...runtime,
  CopilotKitIntelligence: class extends runtime.CopilotKitIntelligence {
    constructor(
      ...args: ConstructorParameters<typeof runtime.CopilotKitIntelligence>
    ) {
      super(...args);
      throw new Error("FORBIDDEN_INTELLIGENCE_CLIENT");
    }
  },
}));
const originalFetch = globalThis.fetch;
globalThis.fetch = ((
  input: Parameters<typeof fetch>[0],
  options?: Parameters<typeof fetch>[1],
) => {
  const url = new URL(input instanceof Request ? input.url : String(input));
  if (!["localhost", "127.0.0.1", "[::1]"].includes(url.hostname))
    throw new Error(`FORBIDDEN_EGRESS:${url.hostname}`);
  return originalFetch(input, options);
}) as typeof fetch;
const OriginalWebSocket = globalThis.WebSocket;
globalThis.WebSocket = class extends OriginalWebSocket {
  constructor(url: string | URL, protocols?: string | string[]) {
    if (!["localhost", "127.0.0.1", "[::1]"].includes(new URL(url).hostname))
      throw new Error("FORBIDDEN_INTELLIGENCE_WEBSOCKET");
    super(url, protocols);
  }
};
await import("../../src/index");
