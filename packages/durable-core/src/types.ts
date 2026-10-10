/** Supplied by the application after authentication; never recovered from client input. */
export type Scope = { tenantId: string; principalId: string };
export type Thread = {
  id: string;
  ownerId: string;
  agentId: string;
  metadata: Record<string, unknown>;
  state: unknown;
  cursor: number;
};
export type Action = "create" | "read" | "run" | "stop" | "update" | "delete";
/** Ownership and shared membership remain application policy. Called at every public boundary. */
export type Authorize = (
  scope: Scope,
  action: Action,
  thread: Thread,
) => Promise<void>;
export type Event = { type: string; [key: string]: unknown };
export type Message = { id: string; role: string; [key: string]: unknown };
export type Status =
  | "accepted"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "interrupted";
export type Run = {
  id: string;
  threadId: string;
  principalId: string;
  agentId: string;
  input: unknown;
  startCursor: number;
  status: Status;
  fence?: number;
  cancelRequested: boolean;
  error?: string;
};
export type StoredEvent = { cursor: number; runId: string; event: Event };
export type Lease = {
  scope: Scope;
  threadId: string;
  runId: string;
  fence: number;
};
export type Followup = { id: string; payload: unknown };
export class DurableError extends Error {
  constructor(
    public readonly code:
      | "not_found"
      | "forbidden"
      | "conflict"
      | "lease_lost"
      | "cancelled"
      | "invalid",
    message: string,
  ) {
    super(message);
    this.name = "DurableError";
  }
}
export const terminal = (status: Status) =>
  !["accepted", "running"].includes(status);
