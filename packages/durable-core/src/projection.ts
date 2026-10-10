import jsonPatch from "fast-json-patch";
import type { Event, Message } from "./types";

type Call = {
  id: string;
  type: string;
  function: { name: string; arguments: string };
};
/** One protocol event reduces stable message rows; history snapshots never replace a transcript. */
export function project(
  event: Event,
  existing: Message | undefined,
): Message | undefined {
  const id =
    typeof event.messageId === "string"
      ? event.messageId
      : String(event.toolCallId ?? "");
  switch (event.type) {
    case "TEXT_MESSAGE_START":
      return {
        ...(existing ?? {
          id,
          role: String(event.role ?? "assistant"),
          content: "",
        }),
        ...(event.metadata ? { metadata: event.metadata } : {}),
      };
    case "TEXT_MESSAGE_CONTENT":
      if (!existing)
        throw new Error("Text delta has no committed message start");
      return {
        ...existing,
        content: String(existing.content ?? "") + String(event.delta ?? ""),
      };
    case "TOOL_CALL_START": {
      const message = existing ?? {
        id: String(event.parentMessageId ?? event.toolCallId),
        role: "assistant",
        content: "",
      };
      const calls = (message.toolCalls ?? []) as Call[];
      if (calls.some((call) => call.id === event.toolCallId))
        throw new Error("Tool call has already started");
      return {
        ...message,
        toolCalls: [
          ...calls,
          {
            id: event.toolCallId,
            type: "function",
            function: { name: event.toolCallName, arguments: "" },
          },
        ],
      };
    }
    case "TOOL_CALL_ARGS": {
      if (!existing) throw new Error("Tool delta has no committed tool start");
      const calls = existing.toolCalls as Call[];
      return {
        ...existing,
        toolCalls: calls.map((call) =>
          call.id === event.toolCallId
            ? {
                ...call,
                function: {
                  ...call.function,
                  arguments:
                    call.function.arguments + String(event.delta ?? ""),
                },
              }
            : call,
        ),
      };
    }
    case "TOOL_CALL_RESULT":
      return {
        id: String(event.messageId ?? `result:${event.toolCallId}`),
        role: "tool",
        toolCallId: event.toolCallId,
        content: event.content,
      };
    case "ACTIVITY_SNAPSHOT":
      return event.replace === false && existing
        ? existing
        : {
            id,
            role: "activity",
            activityType: event.activityType,
            content: event.content,
          };
    case "ACTIVITY_DELTA":
      if (!existing)
        throw new Error("Activity delta has no committed snapshot");
      return {
        ...existing,
        content: patchState(existing.content, event.patch),
      };
    default:
      return undefined;
  }
}
/** Validated RFC 6902 patches, including move/copy/test and prototype protection. */
export function patchState(value: unknown, patches: unknown): unknown {
  if (!Array.isArray(patches))
    throw new Error("State delta must be JSON Patch");
  return jsonPatch.applyPatch(
    structuredClone(value ?? {}),
    patches,
    true,
    true,
    true,
  ).newDocument;
}
