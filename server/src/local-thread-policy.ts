import { DurableError, type Authorize } from "@openbot/durable-core";
import { eq } from "drizzle-orm";
import type { AgentProfileStore } from "./agents/profile-store";
import type { AgentActor } from "./agents/profile-types";
import type { ChannelStore } from "./channels/routes";
import type { Database } from "./db/client";
import { intelligenceChannelMappings } from "./db/schema";
import { groupBotThreads } from "./db/schema/group";

/** Every durable boundary rechecks the actor, channel membership and current Bot grants. */
export function localThreadPolicy(options: {
  tenantId: string;
  database: Database;
  actorFor: (id: string) => Promise<AgentActor>;
  profiles: Pick<AgentProfileStore, "get">;
  channels: Pick<ChannelStore, "get">;
}): Authorize {
  return async (scope, action, thread) => {
    if (scope.tenantId !== options.tenantId)
      throw new DurableError("forbidden", "Wrong tenant");
    const actor = await options.actorFor(scope.principalId);
    const mappings = await options.database
      .select({
        channelId: intelligenceChannelMappings.channelId,
        userId: intelligenceChannelMappings.userId,
      })
      .from(intelligenceChannelMappings)
      .where(eq(intelligenceChannelMappings.threadId, thread.id));
    const groups = await options.database
      .select({ channelId: groupBotThreads.channelId })
      .from(groupBotThreads)
      .where(eq(groupBotThreads.threadId, thread.id));
    const related = [...mappings, ...groups];
    if (related.length) {
      // Direct mappings are private to their member. Group Bot threads are shared by the roster.
      const permitted = [
        ...mappings.filter((mapping) => mapping.userId === actor.id),
        ...groups,
      ];
      const visible = await Promise.all(
        permitted.map((mapping) =>
          options.channels.get(actor, mapping.channelId),
        ),
      );
      if (!visible.some(Boolean))
        throw new DurableError("forbidden", "Thread membership is required");
      if (action === "delete")
        throw new DurableError(
          "forbidden",
          "Delete channel conversations through the channel boundary",
        );
    } else if (thread.ownerId !== scope.principalId)
      throw new DurableError("forbidden", "Thread ownership is required");
    if (["create", "run", "stop", "update"].includes(action)) {
      if (thread.agentId === "unassigned" && action !== "run") return;
      const profile = await options.profiles.get(actor, thread.agentId);
      if (!profile)
        throw new DurableError("forbidden", "Bot access is required");
    }
  };
}
