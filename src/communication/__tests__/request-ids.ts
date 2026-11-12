/** @file Operation IDs for peer messages with deliberately chosen requester identities and random bytes. */

import { Schema } from "effect";
import type { AgentId } from "../../identity/agent-cards.js";
import { CollectiveId } from "../messages.js";

/** Reusing a byte across requesters exercises their independent operation namespaces. */
export function collectiveOperationId(
  requester: AgentId,
  byte = 0,
): CollectiveId {
  return Schema.decodeUnknownSync(CollectiveId)(
    `col_${Buffer.concat([
      Buffer.from(requester.slice(4), "base64url"),
      Buffer.alloc(32, byte),
    ]).toString("base64url")}`,
  );
}
