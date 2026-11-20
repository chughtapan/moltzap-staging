/**
 * @file The server agent's saved firewall decisions: one row per completed
 * check, read back for `read_firewall_decisions` and as what the firewall is
 * told of a conversation's earlier messages.
 */

import { SqlClient } from "@effect/sql";
import { Array as Arr, Effect, Schema, Struct } from "effect";
import type { Message } from "../communication/index.js";
import { withAgentRows } from "../database.js";
import { exactStruct } from "../encoding.js";
import {
  type CheckRecord,
  checkRecordSchema,
  type EarlierMessage,
} from "../firewall/index.js";
import { AgentCard } from "../identity.js";

/** Inclusive Unix milliseconds permit polling across checks completed out of start order. */
export const firewallDecisionsRequestSchema = exactStruct({
  since: Schema.optional(Schema.NonNegativeInt),
});

/** Reports each saved check once per peer. */
export const firewallDecisionsSchema = exactStruct({
  decisions: Schema.Array(
    exactStruct({
      ...Struct.omit(checkRecordSchema.fields, "peers"),
      peer: AgentCard,
    }),
  ),
});

const savedRecord = Schema.parseJson(checkRecordSchema);
const savedRows = Schema.Array(
  Schema.Struct({ messageKey: Schema.String, record: savedRecord }),
);

/** The key a decision is saved under for its message: the author's agent id and the message id. */
function messageKey(message: Message): string {
  return `${message.author.agentId}/${message.msgId}`;
}

/** Saves what the firewall of the agent stored under `agentKey` concluded of `message`. */
export const saveDecision = Effect.fn("saveDecision")(function* (
  agentKey: string,
  message: Message,
  record: CheckRecord,
) {
  const sql = yield* SqlClient.SqlClient;
  const saved = yield* Schema.encode(savedRecord)(record);
  yield* withAgentRows(
    agentKey,
    sql`INSERT INTO firewall_checks ${sql.insert({
      agent_key: agentKey,
      message_key: messageKey(message),
      decided_at: record.time,
      record: saved,
    })}`,
  );
}, Effect.orDie);

/**
 * Each of `messages` with the saved decision of the firewall of the agent
 * stored under `agentKey`. A decision that allowed a message wins over one
 * that did not, since a draft the firewall refused can share its id with the
 * message later sent.
 */
export const withDecisions = Effect.fn("withDecisions")(function* (
  agentKey: string,
  messages: readonly Message[],
) {
  const sql = yield* SqlClient.SqlClient;
  const saved = yield* withAgentRows(
    agentKey,
    Effect.forEach(
      Arr.chunksOf(messages.map(messageKey), 400),
      (keys) =>
        sql`SELECT message_key AS "messageKey", record FROM firewall_checks
          WHERE agent_key = ${agentKey} AND ${sql.in("message_key", keys)}
          ORDER BY sequence`.pipe(
          Effect.flatMap(Schema.decodeUnknown(savedRows)),
        ),
      { concurrency: 1 },
    ),
  ).pipe(Effect.map(Arr.flatten));
  return messages.map((message): EarlierMessage => {
    const decisions = saved.filter(
      (row) => row.messageKey === messageKey(message),
    );
    const decided =
      decisions.find((row) => row.record.decision === "allowed") ??
      decisions[0];
    return {
      message,
      allowed: decided?.record.decision === "allowed",
      ...(decided === undefined ? {} : { checkedAt: decided.record.time }),
    };
  });
}, Effect.orDie);

/**
 * The latest hundred decisions since `since` of the firewall of the agent
 * stored under `agentKey`, newest first, one for each peer of a check.
 */
export const readDecisions = Effect.fn("readDecisions")(function* (
  agentKey: string,
  request: typeof firewallDecisionsRequestSchema.Type,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* withAgentRows(
    agentKey,
    sql`SELECT record FROM firewall_checks
      WHERE agent_key = ${agentKey} AND decided_at >= ${request.since ?? 0}
      ORDER BY decided_at DESC, sequence DESC LIMIT 100`,
  );
  const saved = yield* Schema.decodeUnknown(
    Schema.Array(Schema.Struct({ record: savedRecord })),
  )(rows);
  return {
    decisions: saved.flatMap(({ record }) =>
      record.peers.map((peer) => ({ ...Struct.omit(record, "peers"), peer })),
    ),
  };
}, Effect.orDie);
