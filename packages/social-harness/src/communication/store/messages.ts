/** @file Accepted messages own their immutable payload before protocol preparation. */

import { SqlClient } from "@effect/sql";
import { Effect } from "effect";
import { decodeCanonical } from "../../encoding/canonical-json.js";
import { Message, type MessageReference } from "../message.js";
import {
  EndpointStoreError,
  readBytes,
  readOptionalText,
  readText,
  requireEqual,
  requireSameBytes,
  requireText,
  storeOperation,
} from "./database/index.js";
import { readStoredIdentity } from "./rows/index.js";

/** Acceptance stays in the caller's SQL transaction and performs no network work. */
export function acceptMessage(input: {
  readonly authorAgentId: string;
  readonly msgId: string;
  readonly conversationId: string;
  readonly canonicalMessage: Uint8Array;
}) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const identity = yield* readStoredIdentity();
    if (identity === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(identity.agentId, input.authorAgentId);
    yield* sql.unsafe(
      `INSERT INTO message_intents(author_agent_id,msg_id,conversation_id,canonical_message)
       VALUES(?,?,?,?) ON CONFLICT(author_agent_id,msg_id) DO NOTHING`,
      [
        input.authorAgentId,
        input.msgId,
        input.conversationId,
        input.canonicalMessage,
      ],
    );
    const [row] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT conversation_id,canonical_message FROM message_intents WHERE author_agent_id=? AND msg_id=?",
      [input.authorAgentId, input.msgId],
    );
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    requireEqual(readText(row, "conversation_id"), input.conversationId);
    requireSameBytes(
      readBytes(row, "canonical_message"),
      input.canonicalMessage,
    );
  }).pipe(storeOperation, Effect.withSpan("acceptMessage"));
}

/** The owning reader returns the retained Message and durable terminal outcome. */
export function readAcceptedMessage(reference: MessageReference) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const [row] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT author_agent_id,msg_id,canonical_message,completed_record_hash,failure_reason FROM message_intents WHERE author_agent_id=? AND msg_id=?",
      [reference.authorAgentId, reference.msgId],
    );
    return row === undefined ? undefined : yield* readAcceptedRow(row);
  }).pipe(storeOperation, Effect.withSpan("readAcceptedMessage"));
}

/** Each conversation prepares its earliest accepted message before offering the next. */
export function readUnpreparedMessages() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return yield* Effect.forEach(
      yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT author_agent_id,msg_id,canonical_message,completed_record_hash,failure_reason
         FROM message_intents WHERE rowid IN (
           SELECT MIN(rowid) FROM message_intents
           WHERE canonical_intent IS NULL AND completed_record_hash IS NULL AND failure_reason IS NULL
           GROUP BY conversation_id
         ) ORDER BY rowid`,
      ),
      readAcceptedRow,
      { concurrency: 1 },
    );
  }).pipe(storeOperation, Effect.withSpan("readUnpreparedMessages"));
}

/** Terminal preparation failures remain observable after the process restarts. */
export function failMessage(reference: MessageReference, reason: string) {
  return Effect.gen(function* () {
    requireText(reason);
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      `UPDATE message_intents SET failure_reason=COALESCE(failure_reason,?)
       WHERE author_agent_id=? AND msg_id=? AND completed_record_hash IS NULL`,
      [reason, reference.authorAgentId, reference.msgId],
    );
  }).pipe(storeOperation, Effect.withSpan("failMessage"));
}

/** Pending status includes accepted messages whose membership is not prepared yet. */
export function hasPendingMessages(conversationId: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe(
      "SELECT 1 FROM message_intents WHERE conversation_id=? AND completed_record_hash IS NULL AND failure_reason IS NULL LIMIT 1",
      [conversationId],
    );
    return rows.length > 0;
  }).pipe(storeOperation, Effect.withSpan("hasPendingMessages"));
}

function readAcceptedRow(row: Readonly<Record<string, unknown>>) {
  return Effect.gen(function* () {
    const message = yield* decodeCanonical(
      Message,
      readBytes(row, "canonical_message"),
    );
    requireEqual(message.author.agentId, readText(row, "author_agent_id"));
    requireEqual(message.msgId, readText(row, "msg_id"));
    const failure = readOptionalText(row, "failure_reason");
    return {
      message,
      completed: readOptionalText(row, "completed_record_hash") !== undefined,
      failure,
    };
  }).pipe(storeOperation);
}
