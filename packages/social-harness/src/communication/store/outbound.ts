/** @file Durable exact-envelope ownership across Router retries and restart. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect } from "effect";
import type {
  OutboundAttempt,
  OutboundMessageInput,
  StoredOutboundMessage,
  StoreMutation,
} from "./types.js";

import {
  copyBytes,
  EndpointStoreError,
  readBytes,
  readInteger,
  readOptionalText,
  readText,
  requireBytes,
  requireEqual,
  requireSameBytes,
  requireText,
  storeOperation,
  StoreSignal,
  transaction,
} from "./database/index.js";
type OutboundDisposition = "accepted" | "discarded" | "pending";
interface RetainedOutbound {
  readonly outbound: StoredOutboundMessage;
  readonly attempted: boolean;
  readonly disposition: OutboundDisposition;
}
/**
 * Stages one complete outer envelope before it can reach Router transport.
 *
 * @param message Canonical complete signed envelope.
 * @returns The retained envelope under its envelope identity.
 */
export function enqueueOutbound(
  message: OutboundMessageInput,
): Effect.Effect<
  StoredOutboundMessage,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return transaction(enqueueOutboundInTransaction(message)).pipe(
    storeOperation,
    Effect.withSpan("enqueueOutbound"),
  );
}
/**
 * The caller's transaction keeps lookup and insertion on one leased connection.
 * Repeated envelope identities retain the same complete bytes and conversation.
 * @param message Canonical complete signed envelope.
 * @returns The retained envelope under its envelope identity.
 */
export function enqueueOutboundInTransaction(
  message: OutboundMessageInput,
): Effect.Effect<
  StoredOutboundMessage,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    validateMessageInput(message);
    yield* requireConversation(message.conversationId);
    const existing = yield* findOutbound(message.envelopeId);
    if (existing !== undefined) {
      requireEqual(existing.outbound.conversationId, message.conversationId);
      requireEqual(existing.outbound.wireVersion, message.wireVersion);
      requireSameBytes(
        existing.outbound.canonicalSignedMessage,
        message.canonicalSignedMessage,
      );
      return existing.outbound;
    }
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO outbound_messages
        (conversation_id, envelope_id,
         canonical_signed_message, wire_version, record_hash, attempted, disposition)
       VALUES (?, ?, ?, ?, ?, 0, 'pending')`,
      [
        message.conversationId,
        message.envelopeId,
        copyBytes(message.canonicalSignedMessage),
        message.wireVersion,
        message.recordHash ?? null,
      ],
    );
    return copyOutbound(message);
  }).pipe(storeOperation, Effect.withSpan("enqueueOutboundInTransaction"));
}
/**
 * Selects the exact durable envelope and retry mode for one Router attempt.
 *
 * @param envelopeId Stable identity of the signed envelope.
 * @returns The current stored bytes, or an inactive terminal disposition.
 */
export function beginOutbound(
  envelopeId: string,
): Effect.Effect<OutboundAttempt, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    requireText(envelopeId);
    return yield* transaction(
      Effect.gen(function* () {
        const retained = yield* requireOutbound(envelopeId);
        if (retained.disposition !== "pending") {
          return Object.freeze({ kind: "inactive" });
        }
        const mode = retained.attempted ? "retry" : "initial";
        if (!retained.attempted) {
          yield* database.unsafe<Readonly<Record<string, unknown>>>(
            `UPDATE outbound_messages SET attempted = 1
           WHERE envelope_id = ? AND disposition = 'pending'`,
            [envelopeId],
          );
        }
        return Object.freeze({
          kind: "pending",
          mode,
          outbound: retained.outbound,
        });
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("beginOutbound"));
}
/**
 * Marks one exact stored envelope complete after Router accepts its bytes.
 *
 * @param outbound Exact envelope whose Router digest was verified.
 * @returns Whether this call recorded acceptance or observed it already.
 */
export function completeOutbound(
  outbound: StoredOutboundMessage,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    validateMessageInput(outbound);
    return yield* transaction(
      Effect.gen(function* () {
        const retained = yield* requireOutbound(outbound.envelopeId);
        requireSameOutbound(retained.outbound, outbound);
        switch (retained.disposition) {
          case "accepted":
            return "existing";
          case "discarded":
            return yield* new EndpointStoreError({ reason: "conflict" });
          case "pending":
            yield* database.unsafe<Readonly<Record<string, unknown>>>(
              `UPDATE outbound_messages SET disposition = 'accepted'
             WHERE envelope_id = ? AND disposition = 'pending'`,
              [outbound.envelopeId],
            );
            return "inserted";
          default: {
            const exhaustive: never = retained.disposition;
            return exhaustive;
          }
        }
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("completeOutbound"));
}
/**
 * Atomically invalidates exact pending envelopes before protocol regeneration.
 *
 * @param outbounds Complete expected envelopes to invalidate.
 * @returns Whether at least one pending envelope became inactive.
 */
export function discardOutbound(
  outbounds: readonly StoredOutboundMessage[],
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const distinctIds = new Set<string>();
    for (const outbound of outbounds) {
      validateMessageInput(outbound);
      if (distinctIds.has(outbound.envelopeId)) {
        return yield* new EndpointStoreError({ reason: "invalid-input" });
      }
      distinctIds.add(outbound.envelopeId);
    }
    return yield* transaction(
      Effect.gen(function* () {
        const retained = [];
        for (const outbound of outbounds) {
          const row = yield* requireOutbound(outbound.envelopeId);
          requireSameOutbound(row.outbound, outbound);
          if (row.disposition === "accepted") {
            return yield* new EndpointStoreError({ reason: "conflict" });
          }
          retained.push(row);
        }
        const inserted = retained.some(
          (outbound) => outbound.disposition === "pending",
        );
        for (const outbound of retained) {
          if (outbound.disposition === "pending") {
            yield* database.unsafe<Readonly<Record<string, unknown>>>(
              `UPDATE outbound_messages SET disposition = 'discarded'
             WHERE envelope_id = ? AND disposition = 'pending'`,
              [outbound.outbound.envelopeId],
            );
          }
        }
        return inserted ? "inserted" : "existing";
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("discardOutbound"));
}
/**
 * Reads every pending stored envelope in durable insertion order.
 *
 * @param conversationId Restrict the read to this conversation when provided.
 * @returns Detached pending envelopes, excluding accepted and discarded rows.
 */
export function readPendingOutbound(
  conversationId?: string,
): Effect.Effect<
  readonly StoredOutboundMessage[],
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, envelope_id,
                canonical_signed_message, wire_version, record_hash, attempted, disposition
         FROM outbound_messages WHERE disposition = 'pending' ${conversationId === undefined ? "" : "AND conversation_id = ?"}
         ORDER BY outbound_sequence`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map((row) => readOutbound(row).outbound),
    );
  }).pipe(storeOperation, Effect.withSpan("readPendingOutbound"));
}

/** The outbox itself owns the exact signed envelope for one staged record. */
export function readRecordOutbound(conversationId: string, recordHash: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const [row] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id,envelope_id,canonical_signed_message,wire_version,record_hash,attempted,disposition
       FROM outbound_messages WHERE conversation_id=? AND record_hash=?
       ORDER BY disposition='discarded',outbound_sequence DESC LIMIT 1`,
      [conversationId, recordHash],
    );
    return row === undefined ? undefined : readOutbound(row);
  }).pipe(storeOperation, Effect.withSpan("readRecordOutbound"));
}

function requireOutbound(
  envelopeId: string,
): Effect.Effect<RetainedOutbound, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const retained = yield* findOutbound(envelopeId);
    if (retained === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    return retained;
  }).pipe(storeOperation);
}
function findOutbound(
  envelopeId: string,
): Effect.Effect<
  RetainedOutbound | undefined,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id, envelope_id,
              canonical_signed_message, wire_version, record_hash, attempted, disposition
       FROM outbound_messages WHERE envelope_id = ?`,
      [envelopeId],
    ))[0];
    return row === undefined ? undefined : readOutbound(row);
  }).pipe(storeOperation);
}

function readOutbound(
  row: Readonly<Record<string, unknown>>,
): RetainedOutbound {
  const attempted = readInteger(row, "attempted");
  if (attempted !== 0 && attempted !== 1) {
    throw new StoreSignal("corrupt");
  }
  const recordHash = readOptionalText(row, "record_hash");
  return Object.freeze({
    outbound: Object.freeze({
      conversationId: readText(row, "conversation_id"),
      envelopeId: readText(row, "envelope_id"),
      wireVersion: readText(row, "wire_version"),
      ...(recordHash === undefined ? {} : { recordHash }),
      canonicalSignedMessage: readBytes(row, "canonical_signed_message"),
    }),
    attempted: attempted === 1,
    disposition: readDisposition(row),
  });
}
function readDisposition(
  row: Readonly<Record<string, unknown>>,
): OutboundDisposition {
  const disposition = readText(row, "disposition");
  switch (disposition) {
    case "accepted":
    case "discarded":
    case "pending":
      return disposition;
    default:
      throw new StoreSignal("corrupt");
  }
}
function requireConversation(
  conversationId: string,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    if (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT 1 AS retained FROM conversation_state
         WHERE conversation_id = ?`,
        [conversationId],
      ))[0] === undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(storeOperation);
}
function validateMessageInput(message: OutboundMessageInput): void {
  requireText(message.wireVersion);
  requireText(message.conversationId);
  requireText(message.envelopeId);
  requireBytes(message.canonicalSignedMessage);
}
function requireSameOutbound(
  left: StoredOutboundMessage,
  right: StoredOutboundMessage,
): void {
  requireEqual(left.conversationId, right.conversationId);
  requireEqual(left.envelopeId, right.envelopeId);
  requireEqual(left.wireVersion, right.wireVersion);
  requireSameBytes(left.canonicalSignedMessage, right.canonicalSignedMessage);
}
function copyOutbound(outbound: StoredOutboundMessage): StoredOutboundMessage {
  return Object.freeze({
    ...outbound,
    canonicalSignedMessage: copyBytes(outbound.canonicalSignedMessage),
  });
}
