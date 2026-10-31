/** @file Recovery and fixed-head reads over certified conversation history. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect, Schema } from "effect";
import type {
  CertifiedRecord,
  EndpointRecovery,
  HistoryPage,
  MessageIntent,
  StoredAnchor,
  StoredMembership,
} from "./types.js";

import {
  EndpointStoreError,
  readBytes,
  readInteger,
  readOptionalText,
  readText,
  storeOperation,
  StoreSignal,
  transaction,
} from "./database/index.js";
import { readPendingOutbound } from "./queues/outbound.js";
import {
  readCertifiedRecord,
  readConversationPosition,
  readProposalLock,
  readProtocolEvidence,
  readRecoveredReanchor,
  readStagedRecord,
  readStoredIdentity,
} from "./rows/index.js";
const HISTORY_PAGE_SIZE = 50;
const historySelect = `
  SELECT certified.rowid AS sequence, staged.conversation_id, staged.record_hash,
         staged.previous_record_hash, staged.membership_hash,
         staged.anchor_hash, staged.action_hash, staged.author_agent_id,
         staged.msg_id, staged.canonical_record_core
  FROM certified_records AS certified
  JOIN staged_records AS staged
    ON staged.conversation_id = certified.conversation_id
   AND staged.record_hash = certified.record_hash
`;
const historyCursor = Schema.Struct({
  conversationId: Schema.NonEmptyString,
  afterRecordHash: Schema.NonEmptyString,
  throughRecordHash: Schema.NonEmptyString,
});
const encodeCursor = Schema.encodeSync(Schema.parseJson(historyCursor));
const decodeCursor = Schema.decodeUnknown(Schema.parseJson(historyCursor), {
  onExcessProperty: "error",
});

/**
 * Recovers all durable endpoint state needed to resume protocol work.
 *
 * @param conversationId Restrict protocol rows to one conversation when provided.
 * @returns A detached snapshot of the selected durable endpoint protocol state.
 */
export function recoverStoredState(
  conversationId?: string,
): Effect.Effect<EndpointRecovery, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const identity = yield* readStoredIdentity();
    return Object.freeze({
      ...(identity === undefined ? {} : { identity }),
      messageIntents: yield* readMessageIntents(conversationId),
      memberships: yield* readMemberships(conversationId),
      anchors: yield* readAnchors(conversationId),
      positions: yield* readPositions(conversationId),
      proposalLocks: Object.freeze(
        (yield* database.unsafe<Readonly<Record<string, unknown>>>(
          `SELECT conversation_id, previous_record_hash, action_hash,
                  canonical_action_core
           FROM proposal_locks ${conversationId === undefined ? "" : "WHERE conversation_id = ?"} ORDER BY conversation_id, predecessor_key`,
          [...(conversationId === undefined ? [] : [conversationId])],
        )).map(readProposalLock),
      ),
      stagedRecords: Object.freeze(
        (yield* database.unsafe<Readonly<Record<string, unknown>>>(
          `SELECT conversation_id, record_hash, previous_record_hash,
                  membership_hash, anchor_hash, action_hash, author_agent_id,
                  msg_id, canonical_record_core
           FROM staged_records ${conversationId === undefined ? "" : "WHERE conversation_id = ?"} ORDER BY conversation_id, rowid`,
          [...(conversationId === undefined ? [] : [conversationId])],
        )).map(readStagedRecord),
      ),
      evidence: yield* readEvidence(conversationId),
      certifiedRecords: yield* readAllCertifiedRecords(conversationId),
      stagedReanchors: yield* readReanchors(conversationId),
      outboundMessages: yield* readPendingOutbound(conversationId),
    });
  }).pipe(storeOperation, Effect.withSpan("recoverStoredState"));
}

/** Pages stop at the original certified head and can be replayed after restart. */
export function readStoredConversation(
  request:
    | Readonly<{
        conversationId: string;
        afterRecordHash?: string;
      }>
    | Readonly<{
        continuation: string;
      }>,
): Effect.Effect<HistoryPage, EndpointStoreError, SqlClient.SqlClient> {
  return transaction(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const { conversationId, after, through, throughRecordHash } =
        yield* resolveHistoryWindow(request);
      const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        historySelect +
          " WHERE certified.conversation_id=? AND certified.ordinal>? AND certified.ordinal<=? ORDER BY certified.ordinal LIMIT ?",
        [conversationId, after, through, HISTORY_PAGE_SIZE + 1],
      );
      const selected = rows.slice(0, HISTORY_PAGE_SIZE);
      const records = yield* Effect.forEach(selected, readCertifiedRecord, {
        concurrency: 1,
      });
      const last = selected.at(-1);
      const continuation =
        rows.length > HISTORY_PAGE_SIZE && last !== undefined
          ? Buffer.from(
              encodeCursor({
                conversationId,
                afterRecordHash: readText(last, "record_hash"),
                throughRecordHash,
              }),
            ).toString("base64url")
          : null;
      return {
        records,
        memberships: yield* readMemberships(conversationId),
        anchors: yield* readAnchors(conversationId, [
          ...new Set(records.map((record) => record.anchorHash)),
        ]),
        authoredRecordHashes: yield* readAuthoredRecordHashes(records),
        continuation,
      };
    }),
  ).pipe(Effect.withSpan("readStoredConversation"));
}
/** Recent certified rows and their verification dependencies share the caller's snapshot. */
export function readRecentConversation(input: {
  readonly conversationId: string;
  readonly maximumRecords: number;
  readonly candidateRecordHash?: string;
}) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      historySelect +
        " WHERE certified.conversation_id=? AND certified.record_hash<>? ORDER BY certified.ordinal DESC LIMIT ?",
      [
        input.conversationId,
        input.candidateRecordHash ?? "",
        input.maximumRecords + 1,
      ],
    );
    const candidateRows =
      input.candidateRecordHash === undefined
        ? []
        : yield* sql.unsafe<Readonly<Record<string, unknown>>>(
            historySelect +
              " WHERE certified.conversation_id=? AND certified.record_hash=?",
            [input.conversationId, input.candidateRecordHash],
          );
    const records = yield* Effect.forEach(
      [...rows, ...candidateRows],
      readCertifiedRecord,
      { concurrency: 1 },
    );
    return {
      records,
      memberships: yield* readMemberships(input.conversationId),
      anchors: yield* readAnchors(
        input.conversationId,
        records.map((record) => record.anchorHash),
      ),
      positions: [...rows, ...candidateRows].map((row) => ({
        recordHash: readText(row, "record_hash"),
        sequence: readInteger(row, "sequence"),
      })),
    };
  }).pipe(storeOperation, Effect.withSpan("readRecentConversation"));
}

/** Reads remote certified records in their durable local commit order until consumed. */
export function readUnconsumedRecords() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const records = yield* Effect.forEach(
      yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        historySelect +
          ` WHERE certified.consumed=0
          AND staged.author_agent_id<>(SELECT agent_id FROM identity_binding WHERE singleton=1)
          ORDER BY certified.rowid`,
      ),
      readCertifiedRecord,
      { concurrency: 1 },
    );
    return {
      records,
      memberships: yield* readMemberships(),
      anchors: yield* readAnchors(),
    };
  }).pipe(storeOperation, Effect.withSpan("readUnconsumedRecords"));
}

/** A successful handler retires only its exact certified record, retaining history. */
export function consumeRecord(recordHash: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "UPDATE certified_records SET consumed=1 WHERE record_hash=? RETURNING record_hash",
      [recordHash],
    );
    if (rows.length !== 1) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(storeOperation, Effect.withSpan("consumeRecord"));
}

/** Protocol startup needs each retained membership, including conversations without a certified head. */
export function readMemberships(
  conversationId?: string,
): Effect.Effect<
  readonly StoredMembership[],
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, membership_hash, canonical_membership
         FROM memberships ${conversationId === undefined ? "" : "WHERE conversation_id = ?"} ORDER BY conversation_id`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map(readMembership),
    );
  }).pipe(storeOperation, Effect.withSpan("readMemberships"));
}
function readPositions(conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, membership_hash, current_anchor_hash,
                  head_record_hash FROM conversation_state
           ${conversationId === undefined ? "" : "WHERE conversation_id = ?"}
           ORDER BY conversation_id`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map(readConversationPosition),
    );
  }).pipe(storeOperation);
}
function readMessageIntents(
  conversationId?: string,
): Effect.Effect<
  readonly MessageIntent[],
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, membership_hash, author_agent_id, msg_id,
                canonical_intent, canonical_message, completed_record_hash
         FROM message_intents WHERE canonical_intent IS NOT NULL AND failure_reason IS NULL ${conversationId === undefined ? "" : "AND conversation_id = ?"} ORDER BY rowid`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map((row) => {
        const completedRecordHash = readOptionalText(
          row,
          "completed_record_hash",
        );
        return Object.freeze({
          conversationId: readText(row, "conversation_id"),
          membershipHash: readText(row, "membership_hash"),
          authorAgentId: readText(row, "author_agent_id"),
          msgId: readText(row, "msg_id"),
          canonicalIntent: readBytes(row, "canonical_intent"),
          canonicalMessage: readBytes(row, "canonical_message"),
          ...(completedRecordHash === undefined ? {} : { completedRecordHash }),
        });
      }),
    );
  }).pipe(storeOperation);
}
function readMembership(
  row: Readonly<Record<string, unknown>>,
): StoredMembership {
  return Object.freeze({
    conversationId: readText(row, "conversation_id"),
    membershipHash: readText(row, "membership_hash"),
    canonicalMembership: readBytes(row, "canonical_membership"),
  });
}
function readAnchors(
  conversationId?: string,
  anchorHashes?: readonly string[],
): Effect.Effect<
  readonly StoredAnchor[],
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    if (anchorHashes?.length === 0) {
      return Object.freeze([]);
    }
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, anchor_hash, previous_anchor_hash,
                selected_record_hash, canonical_anchor
         FROM anchors WHERE ${conversationId === undefined ? "1" : "conversation_id = ?"}
         ${anchorHashes === undefined ? "" : `AND anchor_hash IN (${anchorHashes.map(() => "?").join(", ")})`}
         ORDER BY conversation_id, rowid`,
        [
          ...(conversationId === undefined ? [] : [conversationId]),
          ...(anchorHashes ?? []),
        ],
      )).map((row) => {
        const previousAnchorHash = readOptionalText(
          row,
          "previous_anchor_hash",
        );
        const selectedRecordHash = readOptionalText(
          row,
          "selected_record_hash",
        );
        return Object.freeze({
          conversationId: readText(row, "conversation_id"),
          anchorHash: readText(row, "anchor_hash"),
          ...(previousAnchorHash === undefined ? {} : { previousAnchorHash }),
          ...(selectedRecordHash === undefined ? {} : { selectedRecordHash }),
          canonicalAnchor: readBytes(row, "canonical_anchor"),
        });
      }),
    );
  }).pipe(storeOperation);
}

function readAuthoredRecordHashes(records: readonly CertifiedRecord[]) {
  return Effect.gen(function* () {
    if (records.length === 0) {
      return Object.freeze([]);
    }
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `WITH page(author_agent_id, msg_id, record_hash) AS (
         VALUES ${records.map(() => "(?, ?, ?)").join(", ")}
       )
       SELECT page.record_hash FROM page
       JOIN message_intents AS intent
         ON intent.author_agent_id = page.author_agent_id
        AND intent.msg_id = page.msg_id
        AND intent.completed_record_hash = page.record_hash`,
      records.flatMap((record) => [
        record.authorAgentId,
        record.msgId,
        record.recordHash,
      ]),
    );
    return Object.freeze(rows.map((row) => readText(row, "record_hash")));
  }).pipe(storeOperation);
}
function readEvidence(conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, evidence_kind, subject_id,
                evidence_key, canonical_evidence
         FROM protocol_evidence
         ${conversationId === undefined ? "" : "WHERE conversation_id = ?"}
         ORDER BY conversation_id, evidence_kind, subject_id, evidence_key`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map(readProtocolEvidence),
    );
  }).pipe(storeOperation);
}
function readAllCertifiedRecords(
  conversationId?: string,
): Effect.Effect<
  readonly CertifiedRecord[],
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      yield* Effect.forEach(
        yield* database.unsafe<Readonly<Record<string, unknown>>>(
          `${historySelect}
         ${conversationId === undefined ? "" : "WHERE certified.conversation_id = ?"}
         ORDER BY certified.conversation_id, certified.ordinal`,
          [...(conversationId === undefined ? [] : [conversationId])],
        ),
        (row) => readCertifiedRecord(row),
        { concurrency: 1 },
      ),
    );
  }).pipe(storeOperation);
}
function readReanchors(conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, anchor_hash, previous_anchor_hash,
                router_instance_id, selected_record_hash, canonical_body,
                canonical_completed_reanchor
         FROM reanchors ${conversationId === undefined ? "" : "WHERE conversation_id = ?"} ORDER BY conversation_id, rowid`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map(readRecoveredReanchor),
    );
  }).pipe(storeOperation);
}

function resolveHistoryWindow(
  request: Parameters<typeof readStoredConversation>[0],
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const selected = yield* resolveHistoryRequest(request);
    const conversationId = selected.conversationId;
    if (conversationId.length === 0) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    const [head] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT record_hash,ordinal FROM certified_records WHERE conversation_id=? ORDER BY ordinal DESC LIMIT 1",
      [conversationId],
    );
    if (head === undefined) {
      return yield* new EndpointStoreError({
        reason: selected.resumed ? "invalid-continuation" : "not-found",
      });
    }
    const throughRecordHash =
      selected.throughRecordHash ?? readText(head, "record_hash");
    const through = yield* historyPosition(
      conversationId,
      throughRecordHash,
      selected.resumed,
    );
    const after =
      selected.afterRecordHash === undefined
        ? -1
        : yield* historyPosition(
            conversationId,
            selected.afterRecordHash,
            selected.resumed,
          );
    if (after > through) {
      return yield* new EndpointStoreError({ reason: "invalid-continuation" });
    }
    return { conversationId, after, through, throughRecordHash };
  });
}
function resolveHistoryRequest(
  request: Parameters<typeof readStoredConversation>[0],
): Effect.Effect<
  {
    readonly conversationId: string;
    readonly afterRecordHash?: string;
    readonly throughRecordHash?: string;
    readonly resumed: boolean;
  },
  EndpointStoreError
> {
  if ("continuation" in request) {
    return decodeHistoryCursor(request.continuation).pipe(
      Effect.map((cursor) => ({ ...cursor, resumed: true })),
    );
  }
  return Effect.succeed({ ...request, resumed: false });
}
function decodeHistoryCursor(value: string) {
  return Effect.try({
    try: () => {
      if (value.length > 4096 || !/^[A-Za-z0-9_-]+$/u.test(value)) {
        throw new StoreSignal("invalid-continuation");
      }
      const bytes = Buffer.from(value, "base64url");
      if (bytes.toString("base64url") !== value) {
        throw new StoreSignal("invalid-continuation");
      }
      return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    },
    catch: () => new EndpointStoreError({ reason: "invalid-continuation" }),
  }).pipe(
    Effect.flatMap(decodeCursor),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new EndpointStoreError({ reason: "invalid-continuation" })),
    ),
  );
}
function historyPosition(
  conversationId: string,
  recordHash: string,
  continuation: boolean,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT ordinal FROM certified_records WHERE conversation_id=? AND record_hash=?",
      [conversationId, recordHash],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({
        reason: continuation ? "invalid-continuation" : "not-found",
      });
    }
    return readInteger(row, "ordinal");
  }).pipe(storeOperation);
}
