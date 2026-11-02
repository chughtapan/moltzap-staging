/** @file Verified application history and live participant pages hide protocol positions. */
// safer-arch-ignore no-module-cycle: History imports only service contract types; the service supplies its implementation.

import { SqlClient } from "@effect/sql/SqlClient";
import { Effect, Option, Schema } from "effect";

import type {
  Conversation,
  ConversationPage,
  ConversationQuery,
  HistoryPage,
  HistoryQuery,
} from "./service.js";
import {
  decodeCanonical,
  encodeCanonical,
} from "../encoding/canonical-json.js";
import {
  compareAscii,
  decodeCanonicalBase64Url,
} from "../encoding/identifier.js";
import {
  AgentCard,
  agentCardOf,
  type AgentName,
  type AgentPublicKey,
  type VerifiedAgentIdentityCard,
} from "../identity/agent.js";
import {
  readStoredHistory,
  verifyStoredMembership,
} from "./conversations/log.js";
import { CommunicationError } from "./errors.js";
import {
  conversationNameKey,
  conversationSortKey,
  Message,
  messageInterpretation,
} from "./message.js";
import { deriveConversationId } from "./records/encoding.js";
import { RecordHash } from "./records/schemas.js";
import {
  compareAgentIds,
  maximumMembers,
  type MessageReference,
} from "./records/values.js";
import { hasPendingMessages } from "./store/messages.js";
import {
  readStoredConversation as readConversation,
  readMemberships,
  readRecentConversation,
} from "./store/reads.js";

const conversationCursor = Schema.Struct({
  operation: Schema.Literal("conversations"),
  session: Schema.String,
  after: Schema.String,
});

const historyCursor = Schema.Struct({
  operation: Schema.Literal("history"),
  session: Schema.String,
  conversation: Schema.String,
  after: Schema.String,
  position: Schema.String,
});

const membershipRow = Schema.Struct({
  conversation_id: Schema.String,
  membership_hash: Schema.String,
  canonical_membership: Schema.Uint8ArrayFromSelf,
  sort_key: Schema.String,
});

/** Live listing visits a bounded semantic-name index, including self-authored conversations. */
export const listConversations = Effect.fn("Communication.listConversations")(
  function* (
    query: ConversationQuery,
    local: VerifiedAgentIdentityCard,
    signer: AgentPublicKey,
    session: string,
  ) {
    let after = "";
    if (query.cursor !== undefined) {
      const cursor = yield* decodeCursor(conversationCursor, query.cursor);
      if (cursor.session !== session) {
        return yield* invalidCursor();
      }
      after = cursor.after;
    }
    const sql = yield* SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `
    SELECT membership.conversation_id, membership.membership_hash, membership.canonical_membership, membership.sort_key
    FROM memberships AS membership JOIN conversation_state AS state USING(conversation_id)
    WHERE state.head_record_hash IS NOT NULL AND membership.sort_key > ?
    ORDER BY membership.sort_key LIMIT 51`,
      [after],
    );
    const selected = rows.slice(0, 50);
    const conversations = yield* Effect.forEach(
      selected,
      (row) => conversationEntry(row, local, signer),
      { concurrency: 1 },
    );
    const last = selected.at(-1);
    const nextCursor =
      rows.length > 50 && last !== undefined
        ? yield* encodeCursor(conversationCursor)({
            operation: "conversations",
            session,
            after: yield* Schema.decodeUnknown(Schema.String)(last.sort_key),
          })
        : undefined;
    return {
      conversations,
      ...(nextCursor === undefined ? {} : { nextCursor }),
    } satisfies ConversationPage;
  },
  mapHistoryErrors,
);

/** Retained membership resolves names without contacting Registry or exposing log identifiers. */
export const getConversation = Effect.fn("Communication.getConversation")(
  function* (
    names: readonly AgentName[],
    local: VerifiedAgentIdentityCard,
    signer: AgentPublicKey,
  ) {
    if (
      names.length < 2 ||
      names.length > maximumMembers ||
      new Set(names).size !== names.length ||
      !names.includes(local.agentName)
    ) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    const sql = yield* SqlClient;
    const [row] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `
    SELECT membership.conversation_id, membership.membership_hash, membership.canonical_membership, membership.sort_key
    FROM memberships AS membership JOIN conversation_state AS state USING(conversation_id)
    WHERE state.head_record_hash IS NOT NULL AND membership.sort_key = ?`,
      [conversationNameKey(names, local.agentName)],
    );
    return row === undefined
      ? undefined
      : yield* conversationEntry(row, local, signer);
  },
  mapHistoryErrors,
);

/** History returns only application messages, preserving one fixed private-log boundary. */
export const readHistory = Effect.fn("Communication.readHistory")(function* (
  query: HistoryQuery,
  reader: {
    readonly local: VerifiedAgentIdentityCard;
    readonly signer: AgentPublicKey;
    readonly session: string;
    readonly maximumHistoryRecords: number;
  },
) {
  const { local, signer, session, maximumHistoryRecords } = reader;
  const members = query.members;
  const conversationId = yield* historyConversationId(
    query,
    local,
    maximumHistoryRecords,
  );
  const resume =
    query.order !== "newest" && query.cursor !== undefined
      ? yield* decodeCursor(historyCursor, query.cursor)
      : undefined;
  const after =
    query.order !== "newest" && query.afterMessage !== undefined
      ? `${query.afterMessage.authorAgentId}:${query.afterMessage.msgId}`
      : "";
  if (
    resume !== undefined &&
    (resume.session !== session ||
      resume.conversation !== conversationId ||
      resume.after !== after)
  ) {
    return yield* invalidCursor();
  }
  const [stored] = yield* readMemberships(conversationId);
  if (stored === undefined) {
    if (query.order === "newest" && query.beforeMessage === undefined) {
      return {
        entries: [],
        earlierMessagesOmitted: false,
        pendingOutgoing: yield* hasPendingMessages(conversationId),
      };
    }
    return yield* historyGap();
  }
  const verified = yield* verifyStoredMembership(stored, signer);
  const byName = (left: AgentCard, right: AgentCard) =>
    compareAscii(left.agentName, right.agentName);
  if (
    !Schema.equivalence(Schema.Array(AgentCard))(
      [...members].sort(byName),
      verified.members.map(agentCardOf).sort(byName),
    )
  ) {
    return yield* new CommunicationError({ reason: "membership-invalid" });
  }
  const page =
    query.order === "newest"
      ? yield* readNewest(query, conversationId, signer)
      : yield* readOldest(query, {
          conversationId,
          signer,
          session,
          initialPosition: resume?.position,
        });
  return {
    ...page,
    pendingOutgoing: yield* hasPendingMessages(conversationId),
  } satisfies HistoryPage;
}, mapHistoryErrors);

/** Reserved close and malformed traffic stays in the private log. */
export function isApplicationMessage(message: Message) {
  return messageInterpretation(message).pipe(
    Effect.match({
      onFailure: () => false,
      onSuccess: (value) =>
        Option.isNone(value) || value.value.kind !== "close",
    }),
  );
}

function invalidCursor() {
  return new CommunicationError({ reason: "invalid-cursor" });
}

function conversationEntry(
  row: Readonly<Record<string, unknown>>,
  local: VerifiedAgentIdentityCard,
  signer: AgentPublicKey,
): Effect.Effect<Conversation, CommunicationError> {
  return Effect.gen(function* () {
    const stored = yield* Schema.decodeUnknown(membershipRow)(row);
    const membership = yield* verifyStoredMembership(
      {
        conversationId: stored.conversation_id,
        membershipHash: stored.membership_hash,
        canonicalMembership: stored.canonical_membership,
      },
      signer,
    );
    const members = membership.members
      .map(agentCardOf)
      .sort((left, right) => compareAscii(left.agentName, right.agentName));
    if (
      !members.some((member) => member.agentId === local.agentId) ||
      conversationSortKey(members, local.agentId) !== stored.sort_key
    ) {
      return yield* historyGap();
    }
    const [first, second, ...rest] = members;
    if (first === undefined || second === undefined) {
      return yield* historyGap();
    }
    return [first, second, ...rest] satisfies Conversation;
  }).pipe(mapHistoryErrors);
}

/** Typed participants still need a coherent local selection and a bounded window. */
function historyConversationId(
  query: HistoryQuery,
  local: VerifiedAgentIdentityCard,
  maximumHistoryRecords: number,
) {
  return Effect.gen(function* () {
    const members = query.members;
    if (
      members.length < 2 ||
      members.length > maximumMembers ||
      new Set(members.map((member) => member.agentId)).size !==
        members.length ||
      new Set(members.map((member) => member.agentName)).size !== members.length
    ) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    if (
      query.order === "newest" &&
      (!Number.isSafeInteger(query.limit) ||
        query.limit < 1 ||
        query.limit > maximumHistoryRecords)
    ) {
      return yield* new CommunicationError({ reason: "content-invalid" });
    }
    if (!members.some((member) => member.agentId === local.agentId)) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    const ids = members.map((member) => member.agentId).sort(compareAgentIds);
    const [first, second, ...rest] = ids;
    if (first === undefined || second === undefined) {
      return yield* historyGap();
    }
    return yield* deriveConversationId([first, second, ...rest]);
  });
}

function readOldest(
  query: Extract<HistoryQuery, { readonly order?: "oldest" }>,
  reader: {
    readonly conversationId: string;
    readonly signer: AgentPublicKey;
    readonly session: string;
    readonly initialPosition?: string;
  },
) {
  const { conversationId, signer, session, initialPosition } = reader;
  return Effect.gen(function* () {
    const after =
      query.afterMessage === undefined
        ? ""
        : `${query.afterMessage.authorAgentId}:${query.afterMessage.msgId}`;
    let position = initialPosition;
    const entries: Array<{
      readonly message: Message;
      readonly messageBytes: number;
    }> = [];
    let firstPage = true;
    while (firstPage || position !== undefined) {
      firstPage = false;
      const request =
        position === undefined
          ? {
              conversationId,
              ...(query.afterMessage === undefined
                ? {}
                : {
                    afterRecordHash: yield* boundaryRecord(
                      conversationId,
                      query.afterMessage,
                    ),
                  }),
              maximumRecords: 50 - entries.length,
            }
          : { continuation: position, maximumRecords: 50 - entries.length };
      const page = yield* readConversation(request);
      const records = yield* readStoredHistory(signer, page, page.records);
      entries.push(...(yield* applicationEntries(records)));
      position = page.continuation ?? undefined;
      if (entries.length === 50 || position === undefined) {
        break;
      }
    }
    if (
      position !== undefined &&
      !(yield* hasLaterApplicationMessage(position, signer))
    ) {
      position = undefined;
    }
    const nextCursor =
      position === undefined
        ? undefined
        : yield* encodeCursor(historyCursor)({
            operation: "history",
            session,
            conversation: conversationId,
            after,
            position,
          });
    return {
      entries,
      ...(nextCursor === undefined ? {} : { nextCursor }),
      earlierMessagesOmitted: query.afterMessage !== undefined,
    };
  });
}

/** A continuation is present only when an application message remains in its fixed window. */
function hasLaterApplicationMessage(
  initialPosition: string,
  signer: AgentPublicKey,
) {
  return Effect.gen(function* () {
    let position: string | undefined = initialPosition;
    while (position !== undefined) {
      const page: Effect.Effect.Success<ReturnType<typeof readConversation>> =
        yield* readConversation({ continuation: position });
      const records = yield* readStoredHistory(signer, page, page.records);
      for (const record of records) {
        if (yield* isApplicationMessage(record.message)) {
          return true;
        }
      }
      position = page.continuation ?? undefined;
    }
    return false;
  });
}

function readNewest(
  query: Extract<HistoryQuery, { readonly order: "newest" }>,
  conversationId: string,
  signer: AgentPublicKey,
) {
  return Effect.gen(function* () {
    let before =
      query.beforeMessage === undefined
        ? undefined
        : yield* boundaryRecord(conversationId, query.beforeMessage);
    const entries: Array<{
      readonly message: Message;
      readonly messageBytes: number;
    }> = [];
    while (entries.length <= query.limit) {
      const snapshot = yield* readRecentConversation({
        conversationId,
        maximumRecords: Math.min(50, query.limit + 1 - entries.length),
        ...(before === undefined ? {} : { candidateRecordHash: before }),
      });
      const records = yield* readStoredHistory(
        signer,
        snapshot,
        snapshot.records,
      );
      const boundary = before;
      const candidates =
        boundary === undefined
          ? records
          : records.filter((record) => record.recordHash !== boundary);
      if (candidates.length === 0) {
        break;
      }
      entries.push(...(yield* applicationEntries(candidates)));
      before = candidates.at(-1)?.recordHash;
    }
    return {
      entries: entries.slice(0, query.limit).reverse(),
      earlierMessagesOmitted: entries.length > query.limit,
    };
  });
}

function applicationEntries(
  records: ReadonlyArray<{ readonly message: Message }>,
) {
  return Effect.forEach(
    records,
    (record) =>
      Effect.gen(function* () {
        if (!(yield* isApplicationMessage(record.message))) {
          return [];
        }
        return [
          {
            message: record.message,
            messageBytes: (yield* encodeCanonical(Message, record.message))
              .byteLength,
          },
        ];
      }),
    { concurrency: 1 },
  ).pipe(Effect.map((entries) => entries.flat()));
}

function boundaryRecord(conversationId: string, reference: MessageReference) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT staged.record_hash FROM staged_records AS staged JOIN certified_records AS certified USING(conversation_id,record_hash) WHERE staged.conversation_id=? AND staged.author_agent_id=? AND staged.msg_id=?`,
      [conversationId, reference.authorAgentId, reference.msgId],
    );
    if (row === undefined) {
      return yield* historyGap();
    }
    return yield* Schema.decodeUnknown(RecordHash)(row.record_hash);
  });
}

function decodeCursor<A, I>(schema: Schema.Schema<A, I>, cursor: string) {
  return Effect.fromNullable(decodeCanonicalBase64Url(cursor)).pipe(
    Effect.mapError(invalidCursor),
    Effect.flatMap((bytes) => decodeCanonical(schema, bytes)),
    Effect.mapError(invalidCursor),
  );
}

function encodeCursor<A, I>(schema: Schema.Schema<A, I>) {
  return (cursor: A) =>
    encodeCanonical(schema, cursor).pipe(
      Effect.map((bytes) => Buffer.from(bytes).toString("base64url")),
    );
}

function mapHistoryErrors<A, E, R>(effect: Effect.Effect<A, E, R>) {
  return effect.pipe(
    Effect.catchAll((error) => {
      if (error instanceof CommunicationError) {
        return Effect.fail(error);
      }
      if (
        typeof error === "object" &&
        error !== null &&
        "_tag" in error &&
        error._tag === "EndpointStoreError"
      ) {
        const reason = "reason" in error ? error.reason : undefined;
        if (reason === "invalid-continuation") {
          return Effect.fail(invalidCursor());
        }
        if (reason === "persistence" || reason === "closed") {
          return Effect.fail(
            new CommunicationError({ reason: "persistence-failed" }),
          );
        }
        return Effect.fail(historyGap());
      }
      if (
        typeof error === "object" &&
        error !== null &&
        "_tag" in error &&
        error._tag === "SqlError"
      ) {
        return Effect.fail(
          new CommunicationError({ reason: "persistence-failed" }),
        );
      }
      return Effect.fail(historyGap());
    }),
  );
}

function historyGap() {
  return new CommunicationError({ reason: "history-gap" });
}
