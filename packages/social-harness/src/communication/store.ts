/** @file Retains accepted messages, ordered history, delivery acknowledgments and request results. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Data, Effect, Option, Schema } from "effect";

import { withAgentTransaction } from "../database/transactions.js";
import {
  canonicalBytes,
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../encoding/index.js";
import { AgentId } from "../identity/index.js";
import { type SendFailure, sendFailureReasons } from "./errors.js";
import {
  CollectiveId,
  type CollectiveValue,
  maximumMembers,
  Message,
  messageInterpretation,
  MessageReference,
  Request,
  ResultDelivery,
} from "./messages.js";

/** One closed endpoint-store failure. */
export class EndpointStoreError extends Data.TaggedError("EndpointStoreError")<{
  readonly reason: EndpointStoreFailure;
}> {}

/** Internal control-flow signal mapped to the closed store error. */
export class StoreSignal extends Error {
  readonly reason: EndpointStoreFailure;
  constructor(reason: EndpointStoreFailure) {
    super(reason);
    this.reason = reason;
  }
}

/** Closed storage failures that never expose database diagnostics. */
export type EndpointStoreFailure =
  | "closed"
  | "conflict"
  | "corrupt"
  | "incompatible"
  | "invalid-continuation"
  | "invalid-input"
  | "not-found"
  | "persistence";

/** Whether an idempotent mutation inserted state or observed the same state. */
export type StoreMutation = "inserted" | "existing";

const decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Encode immutable runtime values before they enter the endpoint store.
 * @param value Validated runtime value without process resources.
 * @returns Canonical bytes or a closed persistence failure.
 */
export const encodeRuntimeValue = (
  value: unknown,
): Effect.Effect<Uint8Array, EndpointStoreError> =>
  canonicalBytes(value).pipe(
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.fail(new EndpointStoreError({ reason: "invalid-input" })),
    ),
  );

/**
 * Validate stored bytes at their owning runtime boundary before reuse.
 * @param schema Closed schema of the stored runtime value.
 * @param bytes Retained canonical UTF-8 bytes.
 * @returns The decoded value or a closed corruption failure.
 */
const decodeRuntimeValue = <A, I>(
  schema: Schema.Schema<A, I>,
  bytes: Uint8Array,
): Effect.Effect<A, EndpointStoreError> =>
  Effect.try({
    try: () => decoder.decode(bytes),
    catch: () => new EndpointStoreError({ reason: "corrupt" }),
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(Schema.parseJson(schema))),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
    ),
  );

type OperationValue = Extract<CollectiveValue, { readonly kind: "operation" }>;

type CloseValue = Extract<CollectiveValue, { readonly kind: "close" }>;

interface OperationState {
  readonly operationId: CollectiveId;
  readonly deadlineAt: number;
  readonly phase: "collecting" | "finalizing" | "completed";
  readonly close?: CloseValue;
  readonly result?: {
    readonly item: ResultDelivery["item"];
    readonly answers: readonly MessageReference[];
  };
}

/** Disk readers distinguish local captured questions from recognized peer requests. */
export type StoredCollectiveOperation = OperationState &
  (
    | { readonly origin: "local"; readonly request: Request }
    | {
        readonly origin: "remote";
        readonly request: {
          readonly message: Message;
          readonly value: OperationValue;
        };
      }
  );

/**
 * Communication retains the accepted Message. A collective retains its role
 * and identity.
 */
export interface StoredCollectiveMessage {
  readonly operationId: CollectiveId;
  readonly role: "request" | "response" | "close";
  readonly destination: string;
  readonly reference: MessageReference;
}

/** Results become available only after their required close certifies. */
export const readNextCollectiveResult = (agentKey: string) =>
  readOperations(agentKey).pipe(
    Effect.map((rows) => rows[0]),
    Effect.withSpan("readNextCollectiveResult"),
  );

/** A published message has one immutable position and a separate delivery acknowledgment. */
export interface StoredMessage {
  readonly conversationId: string;
  readonly ordinal: number;
  readonly sequence: number;
  readonly message: Message;
}

/** Each construction checks the captured agent before touching retained protocol work. */
export function initializeCommunication(
  agentKey: string,
  identity: { readonly agentId: string },
) {
  return transaction(
    agentKey,
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      yield* sql`INSERT INTO communication_metadata(agent_key,agent_id)
      VALUES(${agentKey},${identity.agentId}) ON CONFLICT(agent_key) DO NOTHING`;
      const [row] = yield* sql<{
        readonly agentId: string;
      }>`SELECT agent_id AS "agentId"
      FROM communication_metadata WHERE agent_key=${agentKey}`;
      if (row?.agentId !== identity.agentId) {
        return yield* new EndpointStoreError({ reason: "conflict" });
      }
    }),
  ).pipe(Effect.withSpan("initializeCommunication"));
}

/**
 * Whether a store error refuses the input itself, because it conflicts with
 * durable state or is invalid, rather than reporting a failed store.
 * @param error A closed endpoint-store error.
 * @returns Whether the store refused the input.
 */
export function isSemanticStoreRejection(error: EndpointStoreError): boolean {
  return error.reason === "conflict" || error.reason === "invalid-input";
}

/** Acceptance stays in the caller's SQL transaction and performs no network work. */
export function acceptMessage(
  agentKey: string,
  input: {
    readonly authorAgentId: string;
    readonly msgId: string;
    readonly conversationId: string;
    readonly canonicalMessage: Uint8Array;
  },
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const identity = yield* readStoredIdentity(agentKey);
    if (identity === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(identity.agentId, input.authorAgentId);
    yield* sql`INSERT INTO message_intents(agent_key,author_agent_id,msg_id,conversation_id,canonical_message)
       VALUES(${agentKey},${input.authorAgentId},${input.msgId},${input.conversationId},${input.canonicalMessage}) ON CONFLICT(agent_key,author_agent_id,msg_id) DO NOTHING`;
    const [row] = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id,canonical_message FROM message_intents WHERE agent_key=${agentKey} AND author_agent_id=${input.authorAgentId} AND msg_id=${input.msgId}`;
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    requireEqual(readText(row, "conversation_id"), input.conversationId);
    requireSameBytes(
      readBytes(row, "canonical_message"),
      input.canonicalMessage,
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("acceptMessage"));
}

/** The owning reader returns the retained Message and durable terminal outcome. */
export function readAcceptedMessage(
  agentKey: string,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT author_agent_id,msg_id,canonical_message,failure_reason, CASE WHEN EXISTS(SELECT 1 FROM message_log WHERE message_log.agent_key=message_intents.agent_key AND message_log.author_agent_id=message_intents.author_agent_id AND message_log.msg_id=message_intents.msg_id) THEN 1 ELSE 0 END AS completed FROM message_intents WHERE agent_key=${agentKey} AND author_agent_id=${reference.authorAgentId} AND msg_id=${reference.msgId}`;
    return row === undefined ? undefined : yield* readAcceptedRow(row);
  }).pipe(agentOperation(agentKey), Effect.withSpan("readAcceptedMessage"));
}

/** Terminal preparation failures remain observable after the process restarts. */
export function failMessage(
  agentKey: string,
  reference: MessageReference,
  reason: SendFailure,
) {
  return Effect.gen(function* () {
    requireText(reason);
    const sql = yield* SqlClient;
    yield* sql`UPDATE message_intents SET failure_reason=COALESCE(failure_reason,${reason})
       WHERE agent_key=${agentKey} AND author_agent_id=${reference.authorAgentId} AND msg_id=${reference.msgId} AND NOT EXISTS(SELECT 1 FROM message_log WHERE message_log.agent_key=message_intents.agent_key AND message_log.author_agent_id=message_intents.author_agent_id AND message_log.msg_id=message_intents.msg_id)`;
  }).pipe(agentOperation(agentKey), Effect.withSpan("failMessage"));
}

/**
 * Accepted messages of this conversation that have neither completed nor
 * failed, in the order they were accepted, including those whose membership is
 * not prepared yet.
 */
export function readPendingMessages(agentKey: string, conversationId: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT author_agent_id,msg_id,canonical_message,failure_reason, CASE WHEN EXISTS(SELECT 1 FROM message_log WHERE message_log.agent_key=message_intents.agent_key AND message_log.author_agent_id=message_intents.author_agent_id AND message_log.msg_id=message_intents.msg_id) THEN 1 ELSE 0 END AS completed FROM message_intents WHERE agent_key=${agentKey} AND conversation_id=${conversationId} AND NOT EXISTS(SELECT 1 FROM message_log WHERE message_log.agent_key=message_intents.agent_key AND message_log.author_agent_id=message_intents.author_agent_id AND message_log.msg_id=message_intents.msg_id) AND failure_reason IS NULL ORDER BY sequence`;
    return yield* Effect.forEach(
      rows,
      (row) => readAcceptedRow(row).pipe(Effect.map(({ message }) => message)),
      { concurrency: 1 },
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("readPendingMessages"));
}

/** A repeated operation must preserve its original request and authority. */
export function putCollectiveOperation(
  agentKey: string,
  input:
    | { readonly origin: "local"; readonly request: Request }
    | {
        readonly origin: "remote";
        readonly request: Message;
        readonly value: OperationValue;
      },
) {
  return transaction(
    agentKey,
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      const operationId =
        input.origin === "local"
          ? input.request.operationId
          : input.value.operationId;
      const deadlineAt =
        input.origin === "local"
          ? input.request.deadlineAt
          : input.value.deadlineAt;
      const canonicalRequest = yield* input.origin === "local"
        ? encodeCanonical(Request, input.request)
        : encodeCanonical(Message, input.request);
      const [existing] = yield* sql<
        Readonly<Record<string, unknown>>
      >`SELECT canonical_request,origin,creator_agent_id,deadline_at FROM collective_operations WHERE agent_key=${agentKey} AND operation_id=${operationId}`;
      if (existing !== undefined) {
        requireSameBytes(
          readBytes(existing, "canonical_request"),
          canonicalRequest,
        );
        if (
          readText(existing, "origin") !== input.origin ||
          readText(existing, "creator_agent_id") !==
            input.request.author.agentId ||
          readInteger(existing, "deadline_at") !== deadlineAt
        ) {
          return yield* new EndpointStoreError({ reason: "conflict" });
        }
        return;
      }
      yield* sql`INSERT INTO collective_operations(agent_key,operation_id,creator_agent_id,origin,canonical_request,deadline_at) VALUES(${agentKey},${operationId},${input.request.author.agentId},${input.origin},${canonicalRequest},${deadlineAt})`;
    }),
  ).pipe(agentOperation(agentKey), Effect.withSpan("putCollectiveOperation"));
}

/** Workers load each operation's content inside its completion transaction. */
export function readCollectiveOperationIds(
  agentKey: string,
  origin: StoredCollectiveOperation["origin"],
  phase: StoredCollectiveOperation["phase"],
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT operation_id FROM collective_operations WHERE agent_key=${agentKey} AND origin=${origin} AND phase=${phase} ORDER BY sequence`;
    return yield* Effect.forEach(
      rows,
      (row) => decodeValue(CollectiveId, readText(row, "operation_id")),
      { concurrency: 1 },
    );
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("readCollectiveOperationIds"),
  );
}

/** Role uniqueness and Communication acceptance commit in the same caller transaction. */
export function putCollectiveMessage(
  agentKey: string,
  input: StoredCollectiveMessage,
) {
  return transaction(
    agentKey,
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      const prior = (yield* sql<
        Readonly<Record<string, unknown>>
      >`SELECT author_agent_id,msg_id FROM collective_messages WHERE agent_key=${agentKey} AND operation_id=${input.operationId} AND role=${input.role} AND destination=${input.destination}`)[0];
      if (prior !== undefined) {
        if (
          readText(prior, "author_agent_id") !==
            input.reference.authorAgentId ||
          readText(prior, "msg_id") !== input.reference.msgId
        ) {
          return yield* new EndpointStoreError({ reason: "conflict" });
        }
        return false;
      }
      yield* sql`INSERT INTO collective_messages(agent_key,operation_id,role,destination,author_agent_id,msg_id) VALUES(${agentKey},${input.operationId},${input.role},${input.destination},${input.reference.authorAgentId},${input.reference.msgId})`;
      return true;
    }),
  ).pipe(agentOperation(agentKey), Effect.withSpan("putCollectiveMessage"));
}

/** An operation's accepted messages retain their role and fixed identity. */
export function readCollectiveMessages(
  agentKey: string,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT operation_id,role,destination,author_agent_id,msg_id
      FROM collective_messages WHERE agent_key=${agentKey} AND operation_id=${operationId} ORDER BY sequence`;
    return yield* readMessageRows(agentKey, rows);
  }).pipe(agentOperation(agentKey), Effect.withSpan("readCollectiveMessages"));
}

/** One bounded worker observes only durable completion or terminal failure. */
export function readReadyCollectiveMessages(agentKey: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT message.operation_id,message.role,message.destination,message.author_agent_id,message.msg_id
      FROM collective_messages AS message
      JOIN collective_operations AS operation ON operation.agent_key=message.agent_key AND operation.operation_id=message.operation_id
      JOIN message_intents AS intent ON intent.agent_key=message.agent_key AND intent.author_agent_id=message.author_agent_id AND intent.msg_id=message.msg_id
      WHERE message.agent_key=${agentKey} AND operation.phase<>'completed' AND message.role IN ('response','close')
      AND (EXISTS(SELECT 1 FROM message_log WHERE message_log.agent_key=intent.agent_key AND message_log.author_agent_id=intent.author_agent_id AND message_log.msg_id=intent.msg_id) OR (intent.failure_reason IS NOT NULL AND (message.role='close' OR operation.phase='finalizing')))
      AND (message.role='close' OR NOT EXISTS (SELECT 1 FROM collective_answers AS answer WHERE answer.agent_key=message.agent_key AND answer.operation_id=message.operation_id AND answer.member=message.author_agent_id))
      ORDER BY message.sequence`;
    return yield* readMessageRows(agentKey, rows);
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("readReadyCollectiveMessages"),
  );
}

/** Only the collector's first accepted answer before its deadline is selectable. */
export function recordCollectiveAnswer(
  agentKey: string,
  operationId: CollectiveId,
  message: Message,
  now: number,
) {
  return transaction(
    agentKey,
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      const operation = yield* readCollectiveOperation(agentKey, operationId);
      if (operation === undefined || operation.phase === "completed") {
        return false;
      }
      if (
        operation.origin === "local" &&
        (operation.phase !== "collecting" || now >= operation.deadlineAt)
      ) {
        return false;
      }
      const member = message.author.agentId;
      const prior =
        yield* sql`SELECT 1 FROM collective_answers WHERE agent_key=${agentKey} AND operation_id=${operationId} AND member=${member}`;
      if (prior.length > 0) {
        return false;
      }
      yield* sql`INSERT INTO collective_answers(agent_key,operation_id,member,canonical_message) VALUES(${agentKey},${operationId},${member},${yield* encodeCanonical(Message, message)})`;
      return true;
    }),
  ).pipe(agentOperation(agentKey), Effect.withSpan("recordCollectiveAnswer"));
}

/** Related domain writes use the driver's connection and transaction context. */
export function transaction<A, E, R>(
  agentKey: string,
  operation: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R | SqlClient> {
  return withAgentTransaction(agentKey, operation).pipe(
    agentOperation(agentKey),
  );
}

/** Stored requests and their identifiers are validated before business operations receive them. */
export function readCollectiveOperation(
  agentKey: string,
  operationId: CollectiveId,
) {
  return readOperations(agentKey, operationId).pipe(
    Effect.map((rows) => rows[0]),
    Effect.withSpan("readCollectiveOperation"),
  );
}

/** First-answer messages are decoded once at their persistence boundary. */
export function readCollectiveAnswers(
  agentKey: string,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT canonical_message FROM collective_answers WHERE agent_key=${agentKey} AND operation_id=${operationId} ORDER BY sequence`;
    return yield* Effect.forEach(
      rows,
      (row) => decodeRuntimeValue(Message, readBytes(row, "canonical_message")),
      { concurrency: 1 },
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("readCollectiveAnswers"));
}

/** The first authenticated remote close remains available while an own answer certifies. */
export function saveRemoteClose(
  agentKey: string,
  operationId: CollectiveId,
  message: Message,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql`UPDATE collective_operations SET phase='finalizing',canonical_close=${yield* encodeCanonical(Message, message)} WHERE agent_key=${agentKey} AND operation_id=${operationId} AND origin='remote' AND phase='collecting'`;
  }).pipe(agentOperation(agentKey), Effect.withSpan("saveRemoteClose"));
}

/**
 * Freezes final bytes and exact source identities. Close certification
 * changes only their visibility.
 */
export function saveCollectiveResult(
  agentKey: string,
  input: {
    readonly operationId: CollectiveId;
    readonly item: ResultDelivery["item"];
    readonly answers: readonly MessageReference[];
    readonly complete: boolean;
  },
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql`UPDATE collective_operations SET phase=${input.complete ? "completed" : "finalizing"},canonical_result=${yield* encodeCanonical(ResultDelivery.fields.item, input.item)},canonical_answer_refs=${yield* encodeRuntimeValue(input.answers)} WHERE agent_key=${agentKey} AND operation_id=${input.operationId} AND phase<>'completed' AND canonical_result IS NULL`;
  }).pipe(agentOperation(agentKey), Effect.withSpan("saveCollectiveResult"));
}

/** A selected result becomes deliverable without rebuilding its content or sources. */
export function completeCollectiveOperation(
  agentKey: string,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql`UPDATE collective_operations SET phase='completed' WHERE agent_key=${agentKey} AND operation_id=${operationId} AND canonical_result IS NOT NULL`;
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("completeCollectiveOperation"),
  );
}

/** A terminal close failure replaces only an unpublished result. */
export function failCollectiveClose(
  agentKey: string,
  operationId: CollectiveId,
  item: ResultDelivery["item"],
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql`UPDATE collective_operations SET phase='completed',canonical_result=${yield* encodeCanonical(ResultDelivery.fields.item, item)},canonical_answer_refs=${yield* encodeRuntimeValue([])} WHERE agent_key=${agentKey} AND operation_id=${operationId} AND phase='finalizing' AND delivered=0`;
  }).pipe(agentOperation(agentKey), Effect.withSpan("failCollectiveClose"));
}

/** Successful handling retires the same result while keeping its content readable. */
export function retireCollectiveResult(
  agentKey: string,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql`UPDATE collective_operations SET delivered=1 WHERE agent_key=${agentKey} AND operation_id=${operationId} AND phase='completed'`;
  }).pipe(agentOperation(agentKey), Effect.withSpan("retireCollectiveResult"));
}

/** An invalid close consumes protocol state without inventing a result. */
export function retireCollectiveOperation(
  agentKey: string,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql`UPDATE collective_operations SET phase='completed',delivered=1 WHERE agent_key=${agentKey} AND operation_id=${operationId}`;
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("retireCollectiveOperation"),
  );
}

/** Shared readers validate stored identity keys before interpreting completion. */
export function readAcceptedRow(row: Readonly<Record<string, unknown>>) {
  return Effect.gen(function* () {
    const canonicalMessage = readBytes(row, "canonical_message");
    const message = yield* decodeCanonical(Message, canonicalMessage);
    requireEqual(message.author.agentId, readText(row, "author_agent_id"));
    requireEqual(message.msgId, readText(row, "msg_id"));
    const savedFailure = readOptionalText(row, "failure_reason");
    const failure =
      savedFailure === undefined
        ? undefined
        : yield* Schema.decodeUnknown(Schema.Literal(...sendFailureReasons))(
            savedFailure,
          );
    return {
      message,
      canonicalMessage,
      completed: row.completed === 1,
      failure,
    };
  }).pipe(storeOperation, Effect.withSpan("readAcceptedRow"));
}

/** The communication owner binds one immutable agent identity to its protocol state. */
export function readStoredIdentity(agentKey: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql<{
      readonly agentId: string;
    }>`SELECT agent_id AS "agentId" FROM communication_metadata WHERE agent_key=${agentKey}`;
    return row === undefined
      ? undefined
      : {
          agentId: yield* Schema.decodeUnknown(AgentId)(row.agentId).pipe(
            Effect.catchTag(
              "ParseError",
              () => new EndpointStoreError({ reason: "corrupt" }),
            ),
          ),
        };
  }).pipe(
    Effect.withSpan("readStoredIdentity"),
    Effect.catchTag(
      "SqlError",
      () => new EndpointStoreError({ reason: "persistence" }),
    ),
  );
}

/**
 * Reads one nullable nonempty SQL text.
 *
 * @param row SQL result row.
 * @param key Selected column name.
 * @returns The exact text, or undefined for SQL NULL.
 */
export function readOptionalText(
  row: Readonly<Record<string, unknown>>,
  key: string,
): string | undefined {
  const value = row[key];
  if (value === null) {
    return undefined;
  }
  return readText(row, key);
}

/**
 * Rejects empty and NUL-bearing values before they reach SQLite.
 *
 * @param value Opaque private identifier or hash.
 */
export function requireText(value: string): void {
  if (value.length === 0 || value.includes("\u0000")) {
    throw new StoreSignal("invalid-input");
  }
}

/**
 * Reads and copies one nullable SQLite BLOB.
 *
 * @param row SQL result row.
 * @param key Selected column name.
 * @returns An isolated byte copy, or undefined for SQL NULL.
 */
export function readOptionalBytes(
  row: Readonly<Record<string, unknown>>,
  key: string,
): Uint8Array | undefined {
  const value = row[key];
  if (value === null) {
    return undefined;
  }
  return readBytes(row, key);
}

/** SQL failures and row corruption leave through one closed storage error channel. */
export function storeOperation<A, E, R>(
  operation: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R> {
  return operation.pipe(
    Effect.catchAllCause((cause) => {
      if (Cause.isInterruptedOnly(cause)) {
        return Effect.failCause(Cause.stripFailures(cause));
      }
      const failure = Cause.failureOption(cause);
      const defect = Cause.dieOption(cause);
      return Effect.fail(
        mapStoreFailure(
          Option.getOrElse(failure, () =>
            Option.getOrElse(defect, () => undefined),
          ),
          "persistence",
        ),
      );
    }),
  );
}

/** Publish only after the selected delivery protocol has established completion. */
export function completeMessage(
  agentKey: string,
  conversationId: string,
  ordinal: number,
  message: Message,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const bytes = yield* encodeCanonical(Message, message);
    yield* sql`INSERT INTO message_log(agent_key,conversation_id,ordinal,author_agent_id,msg_id,canonical_message)
      VALUES(${agentKey},${conversationId},${ordinal},${message.author.agentId},${message.msgId},${bytes})
      ON CONFLICT(agent_key,author_agent_id,msg_id) DO NOTHING`;
    const [row] = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id,ordinal,canonical_message FROM message_log
      WHERE agent_key=${agentKey} AND author_agent_id=${message.author.agentId} AND msg_id=${message.msgId}`;
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    requireEqual(readText(row, "conversation_id"), conversationId);
    requireEqual(readInteger(row, "ordinal"), ordinal);
    requireSameBytes(readBytes(row, "canonical_message"), bytes);
  }).pipe(agentOperation(agentKey), Effect.withSpan("completeMessage"));
}

/**
 * Requires canonical byte equality for idempotent state.
 *
 * @param left Previously retained canonical bytes.
 * @param right Candidate canonical bytes.
 */
export function requireSameBytes(left: Uint8Array, right: Uint8Array): void {
  if (!sameBytes(left, right)) {
    throw new StoreSignal("conflict");
  }
}

/** Read a bounded range from the same message log for every delivery mode. */
export function readMessages(
  agentKey: string,
  input: {
    readonly conversationId?: string;
    readonly after?: number;
    readonly through?: number;
    readonly newest?: boolean;
    readonly pending?: boolean;
    readonly limit: number;
  },
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const conversation =
      input.conversationId === undefined
        ? sql``
        : sql`AND conversation_id=${input.conversationId}`;
    const after =
      input.after === undefined ? sql`` : sql`AND ordinal>${input.after}`;
    const through =
      input.through === undefined ? sql`` : sql`AND ordinal<=${input.through}`;
    const pending = input.pending
      ? sql`AND consumed=0 AND author_agent_id<>(SELECT agent_id FROM communication_metadata WHERE agent_key=${agentKey})`
      : sql``;
    const ascending =
      input.conversationId === undefined
        ? sql`ORDER BY sequence`
        : sql`ORDER BY ordinal`;
    const order = input.newest ? sql`ORDER BY ordinal DESC` : ascending;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id,ordinal,sequence,author_agent_id,msg_id,canonical_message FROM message_log WHERE agent_key=${agentKey} ${conversation} ${after} ${through} ${pending} ${order} LIMIT ${input.limit}`;
    return yield* Effect.forEach(
      rows,
      (row) =>
        decodeCanonical(Message, readBytes(row, "canonical_message")).pipe(
          Effect.map((message) => {
            requireEqual(
              message.author.agentId,
              readText(row, "author_agent_id"),
            );
            requireEqual(message.msgId, readText(row, "msg_id"));
            return {
              conversationId: readText(row, "conversation_id"),
              ordinal: readInteger(row, "ordinal"),
              sequence: readInteger(row, "sequence"),
              message,
            };
          }),
        ),
      { concurrency: 1 },
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("readMessages"));
}

/**
 * Requires two exact bindings, including optional bindings, to agree.
 *
 * @param left Previously retained binding.
 * @param right Candidate binding.
 */
export function requireEqual<Value>(left: Value, right: Value): void {
  if (left !== right) {
    throw new StoreSignal("conflict");
  }
}

/**
 * Reads one safe SQL integer.
 *
 * @param row SQL result row when one was selected.
 * @param key Selected column name.
 * @returns The exact safe integer.
 */
export function readInteger(
  row: Readonly<Record<string, unknown>>,
  key: string,
): number {
  const stored = row[key];
  const value =
    typeof stored === "string" && /^-?\d+$/u.test(stored)
      ? Number(stored)
      : stored;
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new StoreSignal("corrupt");
  }
  return value;
}

/**
 * Reads one required nonempty SQL text.
 *
 * @param row SQL result row when one was selected.
 * @param key Selected column name.
 * @returns The exact nonempty text.
 */
export function readText(
  row: Readonly<Record<string, unknown>>,
  key: string,
): string {
  const value = row[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new StoreSignal("corrupt");
  }
  return value;
}

/**
 * Reads and copies one required SQLite BLOB.
 *
 * @param row SQL result row.
 * @param key Selected column name.
 * @returns An isolated nonempty byte copy.
 */
export function readBytes(
  row: Readonly<Record<string, unknown>>,
  key: string,
): Uint8Array {
  const value = row[key];
  if (!(value instanceof Uint8Array) || value.byteLength === 0) {
    throw new StoreSignal("corrupt");
  }
  return copyBytes(value);
}

/**
 * Returns an isolated copy of canonical bytes.
 *
 * @param value Canonical bytes crossing the storage boundary.
 * @returns A detached copy suitable for the database or a caller.
 */
export function copyBytes(value: Uint8Array): Uint8Array {
  return new Uint8Array(value);
}

/** A failed or interrupted handler leaves the exact message available for another attempt. */
export function acknowledgeMessage(
  agentKey: string,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql`UPDATE message_log SET consumed=1 WHERE agent_key=${agentKey} AND author_agent_id=${reference.authorAgentId} AND msg_id=${reference.msgId}`;
  }).pipe(agentOperation(agentKey), Effect.withSpan("acknowledgeMessage"));
}

/**
 * The protocol identity row serializes one agent's transactions on PostgreSQL,
 * matching SQLite's connection serialization. Nested calls retain the lock
 * until the enclosing transaction commits; other agents use their own rows.
 * Store operations perform no network or model work.
 */
export function agentOperation(agentKey: string) {
  return <A, E, R>(work: Effect.Effect<A, E, R>) =>
    withAgentTransaction(
      agentKey,
      Effect.gen(function* () {
        const sql = yield* SqlClient;
        yield* sql.onDialectOrElse({
          pg: () =>
            sql`SELECT agent_key FROM communication_metadata WHERE agent_key=${agentKey} FOR UPDATE`.pipe(
              Effect.asVoid,
            ),
          orElse: () => Effect.void,
        });
        return yield* work;
      }),
    ).pipe(storeOperation, Effect.withSpan("agentOperation"));
}

function readMessageRows(
  agentKey: string,
  rows: ReadonlyArray<Readonly<Record<string, unknown>>>,
) {
  return Effect.gen(function* () {
    return yield* Effect.forEach(
      rows,
      (row) =>
        Effect.gen(function* () {
          const role = readText(row, "role");
          if (role !== "request" && role !== "response" && role !== "close") {
            return yield* new EndpointStoreError({ reason: "corrupt" });
          }
          return {
            operationId: yield* decodeValue(
              CollectiveId,
              readText(row, "operation_id"),
            ),
            role,
            destination: yield* decodeValue(
              Schema.String,
              readText(row, "destination"),
            ),
            reference: yield* decodeValue(MessageReference, {
              authorAgentId: readText(row, "author_agent_id"),
              msgId: readText(row, "msg_id"),
            }),
          } satisfies StoredCollectiveMessage;
        }),
      { concurrency: 1 },
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("readCollectiveMessages"));
}

function readOperations(agentKey: string, operationId?: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const selection =
      operationId === undefined
        ? sql`AND phase='completed' AND delivered=0 AND canonical_result IS NOT NULL`
        : sql`AND operation_id=${operationId}`;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT * FROM collective_operations WHERE agent_key=${agentKey}
      ${selection}
      ORDER BY sequence LIMIT 1`;
    return yield* Effect.forEach(rows, readOperation, { concurrency: 1 });
  }).pipe(agentOperation(agentKey));
}

function readOperation(
  row: Readonly<Record<string, unknown>>,
): Effect.Effect<StoredCollectiveOperation, EndpointStoreError> {
  return Effect.gen(function* () {
    const operationId = yield* decodeValue(
      CollectiveId,
      readText(row, "operation_id"),
    );
    const creatorAgentId = yield* decodeValue(
      AgentId,
      readText(row, "creator_agent_id"),
    );
    const origin = yield* decodeValue(
      Schema.Literal("local", "remote"),
      readText(row, "origin"),
    );
    const phase = yield* decodeValue(
      Schema.Literal("collecting", "finalizing", "completed"),
      readText(row, "phase"),
    );
    const closeBytes = readOptionalBytes(row, "canonical_close");
    const close =
      closeBytes === undefined
        ? undefined
        : yield* readPart(closeBytes, "close");
    const result = yield* readResult(row, operationId);
    if (close !== undefined) {
      if (
        origin !== "remote" ||
        close.value.operationId !== operationId ||
        close.message.author.agentId !== creatorAgentId
      ) {
        return yield* new EndpointStoreError({ reason: "corrupt" });
      }
    }
    const state: OperationState = {
      operationId,
      phase,
      deadlineAt: readInteger(row, "deadline_at"),
      ...(close === undefined ? {} : { close: close.value }),
      ...(result === undefined ? {} : { result }),
    };
    if (origin === "local") {
      const request = yield* decodeRuntimeValue(
        Request,
        readBytes(row, "canonical_request"),
      );
      if (
        request.operationId !== operationId ||
        request.author.agentId !== creatorAgentId ||
        request.deadlineAt !== state.deadlineAt
      ) {
        return yield* new EndpointStoreError({ reason: "corrupt" });
      }
      return { ...state, origin, request };
    }
    const request = yield* readPart(
      readBytes(row, "canonical_request"),
      "operation",
    );
    if (
      request.value.operationId !== operationId ||
      request.message.author.agentId !== creatorAgentId ||
      request.value.deadlineAt !== state.deadlineAt
    ) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return { ...state, origin, request };
  });
}

function readResult(
  row: Readonly<Record<string, unknown>>,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    const bytes = readOptionalBytes(row, "canonical_result");
    const refs = readOptionalBytes(row, "canonical_answer_refs");
    if (bytes === undefined && refs === undefined) {
      return undefined;
    }
    if (bytes === undefined || refs === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const item = yield* decodeRuntimeValue(ResultDelivery.fields.item, bytes);
    if (
      (item.kind === "result"
        ? item.result.request.operationId
        : item.request.operationId) !== operationId
    ) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return {
      item,
      answers: yield* decodeRuntimeValue(
        Schema.Array(MessageReference).pipe(
          Schema.maxItems(maximumMembers - 1),
        ),
        refs,
      ),
    };
  });
}

function readPart(
  bytes: Uint8Array,
  kind: "operation",
): Effect.Effect<
  { readonly message: Message; readonly value: OperationValue },
  EndpointStoreError
>;

function readPart(
  bytes: Uint8Array,
  kind: "close",
): Effect.Effect<
  { readonly message: Message; readonly value: CloseValue },
  EndpointStoreError
>;
function readPart(bytes: Uint8Array, kind: "operation" | "close") {
  return Effect.gen(function* () {
    const message = yield* decodeRuntimeValue(Message, bytes);
    const value = yield* messageInterpretation(message).pipe(
      Effect.catchTag(
        "CollectivePartInvalidError",
        () => new EndpointStoreError({ reason: "corrupt" }),
      ),
    );
    if (Option.isNone(value) || value.value.kind !== kind) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return { message, value: value.value };
  });
}

function decodeValue<A, I>(schema: Schema.Schema<A, I>, value: unknown) {
  return Schema.decodeUnknown(schema)(value).pipe(
    Effect.catchTag(
      "ParseError",
      () => new EndpointStoreError({ reason: "corrupt" }),
    ),
  );
}

/**
 * Maps every internal failure to the store's closed error channel.
 *
 * @param failure Internal failure or closed control-flow signal.
 * @param fallback Closed category for unexpected implementation failures.
 * @returns A non-diagnostic endpoint-store error.
 */
function mapStoreFailure(
  failure: unknown,
  fallback: EndpointStoreFailure,
): EndpointStoreError {
  if (failure instanceof EndpointStoreError) {
    return failure;
  }
  return new EndpointStoreError({
    reason: failure instanceof StoreSignal ? failure.reason : fallback,
  });
}
