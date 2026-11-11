/** @file Stores each agent's ordered messages, which of them it has consumed, and its questions, answers and results. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Data, Effect, Option, Schema } from "effect";

import { withAgentTransaction } from "../database/transactions.js";
import {
  canonicalBytes,
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../encoding/index.js";
import { AgentId } from "../identity/agent-cards.js";
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
class StoreSignal extends Error {
  readonly reason: EndpointStoreFailure;
  constructor(reason: EndpointStoreFailure) {
    super(reason);
    this.reason = reason;
  }
}

/** Closed storage failures that never expose database diagnostics. */
type EndpointStoreFailure =
  | "conflict"
  | "corrupt"
  | "invalid-input"
  | "persistence";

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

interface OperationState {
  readonly operationId: CollectiveId;
  readonly deadlineAt: number;
  readonly phase: "collecting" | "completed";
  readonly result?: {
    readonly item: ResultDelivery["item"];
    readonly answers: readonly MessageReference[];
  };
}

/** A question this agent asked (`local`) or one it received (`remote`). */
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

/** The oldest completed result this agent has not consumed. */
export const readNextCollectiveResult = (agentKey: string) =>
  readOperations(agentKey).pipe(
    Effect.map((rows) => rows[0]),
    Effect.withSpan("readNextCollectiveResult"),
  );

/** A message at its position in a conversation, as one agent's history holds it. */
export interface StoredMessage {
  readonly conversationId: string;
  readonly ordinal: number;
  readonly sequence: number;
  readonly message: Message;
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

/** The questions this agent asked that are still collecting answers, oldest first. */
export function readCollectingQuestionIds(agentKey: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT operation_id FROM collective_operations WHERE agent_key=${agentKey} AND origin='local' AND phase='collecting' ORDER BY sequence`;
    return yield* Effect.forEach(
      rows,
      (row) => decodeValue(CollectiveId, readText(row, "operation_id")),
      { concurrency: 1 },
    );
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("readCollectingQuestionIds"),
  );
}

/**
 * Saves a member's first answer and reports whether it was saved. A question
 * this agent asked takes answers only while collecting and before its
 * deadline.
 */
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
      if (
        operation === undefined ||
        operation.phase === "completed" ||
        (operation.origin === "local" && now >= operation.deadlineAt)
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

/** Completes an open question with its result and the answers the result selected. */
export function saveCollectiveResult(
  agentKey: string,
  input: {
    readonly operationId: CollectiveId;
    readonly item: ResultDelivery["item"];
    readonly answers: readonly MessageReference[];
  },
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql`UPDATE collective_operations SET phase='completed',canonical_result=${yield* encodeCanonical(ResultDelivery.fields.item, input.item)},canonical_answer_refs=${yield* encodeRuntimeValue(input.answers)} WHERE agent_key=${agentKey} AND operation_id=${input.operationId} AND phase='collecting'`;
  }).pipe(agentOperation(agentKey), Effect.withSpan("saveCollectiveResult"));
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

/** Ends a question without a result, so that it takes no more answers. */
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

/**
 * Whether this agent's history holds the message. A saved message with the
 * same author and identifier but another conversation or content is a
 * conflict.
 */
export function hasMessage(
  agentKey: string,
  conversationId: string,
  message: Message,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id,canonical_message FROM message_log
      WHERE agent_key=${agentKey} AND author_agent_id=${message.author.agentId} AND msg_id=${message.msgId}`;
    if (row === undefined) {
      return false;
    }
    requireEqual(readText(row, "conversation_id"), conversationId);
    requireSameBytes(
      readBytes(row, "canonical_message"),
      yield* encodeCanonical(Message, message),
    );
    return true;
  }).pipe(agentOperation(agentKey), Effect.withSpan("hasMessage"));
}

/**
 * Adds a message to one agent's history at its position. It takes no lock on
 * the agent, so a sender can save for every member while holding only its own
 * agent's lock and the conversation's.
 */
export function saveMessage(
  agentKey: string,
  conversationId: string,
  ordinal: number,
  message: Message,
) {
  return withAgentTransaction(
    agentKey,
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      yield* sql`INSERT INTO message_log(agent_key,conversation_id,ordinal,author_agent_id,msg_id,canonical_message)
        VALUES(${agentKey},${conversationId},${ordinal},${message.author.agentId},${message.msgId},${yield* encodeCanonical(Message, message)})`;
    }),
  ).pipe(storeOperation, Effect.withSpan("saveMessage"));
}

/** A bounded range of one agent's messages, in conversation order or in the order they were saved. */
export function readMessages(
  agentKey: string,
  input: {
    readonly conversationId?: string;
    readonly after?: number;
    readonly through?: number;
    readonly newest?: boolean;
    /** Selects messages this agent has not consumed and did not write. */
    readonly pendingFor?: AgentId;
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
    const pending =
      input.pendingFor === undefined
        ? sql``
        : sql`AND consumed=0 AND author_agent_id<>${input.pendingFor}`;
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
      Schema.Literal("collecting", "completed"),
      readText(row, "phase"),
    );
    const result = yield* readResult(row, operationId);
    const state: OperationState = {
      operationId,
      phase,
      deadlineAt: readInteger(row, "deadline_at"),
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
    const request = yield* readRequest(readBytes(row, "canonical_request"));
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
    if (item.result.request.operationId !== operationId) {
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

function readRequest(bytes: Uint8Array) {
  return Effect.gen(function* () {
    const message = yield* decodeRuntimeValue(Message, bytes);
    const value = yield* messageInterpretation(message).pipe(
      Effect.catchTag(
        "CollectivePartInvalidError",
        () => new EndpointStoreError({ reason: "corrupt" }),
      ),
    );
    if (Option.isNone(value) || value.value.kind !== "operation") {
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
 * Locking the agent's own row serializes one agent's transactions on
 * PostgreSQL, matching SQLite's connection serialization. Nested calls retain
 * the lock until the enclosing transaction commits; other agents use their own
 * rows.
 */
function agentOperation(agentKey: string) {
  return <A, E, R>(work: Effect.Effect<A, E, R>) =>
    withAgentTransaction(
      agentKey,
      Effect.gen(function* () {
        const sql = yield* SqlClient;
        yield* sql.onDialectOrElse({
          pg: () =>
            sql`SELECT agent_key FROM agent_profiles WHERE agent_key=${agentKey} FOR UPDATE`.pipe(
              Effect.asVoid,
            ),
          orElse: () => Effect.void,
        });
        return yield* work;
      }),
    ).pipe(storeOperation, Effect.withSpan("agentOperation"));
}

/** SQL failures and row corruption leave through one closed storage error channel. */
function storeOperation<A, E, R>(
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

/**
 * Reads and copies one nullable SQLite BLOB.
 *
 * @param row SQL result row.
 * @param key Selected column name.
 * @returns An isolated byte copy, or undefined for SQL NULL.
 */
function readOptionalBytes(
  row: Readonly<Record<string, unknown>>,
  key: string,
): Uint8Array | undefined {
  const value = row[key];
  if (value === null) {
    return undefined;
  }
  return readBytes(row, key);
}

/**
 * Reads and copies one required SQLite BLOB.
 *
 * @param row SQL result row.
 * @param key Selected column name.
 * @returns An isolated nonempty byte copy.
 */
function readBytes(
  row: Readonly<Record<string, unknown>>,
  key: string,
): Uint8Array {
  const value = row[key];
  if (!(value instanceof Uint8Array) || value.byteLength === 0) {
    throw new StoreSignal("corrupt");
  }
  return new Uint8Array(value);
}

/**
 * Reads one required nonempty SQL text.
 *
 * @param row SQL result row when one was selected.
 * @param key Selected column name.
 * @returns The exact nonempty text.
 */
function readText(row: Readonly<Record<string, unknown>>, key: string): string {
  const value = row[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new StoreSignal("corrupt");
  }
  return value;
}

/**
 * Reads one safe SQL integer.
 *
 * @param row SQL result row when one was selected.
 * @param key Selected column name.
 * @returns The exact safe integer.
 */
function readInteger(
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
 * Requires canonical byte equality for idempotent state.
 *
 * @param left Previously retained canonical bytes.
 * @param right Candidate canonical bytes.
 */
function requireSameBytes(left: Uint8Array, right: Uint8Array): void {
  if (!sameBytes(left, right)) {
    throw new StoreSignal("conflict");
  }
}

/**
 * Requires two exact bindings, including optional bindings, to agree.
 *
 * @param left Previously retained binding.
 * @param right Candidate binding.
 */
function requireEqual<Value>(left: Value, right: Value): void {
  if (left !== right) {
    throw new StoreSignal("conflict");
  }
}
