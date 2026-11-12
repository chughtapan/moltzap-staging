/** @file Stores each agent's ordered messages, which of them it has consumed, and its questions, answers and results. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Data, Effect, Option, Schema } from "effect";

import { executeQuery, type Generated, makeQueries } from "../database/sql.js";
import { withAgentTransaction } from "../database/transactions.js";
import {
  decodeJson,
  encodeJson,
  jsonText,
  sameJson,
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

/** Closed storage failures that never expose database diagnostics. */
type EndpointStoreFailure =
  | "conflict"
  | "corrupt"
  | "invalid-input"
  | "persistence";

type CommunicationTables = Record<
  "message_log",
  Record<
    "agent_key" | "conversation_id" | "author_agent_id" | "msg_id" | "message",
    string
  > &
    Record<"ordinal", number> &
    Record<"sequence" | "consumed", Generated<number>>
> &
  Record<
    "collective_operations",
    Record<
      "agent_key" | "operation_id" | "creator_agent_id" | "request",
      string
    > &
      Record<"origin", "local" | "remote"> &
      Record<"phase", Generated<"collecting" | "completed">> &
      Record<"deadline_at", number> &
      Record<"result" | "answer_refs", string | null> &
      Record<"sequence" | "delivered", Generated<number>>
  > &
  Record<
    "collective_answers",
    Record<"agent_key" | "operation_id" | "member" | "message", string> &
      Record<"sequence", Generated<number>>
  > &
  Record<
    "message_sequences",
    Record<"conversation_id", string> & Record<"next_ordinal", number>
  > &
  Record<
    "communication_mode",
    Record<"singleton", number> & Record<"kind", string>
  > &
  Record<
    "agent_profiles",
    Record<"agent_key" | "agent_id" | "agent_name" | "principal_id", string>
  >;

/** Builds queries over the tables that hold messages, questions and the agents they name. */
export const communicationQueries = makeQueries<CommunicationTables>();

/** One question's row. PostgreSQL returns its 64-bit deadline as text. */
const operationRow = Schema.Struct({
  operation_id: CollectiveId,
  creator_agent_id: AgentId,
  origin: Schema.Literal("local", "remote"),
  phase: Schema.Literal("collecting", "completed"),
  deadline_at: Schema.Union(Schema.Number, Schema.NumberFromString),
  request: Schema.String,
  result: Schema.NullOr(Schema.String),
  answer_refs: Schema.NullOr(Schema.String),
});

const conflict = () => new EndpointStoreError({ reason: "conflict" });
const corrupt = () => new EndpointStoreError({ reason: "corrupt" });

/** The JSON text of a validated value, or `invalid-input` for one JSON cannot hold. */
export const encodeRuntimeValue = (
  value: unknown,
): Effect.Effect<string, EndpointStoreError> =>
  jsonText(value).pipe(
    Effect.catchTag(
      "ClientRepresentationError",
      () => new EndpointStoreError({ reason: "invalid-input" }),
    ),
  );

/** Reads stored JSON text through its schema; text the schema refuses is `corrupt`. */
const decodeStored = <A, I>(
  schema: Schema.Schema<A, I>,
  text: string,
): Effect.Effect<A, EndpointStoreError> =>
  decodeJson(schema, text).pipe(
    Effect.catchTag("ClientRepresentationError", corrupt),
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
  /** The size of the message's stored JSON text. */
  readonly bytes: number;
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
      const db = yield* communicationQueries;
      const { operationId, deadlineAt } =
        input.origin === "local" ? input.request : input.value;
      const request = yield* input.origin === "local"
        ? encodeJson(Request, input.request)
        : encodeJson(Message, input.request);
      const [existing] = yield* operationRows(agentKey, operationId);
      if (existing === undefined) {
        yield* executeQuery(
          db.insertInto("collective_operations").values({
            agent_key: agentKey,
            operation_id: operationId,
            creator_agent_id: input.request.author.agentId,
            origin: input.origin,
            request,
            deadline_at: deadlineAt,
          }),
        );
        return;
      }
      if (
        !sameJson(existing.request, request) ||
        existing.origin !== input.origin ||
        existing.creator_agent_id !== input.request.author.agentId ||
        existing.deadline_at !== deadlineAt
      ) {
        return yield* conflict();
      }
    }),
  ).pipe(agentOperation(agentKey), Effect.withSpan("putCollectiveOperation"));
}

/** The questions this agent asked that are still collecting answers, oldest first. */
export function readCollectingQuestionIds(agentKey: string) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const rows = yield* executeQuery(
      db
        .selectFrom("collective_operations")
        .select("operation_id")
        .where("agent_key", "=", agentKey)
        .where("origin", "=", "local")
        .where("phase", "=", "collecting")
        .orderBy("sequence"),
    );
    return yield* decodeValue(Schema.Array(CollectiveId))(
      rows.map((row) => row.operation_id),
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
      const db = yield* communicationQueries;
      const operation = yield* readCollectiveOperation(agentKey, operationId);
      if (
        operation === undefined ||
        operation.phase === "completed" ||
        (operation.origin === "local" && now >= operation.deadlineAt)
      ) {
        return false;
      }
      const member = message.author.agentId;
      const prior = yield* executeQuery(
        storedAnswers(db, agentKey, operationId)
          .select("member")
          .where("member", "=", member),
      );
      if (prior.length > 0) {
        return false;
      }
      yield* executeQuery(
        db.insertInto("collective_answers").values({
          agent_key: agentKey,
          operation_id: operationId,
          member,
          message: yield* encodeJson(Message, message),
        }),
      );
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
    const rows = yield* executeQuery(
      storedAnswers(yield* communicationQueries, agentKey, operationId)
        .select("message")
        .orderBy("sequence"),
    );
    return yield* Effect.forEach(
      rows,
      (row) => decodeStored(Message, row.message),
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
    yield* executeQuery(
      operationUpdate(yield* communicationQueries, agentKey, input.operationId)
        .set({
          phase: "completed",
          result: yield* encodeJson(ResultDelivery.fields.item, input.item),
          answer_refs: yield* encodeRuntimeValue(input.answers),
        })
        .where("phase", "=", "collecting"),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("saveCollectiveResult"));
}

/** Successful handling retires the same result while keeping its content readable. */
export function retireCollectiveResult(
  agentKey: string,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    yield* executeQuery(
      operationUpdate(yield* communicationQueries, agentKey, operationId)
        .set({ delivered: 1 })
        .where("phase", "=", "completed"),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("retireCollectiveResult"));
}

/** Ends a question without a result, so that it takes no more answers. */
export function retireCollectiveOperation(
  agentKey: string,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    yield* executeQuery(
      operationUpdate(yield* communicationQueries, agentKey, operationId).set({
        phase: "completed",
        delivered: 1,
      }),
    );
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
    const db = yield* communicationQueries;
    const [row] = yield* executeQuery(
      db
        .selectFrom("message_log")
        .select(["conversation_id", "message"])
        .where("agent_key", "=", agentKey)
        .where("author_agent_id", "=", message.author.agentId)
        .where("msg_id", "=", message.msgId),
    );
    if (row === undefined) {
      return false;
    }
    if (
      row.conversation_id !== conversationId ||
      !sameJson(row.message, yield* encodeJson(Message, message))
    ) {
      return yield* conflict();
    }
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
      const db = yield* communicationQueries;
      yield* executeQuery(
        db.insertInto("message_log").values({
          agent_key: agentKey,
          conversation_id: conversationId,
          ordinal,
          author_agent_id: message.author.agentId,
          msg_id: message.msgId,
          message: yield* encodeJson(Message, message),
        }),
      );
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
    const db = yield* communicationQueries;
    let query = db
      .selectFrom("message_log")
      .select([
        "conversation_id",
        "ordinal",
        "sequence",
        "author_agent_id",
        "msg_id",
        "message",
      ])
      .where("agent_key", "=", agentKey)
      .limit(input.limit);
    if (input.conversationId !== undefined) {
      query = query.where("conversation_id", "=", input.conversationId);
    }
    if (input.after !== undefined) {
      query = query.where("ordinal", ">", input.after);
    }
    if (input.through !== undefined) {
      query = query.where("ordinal", "<=", input.through);
    }
    if (input.pendingFor !== undefined) {
      query = query
        .where("consumed", "=", 0)
        .where("author_agent_id", "<>", input.pendingFor);
    }
    const ascending =
      input.conversationId === undefined ? "sequence" : "ordinal";
    const rows = yield* executeQuery(
      input.newest
        ? query.orderBy("ordinal", "desc")
        : query.orderBy(ascending),
    );
    return yield* Effect.forEach(
      rows,
      (row) =>
        decodeStored(Message, row.message).pipe(
          Effect.filterOrFail(
            (message) =>
              message.author.agentId === row.author_agent_id &&
              message.msgId === row.msg_id,
            conflict,
          ),
          Effect.map(
            (message): StoredMessage => ({
              conversationId: row.conversation_id,
              ordinal: row.ordinal,
              sequence: row.sequence,
              message,
              bytes: Buffer.byteLength(row.message),
            }),
          ),
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
    const db = yield* communicationQueries;
    yield* executeQuery(
      db
        .updateTable("message_log")
        .set({ consumed: 1 })
        .where("agent_key", "=", agentKey)
        .where("author_agent_id", "=", reference.authorAgentId)
        .where("msg_id", "=", reference.msgId),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("acknowledgeMessage"));
}

type Queries = Effect.Effect.Success<typeof communicationQueries>;

function storedAnswers(
  db: Queries,
  agentKey: string,
  operationId: CollectiveId,
) {
  return db
    .selectFrom("collective_answers")
    .where("agent_key", "=", agentKey)
    .where("operation_id", "=", operationId);
}

function operationUpdate(
  db: Queries,
  agentKey: string,
  operationId: CollectiveId,
) {
  return db
    .updateTable("collective_operations")
    .where("agent_key", "=", agentKey)
    .where("operation_id", "=", operationId);
}

function readOperations(agentKey: string, operationId?: CollectiveId) {
  return operationRows(agentKey, operationId).pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(rows, readOperation, { concurrency: 1 }),
    ),
    agentOperation(agentKey),
  );
}

/** The row of one question, or without `operationId` of the oldest completed result not yet consumed. */
function operationRows(agentKey: string, operationId?: CollectiveId) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const stored = db
      .selectFrom("collective_operations")
      .selectAll()
      .where("agent_key", "=", agentKey)
      .orderBy("sequence")
      .limit(1);
    return yield* executeQuery(
      operationId === undefined
        ? stored
            .where("phase", "=", "completed")
            .where("delivered", "=", 0)
            .where("result", "is not", null)
        : stored.where("operation_id", "=", operationId),
    ).pipe(Effect.flatMap(decodeValue(Schema.Array(operationRow))));
  });
}

function readOperation(
  row: typeof operationRow.Type,
): Effect.Effect<StoredCollectiveOperation, EndpointStoreError> {
  return Effect.gen(function* () {
    const result = yield* readResult(row);
    const state: OperationState = {
      operationId: row.operation_id,
      phase: row.phase,
      deadlineAt: row.deadline_at,
      ...(result === undefined ? {} : { result }),
    };
    if (row.origin === "local") {
      const request = yield* decodeStored(Request, row.request);
      if (
        request.operationId !== row.operation_id ||
        request.author.agentId !== row.creator_agent_id ||
        request.deadlineAt !== row.deadline_at
      ) {
        return yield* corrupt();
      }
      return { ...state, origin: row.origin, request };
    }
    const request = yield* readRequest(row.request);
    if (
      request.value.operationId !== row.operation_id ||
      request.message.author.agentId !== row.creator_agent_id ||
      request.value.deadlineAt !== row.deadline_at
    ) {
      return yield* corrupt();
    }
    return { ...state, origin: row.origin, request };
  });
}

function readResult(row: typeof operationRow.Type) {
  return Effect.gen(function* () {
    if (row.result === null && row.answer_refs === null) {
      return undefined;
    }
    if (row.result === null || row.answer_refs === null) {
      return yield* corrupt();
    }
    const item = yield* decodeStored(ResultDelivery.fields.item, row.result);
    if (item.result.request.operationId !== row.operation_id) {
      return yield* corrupt();
    }
    return {
      item,
      answers: yield* decodeStored(
        Schema.Array(MessageReference).pipe(
          Schema.maxItems(maximumMembers - 1),
        ),
        row.answer_refs,
      ),
    };
  });
}

function readRequest(text: string) {
  return Effect.gen(function* () {
    const message = yield* decodeStored(Message, text);
    const value = yield* messageInterpretation(message).pipe(
      Effect.catchTag("CollectivePartInvalidError", corrupt),
    );
    if (Option.isNone(value) || value.value.kind !== "operation") {
      return yield* corrupt();
    }
    return { message, value: value.value };
  });
}

function decodeValue<A, I>(schema: Schema.Schema<A, I>) {
  return (value: unknown) =>
    Schema.decodeUnknown(schema)(value).pipe(
      Effect.catchTag("ParseError", corrupt),
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
        const db = yield* communicationQueries;
        yield* sql.onDialectOrElse({
          pg: () =>
            executeQuery(
              db
                .selectFrom("agent_profiles")
                .select("agent_key")
                .where("agent_key", "=", agentKey)
                .forUpdate(),
            ).pipe(Effect.asVoid),
          orElse: () => Effect.void,
        });
        return yield* work;
      }),
    ).pipe(storeOperation, Effect.withSpan("agentOperation"));
}

/** SQL failures, defects and row corruption leave through one closed storage error channel. */
function storeOperation<A, E, R>(
  operation: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R> {
  return operation.pipe(
    Effect.catchAllCause((cause) => {
      if (Cause.isInterruptedOnly(cause)) {
        return Effect.failCause(Cause.stripFailures(cause));
      }
      const failure = Cause.failureOption(cause);
      return Effect.fail(
        Option.isSome(failure) && failure.value instanceof EndpointStoreError
          ? failure.value
          : new EndpointStoreError({ reason: "persistence" }),
      );
    }),
  );
}
