/** @file Stores each agent's ordered messages, which of them it has consumed, and its collectives: each request with its copies, responses and result. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Data, Effect, Option, Schema } from "effect";

import {
  executeQuery,
  type Generated,
  makeQueries,
  withAgentTransaction,
} from "../database/index.js";
import { decodeJson, encodeJson, sameJson } from "../encoding.js";
import { AgentId } from "../identity.js";
import {
  CollectiveRequest,
  CollectiveRequestMessage,
  CollectiveResult,
  maximumMembers,
  Message,
  MessageReference,
  MsgId,
} from "./messages.js";

/** One closed store failure. */
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
    "collectives",
    Record<
      "agent_key" | "request_id" | "requester_agent_id" | "request",
      string
    > &
      Record<"role", "requester" | "recipient"> &
      Record<"phase", Generated<"collecting" | "completed">> &
      Record<"deadline_at", number> &
      Record<"result" | "response_refs", string | null> &
      Record<"sequence" | "consumed", Generated<number>>
  > &
  Record<
    "collective_copies",
    Record<"agent_key" | "copy_id" | "request_id", string>
  > &
  Record<
    "collective_responses",
    Record<"agent_key" | "request_id" | "recipient" | "message", string> &
      Record<"sequence", Generated<number>>
  > &
  Record<
    "message_sequences",
    Record<"conversation_id", string> & Record<"next_ordinal", number>
  > &
  Record<
    "agent_profiles",
    Record<"agent_key" | "agent_id" | "agent_name" | "principal_id", string>
  >;

/** Builds queries over the tables that hold messages, collectives and the agents they name. */
export const communicationQueries = makeQueries<CommunicationTables>();

/** One collective's row. PostgreSQL returns its 64-bit deadline as text. */
const collectiveRow = Schema.Struct({
  request_id: MsgId,
  requester_agent_id: AgentId,
  role: Schema.Literal("requester", "recipient"),
  phase: Schema.Literal("collecting", "completed"),
  deadline_at: Schema.Union(Schema.Number, Schema.NumberFromString),
  request: Schema.String,
  result: Schema.NullOr(Schema.String),
  response_refs: Schema.NullOr(Schema.String),
});

const responseReferences = Schema.Array(MessageReference).pipe(
  Schema.maxItems(maximumMembers - 1),
);

const conflict = () => new EndpointStoreError({ reason: "conflict" });
const corrupt = () => new EndpointStoreError({ reason: "corrupt" });

/** Reads stored JSON text through its schema; text the schema refuses is `corrupt`. */
const decodeStored = <A, I>(
  schema: Schema.Schema<A, I>,
  text: string,
): Effect.Effect<A, EndpointStoreError> =>
  decodeJson(schema, text).pipe(
    Effect.catchTag("ClientRepresentationError", corrupt),
  );

/**
 * A collective request this agent sent (`requester`) or received
 * (`recipient`), with what has happened to it. `requestId` keys the
 * collective: the message id the requester's send returned, or the id of the
 * request message a recipient received.
 */
export type StoredCollective = CollectiveRole & {
  readonly requestId: MsgId;
  readonly deadlineAt: number;
  readonly phase: "collecting" | "completed";
  readonly completed?: {
    readonly result: CollectiveResult;
    /** The responses the result counts. */
    readonly responses: readonly MessageReference[];
  };
};

/** Whether this agent sent the collective's request or received it. */
type CollectiveRole =
  | { readonly role: "requester"; readonly request: CollectiveRequest }
  | { readonly role: "recipient"; readonly request: CollectiveRequestMessage };

/** Which one collective a read selects. */
type CollectiveSelection =
  | { readonly requestId: MsgId }
  | { readonly copyId: MsgId }
  | "next-result";

/** The oldest completed result this agent has not consumed. */
export const readNextCollectiveResult = Effect.fn("readNextCollectiveResult")(
  (agentKey: string) => readCollectiveRow(agentKey, "next-result"),
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

/**
 * Saves a collective under its request's message id. Saving it again must
 * give the same request, role and deadline.
 */
export const saveCollective = Effect.fn("saveCollective")(function* (
  agentKey: string,
  input: CollectiveRole,
) {
  const db = yield* communicationQueries;
  const requestId = input.request.msgId;
  const deadlineAt =
    input.role === "requester"
      ? input.request.deadlineAt
      : input.request.collectiveRequest.deadlineAt;
  const request = yield* input.role === "requester"
    ? encodeJson(CollectiveRequest, input.request)
    : encodeJson(Message, input.request);
  const [existing] = yield* collectiveRows(agentKey, { requestId });
  if (existing === undefined) {
    yield* executeQuery(
      db.insertInto("collectives").values({
        agent_key: agentKey,
        request_id: requestId,
        requester_agent_id: input.request.author.agentId,
        role: input.role,
        request,
        deadline_at: deadlineAt,
      }),
    );
    return;
  }
  if (
    !sameJson(existing.request, request) ||
    existing.role !== input.role ||
    existing.requester_agent_id !== input.request.author.agentId ||
    existing.deadline_at !== deadlineAt
  ) {
    return yield* conflict();
  }
}, withAgentLock);

/**
 * Records the message id of one copy of a collective's request: the one group
 * message of an all_gather, or one recipient's private copy of a gather. A
 * response that names the copy then finds the collective.
 */
export const recordCollectiveCopy = Effect.fn("recordCollectiveCopy")(
  function* (agentKey: string, requestId: MsgId, copyId: MsgId) {
    const db = yield* communicationQueries;
    yield* executeQuery(
      db.insertInto("collective_copies").values({
        agent_key: agentKey,
        copy_id: copyId,
        request_id: requestId,
      }),
    );
  },
  withAgentLock,
);

/** The collective requests this agent sent that are still collecting responses, oldest first. */
export const readCollectingRequestIds = Effect.fn("readCollectingRequestIds")(
  function* (agentKey: string) {
    const db = yield* communicationQueries;
    const rows = yield* executeQuery(
      db
        .selectFrom("collectives")
        .select("request_id")
        .where("agent_key", "=", agentKey)
        .where("role", "=", "requester")
        .where("phase", "=", "collecting")
        .orderBy("sequence"),
    );
    return yield* decodeValue(Schema.Array(MsgId))(
      rows.map((row) => row.request_id),
    );
  },
  withAgentLock,
);

/** The collective `requestId` keys, validated against its row. */
export const readCollective = Effect.fn("readCollective")(
  (agentKey: string, requestId: MsgId) =>
    readCollectiveRow(agentKey, { requestId }),
);

/** The collective one of whose request copies has the message id `copyId`. */
export const readCollectiveOfCopy = Effect.fn("readCollectiveOfCopy")(
  (agentKey: string, copyId: MsgId) => readCollectiveRow(agentKey, { copyId }),
);

/**
 * Saves a recipient's first response and reports whether it was saved. A
 * collective request this agent sent takes responses only while collecting
 * and before its deadline.
 */
export const recordCollectiveResponse = Effect.fn("recordCollectiveResponse")(
  function* (
    agentKey: string,
    requestId: MsgId,
    message: Message,
    now: number,
  ) {
    const db = yield* communicationQueries;
    const stored = yield* readCollective(agentKey, requestId);
    if (
      stored === undefined ||
      stored.phase === "completed" ||
      (stored.role === "requester" && now >= stored.deadlineAt)
    ) {
      return false;
    }
    const recipient = message.author.agentId;
    const prior = yield* executeQuery(
      storedResponses(db, agentKey, requestId)
        .select("recipient")
        .where("recipient", "=", recipient),
    );
    if (prior.length > 0) {
      return false;
    }
    yield* executeQuery(
      db.insertInto("collective_responses").values({
        agent_key: agentKey,
        request_id: requestId,
        recipient,
        message: yield* encodeJson(Message, message),
      }),
    );
    return true;
  },
  withAgentLock,
);

/** Runs related writes of one agent in the driver's connection and transaction. */
export function agentTransaction<A, E, R>(
  agentKey: string,
  work: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R | SqlClient> {
  return withAgentLock(withAgentTransaction(agentKey, work), agentKey);
}

/** Each recipient's first response to a collective, in the order saved. */
export const readCollectiveResponses = Effect.fn("readCollectiveResponses")(
  function* (agentKey: string, requestId: MsgId) {
    const rows = yield* executeQuery(
      storedResponses(yield* communicationQueries, agentKey, requestId)
        .select("message")
        .orderBy("sequence"),
    );
    return yield* Effect.forEach(
      rows,
      (row) => decodeStored(Message, row.message),
      { concurrency: 1 },
    );
  },
  withAgentLock,
);

/** Completes a collecting collective with its result and the responses the result counts. */
export const saveCollectiveResult = Effect.fn("saveCollectiveResult")(
  function* (
    agentKey: string,
    requestId: MsgId,
    completed: NonNullable<StoredCollective["completed"]>,
  ) {
    yield* executeQuery(
      collectiveUpdate(yield* communicationQueries, agentKey, requestId)
        .set({
          phase: "completed",
          result: yield* encodeJson(CollectiveResult, completed.result),
          response_refs: yield* encodeJson(
            responseReferences,
            completed.responses,
          ),
        })
        .where("phase", "=", "collecting"),
    );
  },
  withAgentLock,
);

/** Marks a completed result consumed; its content stays readable. */
export const markCollectiveResultConsumed = Effect.fn(
  "markCollectiveResultConsumed",
)(function* (agentKey: string, requestId: MsgId) {
  yield* executeQuery(
    collectiveUpdate(yield* communicationQueries, agentKey, requestId)
      .set({ consumed: 1 })
      .where("phase", "=", "completed"),
  );
}, withAgentLock);

/** Ends a collective without a result, so that it takes no more responses. */
export const discardCollective = Effect.fn("discardCollective")(function* (
  agentKey: string,
  requestId: MsgId,
) {
  yield* executeQuery(
    collectiveUpdate(yield* communicationQueries, agentKey, requestId).set({
      phase: "completed",
      consumed: 1,
    }),
  );
}, withAgentLock);

/**
 * Whether this agent's history holds the message. A saved message with the
 * same author and identifier but another conversation or content is a
 * conflict.
 */
export const hasMessage = Effect.fn("hasMessage")(function* (
  agentKey: string,
  conversationId: string,
  message: Message,
) {
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
}, withAgentLock);

/**
 * Adds a message to one agent's history at its position. It takes no lock on
 * the agent, so a sender can save for every member while holding only its own
 * agent's lock and the conversation's.
 */
export const saveMessage = Effect.fn("saveMessage")(
  function* (
    agentKey: string,
    conversationId: string,
    ordinal: number,
    message: Message,
  ) {
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
  },
  (work, agentKey) => withAgentTransaction(agentKey, work),
  asStoreError,
);

/** A bounded range of one agent's messages, in conversation order or in the order they were saved. */
export const readMessages = Effect.fn("readMessages")(function* (
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
  const ascending = input.conversationId === undefined ? "sequence" : "ordinal";
  const rows = yield* executeQuery(
    input.newest ? query.orderBy("ordinal", "desc") : query.orderBy(ascending),
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
}, withAgentLock);

/** Marks a message consumed, so that it is not handed to the consumer again. */
export const markMessageConsumed = Effect.fn("markMessageConsumed")(function* (
  agentKey: string,
  reference: MessageReference,
) {
  const db = yield* communicationQueries;
  yield* executeQuery(
    db
      .updateTable("message_log")
      .set({ consumed: 1 })
      .where("agent_key", "=", agentKey)
      .where("author_agent_id", "=", reference.authorAgentId)
      .where("msg_id", "=", reference.msgId),
  );
}, withAgentLock);

type Queries = Effect.Effect.Success<typeof communicationQueries>;

function storedResponses(db: Queries, agentKey: string, requestId: MsgId) {
  return db
    .selectFrom("collective_responses")
    .where("agent_key", "=", agentKey)
    .where("request_id", "=", requestId);
}

function collectiveUpdate(db: Queries, agentKey: string, requestId: MsgId) {
  return db
    .updateTable("collectives")
    .where("agent_key", "=", agentKey)
    .where("request_id", "=", requestId);
}

function readCollectiveRow(agentKey: string, selection: CollectiveSelection) {
  return withAgentLock(
    collectiveRows(agentKey, selection).pipe(
      Effect.flatMap(([row]) =>
        row === undefined ? Effect.succeed(undefined) : storedCollective(row),
      ),
    ),
    agentKey,
  );
}

/** The row of the selected collective, if this agent has it. */
function collectiveRows(agentKey: string, selection: CollectiveSelection) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const stored = db
      .selectFrom("collectives")
      .selectAll()
      .where("agent_key", "=", agentKey)
      .orderBy("sequence")
      .limit(1);
    let selected;
    if (selection === "next-result") {
      selected = stored
        .where("phase", "=", "completed")
        .where("consumed", "=", 0)
        .where("result", "is not", null);
    } else if ("requestId" in selection) {
      selected = stored.where("request_id", "=", selection.requestId);
    } else {
      selected = stored.where(
        "request_id",
        "in",
        db
          .selectFrom("collective_copies")
          .select("request_id")
          .where("agent_key", "=", agentKey)
          .where("copy_id", "=", selection.copyId),
      );
    }
    return yield* executeQuery(selected).pipe(
      Effect.flatMap(decodeValue(Schema.Array(collectiveRow))),
    );
  });
}

/** Decodes a row and checks each stored JSON value against the row's columns. */
function storedCollective(
  row: typeof collectiveRow.Type,
): Effect.Effect<StoredCollective, EndpointStoreError> {
  return Effect.gen(function* () {
    const completed = yield* readCompleted(row);
    const state = {
      requestId: row.request_id,
      phase: row.phase,
      deadlineAt: row.deadline_at,
      ...(completed === undefined ? {} : { completed }),
    };
    const stored: StoredCollective =
      row.role === "requester"
        ? {
            ...state,
            role: row.role,
            request: yield* decodeStored(CollectiveRequest, row.request),
          }
        : {
            ...state,
            role: row.role,
            request: yield* decodeStored(CollectiveRequestMessage, row.request),
          };
    const deadlineAt =
      stored.role === "requester"
        ? stored.request.deadlineAt
        : stored.request.collectiveRequest.deadlineAt;
    if (
      stored.request.msgId !== row.request_id ||
      stored.request.author.agentId !== row.requester_agent_id ||
      deadlineAt !== row.deadline_at
    ) {
      return yield* corrupt();
    }
    return stored;
  });
}

function readCompleted(row: typeof collectiveRow.Type) {
  return Effect.gen(function* () {
    if (row.result === null && row.response_refs === null) {
      return undefined;
    }
    if (row.result === null || row.response_refs === null) {
      return yield* corrupt();
    }
    const result = yield* decodeStored(CollectiveResult, row.result);
    if (result.request.msgId !== row.request_id) {
      return yield* corrupt();
    }
    return {
      result,
      responses: yield* decodeStored(responseReferences, row.response_refs),
    };
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
function withAgentLock<A, E, R>(
  work: Effect.Effect<A, E, R>,
  agentKey: string,
) {
  return withAgentTransaction(
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
  ).pipe(asStoreError, Effect.withSpan("withAgentLock"));
}

/** SQL failures, defects and row corruption leave through one closed storage error channel. */
function asStoreError<A, E, R>(
  work: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R> {
  return work.pipe(
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
