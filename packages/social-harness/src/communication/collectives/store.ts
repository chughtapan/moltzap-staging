/** @file Collective requests, answer claims and frozen results share the caller's SQL transaction. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect, Option, Schema } from "effect";
import { AgentId } from "../../identity/identifiers.js";
import { Message, MessageReference } from "../message.js";
import { maximumMembers, MessageAddressInput } from "../records/values.js";
import {
  EndpointStoreError,
  mintDeliveryToken,
  readBytes,
  readInteger,
  readOptionalBytes,
  readText,
  requireSameBytes,
  storeOperation,
  transaction,
} from "../store/database/index.js";
import { decodeRuntimeValue, encodeRuntimeValue } from "../store/index.js";
import { DeliveryToken } from "../store/types.js";
import { CollectiveId } from "./contracts.js";
import { Question, ResultDelivery } from "./model.js";
import { type CollectiveValue, readCollectiveValue } from "./wire.js";

type OperationValue = Extract<CollectiveValue, { readonly kind: "operation" }>;
type CloseValue = Extract<CollectiveValue, { readonly kind: "close" }>;

interface OperationState {
  readonly operationId: CollectiveId;
  readonly deadlineAt: number;
  readonly phase: "collecting" | "finalizing" | "completed";
  readonly deliveryToken: DeliveryToken;
  readonly close?: CloseValue;
  readonly result?: {
    readonly item: ResultDelivery["item"];
    readonly answers: readonly MessageReference[];
  };
}

/** Disk readers distinguish local captured questions from recognized peer requests. */
export type StoredCollectiveOperation = OperationState &
  (
    | { readonly origin: "local"; readonly request: Question }
    | {
        readonly origin: "remote";
        readonly request: {
          readonly message: Message;
          readonly value: OperationValue;
        };
      }
  );

/** The accepted Message lives in Communication; a collective retains only its role and identity. */
export interface StoredCollectiveMessage {
  readonly operationId: CollectiveId;
  readonly role: "request" | "response" | "close";
  readonly destination: MessageAddressInput;
  readonly reference: MessageReference;
}

/** A repeated operation must preserve its original request and authority. */
export function putCollectiveOperation(
  input:
    | { readonly origin: "local"; readonly request: Question }
    | {
        readonly origin: "remote";
        readonly request: Message;
        readonly value: OperationValue;
      },
) {
  return transaction(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const operationId =
        input.origin === "local"
          ? input.request.operationId
          : input.value.operationId;
      const deadlineAt =
        input.origin === "local"
          ? input.request.deadlineAt
          : input.value.deadlineAt;
      const canonicalRequest = yield* encodeRuntimeValue(input.request);
      const [existing] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        "SELECT canonical_request,origin,creator_agent_id,deadline_at FROM collective_operations WHERE operation_id=?",
        [operationId],
      );
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
      const deliveryToken = yield* mintDeliveryToken((token) =>
        sql
          .unsafe(
            "SELECT 1 FROM collective_operations WHERE delivery_token=?",
            [token],
          )
          .pipe(Effect.map((rows) => rows.length > 0)),
      );
      yield* sql.unsafe(
        "INSERT INTO collective_operations(operation_id,creator_agent_id,origin,canonical_request,deadline_at,delivery_token) VALUES(?,?,?,?,?,?)",
        [
          operationId,
          input.request.author.agentId,
          input.origin,
          canonicalRequest,
          deadlineAt,
          deliveryToken,
        ],
      );
    }),
  ).pipe(storeOperation, Effect.withSpan("putCollectiveOperation"));
}

/** Stored requests and their identifiers are validated before business operations receive them. */
export function readCollectiveOperation(operationId: CollectiveId) {
  return readOperations(" WHERE operation_id=?", [operationId]).pipe(
    Effect.map((rows) => rows[0]),
    Effect.withSpan("readCollectiveOperation"),
  );
}

/** Workers load each operation's content inside its completion transaction. */
export function readCollectiveOperationIds(
  origin: StoredCollectiveOperation["origin"],
  phase: StoredCollectiveOperation["phase"],
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT operation_id FROM collective_operations WHERE origin=? AND phase=? ORDER BY rowid",
      [origin, phase],
    );
    return yield* Effect.forEach(
      rows,
      (row) => decodeValue(CollectiveId, readText(row, "operation_id")),
      { concurrency: 1 },
    );
  }).pipe(storeOperation, Effect.withSpan("readCollectiveOperationIds"));
}

/** Results become available only after their required close certifies. */
export const readNextCollectiveResult = () =>
  readOperations(
    " WHERE phase='completed' AND delivered=0 AND canonical_result IS NOT NULL ORDER BY rowid LIMIT 1",
    [],
  ).pipe(
    Effect.map((rows) => rows[0]),
    Effect.withSpan("readNextCollectiveResult"),
  );

/** Role uniqueness and Communication acceptance commit in the same caller transaction. */
export function putCollectiveMessage(input: StoredCollectiveMessage) {
  return transaction(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const prior = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        "SELECT author_agent_id,msg_id FROM collective_messages WHERE operation_id=? AND role=? AND destination=?",
        [input.operationId, input.role, input.destination],
      ))[0];
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
      yield* sql.unsafe(
        "INSERT INTO collective_messages(operation_id,role,destination,author_agent_id,msg_id) VALUES(?,?,?,?,?)",
        [
          input.operationId,
          input.role,
          input.destination,
          input.reference.authorAgentId,
          input.reference.msgId,
        ],
      );
      return true;
    }),
  ).pipe(storeOperation, Effect.withSpan("putCollectiveMessage"));
}

/** An operation reads all associations; startup reads only unfinished response and close observations. */
export function readCollectiveMessages(operationId?: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT operation_id,role,destination,author_agent_id,msg_id FROM collective_messages" +
        (operationId === undefined
          ? " WHERE role IN ('response','close') AND operation_id IN (SELECT operation_id FROM collective_operations WHERE phase!='completed') ORDER BY rowid"
          : " WHERE operation_id=? ORDER BY rowid"),
      operationId === undefined ? [] : [operationId],
    );
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
              MessageAddressInput,
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
  }).pipe(storeOperation, Effect.withSpan("readCollectiveMessages"));
}

/** Only the collector's first accepted answer before its deadline is selectable. */
export function recordCollectiveAnswer(
  operationId: CollectiveId,
  message: Message,
  now: number,
) {
  return transaction(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const operation = yield* readCollectiveOperation(operationId);
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
      const prior = yield* sql.unsafe(
        "SELECT 1 FROM collective_answers WHERE operation_id=? AND member=?",
        [operationId, member],
      );
      if (prior.length > 0) {
        return false;
      }
      yield* sql.unsafe(
        "INSERT INTO collective_answers(operation_id,member,canonical_message) VALUES(?,?,?)",
        [operationId, member, yield* encodeRuntimeValue(message)],
      );
      return true;
    }),
  ).pipe(storeOperation, Effect.withSpan("recordCollectiveAnswer"));
}

/** First-answer messages are decoded once at their persistence boundary. */
export function readCollectiveAnswers(operationId: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT canonical_message FROM collective_answers WHERE operation_id=? ORDER BY rowid",
      [operationId],
    );
    return yield* Effect.forEach(
      rows,
      (row) => decodeRuntimeValue(Message, readBytes(row, "canonical_message")),
      { concurrency: 1 },
    );
  }).pipe(storeOperation, Effect.withSpan("readCollectiveAnswers"));
}

/** The first authenticated remote close remains available while an own answer certifies. */
export function saveRemoteClose(operationId: CollectiveId, message: Message) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase='finalizing',canonical_close=? WHERE operation_id=? AND origin='remote' AND phase='collecting'",
      [yield* encodeRuntimeValue(message), operationId],
    );
  }).pipe(storeOperation, Effect.withSpan("saveRemoteClose"));
}

/** Final bytes and exact source identities freeze once; close certification only changes visibility. */
export function saveCollectiveResult(input: {
  readonly operationId: CollectiveId;
  readonly item: ResultDelivery["item"];
  readonly answers: readonly MessageReference[];
  readonly complete: boolean;
}) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase=?,canonical_result=?,canonical_answer_refs=? WHERE operation_id=? AND phase<>'completed' AND canonical_result IS NULL",
      [
        input.complete ? "completed" : "finalizing",
        yield* encodeRuntimeValue(input.item),
        yield* encodeRuntimeValue(input.answers),
        input.operationId,
      ],
    );
  }).pipe(storeOperation, Effect.withSpan("saveCollectiveResult"));
}

/** A selected result becomes deliverable without rebuilding its content or sources. */
export function completeCollectiveOperation(operationId: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase='completed' WHERE operation_id=? AND canonical_result IS NOT NULL",
      [operationId],
    );
  }).pipe(storeOperation, Effect.withSpan("completeCollectiveOperation"));
}

/** A terminal close failure replaces only an unpublished result. */
export function failCollectiveClose(
  operationId: CollectiveId,
  item: ResultDelivery["item"],
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase='completed',canonical_result=?,canonical_answer_refs=? WHERE operation_id=? AND phase='finalizing' AND delivered=0",
      [
        yield* encodeRuntimeValue(item),
        yield* encodeRuntimeValue([]),
        operationId,
      ],
    );
  }).pipe(storeOperation, Effect.withSpan("failCollectiveClose"));
}

/** Successful handling retires the same result while keeping its content readable. */
export function retireCollectiveResult(operationId: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET delivered=1 WHERE operation_id=? AND phase='completed'",
      [operationId],
    );
  }).pipe(storeOperation, Effect.withSpan("retireCollectiveResult"));
}

/** An invalid close consumes protocol state without inventing a result. */
export function retireCollectiveOperation(operationId: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase='completed',delivered=1 WHERE operation_id=?",
      [operationId],
    );
  }).pipe(storeOperation, Effect.withSpan("retireCollectiveOperation"));
}

function readOperations(where: string, parameters: readonly string[]) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT * FROM collective_operations" + where,
      parameters,
    );
    return yield* Effect.forEach(rows, readOperation, { concurrency: 1 });
  }).pipe(storeOperation);
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
      deliveryToken: yield* decodeValue(
        DeliveryToken,
        readText(row, "delivery_token"),
      ),
      ...(close === undefined ? {} : { close: close.value }),
      ...(result === undefined ? {} : { result }),
    };
    if (origin === "local") {
      const request = yield* decodeRuntimeValue(
        Question,
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
    if (item.operationId !== operationId) {
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
    const value = yield* readCollectiveValue(message.content).pipe(
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
