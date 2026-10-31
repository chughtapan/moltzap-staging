/** @file Keeps operation audiences and message identities fixed. */

import * as SqlClient from "@effect/sql/SqlClient";
import {
  type Context,
  type Deferred,
  Effect,
  type Queue,
  Schema,
  type Scope,
} from "effect";
import type { Identity } from "../../identity/local.js";
import type { Communication } from "../index.js";
import type {
  StoredCollectiveMessage,
  StoredCollectiveOperation,
} from "./store.js";
import type { CollectiveValue } from "./wire.js";
import { AgentView } from "../../identity/cards.js";
import {
  type Message,
  messageFromParticipants,
  messageMembers,
  replyAddress,
} from "../message.js";
import { mintMsgId } from "../records/index.js";
import { agentAddressOf } from "../records/values.js";
import { EndpointStoreError } from "../store/database/index.js";
import { type CollectiveId, CollectivesError } from "./contracts.js";
import * as Store from "./store.js";

/** Identity registration is independent of protocol startup. */
export function self(environment: Environment) {
  return environment.identity.me().pipe(
    Effect.catchTag(
      "IdentityError",
      () => new CollectivesError({ reason: "not-registered" }),
    ),
    Effect.flatMap((status) =>
      status.kind === "registered"
        ? Effect.succeed(status.agent)
        : Effect.fail(new CollectivesError({ reason: "not-registered" })),
    ),
  );
}

/** Request and close identities are fixed before Communication accepts them. */
export function makeMessage(
  author: AgentView,
  recipients: readonly AgentView[],
  content: Message["content"],
) {
  return Effect.gen(function* () {
    return yield* messageFromParticipants(author, recipients, {
      msgId: yield* mintMsgId().pipe(
        Effect.catchTag(
          "ClientRepresentationError",
          () => new CollectivesError({ reason: "invalid-question" }),
        ),
      ),
      content,
    }).pipe(
      Effect.catchTag(
        "ParseError",
        () => new CollectivesError({ reason: "invalid-question" }),
      ),
    );
  }).pipe(Effect.withSpan("makeMessage"));
}

/** The recognized question keeps the original message identity and participants. */
export function requestItem(
  message: Message,
  value: Extract<CollectiveValue, { readonly kind: "operation" }>,
) {
  const question = questionText(message.content);
  return {
    kind: "collectiveRequest" as const,
    message: {
      ...message,
      content: {
        op: value.op,
        operationId: value.operationId,
        question,
        requestedSchema: value.requestedSchema,
        deadlineAt: value.deadlineAt,
      },
    },
  };
}

/** Stored questions are decoded once by their owning reader. */
export function operationParticipants(operation: StoredCollectiveOperation) {
  if (operation.origin === "local") {
    const question = operation.request;
    return {
      op: question.op,
      members: question.recipients,
      question: question.text,
      requestedSchema: question.requestedSchema,
      request: question,
      to: outgoingAddress(question),
    };
  }
  const { message, value } = operation.request;
  return {
    op: value.op,
    members: message.recipients,
    question: questionText(message.content),
    requestedSchema: value.requestedSchema,
    request: message,
    to: replyAddress(message),
  };
}

/** Acceptance and the collective claim use the same caller transaction. */
export function saveMessage(
  environment: Environment,
  operationId: CollectiveId,
  role: "request" | "response" | "close",
  message: Message,
) {
  return Effect.gen(function* () {
    const inserted = yield* Store.putCollectiveMessage({
      operationId,
      role,
      destination: outgoingAddress(message),
      reference: {
        authorAgentId: message.author.agentId,
        msgId: message.msgId,
      },
    });
    yield* environment.communication.send(message).pipe(
      Effect.catchTag(
        "CommunicationError",
        (error) =>
          new CollectivesError({
            reason: "message-failed",
            detail: error.reason,
          }),
      ),
    );
    return inserted;
  }).pipe(Effect.withSpan("saveMessage"));
}

/** A direct outgoing message names its recipient rather than its author. */
export function outgoingAddress(
  message: Pick<Message, "author" | "recipients">,
) {
  return message.recipients.length === 1
    ? agentAddressOf(message.recipients[0])
    : replyAddress(message);
}

/** Fixed identities, names and principals must agree before operation traffic is accepted. */
export function sameParticipants(
  left: Pick<Message, "author" | "recipients">,
  right: Pick<Message, "author" | "recipients">,
) {
  return Schema.equivalence(Schema.Array(AgentView))(
    messageMembers(left),
    messageMembers(right),
  );
}

/** One service scope supervises deadlines and certification observations. */
export interface Environment {
  readonly sql: SqlClient.SqlClient;
  readonly communication: Context.Tag.Service<typeof Communication>;
  readonly identity: Context.Tag.Service<typeof Identity>;
  readonly scope: Scope.Scope;
  readonly failed: Deferred.Deferred<never, CollectivesError>;
  readonly observations: Queue.Queue<StoredCollectiveMessage>;
}

/** Business refusals and handler failures keep their type when SQL rolls back. */
export function transaction<A, E, R>(work: Effect.Effect<A, E, R>) {
  return Effect.flatMap(SqlClient.SqlClient, (sql) =>
    sql.withTransaction(work),
  ).pipe(
    Effect.catchTag(
      "SqlError",
      () => new EndpointStoreError({ reason: "persistence" }),
    ),
  );
}

function questionText(content: Message["content"]): string {
  return content
    .flatMap((part) => (part.type === "text" ? [part.text] : []))
    .join("\n");
}
