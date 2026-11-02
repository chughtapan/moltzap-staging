/** @file Messages retain their authors, recipients and content across delivery and recovery. */

import { Effect, Either, Option, ParseResult, Schema, SchemaAST } from "effect";
import { exactStruct } from "../encoding/exact.js";
import { compareAscii } from "../encoding/identifier.js";
import { AgentCard, type AgentId } from "../identity/agent.js";
import { CommunicationError } from "./errors.js";
import { mintMsgId } from "./records/encoding.js";
import { Content, maximumMembers, MsgId } from "./records/values.js";
import {
  type CollectivePartInvalidError,
  type CollectiveValue,
  readCollectiveValue,
  RequestedSchema,
} from "./requests/answers.js";
import {
  AcceptResponse,
  CollectiveId,
  DeclineResponse,
  EpochMillis,
  RequestOperation,
} from "./requests/contracts.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their names with the values they decode. */

/** Omit cursor to begin and pass the returned nextCursor unchanged. */
export const PaginatedRequest = exactStruct({
  cursor: Schema.optional(Schema.String),
});
/** Selection arguments remain unchanged between pages. */
export type PaginatedRequest = typeof PaginatedRequest.Type;
/** Absence of nextCursor marks the end of an operation's named collection. */
export const PaginatedResult = exactStruct({
  nextCursor: Schema.optional(Schema.String),
});
/** Cursors confer no access and remain private to their operation. */
export type PaginatedResult = typeof PaginatedResult.Type;

/** Recipients exclude the author and appear once each in agent-name order. */
export const MessageParticipants = exactStruct({
  author: AgentCard,
  recipients: Schema.NonEmptyArray(AgentCard).pipe(
    Schema.maxItems(maximumMembers - 1),
  ),
}).pipe(
  Schema.filter(
    (message) =>
      message.recipients.every(
        (recipient, index) =>
          recipient.agentId !== message.author.agentId &&
          recipient.agentName !== message.author.agentName &&
          (index === 0 ||
            compareAscii(
              message.recipients[index - 1]?.agentName ?? "",
              recipient.agentName,
            ) < 0),
      ) &&
      new Set(message.recipients.map((recipient) => recipient.agentId)).size ===
        message.recipients.length,
  ),
  Schema.annotations({ identifier: "MessageParticipants" }),
);

/** Adds the author-scoped identity to the captured participants. */
export const MessageHeader = MessageParticipants.pipe(
  Schema.extend(exactStruct({ msgId: MsgId })),
);

/** Decoded request and reply meaning stays outside signed content bytes. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- MCP composes the owning decoded operation schema without parsing reserved content.
export const MessageOperation = Schema.Union(
  exactStruct({
    kind: Schema.Literal("request"),
    op: RequestOperation,
    operationId: CollectiveId,
    question: Schema.String,
    requestedSchema: RequestedSchema,
    deadlineAt: EpochMillis,
  }),
  exactStruct({ kind: Schema.Literal("response"), ...AcceptResponse.fields }),
  exactStruct({ kind: Schema.Literal("response"), ...DeclineResponse.fields }),
);
/** Communication derives operation metadata from reserved content. */
export type MessageOperation = typeof MessageOperation.Type;

const encodedMessage = MessageHeader.pipe(
  Schema.extend(exactStruct({ content: Content })),
);
const decodedMessage = encodedMessage.pipe(
  Schema.extend(exactStruct({ operation: Schema.optional(MessageOperation) })),
);
/** Authorship, recipients and content remain fixed through delivery and restart. */
export type Message = typeof decodedMessage.Type;

const interpretations = new WeakMap<
  Message,
  Either.Either<Option.Option<CollectiveValue>, CollectivePartInvalidError>
>();

const messageSchema = Schema.transformOrFail(
  encodedMessage,
  Schema.declare<Message>(Schema.is(decodedMessage), {
    [SchemaAST.SurrogateAnnotationId]: Schema.typeSchema(decodedMessage).ast,
  }),
  {
    strict: true,
    decode: (message) =>
      readCollectiveValue(message.content).pipe(
        Effect.either,
        Effect.map((interpretation) => {
          const value = Either.match(interpretation, {
            onLeft: () => undefined,
            onRight: Option.getOrUndefined,
          });
          let operation: MessageOperation | undefined;
          if (value?.kind === "operation") {
            operation = {
              kind: "request",
              op: value.op,
              operationId: value.operationId,
              question: message.content
                .flatMap((part) => (part.type === "text" ? [part.text] : []))
                .join("\n"),
              requestedSchema: value.requestedSchema,
              deadlineAt: value.deadlineAt,
            };
          } else if (value?.kind === "response") {
            operation = value;
          }
          const result: Message = {
            ...message,
            ...(operation === undefined ? {} : { operation }),
          };
          interpretations.set(result, interpretation);
          return result;
        }),
      ),
    encode: (message) =>
      ParseResult.succeed({
        author: message.author,
        recipients: message.recipients,
        msgId: message.msgId,
        content: message.content,
      }),
  },
).annotations({ identifier: "Message" });

/** Owns construction and the one decoded interpretation of reserved content. */
export const Message = Object.assign(messageSchema, {
  create(input: Pick<Message, "author" | "recipients" | "content">) {
    return Effect.gen(function* () {
      return yield* messageFromParticipants(input.author, input.recipients, {
        msgId: yield* mintMsgId(),
        content: input.content,
      });
    }).pipe(
      Effect.withSpan("Message.create"),
      Effect.catchTags({
        ParseError: () => new CommunicationError({ reason: "content-invalid" }),
        ClientRepresentationError: () =>
          new CommunicationError({ reason: "content-invalid" }),
      }),
    );
  },
});

/** Private protocol consumers reuse the interpretation made by the owning reader. */
export function messageInterpretation(
  message: Message,
): Effect.Effect<Option.Option<CollectiveValue>, CollectivePartInvalidError> {
  const retained = interpretations.get(message);
  if (retained !== undefined) {
    return Either.match(retained, {
      onLeft: Effect.fail,
      onRight: Effect.succeed,
    });
  }
  return readCollectiveValue(message.content).pipe(
    Effect.tapBoth({
      onFailure: (error) =>
        Effect.sync(() => interpretations.set(message, Either.left(error))),
      onSuccess: (value) =>
        Effect.sync(() => interpretations.set(message, Either.right(value))),
    }),
  );
}

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after Schema/type pairs. */

type Participants = Pick<Message, "author" | "recipients">;

/** Captures the known author and recipients without reconstructing a member list. */
export function messageFromParticipants(
  author: Message["author"],
  recipients: ReadonlyArray<Message["author"]>,
  intent: Pick<Message, "msgId" | "content">,
) {
  return Effect.try({
    try: () =>
      structuredClone({
        author,
        recipients: [...recipients].sort((left, right) =>
          compareAscii(left.agentName, right.agentName),
        ),
        ...intent,
      }),
    catch: () =>
      new ParseResult.ParseError({
        issue: new ParseResult.Type(
          Message.ast,
          undefined,
          "Message cannot be captured",
        ),
      }),
  }).pipe(
    Effect.flatMap((captured) => Schema.decodeUnknown(Message)(captured)),
  );
}

/** Constructs one message from fixed participants and the author's retained content. */
export function messageFromIntent(
  members: readonly AgentCard[],
  intent: {
    readonly authorAgentId: AgentId;
    readonly msgId: MsgId;
    readonly content: Content;
  },
) {
  return Schema.decodeUnknown(Message)({
    msgId: intent.msgId,
    author: members.find((member) => member.agentId === intent.authorAgentId),
    recipients: members
      .filter((member) => member.agentId !== intent.authorAgentId)
      .sort((left, right) => compareAscii(left.agentName, right.agentName)),
    content: intent.content,
  });
}

/** Includes the author and every recipient in agent-name order. */
export function messageMembers(message: Participants): readonly AgentCard[] {
  return [message.author, ...message.recipients].sort((left, right) =>
    compareAscii(left.agentName, right.agentName),
  );
}

/** Private semantic index keys put directs before group member-name tuples. */
export function conversationSortKey(
  members: ReadonlyArray<Message["author"]>,
  localAgentId: Message["author"]["agentId"],
) {
  return conversationNameKey(
    members.map((member) => member.agentName),
    members.find((member) => member.agentId === localAgentId)?.agentName,
  );
}

/** The retained membership index orders direct peers before group participant tuples. */
export function conversationNameKey(
  names: ReadonlyArray<Message["author"]["agentName"]>,
  localName?: Message["author"]["agentName"],
) {
  const ordered = [...names].sort(compareAscii);
  return ordered.length === 2
    ? "0:" + ordered.filter((name) => name !== localName).join(",")
    : "1:" + ordered.join(",");
}
