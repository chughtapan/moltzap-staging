/** @file Messages as validated values, with their content grammar and constructors. */

import { Data, Effect, Either, ParseResult, Schema } from "effect";
import { randomBytes } from "node:crypto";
import {
  canonicalIdentifier,
  compareAscii,
  exactStruct,
  type JsonRepresentationError,
  representationFailure,
} from "../encoding.js";
import { AgentCard, AgentId } from "../identity.js";

/** A refused send or read names one of these reasons. */
export class CommunicationError extends Data.TaggedError("CommunicationError")<{
  readonly reason:
    | "membership-invalid"
    | "content-invalid"
    | "persistence-failed"
    | "message-conflict"
    | "invalid-cursor";
}> {}

/** The byte limit of one message's content as JSON text. */
const maximumContentBytes = 32_768;
const utf8Encoder = new TextEncoder();

/**
 * One author-scoped message id. Retrying a persisted message reuses it; a new
 * message receives a fresh one.
 */
export const MsgId = canonicalIdentifier("MsgId", "msg_", 32);
/**
 * A validated author-scoped message id.
 * @useDeclaredType
 */
export type MsgId = typeof MsgId.Type;

/** The author and its message id identify the same message through retries and recovery. */
export const MessageReference = exactStruct({
  authorAgentId: AgentId,
  msgId: MsgId,
});
/**
 * An author's message id identifies the same message through delivery and restart.
 * @useDeclaredType
 */
export type MessageReference = typeof MessageReference.Type;

/** A string without lone surrogates, so it survives UTF-8. */
export const wellFormedString = Schema.String.pipe(
  Schema.filter(hasWellFormedUnicode, {
    identifier: "WellFormedUnicodeString",
  }),
);

/* eslint-disable agent-code-guard/no-nullish-type-aliases -- JSON includes null as a first-class value. */
/** A value accepted by the closed semantic content boundary. */
type JsonValue =
  | null
  | boolean
  | number
  | string
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };
/* eslint-enable agent-code-guard/no-nullish-type-aliases -- Restore the absence rule outside JSON values. */

/** Runtime validation for the closed recursive JSON value. */
const JsonValue: Schema.Schema<JsonValue> = Schema.suspend(() =>
  Schema.Union(
    Schema.Null,
    Schema.Boolean,
    Schema.JsonNumber,
    wellFormedString,
    Schema.Array(JsonValue),
    Schema.Record({ key: wellFormedString, value: JsonValue }),
  ),
).annotations({ identifier: "JsonValue" });

/** One exact semantic part of a message. */
export const ContentPart = Schema.Union(
  exactStruct({ type: Schema.Literal("text"), text: wellFormedString }),
  exactStruct({ type: Schema.Literal("data"), value: JsonValue }),
).annotations({ identifier: "ContentPart" });
/**
 * A validated semantic message part.
 * @useDeclaredType
 */
export type ContentPart = typeof ContentPart.Type;

const contentStructure = Schema.NonEmptyArray(ContentPart);

/** Nonempty semantic content whose JSON text is at most 32,768 bytes. */
export const Content = contentStructure.pipe(
  Schema.filter(contentFits),
  Schema.annotations({ identifier: "Content" }),
);
/** Validated nonempty semantic content. */
export type Content = typeof Content.Type;

/** A fresh author-scoped id uses 32 random bytes before any assessment or send. */
const mintMsgId = (): Effect.Effect<MsgId, JsonRepresentationError> =>
  Effect.try({
    try: () => `msg_${randomBytes(32).toString("base64url")}`,
    catch: representationFailure,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(MsgId)),
    Effect.mapError(representationFailure),
  );

/** Omit cursor to begin and pass the returned nextCursor unchanged. */
export const PaginatedRequest = exactStruct({
  cursor: Schema.optional(Schema.String),
});
/** Selection arguments remain unchanged between pages.
 * @useDeclaredType
 */
export type PaginatedRequest = typeof PaginatedRequest.Type;
/** Absence of nextCursor marks the end of the listing. */
export const Page = exactStruct({
  nextCursor: Schema.optional(Schema.String),
});
/** Cursors confer no access and are valid only for the read that returned them.
 * @useDeclaredType
 */
export type Page = typeof Page.Type;

/** The most inbox entries, history records or conversation addresses one page holds. */
export const PAGE_SIZE = 50;

/** The one recipient is another agent than the author. */
const AuthorAndRecipients = exactStruct({
  author: AgentCard,
  recipients: Schema.Tuple(AgentCard),
}).pipe(
  Schema.filter(
    ({ author, recipients }) =>
      recipients[0].agentId !== author.agentId &&
      recipients[0].agentName !== author.agentName,
  ),
  Schema.annotations({ identifier: "AuthorAndRecipients" }),
);

/** `replyToMsgId` is the id of the message this one answers, when its sender named one. */
const messageSchema = AuthorAndRecipients.pipe(
  Schema.extend(
    exactStruct({
      msgId: MsgId,
      replyToMsgId: Schema.optional(MsgId),
      content: Content,
    }),
  ),
  Schema.annotations({ identifier: "Message" }),
);
/**
 * Authorship, recipients and content remain fixed through delivery and
 * restart.
 */
export type Message = typeof messageSchema.Type;

/** Owns construction of a message. */
export const Message = Object.assign(messageSchema, {
  create(
    input: Pick<Message, "author" | "recipients" | "content" | "replyToMsgId">,
  ) {
    return Effect.gen(function* () {
      return yield* messageWithId(
        input.author,
        input.recipients,
        { msgId: yield* mintMsgId(), replyToMsgId: input.replyToMsgId },
        input.content,
      );
    }).pipe(
      Effect.withSpan("Message.create"),
      Effect.catchTags({
        ParseError: () => new CommunicationError({ reason: "content-invalid" }),
        JsonRepresentationError: () =>
          new CommunicationError({ reason: "content-invalid" }),
      }),
    );
  },
});

/**
 * A message with a message id its caller chose, to recipients in any order.
 * `replyToMsgId` names the message it answers.
 */
export function messageWithId(
  author: AgentCard,
  recipients: readonly AgentCard[],
  { msgId, replyToMsgId }: Pick<Message, "msgId" | "replyToMsgId">,
  content: Content,
) {
  return Effect.try({
    try: () =>
      structuredClone({
        author,
        recipients: [...recipients].sort((left, right) =>
          compareAscii(left.agentName, right.agentName),
        ),
        msgId,
        ...(replyToMsgId === undefined ? {} : { replyToMsgId }),
        content,
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

/** Includes the author and every recipient in agent-name order. */
export function messageMembers(
  message: Pick<Message, "author" | "recipients">,
): readonly AgentCard[] {
  return [message.author, ...message.recipients].sort((left, right) =>
    compareAscii(left.agentName, right.agentName),
  );
}

function hasWellFormedUnicode(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (isTrailingSurrogate(codeUnit)) {
      return false;
    }
    if (!isLeadingSurrogate(codeUnit)) {
      continue;
    }
    index += 1;
    if (!isTrailingSurrogate(value.charCodeAt(index))) {
      return false;
    }
  }
  return true;
}

function isLeadingSurrogate(codeUnit: number): boolean {
  return codeUnit >= 0xd800 && codeUnit <= 0xdbff;
}

function isTrailingSurrogate(codeUnit: number): boolean {
  return codeUnit >= 0xdc00 && codeUnit <= 0xdfff;
}

function contentFits(
  content: readonly [ContentPart, ...ContentPart[]],
): boolean {
  return Either.match(
    Either.try(() => JSON.stringify(content)),
    {
      onLeft: () => false,
      onRight: (text) =>
        utf8Encoder.encode(text).byteLength <= maximumContentBytes,
    },
  );
}
