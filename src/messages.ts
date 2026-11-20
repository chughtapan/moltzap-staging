/** @file The agents of a daemon and the messages they send each other, as validated values. */

import { Data, Effect, Schema } from "effect";
import { randomBytes } from "node:crypto";

/** The name of an agent on its daemon: the server agent's configured name, or the name a client's owner gives it. */
export const AgentName = Schema.String.pipe(
  Schema.minLength(3),
  Schema.maxLength(32),
  Schema.pattern(/^[a-z0-9]+(-[a-z0-9]+)*$/),
  Schema.brand("AgentName"),
  Schema.annotations({
    identifier: "AgentName",
    description:
      "an agent name: 3 to 32 lowercase letters, digits or single hyphens, starting and ending with a letter or digit",
  }),
);
/** Validated nominal value decoded by {@link AgentName}. */
export type AgentName = typeof AgentName.Type;

/** A refused send names this reason. */
export class CommunicationError extends Data.TaggedError("CommunicationError")<{
  readonly reason: "content-invalid";
}> {}

/**
 * One message's id: `msg_` then 32 random bytes in canonical base64url, whose
 * last character carries four bits.
 */
export const MsgId = Schema.String.pipe(
  Schema.pattern(/^msg_[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$/u),
  Schema.brand("MsgId"),
  Schema.annotations({
    identifier: "MsgId",
    description: "MsgId: msg_ then 43 base64url characters",
  }),
);
/**
 * A validated author-scoped message id.
 * @useDeclaredType
 */
export type MsgId = typeof MsgId.Type;

/** The byte limit of one message's text as JSON text. */
const maximumTextBytes = 32_768;

/** A string without lone surrogates, so it survives UTF-8. */
export const wellFormedString = Schema.String.pipe(
  Schema.filter((value) => !/\p{Surrogate}/u.test(value), {
    identifier: "WellFormedUnicodeString",
  }),
);

/**
 * One message from its author to another agent, fixed through delivery and
 * restart. Its text is well formed and at most 32,768 bytes as JSON text.
 * `replyToMsgId` is the id of the message it answers, when its sender named
 * one.
 */
const messageSchema = Schema.Struct({
  author: AgentName,
  recipient: AgentName,
  msgId: MsgId,
  replyToMsgId: Schema.optional(MsgId),
  text: wellFormedString.pipe(
    Schema.filter(
      (text) => Buffer.byteLength(JSON.stringify(text)) <= maximumTextBytes,
    ),
  ),
}).pipe(
  Schema.filter(({ author, recipient }) => recipient !== author),
  Schema.annotations({ identifier: "Message" }),
);
/** @useDeclaredType */
export type Message = typeof messageSchema.Type;

/** Owns construction of a message. */
export const Message = Object.assign(messageSchema, {
  /** A new message with a fresh id, or the refusal of what cannot be one. */
  create: (input: Omit<Message, "msgId">) =>
    Effect.suspend(() =>
      Schema.decode(messageSchema)({
        ...input,
        msgId: `msg_${randomBytes(32).toString("base64url")}`,
      }),
    ).pipe(
      Effect.catchTag(
        "ParseError",
        () => new CommunicationError({ reason: "content-invalid" }),
      ),
      Effect.withSpan("Message.create"),
    ),
});
