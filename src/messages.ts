/** @file The agents of a daemon and the messages they send each other, as validated values. */

import { Array, Data, Effect, Encoding, Schema } from "effect";
import { randomBytes } from "node:crypto";

/**
 * An identifier of `prefix` and `byteLength` random bytes in canonical
 * base64url, with the same pattern in runtime validation and generated JSON
 * Schema, and the minting of a new one.
 */
const identifier = <const Name extends string>(
  name: Name,
  prefix: string,
  byteLength: number,
) => {
  const wholeCharacters = Math.floor((byteLength * 8) / 6);
  const remainingBits = (byteLength * 8) % 6;
  const finalCharacter =
    remainingBits === 0
      ? ""
      : `[${Array.makeBy(2 ** remainingBits, (value) =>
          Encoding.encodeBase64Url(
            Uint8Array.of(value << (8 - remainingBits)),
          ).charAt(0),
        ).join("")}]`;
  const characters = Math.ceil((byteLength * 8) / 6);
  const schema = Schema.String.pipe(
    Schema.length(prefix.length + characters),
    Schema.pattern(
      new RegExp(
        `^${prefix}[A-Za-z0-9_-]{${wholeCharacters}}${finalCharacter}$`,
        "u",
      ),
    ),
    Schema.brand(name),
    Schema.annotations({
      identifier: name,
      description: `${name}: ${prefix} then ${String(characters)} base64url characters`,
    }),
  );
  return Object.assign(schema, {
    mint: () =>
      Schema.decodeSync(schema)(
        `${prefix}${randomBytes(byteLength).toString("base64url")}`,
      ),
  });
};

/** Identifies one agent. The daemon mints it when it creates the agent. */
export const AgentId = identifier("AgentId", "agt_", 16);
/** Validated nominal value decoded by {@link AgentId}. */
export type AgentId = typeof AgentId.Type;

/** Opaque identity of the principal represented by an agent. */
export const PrincipalId = identifier("PrincipalId", "prn_", 16);
/** Validated nominal value decoded by {@link PrincipalId}. */
export type PrincipalId = typeof PrincipalId.Type;

/** The rule every agent name meets, as validation errors state it. */
export const AGENT_NAME_RULE =
  "3 to 32 lowercase letters, digits or single hyphens, starting and ending with a letter or digit";

/** The name other agents address an agent by, unique on its daemon. */
export const AgentName = Schema.String.pipe(
  Schema.minLength(3),
  Schema.maxLength(32),
  Schema.pattern(/^[a-z0-9]+(-[a-z0-9]+)*$/),
  Schema.brand("AgentName"),
  Schema.annotations({
    identifier: "AgentName",
    description: `an agent name: ${AGENT_NAME_RULE}`,
  }),
);
/** Validated nominal value decoded by {@link AgentName}. */
export type AgentName = typeof AgentName.Type;

/**
 * Identifies an agent and the principal it represents. Messages, results and
 * policy decisions name their agents with this value.
 */
export const AgentCard = Schema.Struct({
  agentId: AgentId,
  agentName: AgentName,
  principalId: PrincipalId,
}).annotations({ identifier: "AgentCard" });
/**
 * An agent's identifier, name and principal, as its daemon recorded them.
 * @useDeclaredType
 */
export type AgentCard = typeof AgentCard.Type;

/** A refused send names this reason. */
export class CommunicationError extends Data.TaggedError("CommunicationError")<{
  readonly reason: "content-invalid";
}> {}

/**
 * One author-scoped message id. Retrying a persisted message reuses it; a new
 * message receives a fresh one.
 */
export const MsgId = identifier("MsgId", "msg_", 32);
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
  author: AgentCard,
  recipient: AgentCard,
  msgId: MsgId,
  replyToMsgId: Schema.optional(MsgId),
  text: wellFormedString.pipe(
    Schema.filter(
      (text) => Buffer.byteLength(JSON.stringify(text)) <= maximumTextBytes,
    ),
  ),
}).pipe(
  Schema.filter(
    ({ author, recipient }) =>
      recipient.agentId !== author.agentId &&
      recipient.agentName !== author.agentName,
  ),
  Schema.annotations({ identifier: "Message" }),
);
/** @useDeclaredType */
export type Message = typeof messageSchema.Type;

/** Owns construction of a message. */
export const Message = Object.assign(messageSchema, {
  /** A new message with a fresh id, or the refusal of what cannot be one. */
  create: (input: Omit<Message, "msgId">) =>
    Schema.decode(messageSchema)({ ...input, msgId: MsgId.mint() }).pipe(
      Effect.mapError(
        () => new CommunicationError({ reason: "content-invalid" }),
      ),
      Effect.withSpan("Message.create"),
    ),
});
