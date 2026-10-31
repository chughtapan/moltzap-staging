/** @file Messages retain their authors, recipients and content across delivery and recovery. */

import { Effect, ParseResult, Schema } from "effect";
import { exactStruct } from "../encoding/exact.js";
import { compareAscii } from "../encoding/identifier.js";
import { AgentView } from "../identity/cards.js";
import { AgentId } from "../identity/identifiers.js";
import {
  agentAddressOf,
  Content,
  GroupAddress,
  maximumMembers,
  MsgId,
} from "./records/values.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their names with the values they decode. */

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

/** Recipients exclude the author and appear once each in agent-name order. */
export const MessageParticipants = exactStruct({
  author: AgentView,
  recipients: Schema.NonEmptyArray(AgentView).pipe(
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

/** Delivery and history use the same message envelope. */
export const Message = MessageHeader.pipe(
  Schema.extend(exactStruct({ content: Content })),
  Schema.annotations({ identifier: "Message" }),
);
/**
 * Content and recipients stay fixed under the author and message id.
 * @useDeclaredType
 */
export type Message = typeof Message.Type;

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
  members: readonly AgentView[],
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

/** Replies to an incoming message reach its author or the same fixed group. */
export function replyAddress(message: Participants) {
  return message.recipients.length === 1
    ? agentAddressOf(message.author)
    : GroupAddress.make(
        `group:${messageMembers(message)
          .map((member) => member.agentName)
          .join(",")}`,
      );
}

/** Includes the author and every recipient in agent-name order. */
export function messageMembers(message: Participants): readonly AgentView[] {
  return [message.author, ...message.recipients].sort((left, right) =>
    compareAscii(left.agentName, right.agentName),
  );
}
