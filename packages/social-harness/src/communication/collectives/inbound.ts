/** @file The items an endpoint delivers to its host: messages and collective requests, results and failures. */

import { Schema } from "effect";
import { exactStruct } from "../../encoding/exact.js";
import { AgentView } from "../../identity/cards.js";
import { Message, MessageHeader } from "../message.js";
import { MessageAddressInput, wellFormedString } from "../records/values.js";
import {
  AnswerContent,
  collectiveClosure,
  CollectiveId,
  EpochMillis,
} from "./contracts.js";
import { FormModeSchema } from "./wire.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their domain names with the nominal values they decode. */

/** An ordinary message retains its author, identity, recipients and content. */
const multicastItem = exactStruct({
  kind: Schema.Literal("multicast"),
  message: Message,
});

/** Which collecting operation an item belongs to. */
const collectingOp = Schema.Literal("gather", "all_gather");

/** The question and answer form recognized in a received message. */
const RequestMessage = MessageHeader.pipe(
  Schema.extend(
    exactStruct({
      content: exactStruct({
        op: collectingOp,
        operationId: CollectiveId,
        question: wellFormedString,
        requestedSchema: FormModeSchema,
        deadlineAt: EpochMillis,
      }),
    }),
  ),
  Schema.annotations({ identifier: "RequestMessage" }),
);

const collectiveRequestItem = exactStruct({
  kind: Schema.Literal("collectiveRequest"),
  message: RequestMessage,
});

/**
 * One member's outcome in a completed gather or all_gather, discriminated by
 * `kind`. `no-answer` covers every member that did not answer: one whose
 * request message was refused or not certified in time, and one silent at the
 * deadline.
 */
const memberOutcome = Schema.Union(
  exactStruct({ kind: Schema.Literal("answered"), content: AnswerContent }),
  exactStruct({ kind: Schema.Literal("declined") }),
  exactStruct({ kind: Schema.Literal("invalid"), reason: Schema.String }),
  exactStruct({ kind: Schema.Literal("no-answer") }),
);
/**
 * One member's canonical protocol outcome.
 * @useDeclaredType
 */
export type CollectiveMemberOutcome = typeof memberOutcome.Type;

/**
 * The result of a gather this endpoint started, or of an all_gather this
 * endpoint started or was asked: exactly one outcome per member, in member
 * order. `to` is the address the operation named. Every member's all_gather
 * result is built from exactly the answers the requester's close lists.
 * The question is the requester's original text.
 */
export const collectiveResultItem = exactStruct({
  kind: Schema.Literal("collectiveResult"),
  op: collectingOp,
  operationId: CollectiveId,
  ...collectiveClosure.fields,
  to: MessageAddressInput,
  question: wellFormedString,
  outcomes: Schema.NonEmptyArray(
    exactStruct({ member: AgentView, outcome: memberOutcome }),
  ),
});

/**
 * An all_gather close refused before binding. The operation already started,
 * so its failure arrives through the inbox. A bound close remains pending
 * until certification and cannot produce this item.
 */
const operationFailedItem = exactStruct({
  kind: Schema.Literal("operationFailed"),
  operationId: CollectiveId,
  to: MessageAddressInput,
  error: Schema.String,
});

/**
 * One inbound item, discriminated by `kind`. The endpoint consumes the
 * collective layer's internal messages and messages with malformed or duplicate
 * collective parts. Ordinary messages retain their content, and the endpoint
 * emits results and failures.
 */
export const InboundItem = Schema.Union(
  multicastItem,
  collectiveRequestItem,
  collectiveResultItem,
  operationFailedItem,
).annotations({
  identifier: "InboundItem",
});
/**
 * Host-visible peer content or a result produced by the local daemon.
 * @useDeclaredType
 */
export type InboundItem = typeof InboundItem.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules after the Schema/type pairs. */
