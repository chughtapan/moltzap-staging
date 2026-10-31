/** @file Adapts messages and saved visibility to firewall assessment inputs. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Clock, Effect, Array as EffectArray, Option, Order } from "effect";
import { createHash } from "node:crypto";
import type {
  FirewallOperations,
  PreparedFirewallCheck,
} from "../../firewall/index.js";
import type { InboundItem } from "../../host-protocol/items.js";
import type { Ed25519PublicKey } from "../../identity/keys.js";
import type {
  FirewallEvidence,
  FirewallHistoryEntry,
  FirewallLocalEntry,
  IncomingEntry,
} from "./types.js";
import {
  type CollectiveValue,
  readCollectiveValue,
} from "../../communication/collectives/index.js";
import { Question } from "../../communication/collectives/model.js";
import {
  messageMembers,
  replyAddress,
  type Message as SharedMessage,
  Message as SharedMessageSchema,
} from "../../communication/message.js";
import {
  type Content,
  deriveConversationId,
} from "../../communication/records/index.js";
import {
  type AgentAddress,
  agentAddressOf,
} from "../../communication/records/values.js";
import { EndpointStoreError } from "../../communication/store/index.js";
import { encodeCanonical } from "../../encoding/canonical-json.js";
import { compareAgentIds } from "../../encoding/identifier.js";
import {
  ENDPOINT_SENDER,
  type HistoryEntry,
  type Message,
  type MessageContext,
} from "../../firewall/check.js";
import {
  questionText,
  renderCollectiveQuestion,
  renderCollectiveRequestBody,
  renderCollectiveResult,
  renderContent,
  renderOutcome,
} from "../../host-protocol/render.js";
import {
  type AgentView,
  agentViewOf,
  type VerifiedAgentCard,
} from "../../identity/cards.js";
import { readFirewallEvidence } from "./evidence.js";

/** The daemon supplies verified attribution and its existing durable evidence capability. */
export interface FirewallEnvironment {
  readonly sql: SqlClient.SqlClient;
  readonly firewall: FirewallOperations;
  readonly localAgentCard: VerifiedAgentCard;
  readonly registrySignerPublicKey: Ed25519PublicKey;
}

/** Assessment text and whether the agent may receive it. */
interface MessageProjection {
  readonly message: Message;
  readonly supported: boolean;
}

/** The text assessed for a shared message and its verified participants. */
interface ProjectedHistoryMessage extends MessageProjection {
  readonly sharedMessage: SharedMessage;
  readonly cards: readonly AgentView[];
  readonly author: AgentAddress;
}

/** One check's candidate, the evidence it is assessed against, and its attribution. */
export interface MessageCheckInput {
  readonly direction: "incoming" | "outgoing";
  readonly evidence: FirewallEvidence;
  readonly canonicalCandidate: Uint8Array;
  readonly message?: Message;
  readonly cards: readonly AgentView[];
}

/**
 * Project one message by its collective part: a multicast as its text, a request
 * as exactly the question a member's model reads before the answer
 * instructions, and an answer as its line of a result. A close, or a message
 * whose part does not decode, is projected as its raw content and is never
 * shown to the agent.
 */
const messageProjection = Effect.fn("firewallMessageProjection")(function* (
  shared: SharedMessage,
) {
  const projected = yield* readCollectiveValue(shared.content).pipe(
    Effect.flatMap((part) => projectPart(shared, Option.getOrUndefined(part))),
    Effect.catchTag("CollectivePartInvalidError", () =>
      Effect.succeed<ProjectedText>({
        text: renderContent(shared.content),
        supported: false,
      }),
    ),
  );
  return {
    message: {
      id: messageId(shared),
      sender: shared.author.agentId,
      recipients: EffectArray.sort(
        EffectArray.map(shared.recipients, (agent) => agent.agentId),
        Order.string,
      ),
      text: projected.text,
      ...(projected.operation === undefined
        ? {}
        : { operation: projected.operation }),
    },
    supported: projected.supported,
  } satisfies MessageProjection;
});

/** Communication has already verified the message before local policy reads it. */
export const projectHistoryMessage = Effect.fn("projectHistoryMessage")(
  function* (entry: FirewallHistoryEntry) {
    return {
      ...(yield* messageProjection(entry.message)),
      sharedMessage: entry.message,
      cards: messageMembers(entry.message),
      author: agentAddressOf(entry.message.author),
    } satisfies ProjectedHistoryMessage;
  },
);

/** Certified local sends are admitted evidence without granting host visibility to owner history. */
const contextFrom = Effect.fn("firewallContext")(function* (
  environment: FirewallEnvironment,
  evidence: FirewallEvidence,
  candidateCards: readonly AgentView[],
) {
  const localAgentId = environment.localAgentCard.agentId;
  const history = yield* Effect.forEach(
    evidence.history,
    (entry) => decodeHistory(entry, localAgentId),
    { concurrency: 1 },
  );
  const cards = new Map<string, AgentView>();
  for (const card of [
    agentViewOf(environment.localAgentCard),
    ...candidateCards,
    ...history.flatMap((entry) => entry.cards),
  ]) {
    cards.set(card.agentId, card);
  }
  const context: MessageContext = {
    agentId: localAgentId,
    principal: environment.localAgentCard.principalId,
    participants: [...cards.values()].map((card) => ({
      agentId: card.agentId,
      principal: card.principalId,
    })),
    history: history.map(({ entry }) => entry),
    earlierHistoryOmitted: evidence.earlierHistoryOmitted,
    now: yield* Clock.currentTimeMillis,
    facts: environment.firewall.configuration.facts,
    gaps: evidenceGaps(evidence),
  };
  return { context, cards: [...cards.values()] };
});

/**
 * A message's own fields, without its class, so a projection can replace
 * some of them.
 * @param message The checked message.
 * @returns Its id, attribution, text and any operation.
 */
export function messageFields(message: Message) {
  return {
    id: message.id,
    sender: message.sender,
    recipients: message.recipients,
    text: message.text,
    ...(message.operation === undefined
      ? {}
      : { operation: message.operation }),
  };
}

/** A projected message's text and operation, before attribution. */
interface ProjectedText {
  readonly text: string;
  readonly operation?: Message["operation"];
  readonly supported: boolean;
}

/**
 * Project a message's collective part; a message with none is plain text. An
 * answer's operation follows from its conversation: a gather's answers
 * travel in direct conversations, and an all_gather's in its group, which
 * always has at least three members.
 */
function projectPart(
  message: SharedMessage,
  value?: CollectiveValue,
): Effect.Effect<ProjectedText, EndpointStoreError> {
  if (value === undefined) {
    return Effect.succeed({ text: textOf(message.content), supported: true });
  }
  switch (value.kind) {
    case "operation":
      return projectRequest(message, value);
    case "response":
      return Effect.succeed({
        text: renderOutcome(
          value.action === "accept"
            ? { kind: "answered", content: value.content }
            : { kind: "declined" },
        ),
        operation: {
          kind: "answer",
          op: message.recipients.length === 1 ? "gather" : "all_gather",
          operationId: value.operationId,
        },
        supported: true,
      });
    case "close":
      return Effect.succeed({
        text: renderContent(message.content),
        supported: false,
      });
    default: {
      const exhaustive: never = value;
      return exhaustive;
    }
  }
}

/**
 * The text of content parts as a model reads it, or empty for none.
 * @param content The parts rendered for the model.
 * @returns The rendered text.
 */
function textOf(content: ReadonlyArray<Content[number]>): string {
  const [first, ...rest] = content;
  return first === undefined ? "" : renderContent([first, ...rest]);
}

/** A request message's question, as the members it asks read it. */
function projectRequest(
  message: SharedMessage,
  value: Extract<
    CollectiveValue,
    { readonly kind: "operation"; readonly op: "gather" | "all_gather" }
  >,
): Effect.Effect<ProjectedText, EndpointStoreError> {
  return Effect.succeed({
    text: renderCollectiveRequestBody({
      kind: "collectiveRequest",
      message: {
        ...message,
        content: {
          op: value.op,
          operationId: value.operationId,
          question: questionText(message.content),
          requestedSchema: value.requestedSchema,
          deadlineAt: value.deadlineAt,
        },
      },
    }),
    operation: {
      kind: "request",
      op: value.op,
      operationId: value.operationId,
      deadlineAt: value.deadlineAt,
    },
    supported: true,
  });
}

function messageId(message: Pick<SharedMessage, "author" | "msgId">): string {
  return hashedId(`${message.author.agentId}:${message.msgId}`);
}

/**
 * A stable message id derived from an identity no two messages share.
 * @param identity The author-scoped identity the id stands for.
 * @returns The id a check and its guidance refer to.
 */
function hashedId(identity: string): string {
  return `msg_${createHash("sha256").update(identity).digest("hex")}`;
}

/** One history entry and the cards its attribution needs. */
interface DecodedHistory {
  readonly entry: typeof HistoryEntry.Type;
  readonly cards: readonly AgentView[];
}

/**
 * Visibility records whether an inbox item or result showed the message to
 * the agent. Withheld content remains available only to assessment.
 */
function decodeHistory(
  entry: FirewallHistoryEntry | FirewallLocalEntry,
  localAgentId: string,
): Effect.Effect<DecodedHistory, EndpointStoreError> {
  return Effect.gen(function* () {
    if ("item" in entry) {
      const item = entry.item;
      return {
        entry: {
          id: hashedId(`${ENDPOINT_SENDER}:${item.operationId}`),
          sender: ENDPOINT_SENDER,
          recipients: [localAgentId],
          text:
            item.kind === "collectiveResult"
              ? renderCollectiveResult(item)
              : item.error,
          operation:
            item.kind === "collectiveResult"
              ? { kind: "result", op: item.op, operationId: item.operationId }
              : { kind: "failure", operationId: item.operationId },
          visibility: "runtime",
          admission: "admitted",
        },
        cards: [],
      } satisfies DecodedHistory;
    }
    const decoded = yield* projectHistoryMessage(entry);
    const visible = entry.visible && decoded.supported;
    return {
      entry: {
        ...messageFields(decoded.message),
        visibility: visible ? "runtime" : "assessor",
        admission:
          visible || decoded.message.sender === localAgentId
            ? "admitted"
            : "withheld",
      },
      cards: decoded.cards,
    } satisfies DecodedHistory;
  }).pipe(Effect.withSpan("decodeFirewallHistory"));
}

function evidenceGaps(evidence: FirewallEvidence): string[] {
  return [
    ...(evidence.overflow
      ? ["Authorized earlier history exceeds the 2 MiB evidence byte bound."]
      : []),
    ...(evidence.pendingOutgoing
      ? ["An earlier outgoing intent has not reached certified completion."]
      : []),
  ];
}

/** One input joins exact candidate bytes with the permitted verified conversation view. */
export const prepareMessageCheck = Effect.fn("prepareMessageCheck")(function* (
  environment: FirewallEnvironment,
  input: MessageCheckInput,
): Effect.fn.Return<PreparedFirewallCheck, EndpointStoreError> {
  const { context, cards } = yield* contextFrom(
    environment,
    input.evidence,
    input.cards,
  );
  return {
    direction: input.direction,
    canonicalCandidate: input.canonicalCandidate,
    ...(input.message === undefined ? {} : { message: input.message }),
    context,
    agents: cards,
  };
});

/** Assesses the exact outgoing message captured by the host operation. */
export const prepareOutgoingCheck = Effect.fn("prepareOutgoingCheck")(
  function* (environment: FirewallEnvironment, shared: SharedMessage) {
    const canonicalCandidate = yield* encodeCanonical(
      SharedMessageSchema,
      shared,
    ).pipe(
      Effect.catchTag(
        "ClientRepresentationError",
        () => new EndpointStoreError({ reason: "invalid-input" }),
      ),
    );
    const message = (yield* messageProjection(shared)).message;
    return yield* outgoingCheck(
      environment,
      shared,
      canonicalCandidate,
      message,
    );
  },
);

/** A question is checked once for its fixed recipients before any request message is accepted. */
export const prepareOutgoingQuestionCheck = Effect.fn(
  "prepareOutgoingQuestionCheck",
)(function* (environment: FirewallEnvironment, question: Question) {
  const canonicalCandidate = yield* encodeCanonical(Question, question).pipe(
    Effect.catchTag(
      "ClientRepresentationError",
      () => new EndpointStoreError({ reason: "invalid-input" }),
    ),
  );
  const message: Message = {
    id: hashedId(`${question.author.agentId}:${question.operationId}`),
    sender: question.author.agentId,
    recipients: EffectArray.map(question.recipients, (agent) => agent.agentId),
    text: renderCollectiveQuestion(question),
    operation: {
      kind: "request",
      op: question.op,
      operationId: question.operationId,
      deadlineAt: question.deadlineAt,
    },
  };
  return yield* outgoingCheck(
    environment,
    question,
    canonicalCandidate,
    message,
  );
});

type Participants = Pick<SharedMessage, "author" | "recipients">;

function outgoingCheck(
  environment: FirewallEnvironment,
  participants: Participants,
  canonicalCandidate: Uint8Array,
  message: Message,
) {
  return Effect.gen(function* () {
    const cards = [...messageMembers(participants)].sort((left, right) =>
      compareAgentIds(left.agentId, right.agentId),
    );
    const [first, second, ...rest] = cards;
    if (first === undefined || second === undefined) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    const conversationId = yield* deriveConversationId([
      first.agentId,
      second.agentId,
      ...rest.map((agent) => agent.agentId),
    ]).pipe(
      Effect.catchTag(
        "ClientRepresentationError",
        () => new EndpointStoreError({ reason: "invalid-input" }),
      ),
    );
    const conversation = replyAddress(participants);
    const evidence = yield* readFirewallEvidence({
      conversationId,
      conversation,
      maximumRecords: environment.firewall.configuration.maximumHistoryRecords,
      registrySignerPublicKey: environment.registrySignerPublicKey,
    }).pipe(Effect.provideService(SqlClient.SqlClient, environment.sql));
    return yield* prepareMessageCheck(environment, {
      direction: "outgoing",
      evidence,
      canonicalCandidate,
      message,
      cards,
    });
  });
}

/** Queued peer messages carry the verified content policy assesses. */
export const prepareIncomingCheck = Effect.fn("prepareIncomingCheck")(
  function* (
    environment: FirewallEnvironment,
    entry: IncomingEntry,
    item: Extract<
      InboundItem,
      { readonly kind: "multicast" | "collectiveRequest" }
    >,
  ): Effect.fn.Return<PreparedFirewallCheck, EndpointStoreError> {
    const evidence = yield* readFirewallEvidence({
      message: item.message,
      conversation: entry.conversation,
      maximumRecords: environment.firewall.configuration.maximumHistoryRecords,
      registrySignerPublicKey: environment.registrySignerPublicKey,
    }).pipe(Effect.provideService(SqlClient.SqlClient, environment.sql));
    const message: Message =
      item.kind === "multicast"
        ? (yield* messageProjection(item.message)).message
        : {
            id: messageId(item.message),
            sender: item.message.author.agentId,
            recipients: EffectArray.sort(
              EffectArray.map(
                item.message.recipients,
                (agent) => agent.agentId,
              ),
              Order.string,
            ),
            text: renderCollectiveRequestBody(item),
            operation: {
              kind: "request",
              op: item.message.content.op,
              operationId: item.message.content.operationId,
              deadlineAt: item.message.content.deadlineAt,
            },
          };
    return yield* prepareMessageCheck(environment, {
      direction: "incoming",
      evidence,
      canonicalCandidate: entry.canonicalItem,
      message,
      cards: messageMembers(item.message),
    });
  },
);
