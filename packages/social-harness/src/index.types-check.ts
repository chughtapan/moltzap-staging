/**
 * @file Public messages retain author, recipients and content in every direction.
 * These canaries pin the supported host boundary so adapters compile against the
 * same messages, collective items, guidance and delivery operations.
 */

import type { DateTime, Effect, Either, Scope, Stream, Types } from "effect";
import type {
  acquireHarnessEndpoint,
  AgentAddress,
  AgentView,
  CollectiveError,
  ConnectError,
  Content,
  ContentPart,
  decodeHarnessSendResult,
  DeliveryAcknowledgeError,
  FirewallSendError,
  GroupAddress,
  HarnessEndpoint,
  HarnessEndpointOptions,
  HistoryExportRecord,
  InboundDelivery,
  InboundItem,
  JsonValue,
  ListenError,
  LocalGuidance,
  Message,
  MessageAddressInput,
  MessageReference,
  MessageTextError,
  MsgId,
  parseMessageText,
  renderLocalGuidance,
  SendError,
  SendInput,
  SendResult,
} from "./index.js";

type Expect<Value extends true> = Value;

type CollectiveId = NonNullable<CollectiveError["operationId"]>;
type ExpectedRequestedSchema = Readonly<{
  $schema?: string;
  type: "object";
  properties: Readonly<Record<string, Readonly<Record<string, JsonValue>>>>;
  required?: readonly string[];
}>;
type ExpectedAnswerContent = Readonly<
  Record<string, string | number | boolean | readonly string[]>
>;
type ExpectedCollectiveOperation =
  | Readonly<{ op?: "multicast" }>
  | Readonly<{
      op: "gather" | "all_gather";
      deadline: number;
      requestedSchema: ExpectedRequestedSchema;
    }>;
type ExpectedCollectiveResponse =
  | Readonly<{
      action: "accept";
      content: ExpectedAnswerContent;
      operationId: CollectiveId;
    }>
  | Readonly<{ action: "decline"; operationId: CollectiveId }>;
type ExpectedSendInput =
  | Readonly<{
      to: MessageAddressInput;
      text: string;
      collective?: ExpectedCollectiveOperation;
    }>
  | Readonly<{
      to: MessageAddressInput;
      collectiveResponse: ExpectedCollectiveResponse;
    }>;
type ExpectedSendResult =
  | Readonly<{
      kind: "multicast";
      msgId: MsgId;
      address: AgentAddress | GroupAddress;
      operationId?: never;
      guidance?: ExpectedCheckedGuidance;
    }>
  | Readonly<{
      kind: "response";
      operationId: CollectiveId;
      msgId: MsgId;
      address: AgentAddress | GroupAddress;
      guidance?: ExpectedCheckedGuidance;
    }>
  | Readonly<{
      kind: "collective";
      operationId: CollectiveId;
      msgId?: never;
      address?: never;
      guidance?: ExpectedCheckedGuidance;
    }>;
type ExpectedMessageHeader = Readonly<{
  msgId: MsgId;
  author: AgentView;
  recipients: readonly [AgentView, ...AgentView[]];
}>;
type ExpectedMessage = ExpectedMessageHeader & Readonly<{ content: Content }>;
type ExpectedMessageReference = Readonly<{
  authorAgentId: AgentView["agentId"];
  msgId: MsgId;
}>;
type ExpectedWithheld = Readonly<{
  kind: "withheld";
  checkId: string;
  reason: "rule-violation" | "unresolved-evidence" | "check-unavailable";
}>;
type ExpectedMemberOutcome =
  | Readonly<{ kind: "answered"; content: ExpectedAnswerContent }>
  | Readonly<{ kind: "declined" }>
  | Readonly<{ kind: "invalid"; reason: string }>
  | Readonly<{ kind: "no-answer" }>
  | ExpectedWithheld;
type ExpectedOutcomeEntry = Readonly<{
  member: AgentView;
  outcome: ExpectedMemberOutcome;
}>;
type ExpectedInboundItem =
  | Readonly<{ kind: "multicast"; message: Message }>
  | Readonly<{
      kind: "collectiveRequest";
      message: ExpectedMessageHeader &
        Readonly<{
          content: Readonly<{
            op: "gather" | "all_gather";
            operationId: CollectiveId;
            question: string;
            requestedSchema: ExpectedRequestedSchema;
            deadlineAt: number;
          }>;
        }>;
    }>
  | Readonly<{
      kind: "collectiveResult";
      closedAt: number;
      closedBy: "settled" | "deadline";
      op: "gather" | "all_gather";
      operationId: CollectiveId;
      to: MessageAddressInput;
      question: string | ExpectedWithheld;
      outcomes: readonly [ExpectedOutcomeEntry, ...ExpectedOutcomeEntry[]];
    }>
  | Readonly<{
      kind: "operationFailed";
      operationId: CollectiveId;
      to: MessageAddressInput;
      error: string;
    }>;
type ExpectedGuidanceSubject = Readonly<{
  id: string;
  sender: AgentView | "sharness";
  recipients: readonly AgentView[];
  text: string;
}>;
type ExpectedGuidanceInstruction =
  | Readonly<{ ruleId: string; instruction: string }>
  | Readonly<{
      ruleId: string;
      expectation: "required" | "permitted" | "wait" | "none";
      subjects: readonly string[];
      instruction: string;
    }>
  | Readonly<{
      ruleId: string;
      uncertain: Readonly<{ choice: string; probability: number }>;
      instruction: string;
    }>;
type ExpectedCheckedGuidance = Readonly<{
  checkId: string;
  subjects: readonly ExpectedGuidanceSubject[];
  instructions: readonly ExpectedGuidanceInstruction[];
}>;
type ExpectedLocalGuidance =
  | ExpectedCheckedGuidance
  | Readonly<{
      question?: ExpectedCheckedGuidance;
      answers: ReadonlyArray<
        Readonly<{
          member: AgentView;
          checkId: string;
          subjects: readonly ExpectedGuidanceSubject[];
          instructions: readonly ExpectedGuidanceInstruction[];
        }>
      >;
    }>;
type LocalGuidanceIsExact = Expect<
  Types.Equals<LocalGuidance, ExpectedLocalGuidance>
>;
type GuidanceRenderingIsExact = Expect<
  Types.Equals<
    typeof renderLocalGuidance,
    (
      guidance: LocalGuidance,
      quoted?: { text: string; origin: "peer" | "endpoint" },
    ) => string
  >
>;
type ExpectedDelivery = Readonly<{
  eventId: string;
  item: InboundItem;
  guidance?: LocalGuidance;
  acknowledge: Effect.Effect<void, DeliveryAcknowledgeError>;
}>;
type ExpectedEndpoint = Readonly<{
  send: (
    input: SendInput,
  ) => Effect.Effect<
    SendResult,
    SendError | CollectiveError | FirewallSendError
  >;
  messages: Stream.Stream<InboundDelivery, ListenError>;
}>;

type SendInputIsExact = Expect<Types.Equals<SendInput, ExpectedSendInput>>;
type MessageTextParserIsExact = Expect<
  Types.Equals<
    typeof parseMessageText,
    (
      to: MessageAddressInput,
      text: string,
    ) => Either.Either<SendInput, MessageTextError>
  >
>;
type SendResultIsExact = Expect<Types.Equals<SendResult, ExpectedSendResult>>;
type SendResultDecoderIsExact = Expect<
  Types.Equals<
    Effect.Effect.Success<ReturnType<typeof decodeHarnessSendResult>>,
    ExpectedSendResult
  >
>;
type InboundItemHasPublicFields = Expect<
  InboundItem extends ExpectedInboundItem ? true : false
>;
type MessageIsExact = Expect<
  Types.Equals<Types.Simplify<Message>, Types.Simplify<ExpectedMessage>>
>;
type MessageReferenceIsExact = Expect<
  Types.Equals<MessageReference, ExpectedMessageReference>
>;
type DeliveryIsExact = Expect<Types.Equals<InboundDelivery, ExpectedDelivery>>;
type EndpointIsExact = Expect<Types.Equals<HarnessEndpoint, ExpectedEndpoint>>;
type ExpectedHistoryExportRecord =
  | Readonly<{ kind: "inbound"; item: InboundItem; at: DateTime.Utc }>
  | Readonly<{
      kind: "outbound";
      input: SendInput;
      outcome:
        | Readonly<{
            kind: "sent";
            operationId?: CollectiveId;
            msgIds: readonly MsgId[];
          }>
        | Readonly<{ kind: "failed"; error: string }>;
      at: DateTime.Utc;
    }>
  | Readonly<{ kind: "export-failed"; reason: string; at: DateTime.Utc }>;
type HistoryExportRecordIsExact = Expect<
  Types.Equals<HistoryExportRecord, ExpectedHistoryExportRecord>
>;
type ContentIsNonempty = Expect<
  Content extends readonly [ContentPart, ...ContentPart[]] ? true : false
>;
type AgentAddressIsInput = Expect<
  AgentAddress extends MessageAddressInput ? true : false
>;
type GroupAddressIsInput = Expect<
  GroupAddress extends MessageAddressInput ? true : false
>;
type SendReasonsAreExact = Expect<
  Types.Equals<
    SendError["reason"],
    | "invalid-address"
    | "unknown-agent"
    | "membership-invalid"
    | "message-conflict"
    | "content-invalid"
    | "not-registered"
    | "version-mismatch"
    | "certification-unavailable"
    | "persistence-failed"
    | "network-unavailable"
    | "delivery-pending"
    | "outcome-unknown"
  >
>;
type CollectiveFailureKindsAreExact = Expect<
  Types.Equals<
    CollectiveError["failure"]["kind"],
    | "members-unreachable"
    | "schema-invalid"
    | "answer-invalid"
    | "request-none"
    | "request-answered"
    | "request-expired"
    | "deadline-passed"
  >
>;
type ListenReasonsAreExact = Expect<
  Types.Equals<
    ListenError["reason"],
    | "already-listening"
    | "consumer-revoked"
    | "incompatible-daemon"
    | "transport-failed"
    | "decode-failed"
  >
>;
type AcknowledgeReasonsAreExact = Expect<
  Types.Equals<
    DeliveryAcknowledgeError["reason"],
    | "unknown-delivery"
    | "consumer-revoked"
    | "delivery-conflict"
    | "persistence-failed"
    | "transport-failed"
  >
>;
type ConnectReasonsAreExact = Expect<
  Types.Equals<
    ConnectError["reason"],
    "transport-failed" | "decode-failed" | "incompatible-daemon"
  >
>;
type FirewallRefusalReasonsAreExact = Expect<
  Types.Equals<FirewallSendError["refusal"], ExpectedWithheld["reason"]>
>;
type AcquisitionIsScoped = Expect<
  Types.Equals<
    Parameters<typeof acquireHarnessEndpoint>,
    [endpoint: URL, options: HarnessEndpointOptions]
  >
>;
type AcquisitionResultIsExact = Expect<
  Types.Equals<
    ReturnType<typeof acquireHarnessEndpoint>,
    Effect.Effect<HarnessEndpoint, ConnectError, Scope.Scope>
  >
>;

/** Compile-time witnesses for the accepted public Client boundary. */
export type HarnessEndpointCanaries = [
  SendInputIsExact,
  MessageTextParserIsExact,
  SendResultIsExact,
  SendResultDecoderIsExact,
  InboundItemHasPublicFields,
  LocalGuidanceIsExact,
  GuidanceRenderingIsExact,
  MessageIsExact,
  MessageReferenceIsExact,
  DeliveryIsExact,
  EndpointIsExact,
  ContentIsNonempty,
  AgentAddressIsInput,
  GroupAddressIsInput,
  SendReasonsAreExact,
  FirewallRefusalReasonsAreExact,
  CollectiveFailureKindsAreExact,
  HistoryExportRecordIsExact,
  ListenReasonsAreExact,
  AcknowledgeReasonsAreExact,
  ConnectReasonsAreExact,
  AcquisitionIsScoped,
  AcquisitionResultIsExact,
];
