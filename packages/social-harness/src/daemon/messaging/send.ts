/** @file Captures host commands, checks policy and transfers accepted work to messaging services. */

import { SqlClient } from "@effect/sql";
import { Clock, type Context, DateTime, Effect, Schema } from "effect";
import type {
  Communication,
  CommunicationError,
} from "../../communication/index.js";
import type { Content } from "../../communication/records/values.js";
import type {
  DeliveryOperations,
  HarnessSendRequest,
} from "../../host-protocol/operations.js";
import type { AgentView } from "../../identity/cards.js";
import type { Identity } from "../../identity/local.js";
import type { HistoryExportPort } from "../inbox/host-traffic-log.js";
import {
  CollectiveError,
  type CollectiveOperation,
  type CollectiveResponse,
  type SendInput,
} from "../../communication/collectives/contracts.js";
import {
  type Collectives,
  type CollectivesError,
  encodeCollectiveContent,
  mintCollectiveId,
} from "../../communication/collectives/index.js";
import { Question } from "../../communication/collectives/model.js";
import {
  SendError,
  type SendFailure,
} from "../../communication/conversations/errors.js";
import {
  type Message,
  messageFromParticipants,
  messageMembers,
  replyAddress,
} from "../../communication/message.js";
import { mintMsgId } from "../../communication/records/index.js";
import { saveCheckDecision } from "../../firewall/store/index.js";
import { canonicalMessageAddress } from "../../host-protocol/address.js";
import {
  FirewallSendError,
  refusalReason,
} from "../../host-protocol/advice.js";
import {
  type FirewallEnvironment,
  prepareOutgoingCheck,
  prepareOutgoingQuestionCheck,
} from "../inbox/firewall-context.js";
import { isMessageVisible } from "../inbox/store.js";

/** Service dependencies stay fixed while each invocation captures its command. */
export interface SendContext {
  readonly sql: SqlClient.SqlClient;
  readonly identity: Context.Tag.Service<typeof Identity>;
  readonly communication: Context.Tag.Service<typeof Communication>;
  readonly collectives: Context.Tag.Service<typeof Collectives>;
  readonly firewallEnvironment: Effect.Effect<FirewallEnvironment | undefined>;
  readonly historyExport: HistoryExportPort;
  readonly experiments?: {
    readonly hideGroups: boolean;
    readonly hideCollectives: boolean;
  };
}

/** A repeated host invocation creates a new command, while accepted service work retains its identity. */
export const sendOperation =
  (context: SendContext): DeliveryOperations["send"] =>
  (request) =>
    send(context, request).pipe(
      Effect.tapBoth({
        onFailure: (error) =>
          DateTime.now.pipe(
            Effect.flatMap((at) =>
              context.historyExport.record({
                kind: "outbound",
                input: request.input,
                outcome: { kind: "failed", error: error.message },
                at,
              }),
            ),
          ),
        onSuccess: (result) =>
          DateTime.now.pipe(
            Effect.flatMap((at) =>
              context.historyExport.record({
                kind: "outbound",
                input: request.input,
                outcome: {
                  kind: "sent",
                  msgIds: result.kind === "collective" ? [] : [result.msgId],
                  ...("operationId" in result &&
                  result.operationId !== undefined
                    ? { operationId: result.operationId }
                    : {}),
                },
                at,
              }),
            ),
          ),
      }),
      Effect.provideService(SqlClient.SqlClient, context.sql),
    );

function send(context: SendContext, request: HarnessSendRequest) {
  return Effect.gen(function* () {
    yield* checkExperiments(context, request.input);
    const status = yield* context.identity
      .me()
      .pipe(
        Effect.catchTag(
          "IdentityError",
          () => new SendError({ reason: "persistence-failed" }),
        ),
      );
    if (status.kind !== "registered") {
      return yield* new SendError({ reason: "not-registered" });
    }
    const environment = yield* context.firewallEnvironment;
    const result =
      "collectiveResponse" in request.input
        ? yield* answer(context, status.agent, request.input, environment)
        : yield* start(context, status.agent, request.input, environment);
    return result;
  }).pipe(
    Effect.catchTag("CollectivesError", (error) =>
      Effect.fail(collectiveFailure(error)),
    ),
    Effect.catchTag(
      "EndpointStoreError",
      () => new SendError({ reason: "persistence-failed" }),
    ),
    Effect.catchTag(
      "ClientRepresentationError",
      () => new SendError({ reason: "content-invalid" }),
    ),
    Effect.catchTag(
      "CollectiveContentError",
      () => new SendError({ reason: "content-invalid" }),
    ),
    Effect.catchTag(
      "ParseError",
      () => new SendError({ reason: "content-invalid" }),
    ),
    Effect.withSpan("HostMessaging.send"),
  );
}

function checkExperiments(context: SendContext, input: SendInput) {
  const experiments = context.experiments;
  if (experiments === undefined) {
    return Effect.void;
  }
  if (experiments.hideGroups && input.to.startsWith("group:")) {
    return Effect.fail(
      new SendError({
        reason: "content-invalid",
        detail: "Group addresses are unavailable. Send to an agent address.",
      }),
    );
  }
  if (experiments.hideCollectives && isCollective(input)) {
    return Effect.fail(
      new SendError({
        reason: "content-invalid",
        detail:
          "Collective questions and answers are unavailable. Send a plain message.",
      }),
    );
  }
  return Effect.void;
}

function isCollective(input: SendInput): boolean {
  return (
    "collectiveResponse" in input ||
    input.collective?.op === "gather" ||
    input.collective?.op === "all_gather"
  );
}

function recipients(
  context: SendContext,
  author: AgentView,
  to: SendInput["to"],
) {
  return canonicalMessageAddress(to, author.agentName).pipe(
    Effect.flatMap((address) =>
      context.identity
        .resolve(
          address.kind === "direct"
            ? [address.remoteName]
            : address.memberNames.filter((name) => name !== author.agentName),
        )
        .pipe(Effect.map((agents) => ({ address: address.address, agents }))),
    ),
    Effect.catchTag(
      "IdentityError",
      (error) =>
        new SendError({
          reason:
            error.reason === "unknown-agent"
              ? "unknown-agent"
              : "network-unavailable",
          ...(error.agentName === undefined
            ? {}
            : { detail: `Could not resolve agent:${error.agentName}.` }),
        }),
    ),
  );
}

function start(
  context: SendContext,
  author: AgentView,
  input: Extract<SendInput, { readonly text: string }>,
  environment?: FirewallEnvironment,
) {
  return Effect.gen(function* () {
    const destination = yield* recipients(context, author, input.to);
    if (
      input.collective?.op === "gather" ||
      input.collective?.op === "all_gather"
    ) {
      const question = yield* captureQuestion(
        author,
        destination.agents,
        input.text,
        input.collective,
      );
      const guidance = yield* checkQuestion(question, environment);
      yield* context.collectives.ask(question);
      return {
        kind: "collective" as const,
        operationId: question.operationId,
        ...(guidance === undefined ? {} : { guidance }),
      };
    }
    const message = yield* capturedMessage(author, destination.agents, [
      { type: "text", text: input.text },
    ]);
    const guidance = yield* checkMessage(message, environment);
    yield* context.communication
      .send(message)
      .pipe(Effect.catchTag("CommunicationError", communicationFailure));
    return {
      kind: "multicast" as const,
      msgId: message.msgId,
      address: destination.address,
      ...(guidance === undefined ? {} : { guidance }),
    };
  });
}

function answer(
  context: SendContext,
  author: AgentView,
  input: Extract<
    SendInput,
    { readonly collectiveResponse: CollectiveResponse }
  >,
  environment?: FirewallEnvironment,
) {
  return Effect.gen(function* () {
    const request = yield* context.collectives.getRequest(
      input.collectiveResponse.operationId,
    );
    const visible = yield* isMessageVisible({
      authorAgentId: request.author.agentId,
      msgId: request.msgId,
    });
    const destination = yield* canonicalMessageAddress(
      input.to,
      author.agentName,
    );
    if (!visible || destination.address !== replyAddress(request)) {
      return yield* new CollectiveError({ failure: { kind: "request-none" } });
    }
    const content = yield* encodeCollectiveContent({
      kind: "response",
      ...input.collectiveResponse,
    });
    const message = yield* capturedMessage(
      author,
      messageMembers(request).filter(
        (agent) => agent.agentId !== author.agentId,
      ),
      content,
    );
    const guidance = yield* checkMessage(message, environment);
    yield* context.collectives.answer(message);
    return {
      kind: "response" as const,
      msgId: message.msgId,
      address: destination.address,
      operationId: input.collectiveResponse.operationId,
      ...(guidance === undefined ? {} : { guidance }),
    };
  });
}

function captureQuestion(
  author: AgentView,
  agents: readonly AgentView[],
  text: string,
  collective: Extract<
    CollectiveOperation,
    { readonly op: "gather" | "all_gather" }
  >,
) {
  return Effect.gen(function* () {
    return yield* Schema.decodeUnknown(Question)({
      author,
      recipients: agents,
      operationId: yield* mintCollectiveId(author.agentId),
      op: collective.op,
      text,
      requestedSchema: collective.requestedSchema,
      deadlineAt: (yield* Clock.currentTimeMillis) + collective.deadline * 1000,
    });
  });
}

function checkMessage(message: Message, environment?: FirewallEnvironment) {
  if (environment === undefined) {
    return Effect.succeed(undefined);
  }
  return assess(environment, prepareOutgoingCheck(environment, message));
}

function checkQuestion(question: Question, environment?: FirewallEnvironment) {
  if (environment === undefined) {
    return Effect.succeed(undefined);
  }
  return assess(
    environment,
    prepareOutgoingQuestionCheck(environment, question),
  );
}

function capturedMessage(
  author: AgentView,
  agents: readonly AgentView[],
  content: Content,
) {
  return Effect.gen(function* () {
    return yield* messageFromParticipants(author, agents, {
      msgId: yield* mintMsgId(),
      content,
    });
  });
}

function assess(
  environment: FirewallEnvironment,
  prepared: ReturnType<typeof prepareOutgoingCheck>,
) {
  return prepared.pipe(
    Effect.flatMap((input) =>
      Effect.gen(function* () {
        const [checked] = yield* environment.firewall.check([input]);
        if (checked === undefined) {
          return yield* new SendError({ reason: "persistence-failed" });
        }
        yield* saveCheckDecision(checked.completion);
        if (!checked.completion.admit) {
          return yield* new FirewallSendError({
            checkId: checked.completion.checkId,
            refusal: refusalReason(checked.result.reason),
            feedback: checked.result.feedback,
          });
        }
        return checked.guidance;
      }),
    ),
  );
}

const sendReasons = {
  "outbox-too-old": "outcome-unknown",
  "agent-not-admitted": "network-unavailable",
  "invalid-address": "invalid-address",
  "unknown-agent": "unknown-agent",
  "membership-invalid": "membership-invalid",
  "content-invalid": "content-invalid",
  "message-conflict": "message-conflict",
  "not-registered": "not-registered",
  "version-mismatch": "version-mismatch",
  "certification-unavailable": "certification-unavailable",
  "persistence-failed": "persistence-failed",
  "network-unavailable": "network-unavailable",
  "outcome-unknown": "outcome-unknown",
  "unknown-message": "content-invalid",
  "invalid-continuation": "content-invalid",
  "history-gap": "persistence-failed",
} as const satisfies Readonly<
  Record<CommunicationError["reason"], SendFailure>
>;

function communicationFailure(error: CommunicationError) {
  return new SendError({ reason: sendReasons[error.reason] });
}

const collectiveSendReasons = {
  "not-registered": "not-registered",
  "invalid-question": "content-invalid",
  "answer-invalid": "content-invalid",
  "persistence-failed": "persistence-failed",
  "message-failed": "outcome-unknown",
} as const satisfies Partial<Record<CollectivesError["reason"], SendFailure>>;

function collectiveFailure(error: CollectivesError) {
  if (
    error.reason === "request-none" ||
    error.reason === "request-expired" ||
    error.reason === "request-answered"
  ) {
    return new CollectiveError({ failure: { kind: error.reason } });
  }
  return new SendError({
    reason: collectiveSendReasons[error.reason],
    ...(error.detail === undefined ? {} : { detail: error.detail }),
  });
}
