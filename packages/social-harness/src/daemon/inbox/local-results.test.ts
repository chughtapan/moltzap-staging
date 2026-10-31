/** @file Result assessment checks the text shown to the host while retaining private source custody. */

import { scopedLive as it } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, Layer, Schema } from "effect";
import { expect } from "vitest";
import type { AnswerContent } from "../../communication/collectives/contracts.js";
import type { CollectiveMemberOutcome } from "../../communication/collectives/inbound.js";
import type {
  DecisionRequest,
  MessageMiddleware,
  MiddlewareInput,
} from "../../firewall/check.js";
import { digest } from "../../__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../../__tests__/collective-id-fixtures.js";
import { conversationIdentityFixture } from "../../__tests__/conversation-fixtures.js";
import {
  firewallOptions,
  observeInputs,
  pass,
  policy,
  rule,
} from "../../__tests__/daemon-delivery-fixtures.js";
import {
  encodeCollectiveContent,
  FormModeSchema,
} from "../../communication/collectives/wire.js";
import { messageFromIntent } from "../../communication/message.js";
import { mintMsgId } from "../../communication/records/index.js";
import {
  decodeRuntimeValue,
  DeliveryToken,
  encodeRuntimeValue,
} from "../../communication/store/index.js";
import { Firewall, type FirewallOptions } from "../../firewall/index.js";
import { localGuidanceSchema } from "../../host-protocol/advice.js";
import { InboundItem } from "../../host-protocol/items.js";
import { renderOutcome } from "../../host-protocol/render.js";
import { agentViewOf, type VerifiedAgentCard } from "../../identity/cards.js";
import { saveResultDecision } from "../store.js";
import { checkLocalItem } from "./local-results.js";
import { queueIncomingItem } from "./pending.js";
import { isMessageVisible, readHistoryViews } from "./store.js";

const question = "Which day?\nUse a calendar.";
const privateAnswer = "Private original invalid answer";
type Subject = Effect.Effect.Success<typeof conversationIdentityFixture>;

function sourceMessage(
  subject: Subject,
  author: VerifiedAgentCard,
  content: Parameters<typeof messageFromIntent>[1]["content"],
) {
  return Effect.gen(function* () {
    return yield* messageFromIntent(
      [subject.local.card, subject.remote.card, subject.third.card].map(
        agentViewOf,
      ),
      {
        authorAgentId: author.agentId,
        msgId: yield* mintMsgId(),
        content,
      },
    );
  }).pipe(Effect.orDie);
}

function resultCase(
  subject: Subject,
  raw: AnswerContent = { slot: "mon" },
  outcome: CollectiveMemberOutcome = { kind: "answered", content: raw },
) {
  return Effect.gen(function* () {
    const operationId = collectiveOperationId(subject.remote.card.agentId);
    const request = yield* requestSource(subject, operationId);
    const own = yield* answerSource(subject, subject.local.card, operationId, {
      slot: "tue",
    });
    const remote = yield* answerSource(
      subject,
      subject.third.card,
      operationId,
      raw,
    );
    const decoded = yield* Schema.decodeUnknown(InboundItem)({
      kind: "collectiveResult",
      op: "all_gather",
      operationId,
      to: "group:alice,bob,carol",
      question,
      closedBy: "settled",
      closedAt: Date.now(),
      outcomes: [
        {
          member: agentViewOf(subject.local.card),
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
        { member: agentViewOf(subject.third.card), outcome },
      ],
    });
    if (decoded.kind !== "collectiveResult") {
      return yield* Effect.dieMessage("Expected a collective result");
    }
    const sources = { answers: [own, remote], request };
    const entry = {
      sequence: 1,
      deliveryToken: Schema.decodeUnknownSync(DeliveryToken)(digest("dlv_", 7)),
      conversation: "group:alice,bob,carol",
      canonicalItem: yield* encodeRuntimeValue(decoded),
      canonicalSources: yield* encodeRuntimeValue(sources),
    };
    return { entry, item: decoded, sources, remote };
  });
}

function assessment(
  subject: Subject,
  middleware: MessageMiddleware,
  overrides: Partial<FirewallOptions> = {},
) {
  return Effect.gen(function* () {
    const context = yield* Layer.build(
      Firewall.layer(firewallOptions(middleware, overrides)),
    ).pipe(subject.run);
    const firewall = Context.get(context, Firewall);
    if (firewall.configuration === undefined) {
      return yield* Effect.dieMessage("Expected a configured policy");
    }
    return {
      firewall,
      sql: subject.sql,
      localAgentCard: subject.local.card,
      registrySignerPublicKey: subject.signer,
    };
  });
}

it("checks the remote answer and quoted question while trusting the agent's own answer", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const result = yield* resultCase(subject);
    const seen: MiddlewareInput[] = [];
    const environment = yield* assessment(subject, observeInputs(seen));
    const admitted = yield* checkLocalItem(environment, result);
    expect(seen.map((input) => input.event.message.text)).toEqual([
      renderOutcome(result.item.outcomes[1]?.outcome ?? { kind: "no-answer" }),
      JSON.stringify(question),
    ]);
    expect(seen.map((input) => input.event.message.sender)).toEqual([
      subject.third.card.agentId,
      subject.remote.card.agentId,
    ]);
    expect(
      yield* decodeRuntimeValue(InboundItem, admitted.item.canonicalItem),
    ).toEqual(result.item);
  }));

it("checks only the public invalid-answer reason and keeps its original body private", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const invalid = {
      kind: "invalid" as const,
      reason: "Expected an allowed day",
    };
    const result = yield* resultCase(subject, { slot: privateAnswer }, invalid);
    const seen: MiddlewareInput[] = [];
    const environment = yield* assessment(
      subject,
      observeInputs(seen, (input) => ({
        findings: pass.findings,
        expectations: [
          {
            ruleId: rule.id,
            expectation: "required",
            subjects: [input.event.message.id],
          },
        ],
      })),
    );
    const admitted = yield* checkLocalItem(environment, result);
    yield* subject.run(
      queueIncomingItem(result.entry).pipe(
        Effect.zipRight(saveResultDecision(admitted)),
      ),
    );
    const { author, msgId } = result.remote;
    const reference = { authorAgentId: author.agentId, msgId };
    expect(seen[0]?.event.message.text).toBe(renderOutcome(invalid));
    expect(yield* subject.run(isMessageVisible(reference))).toBe(false);
    const [view] = yield* subject.run(readHistoryViews([reference]));
    expect(
      yield* decodeRuntimeValue(
        InboundItem,
        view?.canonicalItem ?? new Uint8Array(),
      ),
    ).toEqual(result.item);
    const guidance = yield* decodeRuntimeValue(
      localGuidanceSchema,
      admitted.item.canonicalGuidance ?? new Uint8Array(),
    );
    expect(
      "answers" in guidance
        ? guidance.answers[0]?.subjects[0]?.text
        : undefined,
    ).toBe(renderOutcome(invalid));
  }));

it("keeps one captured text policy while an owner edit completes during result assessment", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const result = yield* resultCase(subject);
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const environment = yield* assessment(
      subject,
      policy(() =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.zipRight(Deferred.await(release)),
          Effect.as(pass),
        ),
      ),
      batchOptions(),
    );
    const textRule = {
      id: "answer-review",
      applies: "both" as const,
      text: "Require review before showing peer text.",
    };
    yield* environment.firewall.updatePolicy({ add: [textRule] });
    const checking = yield* checkLocalItem(environment, result).pipe(
      Effect.forkScoped,
    );
    yield* Deferred.await(entered);
    yield* environment.firewall.updatePolicy({ remove: [textRule.id] });
    yield* Deferred.succeed(release, undefined);
    const admitted = yield* Fiber.join(checking);
    expect(
      yield* decodeRuntimeValue(InboundItem, admitted.item.canonicalItem),
    ).toMatchObject({
      question: { kind: "withheld", reason: "rule-violation" },
      outcomes: [
        { outcome: { kind: "answered" } },
        { outcome: { kind: "withheld", reason: "rule-violation" } },
      ],
    });
  }));

function batchOptions(): Partial<FirewallOptions> {
  return {
    maximumConcurrentChecks: 1,
    text: {
      provider: {
        decide: (request) =>
          Effect.succeed({
            provider: "test-provider",
            model: "test-model",
            usage: {},
            raw: "",
            answers: Object.fromEntries(
              Object.entries(request.questions).map(([id, entry]) => [
                id,
                batchAnswer(request, entry),
              ]),
            ),
          }),
      },
      settings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 60_000,
        timeoutMilliseconds: 5_000,
      },
    },
  };
}

function batchAnswer(
  request: DecisionRequest,
  question: DecisionRequest["questions"][string],
) {
  const choices = Object.keys(question.criteria);
  const permitting = choices.includes("relevant") ? "relevant" : "none";
  const choice = request.stage === "compliance" ? "violation" : permitting;
  return {
    choice,
    probabilities: Object.fromEntries(
      choices.map((entry) => [entry, entry === choice ? 1 : 0]),
    ),
  };
}

function requestSource(
  subject: Subject,
  operationId: ReturnType<typeof collectiveOperationId>,
) {
  return Effect.gen(function* () {
    const requestedSchema = yield* Schema.decodeUnknown(FormModeSchema)({
      type: "object",
      properties: { slot: { type: "string", enum: ["mon", "tue"] } },
      required: ["slot"],
    });
    return yield* sourceMessage(
      subject,
      subject.remote.card,
      yield* encodeCollectiveContent(
        {
          kind: "operation",
          op: "all_gather",
          operationId,
          deadlineAt: Date.now() + 60_000,
          requestedSchema,
        },
        question,
      ),
    );
  });
}

function answerSource(
  subject: Subject,
  author: VerifiedAgentCard,
  operationId: ReturnType<typeof collectiveOperationId>,
  content: AnswerContent,
) {
  return encodeCollectiveContent({
    kind: "response",
    operationId,
    action: "accept",
    content,
  }).pipe(Effect.flatMap((encoded) => sourceMessage(subject, author, encoded)));
}
