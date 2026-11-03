/** @file Real daemons export traces and retry outcomes over OTLP/HTTP protobuf. */

import { replyAddress } from "@moltzap/social-harness";
import { Duration, Effect, Queue } from "effect";
import { expect, it } from "vitest";
import {
  groupAddress,
  nextItem,
  type Participant,
  send,
  startParticipants,
} from "../agent-fixture.js";
import {
  acquireRouterHoldProxy,
  type RouterHoldProxy,
} from "../communication/router-hold-proxy.js";
import {
  acquireProcessInfrastructure,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  type ProcessInfrastructure,
} from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { anyNumber, anyString } from "../test-matchers.js";
import {
  acquireOtlpReceiver,
  awaitSpans,
  type ExportedSpan,
  type OtlpReceiver,
} from "./otlp-receiver.js";

const question = "Which day works for the review?";

/**
 * Each test starts PGlite, the Registry, the Router and two or three daemons.
 * The marker test also waits for a proposal retry and Router recovery.
 * Each trace wait has the receiver's span-wait limit.
 */
const TELEMETRY_TEST_TIMEOUT_MS = 300_000;

/** An operation's deadline when only its answers should close it: the test's own limit. */
const ANSWERED_DEADLINE_SECONDS = TELEMETRY_TEST_TIMEOUT_MS / 1000;

const slotSchema = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
} as const;

const gatherFrom = (
  requester: Participant,
  to: string,
  op: "gather" | "all_gather",
) =>
  send(requester, {
    to,
    text: question,
    collective: {
      op,
      deadline: ANSWERED_DEADLINE_SECONDS,
      requestedSchema: slotSchema,
    },
  });

interface MarkerScenario {
  readonly infrastructure: ProcessInfrastructure;
  readonly receiver: OtlpReceiver;
  readonly proxy: RouterHoldProxy;
  readonly requester: Participant;
  readonly member: Participant;
  readonly held: Participant;
  readonly group: string;
}

const acquireMarkerParticipants = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const receiver = yield* acquireOtlpReceiver;
  const proxy = yield* acquireRouterHoldProxy(infrastructure.routerOrigin);
  const fixtures = [
    withEnvironment(
      exportingTo(
        yield* makeDaemonProcessFixture(
          infrastructure,
          "otel-requester",
          "guidance",
        ),
        receiver,
      ),
      {
        OTEL_RESOURCE_ATTRIBUTES:
          "service.name=otel-requester-service,sharness.agent=otel-requester",
      },
    ),
    yield* makeDaemonProcessFixture(infrastructure, "otel-member-a"),
    withEnvironment(
      yield* makeDaemonProcessFixture(infrastructure, "otel-member-b"),
      { SHARNESS_ROUTER_ORIGIN: proxy.origin.origin },
    ),
  ];
  const [requester, member, held] = yield* startParticipants(fixtures);
  if (requester === undefined || member === undefined || held === undefined) {
    return yield* Effect.dieMessage("expected three participants");
  }
  const group = groupAddress([requester, member, held]);
  return { infrastructure, receiver, proxy, requester, member, held, group };
});

/** The standard batch delay, shortened from 5 s so spans reach the receiver sooner. */
const BATCH_DELAY = Duration.millis(100);

/**
 * How long a daemon without an endpoint must stay silent after the exporting
 * daemon's spans arrive.
 */
const SILENCE_WINDOW = Duration.times(BATCH_DELAY, 20);

/** An OTLP endpoint enables export only for the daemon configured to use it. */
const exportBehavior = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const receiver = yield* acquireOtlpReceiver;
  const silentReceiver = yield* acquireOtlpReceiver;
  const exporting = exportingTo(
    yield* makeDaemonProcessFixture(infrastructure, "otel-exporting"),
    receiver,
  );
  /**
   * Every exporter setting but a non-empty `OTEL_EXPORTER_OTLP_ENDPOINT`: an
   * empty endpoint is unset, and a traces-only endpoint opts nothing in.
   */
  const silent = withEnvironment(
    yield* makeDaemonProcessFixture(infrastructure, "otel-silent"),
    {
      OTEL_EXPORTER_OTLP_ENDPOINT: "",
      OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: new URL(
        "/v1/traces",
        silentReceiver.endpoint,
      ).href,
      OTEL_BSP_SCHEDULE_DELAY: String(Duration.toMillis(BATCH_DELAY)),
    },
  );
  const [sender, target] = yield* startParticipants([exporting, silent]);
  if (sender === undefined || target === undefined) {
    return yield* Effect.dieMessage("expected two participants");
  }

  yield* send(sender, { to: target.address, text: "sent by the exporter" });
  yield* nextItem(target);
  yield* send(target, { to: sender.address, text: "received by the exporter" });
  yield* nextItem(sender);

  const spans = yield* awaitSpans(receiver, (received) => received.length > 0);
  expect(spans[0]?.resource).toMatchObject({
    "service.name": "sharnessd",
    "sharness.agent": "otel-exporting",
  });
  yield* Effect.sleep(SILENCE_WINDOW);
  expect(yield* silentReceiver.connections).toBe(0);
}).pipe(Effect.scoped);

/**
 * The requester exports and installs a firewall that admits its messages.
 * It names its service through `OTEL_RESOURCE_ATTRIBUTES`. The
 * third daemon reaches the Router through a hold proxy. In a three-member
 * group every message needs every member, so holding that member keeps the
 * requester's proposals uncertified. A restarted Router is a new instance, so
 * afterwards every conversation catches up.
 */
const markerBehavior = Effect.gen(function* () {
  const scenario = yield* acquireMarkerParticipants;
  yield* completeGather(scenario);
  yield* gatherFrom(scenario.requester, scenario.group, "all_gather");
  yield* answerAllGather(scenario);
  /** Every earlier proposal settles too, so the held send's is the one left pending. */
  const completed = yield* awaitSpans(
    scenario.receiver,
    (received) =>
      markers(received, "retry.settled", "kind", "firewall-check").length > 0 &&
      markers(received, "retry.settled", "kind", "outbound").length > 0 &&
      unsettledRetries(received).length === 0,
  );
  expectCompletedRetries(completed);
  yield* scenario.proxy.hold;
  yield* Effect.addFinalizer(() => scenario.proxy.release);
  const accepted = yield* gatherFrom(
    scenario.requester,
    scenario.group,
    "all_gather",
  );
  const pendingId = yield* expectProposalPending(scenario.receiver);
  expect(
    markers(yield* scenario.receiver.spans, "retry.settled", "id", pendingId),
  ).toEqual([]);
  yield* scenario.proxy.release;
  const request = yield* answerAllGather(scenario);
  expect(accepted).toMatchObject({
    id: request.message.operation.operationId,
    address: replyAddress(request.message),
  });
  yield* expectProposalCertified(scenario.receiver, pendingId);
  yield* restartRouterAndExpectRecovery(scenario);
}).pipe(Effect.scoped);

it(
  "exports traces only from the daemon given an OTLP endpoint",
  processTrace(exportBehavior),
  TELEMETRY_TEST_TIMEOUT_MS,
);

it(
  "exports firewall checks, outbox drains, proposal retries and Router recovery",
  processTrace(markerBehavior),
  TELEMETRY_TEST_TIMEOUT_MS,
);

function completeGather({ requester, member, held, group }: MarkerScenario) {
  return Effect.gen(function* () {
    yield* gatherFrom(requester, group, "gather");
    const gatherRequest = yield* nextItem(member);
    if (gatherRequest.kind !== "collectiveRequest") {
      return yield* Effect.dieMessage("expected a gather request");
    }
    yield* nextItem(held);
    yield* send(member, {
      to: requester.address,
      collectiveResponse: {
        action: "accept",
        operationId: gatherRequest.message.operation.operationId,
        content: { slot: "mon" },
      },
    });
    yield* send(held, {
      to: requester.address,
      collectiveResponse: {
        action: "decline",
        operationId: gatherRequest.message.operation.operationId,
      },
    });
    expect(yield* nextItem(requester)).toMatchObject({
      kind: "collectiveResult",
      op: "gather",
      operationId: gatherRequest.message.operation.operationId,
      closedBy: "settled",
      outcomes: [
        {
          member: member.agent,
          outcome: { kind: "answered", content: { slot: "mon" } },
        },
        { member: held.agent, outcome: { kind: "declined" } },
      ],
    });
  });
}

function answerAllGather({ requester, member, held, group }: MarkerScenario) {
  return Effect.gen(function* () {
    const allGatherRequest = yield* nextItem(member);
    if (allGatherRequest.kind !== "collectiveRequest") {
      return yield* Effect.dieMessage("expected an all_gather request");
    }
    yield* nextItem(held);
    yield* send(member, {
      to: group,
      collectiveResponse: {
        operationId: allGatherRequest.message.operation.operationId,
        action: "accept",
        content: { slot: "tue" },
      },
    });
    yield* send(held, {
      to: group,
      collectiveResponse: {
        operationId: allGatherRequest.message.operation.operationId,
        action: "decline",
      },
    });
    const delivery = yield* Queue.take(requester.inbox);
    expect(delivery.address).toBe(replyAddress(allGatherRequest.message));
    const result = delivery.item;
    expect(result).toEqual({
      kind: "collectiveResult",
      op: "all_gather",
      operationId: allGatherRequest.message.operation.operationId,
      closedAt: anyNumber,
      closedBy: "settled",
      question,
      outcomes: [
        {
          member: member.agent,
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
        { member: held.agent, outcome: { kind: "declined" } },
      ],
    });
    expect(yield* nextItem(member)).toEqual(result);
    expect(yield* nextItem(held)).toEqual(result);
    return allGatherRequest;
  });
}

function expectCompletedRetries(completed: readonly ExportedSpan[]): void {
  expect(completed[0]?.resource).toMatchObject({
    "service.name": "otel-requester-service",
    "sharness.agent": "otel-requester",
  });
  expect(
    attributesOf(markers(completed, "retry.settled", "kind", "firewall-check")),
  ).toContainEqual({
    kind: "firewall-check",
    id: anyString,
    outcome: "checked",
  });
  expect(
    attributesOf(markers(completed, "retry.settled", "kind", "outbound")),
  ).toContainEqual({ kind: "outbound", id: anyString, outcome: "sent" });
}

function unsettledProposals(received: readonly ExportedSpan[]) {
  return unsettledRetries(received).filter(
    (span) => span.attributes.kind === "proposal",
  );
}

function expectProposalPending(receiver: OtlpReceiver) {
  return Effect.gen(function* () {
    const [pending] = unsettledProposals(
      yield* awaitSpans(
        receiver,
        (received) => unsettledProposals(received).length > 0,
      ),
    );
    expect(pending?.attributes).toEqual({ kind: "proposal", id: anyString });
    return String(pending?.attributes.id);
  });
}

function expectProposalCertified(receiver: OtlpReceiver, pendingId: string) {
  return Effect.gen(function* () {
    const settled = yield* awaitSpans(
      receiver,
      (received) =>
        markers(received, "retry.settled", "id", pendingId).length > 0,
    );
    expect(
      attributesOf(markers(settled, "retry.settled", "id", pendingId)),
    ).toEqual([{ kind: "proposal", id: pendingId, outcome: "certified" }]);
  });
}

function restartRouterAndExpectRecovery({
  infrastructure,
  receiver,
}: MarkerScenario) {
  return Effect.gen(function* () {
    yield* infrastructure.restartRouter;
    const caughtUp = yield* awaitSpans(
      receiver,
      (received) =>
        markers(received, "retry.pending", "kind", "catch-up").length > 0 &&
        unsettledRetries(received).length === 0,
    );
    expect(
      attributesOf(markers(caughtUp, "retry.settled", "kind", "catch-up")),
    ).toContainEqual({
      kind: "catch-up",
      id: anyString,
      outcome: "recovered",
    });
    expect(
      attributesOf(markers(caughtUp, "retry.settled", "kind", "reanchor")),
    ).toContainEqual({
      kind: "reanchor",
      id: anyString,
      outcome: "recovered",
    });
  });
}

/**
 * The standard variables that point one daemon's spans at `receiver`. The
 * protocol is left to its http/protobuf default.
 */
function exportingTo(
  fixture: DaemonProcessFixture,
  receiver: OtlpReceiver,
): DaemonProcessFixture {
  return withEnvironment(fixture, {
    OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint.origin,
    OTEL_RESOURCE_ATTRIBUTES: `sharness.agent=${fixture.agentName}`,
    OTEL_BSP_SCHEDULE_DELAY: String(Duration.toMillis(BATCH_DELAY)),
  });
}

function withEnvironment(
  fixture: DaemonProcessFixture,
  environment: Readonly<Record<string, string>>,
): DaemonProcessFixture {
  return {
    ...fixture,
    environment: { ...fixture.environment, ...environment },
  };
}

/**
 * Every `retry.pending` span still unsettled. Each settled span settles one
 * pending span with its kind and id, from the same daemon: a re-proposed message
 * starts a second schedule under the same id, and every daemon's outbox
 * drains under one id.
 */
function unsettledRetries(spans: readonly ExportedSpan[]): ExportedSpan[] {
  const key = (span: ExportedSpan) =>
    [
      span.resource["sharness.agent"],
      span.attributes.kind,
      span.attributes.id,
    ].join(" ");
  const settled = named(spans, "retry.settled").map(key);
  return named(spans, "retry.pending").filter((span) => {
    const match = settled.indexOf(key(span));
    if (match === -1) {
      return true;
    }
    settled.splice(match, 1);
    return false;
  });
}

/** Every marker span `name` that carries `attributes.key === value`. */
function markers(
  spans: readonly ExportedSpan[],
  name: string,
  key: string,
  value: string,
): readonly ExportedSpan[] {
  return named(spans, name).filter((span) => span.attributes[key] === value);
}

function named(
  spans: readonly ExportedSpan[],
  name: string,
): readonly ExportedSpan[] {
  return spans.filter((span) => span.name === name);
}

function attributesOf(spans: readonly ExportedSpan[]) {
  return spans.map(({ attributes }) => attributes);
}
