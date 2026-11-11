/** @file A real daemon exports its traces and firewall checks over OTLP/HTTP protobuf. */

import { Duration, Effect } from "effect";
import { expect, it } from "vitest";
import { joinParticipants, nextItem, send } from "../agent-fixture.js";
import { type DaemonOptions, sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { anyString } from "../test-matchers.js";
import {
  acquireOtlpReceiver,
  awaitSpans,
  type ExportedSpan,
  type OtlpReceiver,
} from "./otlp-receiver.js";

/**
 * Each test starts one or two daemons and waits once for spans, for at most
 * the receiver's span-wait limit.
 */
const TELEMETRY_TEST_TIMEOUT_MS = 180_000;

/** The standard batch delay, shortened from 5 s so spans reach the receiver sooner. */
const BATCH_DELAY = Duration.millis(100);

/**
 * How long a daemon without an endpoint must stay silent after the exporting
 * daemon's spans arrive.
 */
const SILENCE_WINDOW = Duration.times(BATCH_DELAY, 20);

/**
 * The standard variables that point a daemon's spans at `receiver` and add
 * `resourceAttributes` to its resource. The protocol is left to its
 * http/protobuf default.
 */
const exportingTo = (receiver: OtlpReceiver, resourceAttributes: string) => ({
  OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint.origin,
  OTEL_RESOURCE_ATTRIBUTES: resourceAttributes,
  OTEL_BSP_SCHEDULE_DELAY: String(Duration.toMillis(BATCH_DELAY)),
});

/**
 * Starts a daemon whose process also reads `environment`, and delivers one
 * message from one of its agents to another.
 */
const deliverOnDaemon = (
  environment: Readonly<Record<string, string>>,
  options: DaemonOptions = {},
) =>
  Effect.gen(function* () {
    const daemon = yield* sharedDaemon(options);
    yield* daemon.start(environment);
    const [sender, recipient] = yield* joinParticipants(daemon, [
      "alice",
      "bob",
    ]);
    if (sender === undefined || recipient === undefined) {
      return yield* Effect.dieMessage("expected two participants");
    }
    yield* send(sender, { to: recipient.address, text: "one traced message" });
    yield* nextItem(recipient);
  });

const exportBehavior = Effect.gen(function* () {
  const receiver = yield* acquireOtlpReceiver;
  const silentReceiver = yield* acquireOtlpReceiver;
  yield* deliverOnDaemon(exportingTo(receiver, "deployment=exporting"));
  /**
   * Every exporter setting but a non-empty `OTEL_EXPORTER_OTLP_ENDPOINT`: an
   * empty endpoint is unset, and a traces-only endpoint opts nothing in.
   */
  yield* deliverOnDaemon({
    OTEL_EXPORTER_OTLP_ENDPOINT: "",
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: new URL(
      "/v1/traces",
      silentReceiver.endpoint,
    ).href,
    OTEL_BSP_SCHEDULE_DELAY: String(Duration.toMillis(BATCH_DELAY)),
  });

  const spans = yield* awaitSpans(receiver, (received) => received.length > 0);
  expect(spans[0]?.resource).toMatchObject({
    "service.name": "sharnessd",
    deployment: "exporting",
  });
  yield* Effect.sleep(SILENCE_WINDOW);
  expect(yield* silentReceiver.connections).toBe(0);
});

/**
 * The daemon runs a message policy, so the sender's firewall checks the
 * message going out and the recipient's checks it coming in. It names its
 * service through `OTEL_RESOURCE_ATTRIBUTES`.
 */
const firewallCheckBehavior = Effect.gen(function* () {
  const receiver = yield* acquireOtlpReceiver;
  yield* deliverOnDaemon(
    exportingTo(receiver, "service.name=otel-firewall-service"),
    { firewall: true },
  );

  const checked = yield* awaitSpans(
    receiver,
    (received) =>
      named(received, "retry.settled").length > 1 &&
      unsettledRetries(received).length === 0,
  );
  expect(checked[0]?.resource).toMatchObject({
    "service.name": "otel-firewall-service",
  });
  const check = { kind: "firewall-check", id: anyString };
  expect(attributesOf(named(checked, "retry.pending"))).toEqual([check, check]);
  expect(attributesOf(named(checked, "retry.settled"))).toEqual([
    { ...check, outcome: "checked" },
    { ...check, outcome: "checked" },
  ]);
});

it(
  "exports traces only from the daemon given an OTLP endpoint",
  processTrace(exportBehavior),
  TELEMETRY_TEST_TIMEOUT_MS,
);

it(
  "exports each firewall check as a pending span and a settled span",
  processTrace(firewallCheckBehavior),
  TELEMETRY_TEST_TIMEOUT_MS,
);

/**
 * Every `retry.pending` span still unsettled. Each settled span settles one
 * pending span with its kind and id.
 */
function unsettledRetries(spans: readonly ExportedSpan[]): ExportedSpan[] {
  const key = (span: ExportedSpan) =>
    [span.attributes.kind, span.attributes.id].join(" ");
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

function named(
  spans: readonly ExportedSpan[],
  name: string,
): readonly ExportedSpan[] {
  return spans.filter((span) => span.name === name);
}

function attributesOf(spans: readonly ExportedSpan[]) {
  return spans.map(({ attributes }) => attributes);
}
