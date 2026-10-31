/**
 * @file A real daemon process with a firewall installed, for the integration
 * traces that run collective operations under one.
 *
 * The daemon runs `Daemon.withFirewall` with one deterministic code policy in
 * the mode `SHARNESS_TEST_FIREWALL_MODE` names, so a trace decides each check
 * by the text it sends: the policy rejects an incoming text that carries
 * `[in]` and an outgoing text that carries `[out]`, asks the agent to
 * respond to an incoming text that carries `[guide]`, and leaves its
 * judgment of a text that carries `[unsure]` unsettled in either direction,
 * beside a settled `none` expectation for an outgoing one.
 * Every other `SHARNESS_*` setting is read as `sharnessd` reads it.
 */

import { NodeRuntime } from "@effect/platform-node";
import {
  Daemon,
  type MessageMiddleware,
  type MiddlewareInput,
  type MiddlewareResult,
} from "@moltzap/social-harness/service";
import { Config, ConfigProvider, Effect, Layer } from "effect";

/** The marker an incoming text carries to be withheld. */
const INCOMING_HOLD = "[in]";
/** The marker an outgoing text carries to be refused. */
const OUTGOING_HOLD = "[out]";
/** The marker an incoming text carries to draw guidance. */
const GUIDE = "[guide]";
/** The marker a text carries for its judgment to be left unsettled. */
const UNSURE = "[unsure]";

const markers: MessageMiddleware = {
  id: "integration-markers",
  revision: "markers-v2",
  rules: [
    {
      id: "marked",
      guidance: "Respond to the marked message.",
      feedback: { nextAction: "revise", text: "Remove the marked text." },
    },
  ],
  check: ({ event }) => {
    const { text } = event.message;
    const held = text.includes(
      event.direction === "incoming" ? INCOMING_HOLD : OUTGOING_HOLD,
    );
    return Effect.succeed({
      findings: [
        text.includes(UNSURE)
          ? {
              ruleId: "marked",
              outcome: "pass",
              reason: "uncertain",
              choice: "none",
              probability: 0.33,
            }
          : { ruleId: "marked", outcome: held ? "violation" : "pass" },
      ],
      expectations: markedExpectations(event),
    });
  },
};

/** A required response to an incoming `[guide]` text, and no response for an outgoing `[unsure]` one. */
function markedExpectations(
  event: MiddlewareInput["event"],
): MiddlewareResult["expectations"] {
  const { text } = event.message;
  if (event.direction === "incoming" && text.includes(GUIDE)) {
    return [
      {
        ruleId: "marked",
        expectation: "required",
        subjects: [event.message.id],
      },
    ];
  }
  return event.direction === "outgoing" && text.includes(UNSURE)
    ? [{ ruleId: "marked", expectation: "none", subjects: [] }]
    : [];
}

const program = Config.literal(
  "guidance",
  "enforcement",
)("SHARNESS_TEST_FIREWALL_MODE").pipe(
  Effect.flatMap((mode) =>
    Layer.launch(
      Daemon.withFirewall({
        mode,
        incoming: [markers],
        outgoing: [markers],
        settings: {
          timeoutMilliseconds: 10_000,
          middlewareTimeoutMilliseconds: 8_000,
          maximumInputBytes: 1_000_000,
          maximumResultBytes: 100_000,
        },
        facts: [],
        codeDigest: "0".repeat(64),
      }),
    ),
  ),
);

program.pipe(
  Effect.withConfigProvider(ConfigProvider.fromEnv()),
  NodeRuntime.runMain,
);
