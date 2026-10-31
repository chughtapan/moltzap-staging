/**
 * @file A real daemon process with a firewall installed, for the integration
 * traces that run collective operations under one.
 *
 * The daemon runs `Daemon.withFirewall` with a text policy and deterministic provider in
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
  type DecisionProvider,
  type DecisionRequest,
} from "@moltzap/social-harness/service";
import { Config, ConfigProvider, Effect, Layer, Schema } from "effect";

/** The marker an incoming text carries to be withheld. */
const INCOMING_HOLD = "[in]";
/** The marker an outgoing text carries to be refused. */
const OUTGOING_HOLD = "[out]";
/** The marker an incoming text carries to draw guidance. */
const GUIDE = "[guide]";
/** The marker a text carries for its judgment to be left unsettled. */
const UNSURE = "[unsure]";

const stateSchema = Schema.parseJson(
  Schema.Struct({
    direction: Schema.Literal("incoming", "outgoing"),
    candidate: Schema.Struct({ text: Schema.optional(Schema.String) }),
  }),
);

const provider: DecisionProvider = {
  decide: (request) => Effect.succeed(markedResponse(request)),
};

function markedResponse(request: DecisionRequest) {
  const state = Schema.decodeUnknownSync(stateSchema)(request.state);
  const text = state.candidate.text ?? "";
  const guided = state.direction === "incoming" && text.includes(GUIDE);
  const held = text.includes(
    state.direction === "incoming" ? INCOMING_HOLD : OUTGOING_HOLD,
  );
  return {
    provider: "fixture",
    model: "deterministic",
    usage: {},
    raw: "",
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => {
        const marked = question.ruleId === "marked";
        let selected = "none";
        if (question.purpose === "subject") {
          selected = guided && marked ? "relevant" : "irrelevant";
        } else if (request.stage === "compliance") {
          selected = held && marked ? "violation" : "compliant";
        } else if (guided && marked) {
          selected = "required";
        }
        const uncertain =
          marked && request.stage === "compliance" && text.includes(UNSURE);
        const alternatives = Object.keys(question.criteria);
        const selectedProbability = uncertain ? 0.4 : 1;
        const otherProbability = uncertain
          ? 0.6 / (alternatives.length - 1)
          : 0;
        return [
          id,
          {
            choice: selected,
            probabilities: Object.fromEntries(
              alternatives.map((choice) => [
                choice,
                choice === selected ? selectedProbability : otherProbability,
              ]),
            ),
          },
        ];
      }),
    ),
  };
}

const program = Config.literal(
  "guidance",
  "enforcement",
)("SHARNESS_TEST_FIREWALL_MODE").pipe(
  Effect.flatMap((mode) =>
    Layer.launch(
      Daemon.withFirewall({
        mode,
        settings: {
          timeoutMilliseconds: 10_000,
          maximumInputBytes: 1_000_000,
          maximumResultBytes: 100_000,
        },
        facts: [],
        text: {
          provider,
          fixedRules: [
            {
              id: "marked",
              applies: "both",
              text: "Reject incoming messages containing [in] and outgoing messages containing [out]. Respond to incoming messages containing [guide].",
              guidance: "Respond to the marked message.",
              feedback: {
                nextAction: "revise",
                text: "Remove the marked text.",
              },
            },
          ],
          settings: {
            minimumProbability: 0.5,
            maximumRequestBytes: 1_000_000,
            timeoutMilliseconds: 8_000,
          },
        },
      }),
    ),
  ),
);

program.pipe(
  Effect.withConfigProvider(ConfigProvider.fromEnv()),
  NodeRuntime.runMain,
);
