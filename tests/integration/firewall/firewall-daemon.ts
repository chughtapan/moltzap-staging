/**
 * @file A real daemon process with a firewall installed, for the integration
 * traces that run collective operations under one.
 *
 * The fixture composes domain services with a text policy and deterministic
 * provider, so a trace decides each check by the text it sends: the policy
 * rejects an incoming text that carries `[in]` and an outgoing text that
 * carries `[out]`, and is unsure about a text that carries `[unsure]` in
 * either direction, which refuses it outgoing and admits it incoming with a
 * reminder. Every other `SHARNESS_*` setting is read as `sharnessd` reads it.
 */

import type {
  DecisionProvider,
  DecisionRequest,
} from "@moltzap/social-harness/service";
import { NodeRuntime } from "@effect/platform-node";
import { ConfigProvider, Effect, Schema } from "effect";
import { runPolicyRuntime } from "./policy-runtime.js";

/** The marker an incoming text carries to be withheld. */
const INCOMING_HOLD = "[in]";
/** The marker an outgoing text carries to be refused. */
const OUTGOING_HOLD = "[out]";
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
        const selected = held && marked ? "violation" : "compliant";
        const probability = marked && text.includes(UNSURE) ? 0.6 : 1;
        return [
          id,
          {
            choice: selected,
            probabilities: Object.fromEntries(
              Object.keys(question.criteria).map((choice) => [
                choice,
                choice === selected ? probability : 1 - probability,
              ]),
            ),
          },
        ];
      }),
    ),
  };
}

runPolicyRuntime({
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
        text: "Reject incoming messages containing [in] and outgoing messages containing [out].",
        guidance: "The marked message may break the rule.",
        feedback: { text: "Remove the marked text." },
      },
    ],
    settings: {
      minimumProbability: 0.8,
      maximumRequestBytes: 1_000_000,
      timeoutMilliseconds: 8_000,
    },
  },
}).pipe(
  Effect.scoped,
  Effect.withConfigProvider(ConfigProvider.fromEnv()),
  NodeRuntime.runMain,
);
