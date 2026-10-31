/** @file Fixed rule fixtures and retained decisions support host policy behavior tests. */

import { SqlClient } from "@effect/sql";
import { Effect, Schedule, Schema } from "effect";
import type {
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
} from "../firewall/check.js";
import type { FirewallOptions } from "../firewall/index.js";
import type { Fixture } from "./conversation-fixtures.js";
import { readDecisions } from "../firewall/store/index.js";

/** One rule supplies attribution for findings and registered advice. */
export const rule = {
  id: "communication",
  applies: "both",
  text: "Coordinate scheduling with the other participants.",
  guidance: "Answer the open scheduling question.",
  feedback: {
    nextAction: "revise",
    text: "Include all required participants.",
  },
} as const;

/** Bounds allow real SQL and live scheduling while keeping an unresponsive check finite. */
const settings = {
  timeoutMilliseconds: 10_000,
  maximumInputBytes: 1_000_000,
  maximumResultBytes: 100_000,
};

/** Tests install a text provider and fixed rule through the daemon owner's options. */
export function firewallOptions(
  provider: DecisionProvider,
  overrides: Partial<FirewallOptions> = {},
): FirewallOptions {
  return {
    settings,
    facts: [],
    mode: "enforcement",
    text: {
      provider,
      fixedRules: [rule],
      settings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 1_000_000,
        timeoutMilliseconds: 8_000,
      },
    },
    ...overrides,
  };
}

/** A deterministic model accounts for every question in the provider's actual request. */
export function decisionResponse(
  request: DecisionRequest,
  compliance: "compliant" | "violation" = "compliant",
  expectation: "required" | "none" = "none",
): DecisionResponse {
  return {
    provider: "fixture",
    model: "deterministic",
    usage: {},
    raw: "",
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => {
        let selected: string =
          request.stage === "compliance" ? compliance : expectation;
        if (question.purpose === "subject") {
          selected = expectation === "required" ? "relevant" : "irrelevant";
        }
        return [
          id,
          {
            choice: selected,
            probabilities: Object.fromEntries(
              Object.keys(question.criteria).map((choice) => [
                choice,
                choice === selected ? 1 : 0,
              ]),
            ),
          },
        ];
      }),
    ),
  };
}

const providerState = Schema.parseJson(
  Schema.Struct({
    direction: Schema.Literal("incoming", "outgoing"),
    candidate: Schema.Struct({
      text: Schema.optional(Schema.String),
      sender: Schema.optional(Schema.String),
      operation: Schema.optional(Schema.Struct({ kind: Schema.String })),
    }),
    history: Schema.Array(Schema.Struct({ text: Schema.String })),
    earlierHistoryOmitted: Schema.optional(Schema.Boolean),
  }),
);

/** The model's state exposes candidate attribution and the bounded history selected by the daemon. */
export const decisionState = Schema.decodeUnknownSync(providerState);

/** An observed decision means its host publication transaction has committed too. */
export function waitForChecks(subject: Pick<Fixture, "sql">, count = 1) {
  return readDecisions({}).pipe(
    Effect.map(
      ({ decisions }) =>
        new Set(decisions.map((decision) => decision.checkId)).size,
    ),
    Effect.repeat({
      until: (checks) => checks >= count,
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.asVoid,
    Effect.timeout("10 seconds"),
    Effect.provideService(SqlClient.SqlClient, subject.sql),
  );
}
