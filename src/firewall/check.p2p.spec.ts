/** @file A message check refuses a message to several recipients, or evidence of several participants, that names one twice or leaves its agent out. */

import { effect as it } from "@effect/vitest";
import { Effect, FastCheck as fc } from "effect";
import { expect } from "vitest";
import { checkCapturedMessage, validateTextRules } from "./check.js";

const agentId = "organizer-agent";
const configuration = {
  textRules: [
    { id: "privacy", applies: "both", text: "Keep private records private." },
  ],
  limits: {
    timeoutMilliseconds: 2000,
    maximumInputBytes: 300000,
    maximumResultBytes: 1000000,
  },
  providerSettings: {
    minimumProbability: 0.5,
    maximumRequestBytes: 60000,
    timeoutMilliseconds: 100,
  },
  revision: "a".repeat(64),
} as const;

const provider = { decide: () => Effect.dieMessage("Unexpected decision") };
const evidence = {
  agentId,
  principal: "organizer",
  participants: [],
  history: [],
  facts: [],
  gaps: [],
};
/** A message of `recipients`, to the local agent's conversation when `incoming` and from the local agent otherwise. */
const message = (
  incoming: boolean,
  recipients: readonly [string, ...string[]],
) => ({
  direction: incoming ? ("incoming" as const) : ("outgoing" as const),
  message: {
    id: "message",
    sender: incoming ? "participant-agent" : agentId,
    recipients,
    text: "Can we meet Tuesday?",
  },
});
/** One or two agents that are not the local agent, the first named again where a case repeats one. */
const others = fc.tuple(
  fc.boolean(),
  fc.uuid(),
  fc.array(fc.uuid(), { maxLength: 2 }),
);
/** The failures of the check for the cases `build` makes from sampled agents. */
const failures = (
  build: (
    ...sampled: [boolean, string, string[]]
  ) => Parameters<typeof checkCapturedMessage>[1],
) =>
  Effect.forEach(
    fc.sample(others, 60),
    (sampled) =>
      validateTextRules(configuration).pipe(
        Effect.zipRight(
          checkCapturedMessage(
            { ...configuration, provider },
            build(...sampled),
          ),
        ),
        Effect.flip,
        Effect.map((refused) => refused._tag),
      ),
    { concurrency: 1 },
  ).pipe(Effect.map((tags) => new Set(tags)));

it("refuses a message in either direction that names a recipient twice", () =>
  Effect.gen(function* () {
    const refused = yield* failures((incoming, repeated, rest) => ({
      event: message(
        incoming,
        incoming
          ? [agentId, repeated, ...rest, repeated]
          : [repeated, ...rest, repeated],
      ),
      evidence,
    }));
    expect(refused).toEqual(new Set(["InvalidCheckError"]));
  }));

it("refuses an incoming message to several agents that does not name its agent", () =>
  Effect.gen(function* () {
    const refused = yield* failures((...[, stranger, rest]) => ({
      event: message(true, [stranger, ...rest]),
      evidence,
    }));
    expect(refused).toEqual(new Set(["InvalidCheckError"]));
  }));

it("refuses evidence of several participants that lists one twice", () =>
  Effect.gen(function* () {
    const refused = yield* failures((incoming, repeated, rest) => ({
      event: message(incoming, incoming ? [agentId] : [repeated]),
      evidence: {
        ...evidence,
        participants: [repeated, ...rest, repeated].map((participant) => ({
          agentId: participant,
          principal: "participant",
        })),
      },
    }));
    expect(refused).toEqual(new Set(["InvalidCheckError"]));
  }));
