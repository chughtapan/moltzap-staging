/** @file A message check refuses a message that names one of its several recipients twice. */

import { effect as it } from "@effect/vitest";
import { Effect, FastCheck as fc } from "effect";
import { expect } from "vitest";
import { checkCapturedMessage, validateChecks } from "./check.js";

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

it("refuses a message in either direction that names a recipient twice", () =>
  Effect.forEach(
    fc.sample(
      fc.tuple(fc.boolean(), fc.uuid(), fc.array(fc.uuid(), { maxLength: 2 })),
      60,
    ),
    ([incoming, repeated, others]) =>
      Effect.gen(function* () {
        yield* validateChecks(configuration);
        const refused = yield* checkCapturedMessage(
          {
            ...configuration,
            provider: {
              decide: () => Effect.dieMessage("Unexpected decision"),
            },
          },
          {
            event: {
              direction: incoming ? "incoming" : "outgoing",
              message: {
                id: "message",
                sender: incoming ? "participant-agent" : agentId,
                recipients: incoming
                  ? [agentId, repeated, ...others, repeated]
                  : [repeated, ...others, repeated],
                text: "Can we meet Tuesday?",
              },
            },
            evidence: {
              agentId,
              principal: "organizer",
              participants: [],
              history: [],
              facts: [],
              gaps: [],
            },
          },
        ).pipe(Effect.flip);
        expect(refused.reason).toBe("message");
      }),
    { concurrency: 1, discard: true },
  ));
