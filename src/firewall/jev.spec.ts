/** @file Jev response validation and credential handling through its provider module. */

import { FetchHttpClient } from "@effect/platform";
import { effect as it } from "@effect/vitest";
import { Effect, FastCheck as fc, Redacted } from "effect";
import { expect } from "vitest";
import type { DecisionRequest } from "./check.js";
import { makeJevDecisionProvider } from "./jev.js";

const secret = "unit-test-credential";
const model = "jev-1.13.0";
const provider = makeJevDecisionProvider({
  apiKey: Redacted.make(secret),
  model,
});
const request: DecisionRequest = {
  state: '{"candidate":"test"}',
  questions: {
    compliance: {
      ruleId: "privacy",
      instructions: "Does this disclose private data?",
      criteria: {
        compliant: "No disclosure.",
        violation: "Private data disclosed.",
      },
    },
  },
};
const wire = {
  model,
  answers: {
    compliance: {
      type: "choice",
      choice: "compliant",
      probabilities: { compliant: 0.8, violation: 0.2 },
      confidence: 0.43,
    },
  },
  usage: { input_tokens: 123, output_tokens: 12 },
};

it.prop(
  "reports an HTTP error as transient exactly when its status is 429 or a server failure, without the credential its body echoes",
  [
    fc.stringMatching(/^[A-Za-z0-9]{1,64}$/u),
    fc.constantFrom(400, 401, 429, 500, 503),
  ],
  ([key, status]) =>
    Effect.gen(function* () {
      const credential = `key-${key}`;
      const decisionProvider = makeJevDecisionProvider({
        apiKey: Redacted.make(credential),
        model,
      });
      const fetch: typeof globalThis.fetch = () =>
        Promise.resolve(
          new Response(`Rejected Bearer ${credential}`, { status }),
        );
      const error = yield* decisionProvider
        .decide(request)
        .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch), Effect.flip);
      expect(error.transient).toBe(status === 429 || status >= 500);
      expect(JSON.stringify(error)).not.toContain(credential);
    }),
);

it("returns the probabilities the provider reports", () =>
  Effect.gen(function* () {
    const fetch: typeof globalThis.fetch = () =>
      Promise.resolve(new Response(JSON.stringify(wire)));
    const response = yield* provider
      .decide(request)
      .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch));
    expect(response.answers.compliance?.probabilities).toEqual(
      wire.answers.compliance.probabilities,
    );
  }));

it.each([
  { name: "invalid JSON", body: "not-json" },
  {
    name: "untrimmed model",
    body: JSON.stringify({ ...wire, model: ` ${model} ` }),
  },
  {
    name: "untrimmed answer id",
    body: JSON.stringify({
      ...wire,
      answers: { " compliance ": wire.answers.compliance },
    }),
  },
  {
    name: "untrimmed choice",
    body: JSON.stringify({
      ...wire,
      answers: {
        compliance: { ...wire.answers.compliance, choice: " compliant " },
      },
    }),
  },
  {
    name: "untrimmed probability key",
    body: JSON.stringify({
      ...wire,
      answers: {
        compliance: {
          ...wire.answers.compliance,
          probabilities: { " compliant ": 1 },
        },
      },
    }),
  },
  {
    name: "untrimmed usage key",
    body: JSON.stringify({ ...wire, usage: { " input_tokens ": 1 } }),
  },
  {
    name: "missing model",
    body: JSON.stringify({ answers: wire.answers, usage: wire.usage }),
  },
  {
    name: "wrong answer primitive",
    body: JSON.stringify({
      ...wire,
      answers: { compliance: { type: "noul", noul: 1 } },
    }),
  },
  {
    name: "invalid probability",
    body: JSON.stringify({
      ...wire,
      answers: {
        compliance: {
          ...wire.answers.compliance,
          probabilities: { compliant: 2 },
        },
      },
    }),
  },
])("rejects malformed wire responses: $name", ({ body }) =>
  Effect.gen(function* () {
    const fetch: typeof globalThis.fetch = () =>
      Promise.resolve(new Response(body));
    const error = yield* provider
      .decide(request)
      .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch), Effect.flip);
    expect(error.transient).toBe(false);
  }),
);

it("reports a transport failure as transient", () =>
  Effect.gen(function* () {
    const fetch: typeof globalThis.fetch = () =>
      Promise.reject(new Error(`Network diagnostic with ${secret}`));
    const error = yield* provider
      .decide(request)
      .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch), Effect.flip);
    expect(error.transient).toBe(true);
    expect(JSON.stringify(error)).not.toContain(secret);
  }));

it.each([200, 503])(
  "rejects an oversized response body at HTTP status %s",
  (status) =>
    Effect.gen(function* () {
      const stream = new ReadableStream<Uint8Array>({
        pull(controller) {
          controller.enqueue(new Uint8Array(512 * 1024));
        },
      });
      const fetch: typeof globalThis.fetch = () =>
        Promise.resolve(new Response(stream, { status }));
      const error = yield* provider
        .decide(request)
        .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch), Effect.flip);
      expect(error.transient).toBe(false);
    }),
);
