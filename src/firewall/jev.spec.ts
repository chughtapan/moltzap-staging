/** @file Jev response validation and credential handling through its provider module. */

import { FetchHttpClient } from "@effect/platform";
import { effect as it } from "@effect/vitest";
import { Effect, FastCheck as fc, Redacted } from "effect";
import { expect } from "vitest";
import type { DecisionRequest } from "./check.js";
import { makeJevDecisionProvider } from "./jev.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- HTTP status, redaction and closed provider failures are the adapter contract under test. */

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

it.each([
  { key: "", selectedModel: model },
  { key: secret, selectedModel: "" },
  { key: secret, selectedModel: ` ${model} ` },
])("rejects invalid provider configuration", ({ key, selectedModel }) =>
  Effect.gen(function* () {
    const invalid = makeJevDecisionProvider({
      apiKey: Redacted.make(key),
      model: selectedModel,
    });
    const fetch: typeof globalThis.fetch = () =>
      Promise.resolve(new Response(JSON.stringify(wire)));
    const error = yield* invalid
      .decide(request)
      .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch), Effect.flip);
    expect(error.reason).toBe("configuration");
  }),
);

it.prop(
  "redacts a credential an HTTP error echoes before retaining private diagnostics",
  [fc.stringMatching(/^[A-Za-z0-9]{1,64}$/u)],
  ([key]) =>
    Effect.gen(function* () {
      const credential = `key-${key}`;
      const decisionProvider = makeJevDecisionProvider({
        apiKey: Redacted.make(credential),
        model,
      });
      const fetch: typeof globalThis.fetch = () =>
        Promise.resolve(
          new Response(`Rejected Bearer ${credential}`, { status: 401 }),
        );
      const error = yield* decisionProvider
        .decide(request)
        .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch), Effect.flip);
      expect(error.reason).toBe("http");
      expect(error.status).toBe(401);
      expect(error.response).toContain("[REDACTED]");
      expect(JSON.stringify(error)).not.toContain(credential);
    }),
);

it("preserves reported probabilities, model, usage and raw response", () =>
  Effect.gen(function* () {
    const fetch: typeof globalThis.fetch = () =>
      Promise.resolve(new Response(JSON.stringify(wire)));
    const response = yield* provider
      .decide(request)
      .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch));
    expect(response.model).toBe(model);
    expect(response.answers.compliance?.probabilities).toEqual(
      wire.answers.compliance.probabilities,
    );
    expect(JSON.parse(response.raw)).toEqual(wire);
    expect(response.usage).toEqual(wire.usage);
  }));

it.each([200, 401])(
  "redacts JSON-escaped credential echoes at HTTP status %s",
  (status) =>
    Effect.gen(function* () {
      const body = JSON.stringify({ ...wire, model: secret }).replace(
        secret,
        `\\u0075${secret.slice(1)}`,
      );
      const fetch: typeof globalThis.fetch = () =>
        Promise.resolve(new Response(body, { status }));
      const outcome = yield* provider
        .decide(request)
        .pipe(
          Effect.provideService(FetchHttpClient.Fetch, fetch),
          Effect.either,
        );
      expect(JSON.stringify(outcome)).not.toContain(secret);
      expect(JSON.stringify(outcome)).toContain("[REDACTED]");
    }),
);

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
    expect(error.reason).toBe("invalid-response");
  }),
);

it("retains transport failure separately from an HTTP or semantic answer", () =>
  Effect.gen(function* () {
    const fetch: typeof globalThis.fetch = () =>
      Promise.reject(new Error(`Network diagnostic with ${secret}`));
    const error = yield* provider
      .decide(request)
      .pipe(Effect.provideService(FetchHttpClient.Fetch, fetch), Effect.flip);
    expect(error.reason).toBe("transport");
    expect(JSON.stringify(error)).not.toContain(secret);
  }));

it.each([200, 503])(
  "rejects an oversized response body at HTTP status %s without retaining it",
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
      expect(error.reason).toBe("budget");
      expect(error.response).toBeUndefined();
    }),
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore assertion rules outside these HTTP contract tests. */
