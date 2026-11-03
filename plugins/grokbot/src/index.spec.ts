/** @file Grok event handoff preserves failures and private HTTP metadata. */

import {
  FetchHttpClient,
  type HttpApp,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { scopedLive as it } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Either,
  FastCheck,
  Redacted,
  Ref,
  Schema,
  Stream,
  Tracer,
} from "effect";
import { describe, expect } from "vitest";
import { localProvider } from "./__tests__/provider.js";
import { deliverToGrokBot } from "./index.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- HTTP statuses, provider fields and completion counts are acceptance contracts. */

const routine = {
  url: new URL(
    "https://routine.example.test/native/wake?route=fixture%20case&flag&x=~",
  ),
  key: Redacted.make("test-routine-key"),
};
const eventId = `dlv_${"A".repeat(43)}`;
const wakeSchema = Schema.parseJson(
  Schema.Struct({ eventId: Schema.String, instruction: Schema.String }),
);

interface ObservedRequest {
  readonly method: string;
  readonly url: string;
  readonly authorization: string;
  readonly body: string;
}

function captureRequest(observed: Deferred.Deferred<ObservedRequest>) {
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    yield* Deferred.succeed(observed, {
      method: request.method,
      url: request.url,
      authorization: request.headers.authorization ?? "",
      body: yield* request.text,
    });
  });
}

function acceptsStatus(status: number, reference = eventId) {
  return Effect.gen(function* () {
    const observed = yield* Deferred.make<ObservedRequest>();
    const result = yield* submit(
      captureRequest(observed).pipe(
        Effect.as(HttpServerResponse.empty({ status })),
      ),
      routine,
      reference,
    );
    expect(result.outcome).toMatchObject({ _tag: "Right" });
    expect(result.completions).toBe(1);
    expect(result.provider.destinations).toEqual([routine.url.href]);
    expect(result.provider.signals).toHaveLength(1);
    expect(result.provider.signals[0]?.aborted).toBe(true);
    const request = yield* Deferred.await(observed);
    expect(request).toMatchObject({
      method: "POST",
      url: "/native/wake?route=fixture%20case&flag&x=~",
      authorization: `Bearer ${Redacted.value(routine.key)}`,
    });
    const wake = yield* Schema.decodeUnknown(wakeSchema)(request.body, {
      onExcessProperty: "error",
    });
    expect(wake.eventId).toBe(reference);
    expect(wake.instruction).toContain("read_event");
    expect(wake.instruction).toContain("not permission to reply or act");
    expect(request.body).not.toContain(Redacted.value(routine.key));
  });
}

function ignoresResponseBody() {
  return Effect.gen(function* () {
    const response = HttpServerResponse.stream(
      Stream.concat(
        Stream.succeed(new TextEncoder().encode("untrusted response")),
        Stream.never,
      ),
    );
    const result = yield* submit(Effect.succeed(response));
    expect(result.outcome).toMatchObject({ _tag: "Right" });
    expect(result.completions).toBe(1);
    expect(result.provider.signals[0]?.aborted).toBe(true);
  });
}

function refusesStatus(status: number) {
  return Effect.gen(function* () {
    const result = yield* submit(
      Effect.succeed(
        HttpServerResponse.text("private provider response", { status }),
      ),
    );
    expect(result.outcome).toMatchObject({
      _tag: "Left",
      left: { reason: "refused" },
    });
    expect(result.completions).toBe(0);
    expect(result.provider.destinations).toHaveLength(1);
    expect(result.provider.signals[0]?.aborted).toBe(true);
    expect(result.errorText).not.toContain("private provider response");
    expect(result.errorText).not.toContain(Redacted.value(routine.key));
    expect(result.errorText).not.toContain(routine.url.href);
  });
}

function refusesRedirect() {
  return Effect.gen(function* () {
    const result = yield* submit(
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        return request.url === "/other"
          ? HttpServerResponse.empty({ status: 200 })
          : HttpServerResponse.empty({
              status: 302,
              headers: { location: "/other" },
            });
      }),
    );
    expect(result.outcome).toMatchObject({
      _tag: "Left",
      left: { reason: "transport" },
    });
    expect(result.completions).toBe(0);
    expect(result.provider.signals[0]?.aborted).toBe(true);
  });
}

function refusesConfiguration(settings: typeof routine) {
  return Effect.gen(function* () {
    const result = yield* submit(
      Effect.succeed(HttpServerResponse.empty()),
      settings,
    );
    expect(result.outcome).toMatchObject({
      _tag: "Left",
      left: { reason: "invalid-configuration" },
    });
    expect(result.provider.destinations).toEqual([]);
    expect(result.completions).toBe(0);
    expect(result.errorText).not.toContain(settings.url.href);
  });
}

function boundsResponseHeaders() {
  return Effect.gen(function* () {
    const result = yield* submit(Effect.never);
    expect(result.outcome).toMatchObject({
      _tag: "Left",
      left: { reason: "timed-out" },
    });
    expect(result.completions).toBe(0);
    expect(result.provider.destinations).toHaveLength(1);
    expect(result.provider.signals[0]?.aborted).toBe(true);
  });
}

function submit<E>(
  application: HttpApp.Default<E>,
  settings = routine,
  reference = eventId,
) {
  return Effect.gen(function* () {
    const provider = yield* localProvider(application);
    const completions = yield* Ref.make(0);
    const outcome = yield* deliverToGrokBot(settings, {
      deliveryToken: reference,
    }).pipe(
      Effect.tap(() => Ref.update(completions, (count) => count + 1)),
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, provider.fetch),
      Effect.either,
    );
    return {
      outcome,
      completions: yield* Ref.get(completions),
      provider,
      errorText: Either.match(outcome, {
        onLeft: (error) => String(error),
        onRight: () => "",
      }),
    };
  });
}

describe("Grok routine acceptance", () => {
  it.each([200, 202, 204])(
    "returns after HTTP %i acceptance and closes the request",
    (status) => acceptsStatus(status),
  );
  it(
    "uses accepted headers without consuming a stalled response body",
    ignoresResponseBody,
  );
  it.prop(
    "preserves opaque references through HTTP acceptance",
    {
      reference: FastCheck.uint8Array({ minLength: 32, maxLength: 32 }).map(
        (bytes) => `dlv_${Buffer.from(bytes).toString("base64url")}`,
      ),
    },
    ({ reference }) => acceptsStatus(202, reference),
    { fastCheck: { numRuns: 20 } },
  );
});

// @agent-code-guard/regression-only: cases distinguish refusal, redirect and timeout handoff boundaries, each with a different transport trigger.
describe("Grok routine failures", () => {
  it.each([401, 429, 503])(
    "leaves HTTP %i unread without retry or response leakage",
    refusesStatus,
  );
  it("refuses redirects without accepting the handoff", refusesRedirect);
  it(
    "bounds missing response headers and closes the request",
    boundsResponseHeaders,
  );
});

// @agent-code-guard/regression-only: examples exercise each forbidden destination component and the missing credential before any HTTP request.
describe("Grok routine configuration", () => {
  it.each([
    "http://routine.example.test/wake",
    "https://user@routine.example.test/wake",
    "https://:password@routine.example.test/wake",
    "https://routine.example.test/wake#fragment",
  ])("rejects unsafe destination %s before sending", (url) =>
    refusesConfiguration({ ...routine, url: new URL(url) }),
  );
  it("requires a routine credential before sending", () =>
    refusesConfiguration({ ...routine, key: Redacted.make("") }));
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore repository defaults. */

const privateQuery = "private-query";
const tracingRoutine = {
  url: new URL(`https://routine.example.test/private?route=${privateQuery}`),
  key: Redacted.make("synthetic-key"),
};
const privateHeader = "private-provider-header";

function collectTracing() {
  const spans: Tracer.Span[] = [];
  return Tracer.tracerWith((base) =>
    Effect.succeed({
      spans,
      tracer: Tracer.make({
        context: (run, fiber) => base.context(run, fiber),
        span: (...args) => {
          const span = base.span(...args);
          spans.push(span);
          return span;
        },
      }),
    }),
  );
}

function keepsHttpMetadataPrivate() {
  return Effect.gen(function* () {
    const provider = yield* localProvider(
      Effect.succeed(
        HttpServerResponse.empty({
          headers: { "x-private-provider": privateHeader },
        }),
      ),
    );
    const { tracer, spans } = yield* collectTracing();
    yield* deliverToGrokBot(tracingRoutine, {
      deliveryToken: eventId,
    }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, provider.fetch),
      Effect.withTracer(tracer),
    );
    expect(spans).not.toEqual([]);
    const attributes = JSON.stringify(
      spans.map((span) => Array.from(span.attributes)),
    );
    expect(attributes).not.toContain(tracingRoutine.url.hostname);
    expect(attributes).not.toContain(privateQuery);
    expect(attributes).not.toContain(privateHeader);
    expect(attributes).not.toContain(Redacted.value(tracingRoutine.key));
  });
}

describe("Grok handoff boundaries", () => {
  it(
    "keeps private HTTP metadata out of exported spans",
    keepsHttpMetadataPrivate,
  );
});
