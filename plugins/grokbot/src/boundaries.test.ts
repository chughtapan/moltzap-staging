/** @file Native HTTP metadata stays private in tracing. */
import { FetchHttpClient, HttpServerResponse } from "@effect/platform";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Redacted, Tracer } from "effect";
import { describe, expect } from "vitest";
import { localProvider } from "./__tests__/provider.js";
import { deliverToGrokBot } from "./index.js";

const privateQuery = "private-query";
const routine = {
  url: new URL(`https://routine.example.test/private?route=${privateQuery}`),
  key: Redacted.make("synthetic-key"),
};
const eventId = `dlv_${"A".repeat(43)}`;
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
    yield* deliverToGrokBot(routine, {
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
    expect(attributes).not.toContain(routine.url.hostname);
    expect(attributes).not.toContain(privateQuery);
    expect(attributes).not.toContain(privateHeader);
    expect(attributes).not.toContain(Redacted.value(routine.key));
  });
}

describe("Grok handoff boundaries", () => {
  it(
    "keeps private HTTP metadata out of exported spans",
    keepsHttpMetadataPrivate,
  );
});
