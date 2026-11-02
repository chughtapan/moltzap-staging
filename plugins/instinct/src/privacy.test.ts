/** @file A deployment sender's HTTP metadata stays outside exported tracing spans. */
import { FetchHttpClient, HttpServerResponse } from "@effect/platform";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Redacted, Tracer } from "effect";
import { expect } from "vitest";
import { localProvider } from "./__tests__/provider.js";
import { deliverToInstinct } from "./index.js";

const sender = {
  apiKey: Redacted.make("synthetic-sender-key"),
  inboxId: "private-sender@example.test",
  recipient: "instinct@example.test",
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

it("keeps the sender URL and provider headers out of exported spans", () =>
  Effect.gen(function* () {
    const provider = yield* localProvider(
      Effect.succeed(
        HttpServerResponse.text('{"message_id":"synthetic-accepted"}', {
          headers: { "x-private-provider": privateHeader },
        }),
      ),
    );
    const { tracer, spans } = yield* collectTracing();
    yield* deliverToInstinct(sender, {
      deliveryToken: `dlv_${"A".repeat(43)}`,
    }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, provider.fetch),
      Effect.withTracer(tracer),
    );
    expect(spans).not.toEqual([]);
    const attributes = JSON.stringify(
      spans.map((span) => Array.from(span.attributes)),
    );
    expect(attributes).not.toContain(sender.inboxId);
    expect(attributes).not.toContain(encodeURIComponent(sender.inboxId));
    expect(attributes).not.toContain(privateHeader);
    expect(attributes).not.toContain(Redacted.value(sender.apiKey));
  }));
