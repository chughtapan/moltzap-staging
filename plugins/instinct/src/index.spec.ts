/** @file Instinct event handoff preserves failures and private HTTP metadata. */

import {
  FetchHttpClient,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { scopedLive as it } from "@effect/vitest";
import {
  Deferred,
  Effect,
  FastCheck,
  Fiber,
  Redacted,
  Ref,
  Schema,
  Stream,
  Tracer,
} from "effect";
import { describe, expect } from "vitest";
import { localProvider } from "./__tests__/provider.js";
import { deliverToInstinct } from "./index.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- HTTP statuses, provider fields and completion counts are acceptance contracts. */

const sender = {
  apiKey: Redacted.make("test-agentmail-key"),
  inboxId: "sender+fixture@example.test",
  recipient: "instinct@example.test",
};
const eventId = `dlv_${"A".repeat(43)}`;
const emailSchema = Schema.parseJson(
  Schema.Struct({
    to: Schema.Array(Schema.String),
    subject: Schema.String,
    text: Schema.String,
  }),
);

interface ObservedRequest {
  readonly method: string;
  readonly url: string;
  readonly authorization: string;
  readonly body: string;
}

function captureRequest(requested: Deferred.Deferred<ObservedRequest>) {
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    yield* Deferred.succeed(requested, {
      method: request.method,
      url: request.url,
      authorization: request.headers.authorization ?? "",
      body: yield* request.text,
    });
  });
}

function waitsForAcceptance() {
  return Effect.gen(function* () {
    const requested = yield* Deferred.make<ObservedRequest>();
    const accepted = yield* Deferred.make<undefined>();
    const provider = yield* localProvider(
      Effect.gen(function* () {
        yield* captureRequest(requested);
        return HttpServerResponse.stream(
          Stream.make('{"message_id":"accepted').pipe(
            Stream.concat(
              Stream.fromEffect(Deferred.await(accepted).pipe(Effect.as('"}'))),
            ),
            Stream.encodeText,
          ),
        );
      }),
    );
    const completions = yield* Ref.make(0);
    const pending = yield* deliver(provider, completions).pipe(
      Effect.forkScoped,
    );
    const request = yield* Deferred.await(requested);
    const email = yield* Schema.decodeUnknown(emailSchema)(request.body);
    expect(request.method).toBe("POST");
    expect(request.url).toBe(
      "/v0/inboxes/sender%2Bfixture%40example.test/messages/send",
    );
    expect(request.authorization).toBe("Bearer test-agentmail-key");
    expect(provider.destinations).toEqual([
      "https://api.agentmail.to/v0/inboxes/sender%2Bfixture%40example.test/messages/send",
    ]);
    expect(email.to).toEqual([sender.recipient]);
    expect(email.subject).toBe(`Social Harness event ${eventId}`);
    expect(email.text).toContain(
      `read_event with eventId ${JSON.stringify(eventId)}`,
    );
    expect(request.body).not.toContain(Redacted.value(sender.apiKey));
    expect(request.body).toContain("does not authorize a reply");
    expect(yield* Ref.get(completions)).toBe(0);
    yield* Deferred.succeed(accepted, undefined);
    yield* Fiber.join(pending);
    expect(yield* Ref.get(completions)).toBe(1);
  });
}

function refusesStatus(status: number) {
  return Effect.gen(function* () {
    const provider = yield* localProvider(
      Effect.succeed(
        HttpServerResponse.text(Redacted.value(sender.apiKey), { status }),
      ),
    );
    const completions = yield* Ref.make(0);
    const failure = yield* deliver(provider, completions).pipe(Effect.flip);
    expect(failure).toMatchObject({
      _tag: "InstinctWakeError",
      reason: "refused",
    });
    expect(String(failure)).not.toContain(Redacted.value(sender.apiKey));
    expect(yield* Ref.get(completions)).toBe(0);
    expect(provider.destinations).toHaveLength(1);
    expect(provider.signals).toHaveLength(1);
    expect(provider.signals.every((signal) => signal.aborted)).toBe(true);
  });
}

function refusesBody(
  body: HttpServerResponse.HttpServerResponse,
  reason: string,
) {
  return Effect.gen(function* () {
    const provider = yield* localProvider(Effect.succeed(body));
    const completions = yield* Ref.make(0);
    expect(
      yield* deliver(provider, completions).pipe(Effect.flip),
    ).toMatchObject({
      _tag: "InstinctWakeError",
      reason,
    });
    expect(yield* Ref.get(completions)).toBe(0);
    expect(provider.destinations).toHaveLength(1);
  });
}

function refusesRedirects() {
  return Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const provider = yield* localProvider(
      Ref.update(calls, (count) => count + 1).pipe(
        Effect.as(
          HttpServerResponse.empty({
            status: 307,
            headers: { location: "/redirect-target" },
          }),
        ),
      ),
    );
    const completions = yield* Ref.make(0);
    expect(
      yield* deliver(provider, completions).pipe(Effect.flip),
    ).toMatchObject({
      _tag: "InstinctWakeError",
      reason: "transport",
    });
    expect(yield* Ref.get(calls)).toBe(1);
    expect(yield* Ref.get(completions)).toBe(0);
  });
}

function carriesReference(reference: string) {
  return Effect.gen(function* () {
    const requested = yield* Deferred.make<ObservedRequest>();
    const provider = yield* localProvider(
      Effect.gen(function* () {
        yield* captureRequest(requested);
        return HttpServerResponse.text(
          '{"message_id":"accepted","future_field":true}',
        );
      }),
    );
    const completions = yield* Ref.make(0);
    yield* deliver(provider, completions, reference);
    const request = yield* Deferred.await(requested);
    const email = yield* Schema.decodeUnknown(emailSchema)(request.body);
    expect(email.text).toContain(
      `read_event with eventId ${JSON.stringify(reference)}`,
    );
    expect(yield* Ref.get(completions)).toBe(1);
  });
}

function deliver(
  provider: { readonly fetch: typeof globalThis.fetch },
  completions: Ref.Ref<number>,
  reference = eventId,
) {
  return deliverToInstinct(sender, {
    deliveryToken: reference,
  }).pipe(
    Effect.tap(() => Ref.update(completions, (count) => count + 1)),
    Effect.provide(FetchHttpClient.layer),
    Effect.provideService(FetchHttpClient.Fetch, provider.fetch),
  );
}

describe("Instinct email handoff", () => {
  it("posts a reference and waits for provider acceptance", waitsForAcceptance);
  it.each([401, 429, 503])(
    "leaves refused HTTP %i work unread without retrying",
    refusesStatus,
  );
  it.each(["not json", "{}", '{"message_id":""}', '{"thread_id":"unused"}'])(
    "rejects incomplete acceptance %s without accepting the handoff",
    (body) => refusesBody(HttpServerResponse.text(body), "invalid-response"),
  );
  it("refuses redirects before forwarding credentials", refusesRedirects);
  it.prop(
    "preserves each opaque reference through accepted handoff",
    {
      reference: FastCheck.uint8Array({ minLength: 32, maxLength: 32 }).map(
        (bytes) => `dlv_${Buffer.from(bytes).toString("base64url")}`,
      ),
    },
    ({ reference }) => carriesReference(reference),
    { fastCheck: { numRuns: 20 } },
  );
});

describe("Instinct response bounds", () => {
  it("counts streamed bytes without Content-Length", () =>
    refusesBody(
      HttpServerResponse.stream(
        Stream.make('{"message_id":"', "🦦".repeat(5000), '"}').pipe(
          Stream.encodeText,
        ),
      ),
      "response-too-large",
    ));
  it("rejects invalid UTF-8 without accepting the handoff", () =>
    refusesBody(
      HttpServerResponse.uint8Array(
        Buffer.concat([
          Buffer.from('{"message_id":"'),
          Buffer.from([0xc3]),
          Buffer.from('"}'),
        ]),
      ),
      "invalid-response",
    ));
  it("bounds a stalled body and leaves its delivery unread", () =>
    refusesBody(
      HttpServerResponse.stream(
        Stream.make('{"message_id":"').pipe(
          Stream.concat(Stream.never),
          Stream.encodeText,
        ),
      ),
      "timed-out",
    ));
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore repository defaults. */

const tracingSender = {
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
    yield* deliverToInstinct(tracingSender, {
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
    expect(attributes).not.toContain(tracingSender.inboxId);
    expect(attributes).not.toContain(encodeURIComponent(tracingSender.inboxId));
    expect(attributes).not.toContain(privateHeader);
    expect(attributes).not.toContain(Redacted.value(tracingSender.apiKey));
  }));
