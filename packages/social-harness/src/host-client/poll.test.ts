/** @file Native polling acquires, renews and relinquishes the real admitted inbox. */

import type { McpHttpHandler } from "@modelcontextprotocol/server";
import { scopedLive as it, scoped as itOnTestClock } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Fiber,
  Queue,
  Redacted,
  Schema,
  Stream,
  TestClock,
} from "effect";
import { describe, expect } from "vitest";
import type { InboundDelivery } from "./capability.js";
import { makeToolCallRequest } from "../__tests__/mcp-http-fixtures.js";
import { localGuidance } from "../__tests__/message-policy-fixtures.js";
import {
  nativeCredential,
  nativePollFixture,
} from "../__tests__/native-poll-fixtures.js";
import { enqueue, item, token } from "../__tests__/webhook-fixtures.js";
import { encodeRuntimeValue } from "../communication/store/index.js";
import { HARNESS_CONSUMER_META_KEY } from "../host-protocol/names.js";
import { acquireHarnessEndpoint } from "./index.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Counts, canonical payloads and closed lifecycle failures are observable receive contracts. */

const options = { credential: nativeCredential };
const releaseRequest = Schema.is(
  Schema.Struct({
    method: Schema.Literal("events/poll"),
    params: Schema.Struct({
      _meta: Schema.Struct({
        [HARNESS_CONSUMER_META_KEY]: Schema.Struct({
          release: Schema.Literal(true),
        }),
      }),
    }),
  }),
);

/** A lost release affects cleanup only; acquisition, renewal, reads and receipts stay real. */
const loseRelease = (handler: McpHttpHandler): McpHttpHandler => ({
  ...handler,
  fetch: (request, requestOptions) =>
    Effect.tryPromise(() => request.clone().json()).pipe(
      Effect.flatMap((body) =>
        releaseRequest(body)
          ? Effect.succeed(new Response(null, { status: 503 }))
          : Effect.tryPromise(() => handler.fetch(request, requestOptions)),
      ),
      Effect.runPromise,
    ),
});

const recoverCanonicalContent = () =>
  Effect.gen(function* () {
    const fixture = yield* nativePollFixture();
    const payload = item("🦦".repeat(80_000));
    const guidance = localGuidance(1);
    yield* fixture.store.putInboxItem({
      deliveryToken: token(1),
      canonicalItem: yield* encodeRuntimeValue(payload),
      canonicalGuidance: yield* encodeRuntimeValue(guidance),
    });
    const endpoint = yield* acquireHarnessEndpoint(fixture.address, options);
    const received = yield* Deferred.make<InboundDelivery>();
    const listening = yield* endpoint.messages.pipe(
      Stream.tap((delivery) => Deferred.succeed(received, delivery)),
      Stream.runDrain,
      Effect.forkScoped,
    );
    const delivery = yield* Deferred.await(received);
    expect(delivery.item).toEqual(payload);
    expect(delivery.guidance).toEqual(guidance);
    expect((yield* fixture.store.readInboxSummary()).pendingCount).toBe(1);
    yield* delivery.acknowledge;
    expect((yield* fixture.store.readInboxSummary()).pendingCount).toBe(0);
    yield* Fiber.interrupt(listening);
    const request = makeToolCallRequest({
      id: "retained-native-work",
      name: "read_event",
      toolArguments: { eventId: delivery.eventId },
    });
    request.headers.set(
      "authorization",
      `Bearer ${Redacted.value(nativeCredential)}`,
    );
    const retained = yield* Effect.tryPromise(() =>
      fixture.handler.fetch(request),
    ).pipe(
      Effect.flatMap((response) => Effect.tryPromise(() => response.json())),
    );
    expect(retained).toMatchObject({
      result: { structuredContent: { item: payload, guidance } },
    });
  });

const signalIdleReadiness = () =>
  Effect.gen(function* () {
    const fixture = yield* nativePollFixture();
    const ready = yield* Deferred.make<undefined>();
    const endpoint = yield* acquireHarnessEndpoint(fixture.address, {
      ...options,
      onReady: Deferred.succeed(ready, undefined).pipe(Effect.asVoid),
    });
    const listening = yield* endpoint.messages.pipe(
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* Deferred.await(ready);
    const other = yield* acquireHarnessEndpoint(fixture.address, options);
    expect(
      (yield* other.messages.pipe(Stream.runDrain, Effect.flip)).reason,
    ).toBe("already-listening");
    yield* Fiber.interrupt(listening);
    yield* enqueue(fixture.store, 2);
    const recovered = yield* other.messages.pipe(
      Stream.take(1),
      Stream.runCollect,
    );
    expect([...recovered].map((delivery) => delivery.item)).toEqual([item()]);
    expect((yield* fixture.store.readInboxSummary()).pendingCount).toBe(1);
  });

const receivesLaterArrival = () =>
  Effect.gen(function* () {
    const fixture = yield* nativePollFixture();
    yield* enqueue(fixture.store, 1, "initial work");
    const endpoint = yield* acquireHarnessEndpoint(fixture.address, options);
    const received = yield* Queue.unbounded<InboundDelivery>();
    const listening = yield* endpoint.messages.pipe(
      Stream.runForEach((delivery) =>
        delivery.acknowledge.pipe(
          Effect.andThen(Queue.offer(received, delivery)),
        ),
      ),
      Effect.forkScoped,
    );
    expect((yield* Queue.take(received)).item).toEqual(item("initial work"));
    yield* enqueue(fixture.store, 2, "later work");
    expect((yield* Queue.take(received)).item).toEqual(item("later work"));
    expect((yield* fixture.store.readInboxSummary()).pendingCount).toBe(0);
    yield* Fiber.interrupt(listening);
  });

const fenceAfterFailedCleanup = () =>
  Effect.gen(function* () {
    const fixture = yield* nativePollFixture(loseRelease);
    yield* enqueue(fixture.store, 1);
    const endpoint = yield* acquireHarnessEndpoint(fixture.address, options);
    const received = yield* Deferred.make<InboundDelivery>();
    yield* endpoint.messages.pipe(
      Stream.tap((delivery) => Deferred.succeed(received, delivery)),
      Stream.take(1),
      Stream.runDrain,
    );
    yield* acquireHarnessEndpoint(fixture.address, options);
    const delivery = yield* Deferred.await(received);
    expect((yield* delivery.acknowledge.pipe(Effect.flip)).reason).toBe(
      "consumer-revoked",
    );
    expect((yield* fixture.store.readInboxSummary()).pendingCount).toBe(1);
  });

const cancelBootstrap = () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const fixture = yield* nativePollFixture(
      undefined,
      Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    );
    const endpoint = yield* acquireHarnessEndpoint(fixture.address, options);
    const listening = yield* endpoint.messages.pipe(
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(listening).pipe(Effect.timeout("2 seconds"));
  });

const renewDuringSlowAcceptance = () =>
  Effect.gen(function* () {
    const fixture = yield* nativePollFixture();
    yield* enqueue(fixture.store, 1);
    const endpoint = yield* acquireHarnessEndpoint(fixture.address, options);
    const received = yield* Deferred.make<InboundDelivery>();
    const listening = yield* endpoint.messages.pipe(
      Stream.tap((delivery) =>
        Deferred.succeed(received, delivery).pipe(Effect.andThen(Effect.never)),
      ),
      Stream.runDrain,
      Effect.forkScoped,
    );
    const delivery = yield* Deferred.await(received);
    yield* TestClock.adjust("61 seconds");
    yield* delivery.acknowledge;
    expect((yield* fixture.store.readInboxSummary()).pendingCount).toBe(0);
    yield* Fiber.interrupt(listening);
  });

const refuseRedirects = () =>
  Effect.gen(function* () {
    let followed = false;
    const fixture = yield* nativePollFixture((handler) => ({
      ...handler,
      fetch: (request, requestOptions) => {
        const target = new URL(request.url);
        if (target.searchParams.has("redirected")) {
          followed = true;
          return handler.fetch(request, requestOptions);
        }
        target.searchParams.set("redirected", "true");
        return Effect.runPromise(
          Effect.succeed(Response.redirect(target, 307)),
        );
      },
    }));
    const failure = yield* acquireHarnessEndpoint(
      fixture.address,
      options,
    ).pipe(Effect.flip);
    expect(failure.reason).toBe("transport-failed");
    expect(followed).toBe(false);
  });

const releaseAfterReadinessFailure = () =>
  Effect.gen(function* () {
    const fixture = yield* nativePollFixture();
    const endpoint = yield* acquireHarnessEndpoint(fixture.address, {
      ...options,
      onReady: Effect.dieMessage("fixture readiness failed"),
    });
    const result = yield* endpoint.messages.pipe(Stream.runDrain, Effect.exit);
    expect(result._tag).toBe("Failure");
  });

const forwardRequest = (
  handler: McpHttpHandler,
  ...args: Parameters<McpHttpHandler["fetch"]>
) => Effect.tryPromise(() => handler.fetch(...args));

/** Cleanup can lose its response while ordinary MCP requests still succeed. */
const stallRelease =
  (
    entered: Deferred.Deferred<undefined>,
    finish: Deferred.Deferred<undefined>,
  ) =>
  (handler: McpHttpHandler): McpHttpHandler => ({
    ...handler,
    fetch: (request, requestOptions) =>
      Effect.tryPromise(() => request.clone().json()).pipe(
        Effect.flatMap((body) =>
          releaseRequest(body)
            ? Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(finish)),
                Effect.as(new Response(null, { status: 503 })),
              )
            : forwardRequest(handler, request, requestOptions),
        ),
        Effect.runPromise,
      ),
  });

const boundCleanup = () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const finish = yield* Deferred.make<undefined>();
    yield* Effect.addFinalizer(() => Deferred.succeed(finish, undefined));
    const fixture = yield* nativePollFixture(stallRelease(entered, finish));
    yield* enqueue(fixture.store, 1);
    const endpoint = yield* acquireHarnessEndpoint(fixture.address, options);
    const received = yield* Deferred.make<InboundDelivery>();
    const listening = yield* endpoint.messages.pipe(
      Stream.tap((delivery) => Deferred.succeed(received, delivery)),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* Deferred.await(entered);
    const delivery = yield* Deferred.await(received);
    expect((yield* delivery.acknowledge.pipe(Effect.flip)).reason).toBe(
      "consumer-revoked",
    );
    yield* TestClock.adjust("5 seconds");
    yield* Fiber.join(listening);
    yield* Deferred.succeed(finish, undefined);
    expect((yield* fixture.store.readInboxSummary()).pendingCount).toBe(1);
  });

describe("authenticated native endpoint", () => {
  it(
    "receives new work after initial catch-up on the same live subscription",
    receivesLaterArrival,
  );
  it(
    "refuses a redirect without forwarding the runtime credential",
    refuseRedirects,
  );
  it(
    "releases ownership when receiver readiness fails",
    releaseAfterReadinessFailure,
  );
  itOnTestClock(
    "bounds stalled cleanup and disables receipts before cleanup completes",
    boundCleanup,
  );
  it(
    "recovers canonical large content and guidance, retiring work only after acceptance",
    recoverCanonicalContent,
  );
  it(
    "signals readiness while idle, rejects a second receiver and recovers after release",
    signalIdleReadiness,
  );
  it(
    "rejects retained receipts even when release fails and the transport is healthy",
    fenceAfterFailedCleanup,
  );
  it(
    "cancels acquisition while the daemon cannot complete its initial poll",
    cancelBootstrap,
  );
  itOnTestClock(
    "renews ownership past a lease while the host callback is still blocked",
    renewDuringSlowAcceptance,
  );
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore repository defaults. */
