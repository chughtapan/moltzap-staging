/** @file Shutdown interrupts ownership waiters before joining their gate holders. */

import { HttpClient, HttpClientRequest } from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import {
  CLIENT_CAPABILITIES_META_KEY,
  CLIENT_INFO_META_KEY,
  createMcpHandler,
  McpServer,
  PROTOCOL_VERSION_META_KEY,
} from "@modelcontextprotocol/server";
import { Deferred, Effect, Exit, Fiber, Scope } from "effect";
import { expect } from "vitest";
import type { PollResult } from "../../host-protocol/index.js";
import { openInboxFixture } from "../../__tests__/inbox-fixture.js";
import {
  loopbackMcpEndpoint,
  mcpIdentity,
} from "../../__tests__/mcp-http-fixtures.js";
import { stateDirectory } from "../../__tests__/store-schema-fixtures.js";
import {
  INBOX_ITEM_EVENT,
  INBOX_PENDING_EVENT,
} from "../../host-protocol/names.js";
import { Identity } from "../../identity/local.js";
import { makeConsumerOwner } from "./consumer.js";
import {
  type HarnessEvents,
  makeHarnessEvents,
  makeHarnessPollEvents,
  makeWebhookEvents,
} from "./events.js";

const info = { name: "consumer-shutdown", version: "1" };
const summary = () => Effect.succeed({ pendingCount: 0, newestSequence: 0 });

/** Signal a waiter reaching the actual semaphore, without changing its behavior. */
const observedGate = (waiting: Deferred.Deferred<undefined>) => {
  const gate = Effect.unsafeMakeSemaphore(1);
  let next: Deferred.Deferred<undefined> | undefined;
  return {
    observe: (signal = waiting) => {
      next = signal;
    },
    gate: {
      ...gate,
      withPermits:
        (permits: number) =>
        <A, E, R>(operation: Effect.Effect<A, E, R>) =>
          Effect.suspend(() => {
            const signal = next;
            next = undefined;
            return signal === undefined
              ? Effect.void
              : Deferred.succeed(signal, undefined);
          }).pipe(Effect.zipRight(gate.withPermits(permits)(operation))),
    },
  };
};

const request = (
  endpoint: URL,
  method: string,
  name: string,
  previous?: PollResult,
) =>
  HttpClient.execute(
    HttpClientRequest.post(endpoint).pipe(
      HttpClientRequest.setHeaders({
        "content-type": "application/json",
        accept: "application/json, text/event-stream",
        "mcp-method": method,
        "mcp-name": name,
      }),
      HttpClientRequest.bodyUnsafeJson({
        jsonrpc: "2.0",
        id: 1,
        method,
        params: {
          name,
          arguments: {},
          cursor: previous?.cursor ?? null,
          _meta: {
            ...previous?._meta,
            [PROTOCOL_VERSION_META_KEY]: "2026-07-28",
            [CLIENT_INFO_META_KEY]: info,
            [CLIENT_CAPABILITIES_META_KEY]: {},
          },
        },
      }),
    ),
  ).pipe(
    Effect.flatMap((response) => response.json),
    Effect.provide(NodeHttpClient.layer),
  );

const eventHandler = (events: HarnessEvents) =>
  createMcpHandler(
    () => {
      const capabilities = { events: {}, logging: {} };
      const server = new McpServer(info, { capabilities });
      events.install(server.server);
      return server;
    },
    { legacy: "reject", responseMode: "auto" },
  );

const blockedReceive = Effect.gen(function* () {
  const entered = yield* Deferred.make<undefined>();
  const release = yield* Deferred.make<undefined>();
  const interrupted = yield* Deferred.make<undefined>();
  const blocked = Deferred.succeed(entered, undefined).pipe(
    Effect.zipRight(Deferred.await(release)),
    Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
  );
  return { entered, release, interrupted, blocked };
});

const acquireFixture = Effect.fn("acquireConsumerFixture")(function* (
  blockedPoll: boolean,
) {
  const store = yield* openInboxFixture(stateDirectory());
  const held = yield* blockedReceive;
  const waiting = yield* Deferred.make<undefined>();
  const gate = observedGate(waiting);
  const owner = yield* makeConsumerOwner;
  const webhook = yield* makeWebhookEvents(store, gate.gate, owner).pipe(
    Effect.provide(NodeHttpClient.layer),
  );
  const poll = yield* makeHarnessPollEvents(
    {
      ...store,
      readInboxSummary: () =>
        blockedPoll ? held.blocked.pipe(Effect.zipRight(summary())) : summary(),
    },
    gate.gate,
    owner,
  );
  const events = yield* makeHarnessEvents({
    gate: gate.gate,
    owner,
    webhook,
    poll,
    summary,
  }).pipe(Effect.provideService(Identity, mcpIdentity(true)));
  const requests = yield* Scope.make();
  const close = Effect.all([events.close, Scope.close(requests, Exit.void)], {
    concurrency: 2,
    discard: true,
  });
  const endpoint = yield* loopbackMcpEndpoint(eventHandler(events));
  yield* Effect.addFinalizer(() =>
    Deferred.succeed(held.release, undefined).pipe(Effect.zipRight(close)),
  );
  return {
    events,
    poll,
    requests,
    endpoint,
    close,
    ...held,
    waiting,
    observe: gate.observe,
  };
});

it("closes a native request when a later consumer revocation waits for its gate", () =>
  Effect.gen(function* () {
    const fixture = yield* acquireFixture(false);
    const grant = yield* fixture.poll.poll(
      { name: INBOX_ITEM_EVENT, arguments: {}, cursor: null },
      {},
      Effect.void,
    );
    const native = yield* fixture.events
      .nativeOperation(grant._meta, fixture.blocked)
      .pipe(Effect.forkIn(fixture.requests));
    yield* Deferred.await(fixture.entered);
    fixture.observe();
    const revoke = yield* fixture.events.revoke.pipe(
      Effect.forkIn(fixture.requests),
    );
    yield* Deferred.await(fixture.waiting);
    yield* fixture.close.pipe(Effect.timeout("2 seconds"));
    expect(yield* Deferred.isDone(fixture.interrupted)).toBe(true);
    yield* Fiber.await(native);
    yield* Fiber.await(revoke);
  }));

it("rejects a bound renewal queued before consumer revocation without renewing its lease", () =>
  Effect.gen(function* () {
    const fixture = yield* acquireFixture(false);
    const grant = yield* fixture.poll.poll(
      { name: INBOX_ITEM_EVENT, arguments: {}, cursor: null },
      {},
      Effect.void,
    );
    const native = yield* fixture.events
      .nativeOperation(grant._meta, fixture.blocked)
      .pipe(Effect.forkIn(fixture.requests));
    yield* Deferred.await(fixture.entered);
    fixture.observe();
    const renewal = yield* request(
      fixture.endpoint,
      "events/poll",
      INBOX_ITEM_EVENT,
      grant,
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(fixture.waiting);
    const revoking = yield* Deferred.make<undefined>();
    fixture.observe(revoking);
    const revoke = yield* fixture.events.revoke.pipe(
      Effect.forkIn(fixture.requests),
    );
    yield* Deferred.await(revoking);
    yield* Deferred.succeed(fixture.release, undefined);
    yield* Fiber.join(native);
    expect(yield* Fiber.join(renewal)).toMatchObject({
      error: { data: { reason: "consumer-revoked" } },
    });
    yield* Fiber.join(revoke);
    expect(yield* fixture.events.status).toMatchObject({ mode: "none" });
  }));

it("closes a poll when a later owner stream waits for its gate", () =>
  Effect.gen(function* () {
    const fixture = yield* acquireFixture(true);
    const poll = yield* request(
      fixture.endpoint,
      "events/poll",
      INBOX_ITEM_EVENT,
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(fixture.entered);
    fixture.observe();
    const stream = yield* request(
      fixture.endpoint,
      "events/stream",
      INBOX_PENDING_EVENT,
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(fixture.waiting);
    yield* fixture.close.pipe(Effect.timeout("2 seconds"));
    expect(yield* Deferred.isDone(fixture.interrupted)).toBe(true);
    yield* Fiber.interrupt(poll);
    yield* Fiber.interrupt(stream);
  }));
