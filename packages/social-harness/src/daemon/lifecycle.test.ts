/** @file The daemon serves registration state while protocol workers start and closes on critical failures. */

import { NodeHttpServer } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { type Context, Deferred, Effect, Fiber } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer.make requires a native server factory for the loopback listener.
import { createServer } from "node:http";
import { expect } from "vitest";
import {
  acquireDaemonClient,
  type Fixture,
  identityLayerFor,
  makeFixture,
} from "../__tests__/daemon-runtime-fixtures.js";
import {
  stateDirectory,
  withDaemonStore,
} from "../__tests__/store-schema-fixtures.js";
import { Router } from "../communication/router/index.js";
import { Firewall } from "../firewall/index.js";
import { HARNESS_SEND_TOOL } from "../host-protocol/names.js";
import { AuthenticationFailedError } from "../identity/index.js";
import { saveLocalIdentity } from "../identity/local-store.js";
import { Registry } from "../identity/registry/index.js";
import { DaemonRuntimeError, runDaemonRuntime } from "./lifecycle.js";

const unusedNetwork = () =>
  Effect.dieMessage("Unregistered status does not access the network");

function runFixture(
  fixture: Fixture,
  registry?: Context.Tag.Service<typeof Registry>,
  router?: Context.Tag.Service<typeof Router>,
) {
  return runDaemonRuntime(fixture.bootstrap).pipe(
    Effect.provide(identityLayerFor(fixture.bootstrap)),
    Effect.provide(Firewall.layer()),
    Effect.provideService(
      Registry,
      registry ??
        Registry.of({ register: unusedNetwork, lookup: unusedNetwork }),
    ),
    Effect.provideService(
      Router,
      router ?? Router.of({ send: unusedNetwork, poll: unusedNetwork }),
    ),
  );
}

// @agent-code-guard/regression-only: finite startup failures exercise real listener ownership and critical-worker shutdown.
it("serves authenticated status before registration", servesUnregisteredStatus);

it(
  "reports an occupied configured listener as a listener failure",
  refusesOccupiedListener,
);

function registerWhileRouterStarts(fixture: Fixture) {
  return Effect.gen(function* () {
    const started = yield* Deferred.make<undefined>();
    const stopped = yield* Deferred.make<undefined>();
    const daemon = yield* runFixture(
      fixture,
      Registry.of({
        register: () =>
          Effect.succeed({ kind: "registered", agentCard: fixture.localCard }),
        lookup: unusedNetwork,
      }),
      Router.of({
        send: unusedNetwork,
        poll: () =>
          Deferred.succeed(started, undefined).pipe(
            Effect.zipRight(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(stopped, undefined)),
          ),
      }),
    ).pipe(Effect.forkScoped);
    const client = yield* acquireDaemonClient(fixture).pipe(
      Effect.raceFirst(stoppedDuringStartup(daemon)),
    );
    const result = yield* Effect.tryPromise(() =>
      client.callTool({ name: "register", arguments: fixture.registerRequest }),
    );
    yield* Deferred.await(started).pipe(
      Effect.raceFirst(stoppedDuringStartup(daemon)),
    );
    const catalog = yield* Effect.tryPromise(() => client.listTools());
    const status = yield* Effect.tryPromise(() =>
      client.callTool({ name: "status", arguments: {} }),
    );
    expect(result.structuredContent).toMatchObject({
      kind: "registered",
      agent: { agentName: fixture.localCard.agentName },
    });
    expect(status.structuredContent).toMatchObject({
      kind: "active",
      agent: { agentName: fixture.localCard.agentName },
    });
    expect(catalog.tools.map((tool) => tool.name)).toContain(HARNESS_SEND_TOOL);
    yield* Fiber.interrupt(daemon);
    yield* Deferred.await(stopped);
  });
}

it(
  "serves saved registration while Router startup waits and cancels its worker on shutdown",
  servesWhileRouterStarts,
);

it(
  "stops when a critical Router worker refuses authentication",
  stopsRejectedRouter,
);

function servesUnregisteredStatus() {
  return withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const daemon = yield* runFixture(fixture).pipe(Effect.forkScoped);
      const client = yield* acquireDaemonClient(fixture).pipe(
        Effect.raceFirst(stoppedDuringStartup(daemon)),
      );
      const status = yield* Effect.tryPromise(() =>
        client.callTool({ name: "status", arguments: {} }),
      );
      expect(status.structuredContent).toEqual({ kind: "unregistered" });
      yield* Fiber.interrupt(daemon);
    }),
  );
}

function refusesOccupiedListener() {
  return withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      yield* NodeHttpServer.make(createServer, {
        host: "127.0.0.1",
        port: fixture.bootstrap.configuration.mcpPort,
      });
      expect(yield* runFixture(fixture).pipe(Effect.flip)).toEqual(
        new DaemonRuntimeError({ phase: "listener" }),
      );
    }),
  );
}

function servesWhileRouterStarts() {
  return withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      yield* registerWhileRouterStarts(yield* makeFixture);
    }),
  );
}

function stopsRejectedRouter() {
  return withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      yield* saveLocalIdentity(fixture.localCard);
      const failure = yield* runFixture(
        fixture,
        Registry.of({ register: unusedNetwork, lookup: unusedNetwork }),
        Router.of({
          send: unusedNetwork,
          poll: () => Effect.fail(new AuthenticationFailedError()),
        }),
      ).pipe(Effect.flip, Effect.timeout("5 seconds"));
      expect(failure).toBeInstanceOf(DaemonRuntimeError);
    }),
  );
}

function stoppedDuringStartup<A, E>(daemon: Fiber.Fiber<A, E>) {
  return Fiber.join(daemon).pipe(
    Effect.zipRight(Effect.dieMessage("Daemon stopped during startup")),
  );
}
