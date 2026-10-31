/** @file Registration reaches durable identity even when its caller stops waiting. */

import { scopedLive as it } from "@effect/vitest";
import { Deferred, Duration, Effect, Exit, Fiber, Layer } from "effect";
import { expect } from "vitest";
import {
  type Fixture,
  identityLayerFor,
  makeFixture,
} from "../__tests__/daemon-runtime-fixtures.js";
import {
  stateDirectory,
  withDaemonStore,
} from "../__tests__/store-schema-fixtures.js";
import { Registry } from "../identity/registry/index.js";
import { makeDaemonManagementOperations } from "./management.js";

function heldRegistry(
  fixture: Fixture,
  entered: Deferred.Deferred<undefined>,
  release: Deferred.Deferred<undefined>,
) {
  return Layer.succeed(
    Registry,
    Registry.of({
      register: () =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.zipRight(Deferred.await(release)),
          Effect.as({
            kind: "registered" as const,
            agentCard: fixture.localCard,
          }),
        ),
      lookup: () =>
        Effect.dieMessage("Registration does not resolve another agent"),
    }),
  );
}

function cancelWhileRegistering(fixture: Fixture) {
  return Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const identities = yield* Layer.build(
      identityLayerFor(fixture.bootstrap).pipe(
        Layer.provide(heldRegistry(fixture, entered, release)),
      ),
    );
    const operations = yield* makeDaemonManagementOperations().pipe(
      Effect.provide(identities),
    );
    const registering = yield* operations
      .register(fixture.registerRequest)
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    const interruption = yield* Fiber.interrupt(registering).pipe(
      Effect.forkScoped,
    );
    yield* Effect.sleep(Duration.zero);
    yield* Deferred.succeed(release, undefined);
    expect(Exit.isInterrupted(yield* Fiber.join(interruption))).toBe(true);
    yield* operations
      .readStatus()
      .pipe(Effect.repeat({ until: (status) => status.kind === "active" }));
  });
}

it("retains registration after cancellation and a store restart", () => {
  const directory = stateDirectory();
  return Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* withDaemonStore(directory, () => cancelWhileRegistering(fixture));
    yield* withDaemonStore(directory, () => checkRestoredRegistration(fixture));
  });
});

function checkRestoredRegistration(fixture: Fixture) {
  return Effect.gen(function* () {
    const operations = yield* makeDaemonManagementOperations().pipe(
      Effect.provide(identityLayerFor(fixture.bootstrap)),
      Effect.provide(
        Layer.succeed(
          Registry,
          Registry.of({
            register: () =>
              Effect.dieMessage("A saved identity does not register again"),
            lookup: () =>
              Effect.dieMessage("Status does not resolve another agent"),
          }),
        ),
      ),
    );
    expect(yield* operations.readStatus()).toMatchObject({
      kind: "active",
      agent: { agentName: fixture.localCard.agentName },
    });
  });
}
