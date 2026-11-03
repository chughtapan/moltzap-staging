/** @file Saved identity, registration failures, and ordered name resolution. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { scopedLive as it } from "@effect/vitest";
import { Context, Effect, Encoding, Layer, Redacted } from "effect";
import { join } from "node:path";
import { describe, expect } from "vitest";
import {
  type IdentityFixture,
  makeIdentityFixture,
} from "./__tests__/authentication-fixtures.js";
import {
  agentCardOf,
  AgentName,
  AgentSigningAuthority,
} from "./agent-cards.js";
import { RegistrationId, Registry, RegistryError } from "./registry.js";
import { Identity } from "./service.js";

function requestFor(fixture: IdentityFixture) {
  return {
    operationId: RegistrationId.make(
      `opn_${Encoding.encodeBase64Url(new Uint8Array(16))}`,
    ),
    principalId: fixture.identityCard.principalId,
    agentName: fixture.identityCard.agentName,
  };
}

function registryFor(
  fixture: IdentityFixture,
): Context.Tag.Service<typeof Registry> {
  return Registry.of({
    register: () =>
      Effect.succeed({
        kind: "registered",
        identityCard: fixture.identityCard,
      }),
    lookup: () => Effect.succeed({ kind: "not_found" }),
  });
}

function openIdentity(
  filename: string,
  fixture: IdentityFixture,
  registry: Context.Tag.Service<typeof Registry>,
  admissionCredential?: Effect.Effect<Redacted.Redacted>,
) {
  return Layer.build(
    Identity.layer({
      signingAuthority: fixture.signingAuthority,
      registrySignerPublicKey: AgentSigningAuthority.publicKey(
        fixture.registryAuthority,
      ),
      admissionCredential:
        admissionCredential ?? Effect.succeed(Redacted.make("test-admission")),
    }).pipe(
      Layer.provide(Layer.succeed(Registry, registry)),
      Layer.provide(SqliteClient.layer({ filename })),
    ),
  ).pipe(Effect.map((context) => Context.get(context, Identity)));
}

describe("Identity", () => {
  // @agent-code-guard/regression-only: Real SQLite exercises saved registration, restart and closed refusal behavior.
  it("reopens saved registration without admission credentials", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const filesystem = yield* FileSystem.FileSystem;
      const directory = yield* filesystem.makeTempDirectoryScoped({
        prefix: "sharness-identity-",
      });
      const filename = join(directory, "identity.sqlite3");
      yield* Effect.scoped(
        Effect.gen(function* () {
          const identity = yield* openIdentity(
            filename,
            fixture,
            registryFor(fixture),
          );
          expect(yield* identity.me()).toEqual({ kind: "unregistered" });
          expect(yield* identity.register(requestFor(fixture))).toBe(
            fixture.identityCard,
          );
          expect(yield* identity.me()).toEqual({
            kind: "registered",
            identityCard: fixture.identityCard,
          });
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const identity = yield* openIdentity(
            filename,
            fixture,
            registryFor(fixture),
            Effect.dieMessage("saved registration does not read admission"),
          );
          expect(yield* identity.me()).toEqual({
            kind: "registered",
            identityCard: fixture.identityCard,
          });
        }),
      );
    }).pipe(Effect.provide(NodeContext.layer)));

  it("keeps a refused registration unregistered", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const identity = yield* openIdentity(":memory:", fixture, {
        ...registryFor(fixture),
        register: () => Effect.succeed({ kind: "name_taken" }),
      });
      expect(
        yield* identity.register(requestFor(fixture)).pipe(Effect.flip),
      ).toMatchObject({ reason: "name_taken" });
      expect(yield* identity.me()).toEqual({ kind: "unregistered" });
    }));

  it("refuses saved registration when the configured signing key changes", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const other = yield* makeIdentityFixture;
      const filesystem = yield* FileSystem.FileSystem;
      const directory = yield* filesystem.makeTempDirectoryScoped({
        prefix: "sharness-identity-key-",
      });
      const filename = join(directory, "identity.sqlite3");
      yield* Effect.scoped(
        Effect.gen(function* () {
          const identity = yield* openIdentity(
            filename,
            fixture,
            registryFor(fixture),
          );
          yield* identity.register(requestFor(fixture));
        }),
      );
      const refusal = yield* openIdentity(
        filename,
        { ...fixture, signingAuthority: other.signingAuthority },
        registryFor(fixture),
      ).pipe(Effect.flip);
      expect(refusal).toMatchObject({ reason: "incompatible-daemon" });
    }).pipe(Effect.provide(NodeContext.layer)));

  it("projects admission refusal without assigning registration", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const identity = yield* openIdentity(":memory:", fixture, {
        ...registryFor(fixture),
        register: () => new RegistryError({ reason: "admission-refused" }),
      });
      expect(
        yield* identity.register(requestFor(fixture)).pipe(Effect.flip),
      ).toMatchObject({ reason: "admission-refused" });
      expect(yield* identity.me()).toEqual({ kind: "unregistered" });
    }));

  it("resolves cards in input order and identifies the first missing name", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const missing = AgentName.make("missing-agent");
      const identity = yield* openIdentity(":memory:", fixture, {
        ...registryFor(fixture),
        lookup: (request) =>
          Effect.succeed(
            "agentName" in request &&
              request.agentName === fixture.identityCard.agentName
              ? { kind: "found", identityCard: fixture.identityCard }
              : { kind: "not_found" },
          ),
      });
      expect(yield* identity.resolve([fixture.identityCard.agentName])).toEqual(
        [agentCardOf(fixture.identityCard)],
      );
      expect(
        yield* identity
          .resolve([fixture.identityCard.agentName, missing])
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "unknown-agent", agentName: missing });
    }));
});
