/** @file Saved identity, registration failures, and ordered name resolution. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { scopedLive as it } from "@effect/vitest";
import { Context, Effect, Encoding, Layer } from "effect";
import { join } from "node:path";
import { describe, expect } from "vitest";
import { migrateDaemon } from "../database/migrations.js";
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
  options: {
    readonly agentKey?: string;
  } = {},
) {
  return Layer.build(
    Identity.layer({
      agentKey: options.agentKey ?? filename,
      signingAuthority: fixture.signingAuthority,
      registrySignerPublicKey: AgentSigningAuthority.publicKey(
        fixture.registryAuthority,
      ),
      registration: requestFor(fixture),
    }).pipe(
      Layer.provide(Layer.succeed(Registry, registry)),
      Layer.provide(
        Layer.effectDiscard(migrateDaemon).pipe(
          Layer.provideMerge(SqliteClient.layer({ filename })),
        ),
      ),
    ),
  ).pipe(Effect.map((context) => Context.get(context, Identity)));
}

function requestFor(fixture: IdentityFixture) {
  return {
    operationId: RegistrationId.make(
      `opn_${Encoding.encodeBase64Url(new Uint8Array(16))}`,
    ),
    principalId: fixture.identityCard.principalId,
    agentName: fixture.identityCard.agentName,
  };
}

it("keeps two agents' saved identity cards separate in one database", () =>
  Effect.gen(function* () {
    const first = yield* makeIdentityFixture;
    const second = yield* makeIdentityFixture;
    const filesystem = yield* FileSystem.FileSystem;
    const directory = yield* filesystem.makeTempDirectoryScoped();
    const filename = join(directory, "shared.sqlite3");
    const firstIdentity = yield* openIdentity(
      filename,
      first,
      registryFor(first),
      { agentKey: "first" },
    );
    const secondIdentity = yield* openIdentity(
      filename,
      second,
      registryFor(second),
      { agentKey: "second" },
    );
    const reopenedFirst = yield* openIdentity(
      filename,
      first,
      registryFor(first),
      { agentKey: "first" },
    );
    const reopenedSecond = yield* openIdentity(
      filename,
      second,
      registryFor(second),
      { agentKey: "second" },
    );
    expect(reopenedFirst.me).toEqual(firstIdentity.me);
    expect(reopenedSecond.me).toEqual(secondIdentity.me);
  }).pipe(Effect.provide(NodeContext.layer)));

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
          expect(identity.me).toBe(fixture.identityCard);
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const identity = yield* openIdentity(filename, fixture, {
            ...registryFor(fixture),
            register: () =>
              Effect.dieMessage("a saved registration is not sent again"),
          });
          expect(identity.me).toEqual(fixture.identityCard);
        }),
      );
    }).pipe(Effect.provide(NodeContext.layer)));

  it("does not open when the Registry refuses the name", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const refusal = yield* openIdentity(":memory:", fixture, {
        ...registryFor(fixture),
        register: () => Effect.succeed({ kind: "name_taken" }),
      }).pipe(Effect.flip);
      expect(refusal).toMatchObject({ reason: "name_taken" });
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
          yield* openIdentity(filename, fixture, registryFor(fixture));
        }),
      );
      const refusal = yield* openIdentity(
        filename,
        { ...fixture, signingAuthority: other.signingAuthority },
        registryFor(fixture),
      ).pipe(Effect.flip);
      expect(refusal).toMatchObject({ reason: "incompatible-daemon" });
    }).pipe(Effect.provide(NodeContext.layer)));

  it("does not open when the Registry refuses admission", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const refusal = yield* openIdentity(":memory:", fixture, {
        ...registryFor(fixture),
        register: () => new RegistryError({ reason: "admission-refused" }),
      }).pipe(Effect.flip);
      expect(refusal).toMatchObject({ reason: "admission-refused" });
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
