/** @file Names, keys and saved cards in a Registry the daemon keeps itself. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { scopedLive as it } from "@effect/vitest";
import { Context, Effect, Encoding, Layer, Redacted, Schema } from "effect";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { expect } from "vitest";
import { migrateDaemon } from "../database/migrations.js";
import { makeIdentityFixture } from "./__tests__/authentication-fixtures.js";
import { AgentIdentityCard, AgentName } from "./agent-cards.js";
import { localRegistry } from "./local-registry.js";
import { RegistrationId, Registry } from "./registry.js";

const unlockKey = Redacted.make(new Uint8Array(randomBytes(32)));

/** Opens the Registry kept in one database file, as each daemon start does. */
const openRegistry = (filename: string) =>
  Effect.gen(function* () {
    const database = yield* Layer.build(
      Layer.effectDiscard(migrateDaemon).pipe(
        Layer.provideMerge(SqliteClient.layer({ filename })),
      ),
    );
    const opened = yield* localRegistry(unlockKey).pipe(
      Effect.provide(database),
    );
    const registry = Context.get(yield* Layer.build(opened.layer), Registry);
    return { ...opened, registry };
  });

const databaseFile = Effect.gen(function* () {
  const filesystem = yield* FileSystem.FileSystem;
  return join(yield* filesystem.makeTempDirectoryScoped(), "registry.sqlite3");
});

const requestFor = (agentName: string) =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    return {
      signingAuthority: fixture.signingAuthority,
      request: {
        operationId: RegistrationId.make(
          `opn_${Encoding.encodeBase64Url(new Uint8Array(randomBytes(16)))}`,
        ),
        principalId: fixture.identityCard.principalId,
        agentName: yield* Schema.decodeUnknown(AgentName)(agentName),
      },
    };
  });

it("signs a card that verifies under its own key and returns it again for the same agent", () =>
  Effect.gen(function* () {
    const { registry, registrySignerPublicKey } = yield* openRegistry(
      yield* databaseFile,
    );
    const call = yield* requestFor("first-agent");
    const registered = yield* registry.register(call);
    if (registered.kind !== "registered") {
      return yield* Effect.dieMessage("Expected a registered agent");
    }
    const verified = yield* AgentIdentityCard.verify({
      identityCard: registered.identityCard,
      registrySignerPublicKey,
    });
    expect(verified.agentName).toBe(call.request.agentName);
    expect(verified.principalId).toBe(call.request.principalId);
    expect(yield* registry.register(call)).toEqual(registered);
  }).pipe(Effect.provide(NodeContext.layer)));

it("refuses a taken name and a key that already names another agent", () =>
  Effect.gen(function* () {
    const { registry } = yield* openRegistry(yield* databaseFile);
    const first = yield* requestFor("taken-name");
    const second = yield* requestFor("taken-name");
    const renamed = yield* requestFor("other-name");
    yield* registry.register(first);
    expect(yield* registry.register(second)).toEqual({ kind: "name_taken" });
    expect(
      yield* registry.register({
        signingAuthority: first.signingAuthority,
        request: renamed.request,
      }),
    ).toEqual({ kind: "key_already_registered" });
  }).pipe(Effect.provide(NodeContext.layer)));

it("finds a card by name and by identifier after the database is reopened", () =>
  Effect.gen(function* () {
    const filename = yield* databaseFile;
    const before = yield* openRegistry(filename);
    const call = yield* requestFor("lasting-agent");
    const registered = yield* before.registry.register(call);
    if (registered.kind !== "registered") {
      return yield* Effect.dieMessage("Expected a registered agent");
    }
    const after = yield* openRegistry(filename);
    const found = { kind: "found", identityCard: registered.identityCard };
    expect(after.registrySignerPublicKey).toEqual(
      before.registrySignerPublicKey,
    );
    expect(
      yield* after.registry.lookup({ agentName: call.request.agentName }),
    ).toEqual(found);
    expect(
      yield* after.registry.lookup({
        agentId: registered.identityCard.agentId,
      }),
    ).toEqual(found);
    expect(
      yield* after.registry.lookup({
        agentName: yield* Schema.decodeUnknown(AgentName)("nobody"),
      }),
    ).toEqual({ kind: "not_found" });
  }).pipe(Effect.provide(NodeContext.layer)));
