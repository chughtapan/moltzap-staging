/** @file Completed Firewall decisions survive repeated saves and PostgreSQL reopening. */
import { FileSystem } from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { AgentCard, Message } from "@moltzap/social-harness";
import { Firewall, Identity, Registry } from "@moltzap/social-harness/service";
import {
  databaseAccess,
  databaseConnection,
  getOrCreateOwnedAgent,
  loadAgentRegistration,
  loadDaemonSettings,
  localOwner,
} from "@moltzap/social-harness/test";
import {
  ConfigProvider,
  Context,
  Duration,
  Effect,
  Layer,
  Schema,
} from "effect";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { expect, it } from "vitest";
import {
  acquireProcessInfrastructure,
  makeDaemonProcessFixture,
} from "../daemon/daemon-fixture.js";
import { acquirePostgres } from "../daemon/postgres-fixture.js";
import { processTrace } from "../scenario-fixture.js";

/** Scoped resources reopen with the same fixed policy and saved signing identity. */
const scenario = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const database = yield* acquirePostgres;
  const filesystem = yield* FileSystem.FileSystem;
  const policyFile = join(infrastructure.temporaryDirectory, "policy.json");
  yield* filesystem.writeFileString(
    policyFile,
    JSON.stringify({
      format: "org.social-harness/message-policy/v2",
      mode: "enforcement",
    }),
  );
  const fixture = yield* makeDaemonProcessFixture(
    infrastructure,
    "firewall-storage",
    undefined,
    { database, settings: { messagePolicyFile: policyFile } },
  );
  const configuration = ConfigProvider.fromMap(
    new Map(Object.entries(fixture.environment)),
  );
  const access = yield* databaseAccess.pipe(
    Effect.withConfigProvider(configuration),
  );
  const saved = yield* Effect.gen(function* () {
    const { settings, agent } = yield* loadDaemonSettings().pipe(
      Effect.withConfigProvider(configuration),
    );
    const agentKey = yield* getOrCreateOwnedAgent(
      localOwner,
      access.unlockKey,
      agent,
    );
    const registration = yield* loadAgentRegistration(
      agentKey,
      access.unlockKey,
    );
    if (settings.admissionCredential === undefined) {
      return yield* Effect.dieMessage(
        "Expected network admission in qualification settings",
      );
    }
    const identity = Identity.layer({
      agentKey,
      signingAuthority: registration.signingAuthority,
      registrySignerPublicKey: settings.registrySignerPublicKey,
      admissionCredential: Effect.succeed(settings.admissionCredential),
    }).pipe(
      Layer.provide(
        Registry.layer({
          origin: settings.registryOrigin,
          registrySignerPublicKey: settings.registrySignerPublicKey,
          requestTimeout: Duration.seconds(5),
        }).pipe(Layer.provide(NodeHttpClient.layer)),
      ),
    );
    const configured = Firewall.layer(agentKey, settings.messagePolicy).pipe(
      Layer.provideMerge(identity),
    );
    const result = yield* Effect.gen(function* () {
      const identityService = yield* Identity;
      const card = yield* identityService.register(registration.registration);
      const firewall = yield* Firewall;
      const message = yield* Message.create({
        author: card,
        recipients: [
          yield* Schema.decodeUnknown(AgentCard)({
            agentName: "peer",
            agentId: `agt_${randomBytes(16).toString("base64url")}`,
            principalId: `prn_${randomBytes(16).toString("base64url")}`,
          }),
        ],
        content: [{ type: "text", text: "Store one completed decision." }],
      });
      const evidence = { history: [], earlierHistoryOmitted: false, gaps: [] };
      const decision = yield* firewall.checkOutgoing(message, evidence);
      yield* Effect.forEach(Array.from({ length: 8 }), () => decision.save, {
        concurrency: 8,
      });
      const retained = yield* firewall.readDecisions({});
      expect(retained.decisions).toHaveLength(1);
      const repeated = yield* firewall.checkOutgoing(message, evidence);
      const sql = yield* SqlClient.SqlClient;
      const rollback = yield* sql
        .withTransaction(
          repeated.save.pipe(Effect.andThen(Effect.fail("rollback"))),
        )
        .pipe(Effect.flip);
      expect(rollback).toBeDefined();
      expect(yield* firewall.readDecisions({})).toEqual(retained);
      return retained;
    }).pipe(Effect.provide(configured));
    return { agentKey, registration, settings, result };
  }).pipe(Effect.provide(databaseConnection(database.runtimeLocator)));
  yield* Effect.gen(function* () {
    const identity = Identity.layer({
      agentKey: saved.agentKey,
      signingAuthority: saved.registration.signingAuthority,
      registrySignerPublicKey: saved.settings.registrySignerPublicKey,
      admissionCredential: Effect.dieMessage(
        "Reopening must use the saved registration",
      ),
    }).pipe(
      Layer.provide(
        Registry.layer({
          origin: saved.settings.registryOrigin,
          registrySignerPublicKey: saved.settings.registrySignerPublicKey,
          requestTimeout: Duration.seconds(5),
        }).pipe(Layer.provide(NodeHttpClient.layer)),
      ),
    );
    const services = yield* Layer.build(
      Firewall.layer(saved.agentKey, saved.settings.messagePolicy).pipe(
        Layer.provide(identity),
      ),
    );
    const firewall = Context.get(services, Firewall);
    expect(yield* firewall.readDecisions({})).toEqual(saved.result);
  }).pipe(Effect.provide(databaseConnection(database.runtimeLocator)));
});

it(
  "retains one completed decision for concurrent identical saves and preserves it across rollback and reopening",
  processTrace(Effect.scoped(scenario)),
  180_000,
);
