/** @file Completed firewall decisions survive repeated saves and PostgreSQL reopening. */
import { FileSystem } from "@effect/platform";
import { SqlClient } from "@effect/sql";
import { AgentCard, Message } from "@social-harness/social-harness";
import { Firewall, Identity } from "@social-harness/social-harness/service";
import {
  databaseConnection,
  getOrCreateOwnedAgent,
  loadDaemonSettings,
  localOwner,
} from "@social-harness/social-harness/test";
import { ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { expect, it } from "vitest";
import { sharedDaemon } from "../daemon/daemon-fixture.js";
import { acquirePostgres } from "../daemon/postgres-fixture.js";
import { processTrace } from "../scenario-fixture.js";

/** A send with no conversation history before it. */
const evidence = {
  history: [],
  earlierHistoryOmitted: false,
  gaps: [],
};

/** One agent's firewall on the daemon's database, with the daemon's message policy. */
const firewallOf = (
  agentKey: string,
  policy: Parameters<typeof Firewall.layer>[1],
) =>
  Firewall.layer(agentKey, policy).pipe(
    Layer.provideMerge(Identity.layer(agentKey)),
  );

/** The firewall is built twice on one database, as a daemon builds it before and after a restart. */
const scenario = Effect.gen(function* () {
  const database = yield* acquirePostgres;
  const daemon = yield* sharedDaemon({ database });
  const filesystem = yield* FileSystem.FileSystem;
  const messagePolicyFile = join(daemon.directory, "policy.json");
  yield* filesystem.writeFileString(
    messagePolicyFile,
    JSON.stringify({ format: "org.social-harness/message-policy/v2" }),
  );
  yield* daemon.configure({ messagePolicyFile });
  const { settings } = yield* loadDaemonSettings().pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(new Map(Object.entries(daemon.environment))),
    ),
  );
  const saved = yield* Effect.gen(function* () {
    const agentKey = yield* getOrCreateOwnedAgent(localOwner);
    const result = yield* Effect.gen(function* () {
      const card = (yield* Identity).me;
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
    }).pipe(Effect.provide(firewallOf(agentKey, settings.messagePolicy)));
    return { agentKey, result };
  }).pipe(Effect.provide(databaseConnection(database.runtimeLocator)));
  yield* Effect.gen(function* () {
    const services = yield* Layer.build(
      firewallOf(saved.agentKey, settings.messagePolicy),
    );
    const firewall = Context.get(services, Firewall);
    expect(yield* firewall.readDecisions({})).toEqual(saved.result);
  }).pipe(Effect.provide(databaseConnection(database.runtimeLocator)));
});

it(
  "retains one completed decision for concurrent identical saves and preserves it across rollback and reopening",
  processTrace(scenario),
  180_000,
);
