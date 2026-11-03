/** @file Qualifies ownership transactions and RLS with a real non-owner PostgreSQL login. */
import { NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import {
  AgentCard,
  AgentSigningAuthority,
  InboxEvent,
  Message,
} from "@moltzap/social-harness";
import {
  Firewall,
  type FirewallOptions,
  Identity,
  Registry,
} from "@moltzap/social-harness/service";
import {
  databaseConnection,
  DecisionProviderError,
  getOrCreateOwnedAgent,
  makeInbox,
  migrateDaemon,
  withAgentTransaction,
} from "@moltzap/social-harness/test";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import {
  Config,
  Context,
  Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Redacted,
  Schema,
} from "effect";
import { generateKeyPairSync, randomBytes } from "node:crypto";
import { expect } from "vitest";

const testAgentCard = (name: string) =>
  Schema.decodeUnknownSync(AgentCard)({
    agentId: `agt_${randomBytes(16).toString("base64url")}`,
    principalId: `prn_${randomBytes(16).toString("base64url")}`,
    agentName: name,
  });

const databaseLocator = Config.redacted("SHARNESS_PG_TEST_URL").pipe(
  Config.option,
  Effect.flatMap(
    Option.match({
      onSome: Effect.succeed,
      onNone: () =>
        Effect.acquireRelease(
          Effect.tryPromise(() =>
            new PostgreSqlContainer("postgres:17-alpine")
              .withDatabase("qualification")
              .withUsername("qualification")
              .withPassword(randomBytes(24).toString("hex"))
              .start(),
          ),
          (container) =>
            Effect.tryPromise(() => container.stop()).pipe(
              Effect.asVoid,
              Effect.orDie,
            ),
        ).pipe(
          Effect.map((container) =>
            Redacted.make(container.getConnectionUri()),
          ),
        ),
    }),
  ),
);

const unlockKey = Redacted.make(new Uint8Array(32).fill(3));
const runtimePassword = randomBytes(24).toString("hex");

const qualifyTenants = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const owner = { issuer: "https://issuer.example", subject: "first" };
  const profiles = yield* Effect.all(
    Array.from({ length: 8 }, () => getOrCreateOwnedAgent(owner, unlockKey)),
    { concurrency: 8 },
  );
  const first = yield* getOrCreateOwnedAgent(owner, unlockKey);
  for (const profile of profiles) {
    expect(profile).toEqual(first);
  }
  const second = yield* getOrCreateOwnedAgent(
    { ...owner, subject: "second" },
    unlockKey,
  );
  const role = yield* sql<{
    readonly rolsuper: boolean;
    readonly rolbypassrls: boolean;
  }>`
    SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user`;
  expect(role[0]?.rolsuper).toBeFalsy();
  expect(role[0]?.rolbypassrls).toBeFalsy();
  const keys = [first.agent_key, second.agent_key];
  yield* Effect.all(
    Array.from({ length: 12 })
      .flatMap(() => keys)
      .map((key) => {
        if (key === undefined) {
          return Effect.dieMessage("Expected a qualification agent");
        }
        return withAgentTransaction(
          key,
          sql<
            Record<"agent_key", string>
          >`SELECT agent_key FROM agent_profiles`,
        ).pipe(
          Effect.map((rows) => {
            expect(rows.map((row) => row.agent_key)).toEqual([key]);
            return rows;
          }),
        );
      }),
    { concurrency: 8 },
  );
  expect(yield* sql`SELECT * FROM agent_profiles`).toHaveLength(0);
  const interrupted = yield* withAgentTransaction(
    first.agent_key,
    sql`UPDATE agent_profiles SET agent_name='rolled-back' WHERE agent_key=${first.agent_key}`.pipe(
      Effect.zipRight(Effect.fail("rollback")),
    ),
  ).pipe(Effect.exit);
  expect(Exit.isFailure(interrupted)).toBeTruthy();
  expect(yield* sql`SELECT * FROM agent_profiles`).toHaveLength(0);
  expect(yield* getOrCreateOwnedAgent(owner, unlockKey)).toEqual(first);
  return keys;
});

const qualifyInbox = (keys: readonly string[]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const message = yield* Message.create({
      author: testAgentCard("alice"),
      recipients: [testAgentCard("bob"), testAgentCard("carol")],
      content: [{ type: "text", text: "Postgres inbox qualification" }],
    });
    const address = yield* Schema.decodeUnknown(InboxEvent.fields.address)(
      "agent:alice",
    );
    for (const key of keys) {
      const inbox = yield* makeInbox(key);
      yield* inbox.queueIncoming(address, { kind: "message", message });
      const pending = (yield* inbox.pending(10))[0];
      if (pending === undefined) {
        return yield* Effect.dieMessage(
          "Expected independent pending work for each agent",
        );
      }
      yield* inbox.publish(pending, {
        kind: "allowed",
        item: { kind: "multicast", message },
        save: Effect.void,
        visibility: [],
      });
      expect(
        (yield* inbox.listInbox()).items.map((item) => item.deliveryToken),
      ).toEqual([pending.token]);
      const reopened = yield* makeInbox(key);
      expect(yield* reopened.getInboxEvent(pending.token)).toEqual(
        yield* inbox.getInboxEvent(pending.token),
      );
    }
    expect(yield* sql`SELECT * FROM harness_items`).toHaveLength(0);
    return keys;
  });

const qualifyFirewall = (keys: readonly string[]) =>
  Effect.gen(function* () {
    const authority = yield* AgentSigningAuthority.fromPkcs8(
      Redacted.make(
        generateKeyPairSync("ed25519").privateKey.export({
          format: "pem",
          type: "pkcs8",
        }),
      ),
    );
    const signer = AgentSigningAuthority.publicKey(authority);
    const registry = Registry.layer({
      origin: new URL("https://registry.example"),
      registrySignerPublicKey: signer,
      requestTimeout: Duration.seconds(5),
    }).pipe(Layer.provide(NodeHttpClient.layer));
    const options: FirewallOptions = {
      mode: "enforcement",
      facts: [],
      settings: {
        timeoutMilliseconds: 5000,
        maximumInputBytes: 100000,
        maximumResultBytes: 100000,
      },
      text: {
        initialRules: [],
        provider: {
          decide: () =>
            Effect.fail(new DecisionProviderError({ reason: "transport" })),
        },
        settings: {
          minimumProbability: 0.5,
          maximumRequestBytes: 60000,
          timeoutMilliseconds: 5000,
        },
      },
    };
    const rules = keys.map(
      (key) =>
        ({
          id: key,
          applies: "both",
          text: `Remember policy ${key}.`,
        }) satisfies {
          readonly id: string;
          readonly applies: "both";
          readonly text: string;
        },
    );
    for (const [index, key] of keys.entries()) {
      const rule = rules[index];
      if (rule === undefined) {
        return yield* Effect.dieMessage("Expected an agent policy");
      }
      const identity = Identity.layer({
        agentKey: key,
        signingAuthority: authority,
        registrySignerPublicKey: signer,
        admissionCredential: Effect.succeed(Redacted.make("qualification")),
      }).pipe(Layer.provide(registry));
      const configured = Firewall.layer(key, options).pipe(
        Layer.provide(identity),
      );
      const saved = yield* Effect.gen(function* () {
        const firewall = yield* Firewall;
        expect((yield* firewall.readPolicy()).rules).toEqual([]);
        yield* firewall.updatePolicy({ add: [rule] });
        return yield* firewall.readDecisions({});
      }).pipe(Effect.provide(configured));
      expect(saved.activations).toHaveLength(1);
      expect(saved.activations[0]?.time).toBeGreaterThan(0);
      yield* Effect.gen(function* () {
        const firewall = yield* Firewall;
        expect((yield* firewall.readPolicy()).rules).toEqual([rule]);
        expect(yield* firewall.readDecisions({})).toEqual(saved);
      }).pipe(
        Effect.provide(
          Firewall.layer(key, options).pipe(Layer.provide(identity)),
        ),
      );
    }
    const sql = yield* SqlClient.SqlClient;
    expect(yield* sql`SELECT * FROM firewall_policy`).toHaveLength(0);
    expect(yield* sql`SELECT * FROM firewall_activations`).toHaveLength(0);
    return keys;
  });

it(
  "isolates concurrent owners and clears transaction context on a pooled non-owner login",
  () =>
    Effect.gen(function* () {
      const locator = yield* databaseLocator;
      const adminLayer = databaseConnection(locator);
      const admin = Context.get(
        yield* Layer.build(adminLayer),
        SqlClient.SqlClient,
      );
      const suffix = randomBytes(8).toString("hex");
      const schema = `qualification_${suffix}`;
      const role = `runtime_${suffix}`;
      yield* admin`CREATE SCHEMA ${admin(schema)}`;
      yield* admin`CREATE ROLE ${admin(role)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD ${admin.literal("'" + runtimePassword + "'")}`;
      yield* Effect.addFinalizer(() =>
        admin`DROP SCHEMA ${admin(schema)} CASCADE`.pipe(
          Effect.zipRight(admin`DROP ROLE ${admin(role)}`),
          Effect.orDie,
        ),
      );
      const schemaUrl = new URL(Redacted.value(locator));
      schemaUrl.searchParams.set("options", `-c search_path=${schema}`);
      yield* migrateDaemon.pipe(
        Effect.provide(databaseConnection(Redacted.make(schemaUrl.href))),
      );
      yield* admin`GRANT USAGE ON SCHEMA ${admin(schema)} TO ${admin(role)}`;
      yield* admin`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${admin(schema)} TO ${admin(role)}`;
      yield* admin`GRANT USAGE ON ALL SEQUENCES IN SCHEMA ${admin(schema)} TO ${admin(role)}`;
      schemaUrl.username = role;
      schemaUrl.password = runtimePassword;
      const keys = yield* qualifyTenants.pipe(
        Effect.flatMap(qualifyInbox),
        Effect.flatMap(qualifyFirewall),
        Effect.provide(databaseConnection(Redacted.make(schemaUrl.href))),
      );
      const retained = yield* admin<
        Record<"agent_key", string>
      >`SELECT agent_key FROM ${admin(schema)}.agent_profiles`;
      expect(new Set(retained.map((row) => row.agent_key))).toEqual(
        new Set(keys),
      );
    }).pipe(Effect.scoped),
  { timeout: 120_000 },
);
