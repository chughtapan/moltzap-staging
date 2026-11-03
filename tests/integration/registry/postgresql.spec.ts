/** @file PostgreSQL Registry persistence, restart, admission, and outage integration tests. */

import * as Reactivity from "@effect/experimental/Reactivity";
import { FileSystem, HttpClient } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { layer as registryLayer } from "@moltzap/registry";
import {
  AgentIdentityCard,
  AgentSigningAuthority,
  type VerifiedAgentIdentityCard,
  WIRE_VERSION,
} from "@moltzap/social-harness";
import { Registry } from "@moltzap/social-harness/service";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import {
  ConfigProvider,
  Duration,
  Effect,
  Layer,
  Redacted,
  Schema,
} from "effect";
import { join } from "node:path";
import { expect, it } from "vitest";
import { clockAt } from "./clock.js";
import {
  BOOTSTRAP_SIGNATURE_INTERVAL_SECONDS,
  callRegister,
  LOOPBACK_HOST,
  makeOperationId,
  makePrivateKeyPem,
  makeRegistryRunner,
  makeSigningAuthority,
  makeVersionedBootstrapRequest,
  managedProcess,
  prepareAgent,
  processTestError,
  registerAgent,
  REGISTRY_BINARY,
  registryEnvironment,
  reservePort,
  startReadyRegistry,
  stopProcess,
  waitForExit,
  waitForHealthStatus,
} from "./registry-fixture.js";

const OK_STATUS = 200;
const UNAUTHORIZED_STATUS = 401;
const AUTHENTICATION_FAILED_BODY = '{"error":"authentication_failed"}';
const SERVICE_UNAVAILABLE_STATUS = 503;
const EMPTY_BODY = "";
const FOUND_KIND = "found";
const REGISTERED_KIND = "registered";
const SIGNER_MISMATCH_REASON = "signer-mismatch";
/** Requests in each concurrency race: exactly one wins and the others are refused. */
const CONTENDERS = 8;

/** Copies of one prepared agent that differ only in operation id, so they race on the same key. */
const sameKeyContenders = (
  first: Effect.Effect.Success<ReturnType<typeof prepareAgent>>,
) =>
  Array.from({ length: CONTENDERS }, () => ({
    ...first,
    request: { ...first.request, operationId: makeOperationId() },
  }));

const stopPostgreSql = (container: StartedPostgreSqlContainer) =>
  Effect.tryPromise({
    try: () => container.stop(),
    catch: (cause) =>
      processTestError("could not stop PostgreSQL container", cause),
  }).pipe(Effect.asVoid);

const managedPostgreSql = Effect.acquireRelease(
  Effect.tryPromise({
    try: () =>
      new PostgreSqlContainer("postgres:16-alpine")
        .withDatabase("registry")
        .withUsername("registry")
        .withPassword("registry-password")
        .start(),
    catch: (cause) =>
      processTestError("could not start PostgreSQL container", cause),
  }),
  (container) => stopPostgreSql(container).pipe(Effect.ignore),
);

/**
 * PostgreSQL publishes its host port before the Registry selects a free
 * listener port, keeping container startup out of that allocation window.
 */
const makePostgreSqlRegistrySetup = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const temporaryDirectory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "sharness-registry-postgresql-",
  });
  const privateKeyPem = makePrivateKeyPem();
  const registryKeyPath = join(temporaryDirectory, "registry-key.pem");
  yield* fileSystem.writeFileString(registryKeyPath, privateKeyPem, {
    mode: 0o600,
  });
  const signingAuthority = yield* makeSigningAuthority(privateKeyPem);
  const registrySignerPublicKey =
    AgentSigningAuthority.publicKey(signingAuthority);
  const container = yield* managedPostgreSql;
  const registryPort = yield* reservePort;
  const origin = new URL(`http://${LOOPBACK_HOST}:${registryPort}`);
  const environment = registryEnvironment({
    port: registryPort,
    postgresqlUrl: container.getConnectionUri(),
    registryKeyPath,
  });
  return {
    container,
    environment,
    origin,
    registrySignerPublicKey,
  };
});

const expectExactCard = (
  actual: VerifiedAgentIdentityCard,
  expected: VerifiedAgentIdentityCard,
) =>
  Effect.all(
    [
      Schema.encode(AgentIdentityCard)(actual),
      Schema.encode(AgentIdentityCard)(expected),
    ] as const,
    { concurrency: 2 },
  ).pipe(
    Effect.map(([actualEncoded, expectedEncoded]) => {
      expect(actualEncoded).toStrictEqual(expectedEncoded);
      return actualEncoded;
    }),
  );

/**
 * Persistence binds the database and signer, independent of the listener origin.
 * Select a port immediately before restart because parallel tests or Docker can
 * claim the first process's port after it stops.
 */
const realPostgreSqlBehavior = Effect.gen(function* () {
  const setup = yield* makePostgreSqlRegistrySetup;
  const firstProcess = yield* startReadyRegistry(
    setup.environment,
    setup.origin,
  );
  const registered = yield* registerAgent(
    "postgresql-agent",
    makeRegistryRunner(setup.origin, setup.registrySignerPublicKey),
  );
  yield* stopProcess(firstProcess);

  const restartPort = yield* reservePort;
  const restartOrigin = new URL(`http://${LOOPBACK_HOST}:${restartPort}`);
  const restarted = yield* startReadyRegistry(
    { ...setup.environment, SHARNESS_REGISTRY_PORT: String(restartPort) },
    restartOrigin,
  );
  const lookup = yield* makeRegistryRunner(
    restartOrigin,
    setup.registrySignerPublicKey,
  )(Registry.lookup({ agentId: registered.identityCard.agentId }));
  expect(lookup.kind).toBe(FOUND_KIND);
  if (lookup.kind !== "found") {
    return yield* processTestError("registered identity was not found");
  }
  yield* expectExactCard(lookup.identityCard, registered.identityCard);

  yield* stopPostgreSql(setup.container);
  const unhealthy = yield* waitForHealthStatus({
    runningProcess: restarted,
    origin: restartOrigin,
    expectedStatus: SERVICE_UNAVAILABLE_STATUS,
  });
  expect(unhealthy).toStrictEqual({
    status: SERVICE_UNAVAILABLE_STATUS,
    body: EMPTY_BODY,
  });
  yield* stopProcess(restarted);
}).pipe(
  Effect.scoped,
  Effect.provide(NodeFileSystem.layer),
  Effect.provide(NodeHttpClient.layer),
);

// @agent-code-guard/regression-only: these scenarios cover distinct database restart, outage, and startup refusal contracts against real PostgreSQL.
it("persists across Registry restart on real PostgreSQL and reports a database outage", () => {
  expect.hasAssertions();
  return Effect.runPromise(realPostgreSqlBehavior);
}, 180_000);

it("refuses a signer mismatch at process startup", () => {
  expect.hasAssertions();
  return Effect.runPromise(
    Effect.gen(function* () {
      const setup = yield* makePostgreSqlRegistrySetup;
      const sql = yield* PgClient.make({
        url: Redacted.make(setup.container.getConnectionUri()),
      }).pipe(Effect.provide(Reactivity.layer));
      const first = yield* startReadyRegistry(setup.environment, setup.origin);
      yield* stopProcess(first);
      yield* sql`UPDATE sharness_registry_metadata SET signer_thumbprint = 'different-signer'`;
      const process = yield* managedProcess(
        REGISTRY_BINARY,
        [],
        setup.environment,
      );
      yield* waitForExit(process).pipe(
        Effect.timeoutFail({
          duration: Duration.seconds(10),
          onTimeout: () =>
            processTestError(
              `Registry did not refuse the metadata mismatch\n${process.logs()}`,
            ),
        }),
      );
      expect(process.child.exitCode).not.toBe(0);
      expect(process.logs()).toContain(SIGNER_MISMATCH_REASON);
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeFileSystem.layer),
      Effect.provide(NodeHttpClient.layer),
    ),
  );
}, 180_000);

it(
  "refuses an occupied schema without the current ledger before changing any table",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const setup = yield* makePostgreSqlRegistrySetup;
        const sql = yield* PgClient.make({
          url: Redacted.make(setup.container.getConnectionUri()),
        }).pipe(Effect.provide(Reactivity.layer));
        yield* sql`CREATE TABLE unrecognized_state (retained text PRIMARY KEY)`;
        yield* sql`INSERT INTO unrecognized_state VALUES ('unchanged')`;
        const tables =
          yield* sql`SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = current_schema() ORDER BY tablename`;
        const incompatibleReason = "incompatible";
        const process = yield* managedProcess(
          REGISTRY_BINARY,
          [],
          setup.environment,
        );
        yield* waitForExit(process);
        expect(process.child.exitCode).not.toBe(0);
        expect(process.logs()).toContain(incompatibleReason);
        expect(
          yield* sql`SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = current_schema() ORDER BY tablename`,
        ).toEqual(tables);
        expect(yield* sql`SELECT retained FROM unrecognized_state`).toEqual([
          { retained: "unchanged" },
        ]);
      }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
    ),
  180_000,
);

const prepareBootstrapAdmission = (origin: URL) =>
  Effect.gen(function* () {
    const prepared = yield* prepareAgent("concurrent-bootstrap");
    const signedAt = 1_700_000_000_000;
    const signed = yield* makeVersionedBootstrapRequest(
      origin,
      prepared,
      WIRE_VERSION,
    ).pipe(Effect.withClock(clockAt(signedAt)));
    return {
      expiresAt: signedAt + BOOTSTRAP_SIGNATURE_INTERVAL_SECONDS * 1_000,
      signed,
    };
  });

it(
  "claims a bootstrap nonce once under concurrent admission at exactly the signature expiry second",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const setup = yield* makePostgreSqlRegistrySetup;
        const prepared = yield* prepareBootstrapAdmission(setup.origin);
        yield* Layer.build(registryLayer).pipe(
          Effect.withConfigProvider(
            ConfigProvider.fromMap(new Map(Object.entries(setup.environment))),
          ),
          Effect.withClock(clockAt(prepared.expiresAt)),
        );
        const client = yield* HttpClient.HttpClient;
        const outcomes = yield* Effect.all(
          Array.from({ length: CONTENDERS }, () =>
            client
              .execute(prepared.signed)
              .pipe(
                Effect.flatMap((response) =>
                  response.text.pipe(
                    Effect.map((body) => ({ status: response.status, body })),
                  ),
                ),
              ),
          ),
          { concurrency: CONTENDERS },
        );
        expect(
          outcomes.map(({ status }) => status).sort((a, b) => a - b),
        ).toEqual([
          OK_STATUS,
          ...Array.from({ length: CONTENDERS - 1 }, () => UNAUTHORIZED_STATUS),
        ]);
        expect(
          outcomes
            .filter(({ status }) => status === UNAUTHORIZED_STATUS)
            .map(({ body }) => body),
        ).toEqual(
          Array.from(
            { length: CONTENDERS - 1 },
            () => AUTHENTICATION_FAILED_BODY,
          ),
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(NodeFileSystem.layer),
        Effect.provide(NodeHttpClient.layer),
      ),
    ),
  180_000,
);

it.each([
  { conflict: "key", refused: "key_already_registered" },
  { conflict: "name", refused: "name_taken" },
])(
  "admits one concurrent registration for the same $conflict on PostgreSQL",
  ({ conflict, refused }) => {
    expect.hasAssertions();
    return Effect.runPromise(
      Effect.gen(function* () {
        const setup = yield* makePostgreSqlRegistrySetup;
        yield* startReadyRegistry(setup.environment, setup.origin);
        const run = makeRegistryRunner(
          setup.origin,
          setup.registrySignerPublicKey,
        );
        const contenders = yield* conflict === "key"
          ? prepareAgent("postgresql-race").pipe(Effect.map(sameKeyContenders))
          : Effect.all(
              Array.from({ length: CONTENDERS }, () =>
                prepareAgent("postgresql-race"),
              ),
              { concurrency: CONTENDERS },
            );
        const results = yield* Effect.forEach(
          contenders,
          (contender) => callRegister(run, contender),
          { concurrency: CONTENDERS },
        );
        expect(
          results.filter((result) => result.kind === REGISTERED_KIND),
        ).toHaveLength(1);
        expect(
          results.filter((result) => result.kind === refused),
        ).toHaveLength(CONTENDERS - 1);
      }).pipe(
        Effect.scoped,
        Effect.provide(NodeFileSystem.layer),
        Effect.provide(NodeHttpClient.layer),
      ),
    );
  },
  180_000,
);
