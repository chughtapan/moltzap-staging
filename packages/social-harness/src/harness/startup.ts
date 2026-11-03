/** @file Composes the per-agent process, its database and network clients. */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import {
  Cause,
  Console,
  Context,
  Data,
  Duration,
  Effect,
  FiberSet,
  Layer,
  Option,
} from "effect";
import { join } from "node:path";
import { Router } from "../communication/router/client.js";
import { Communication, CommunicationError } from "../communication/service.js";
import {
  type LoadedMessagePolicy,
  loadMessagePolicy,
  MessagePolicyConfigurationError,
} from "../firewall/rules.js";
import { Firewall, type FirewallOptions } from "../firewall/service.js";
import { Registry } from "../identity/registry.js";
import { Identity, type RegistrationError } from "../identity/service.js";
import { packageVersion, WIRE_VERSION } from "../version.js";
import {
  type DaemonBootstrap,
  DaemonConfigurationError,
  type DaemonProcessConfiguration,
  loadDaemonBootstrap,
  loadDaemonProcessConfiguration,
  processHelp,
  readCredential,
  spanExport,
} from "./config.js";
import { LOOPBACK_HOST, makeMcpHttpApp, serveMcp } from "./mcp/server.js";
import { SocialHarness } from "./social-harness.js";
import { makeHistoryExport } from "./traffic-export.js";

/** CLI usage comes from the configuration it describes. */
export const daemonHelp = processHelp;

/** The process reports actionable startup failures without exposing retained private state. */
export class DaemonStartupError extends Data.TaggedError("DaemonStartupError")<{
  readonly phase: "configuration" | "storage" | "listener" | "registration";
  readonly reason?:
    | RegistrationError["reason"]
    | "incompatible"
    | "agent-not-admitted"
    | "outbox-too-old";
  readonly detail?: string;
}> {
  override get message(): string {
    const reason = this.reason === undefined ? "" : ` (${this.reason})`;
    const detail = this.detail === undefined ? "" : `: ${this.detail}`;
    let remedy = "";
    switch (this.reason) {
      case "incompatible":
      case "agent-not-admitted":
        remedy =
          ". Use a fresh state directory and register again. Docs: https://docs.moltzap.xyz/reference/spec/daemon#persistence";
        break;
      case "admission-refused":
        remedy =
          ". Supply the admission credential for this Registry. Docs: https://docs.moltzap.xyz/guides/run-an-agent-daemon#run-one-agent";
        break;
      case "outbox-too-old":
        remedy =
          ". Apply a reset release or finish pending sends on the previous release. Docs: https://docs.moltzap.xyz/guides/upgrades#apply-a-reset-release";
        break;
      case undefined:
      case "name_taken":
      case "key_already_registered":
      case "idempotency_conflict":
      case "dependency-unavailable":
      case "persistence-failed":
      case "incompatible-daemon":
      default:
        break;
    }
    return `sharnessd startup failed in phase ${this.phase}${reason}${detail}${remedy}`;
  }
}

/** The environment selects the policy document and the enclosing scope owns the process. */
export const daemonLayer = Layer.scopedDiscard(runDaemon()).pipe(
  Layer.provide(spanExport.pipe(Layer.mapError(configurationFailure))),
);

/** Programmatic policy configuration uses the same rule lifecycle as a policy document. */
export function withFirewall(options: FirewallOptions) {
  return Layer.scopedDiscard(runDaemon(options)).pipe(
    Layer.provide(spanExport.pipe(Layer.mapError(configurationFailure))),
  );
}

function runDaemon(composed?: FirewallOptions) {
  return Effect.gen(function* () {
    const configuration = yield* loadDaemonProcessConfiguration.pipe(
      Effect.mapError(configurationFailure),
    );
    const loaded = yield* loadMessagePolicy(configuration, readCredential).pipe(
      Effect.mapError(configurationFailure),
    );
    if (composed !== undefined && Option.isSome(loaded)) {
      return yield* new DaemonStartupError({
        phase: "configuration",
        detail:
          "Configure a policy document or programmatic rules, using one source.",
      });
    }
    const source = composed ?? Option.getOrUndefined(loaded);
    const bootstrap = yield* loadDaemonBootstrap(configuration).pipe(
      Effect.mapError(configurationFailure),
    );
    const database = databaseLayer(configuration.stateDirectory);
    const local = Identity.layer({
      signingAuthority: bootstrap.signingAuthority,
      registrySignerPublicKey: configuration.registrySignerPublicKey,
      admissionCredential: bootstrap.admissionCredential,
    }).pipe(
      Layer.provideMerge(networkLayer(configuration)),
      Layer.provideMerge(database),
    );
    const services = Firewall.layer(source).pipe(Layer.provideMerge(local));
    return yield* Effect.gen(function* () {
      if ((yield* (yield* Identity).me()).kind === "unregistered") {
        yield* bootstrap.admissionCredential.pipe(
          Effect.mapError(configurationFailure),
        );
      }
      return yield* runDaemonRuntime(bootstrap, source);
    }).pipe(Effect.provide(services), Effect.mapError(startupFailure));
  });
}

function runDaemonRuntime(
  bootstrap: DaemonBootstrap,
  source?: FirewallOptions | LoadedMessagePolicy,
) {
  return Effect.gen(function* () {
    const workers = yield* FiberSet.make();
    const policy =
      source !== undefined && "policy" in source ? source.policy : source;
    const historyExport =
      bootstrap.configuration.historyExport === undefined
        ? undefined
        : yield* makeHistoryExport(bootstrap.configuration.historyExport).pipe(
            Effect.provide(NodeFileSystem.layer),
          );
    const harness = SocialHarness.layer(
      {
        historyExport,
        communication: (identityCard) =>
          Communication.layer({
            identityCard,
            signingAuthority: bootstrap.signingAuthority,
            registrySignerPublicKey:
              bootstrap.configuration.registrySignerPublicKey,
            maximumHistoryRecords: policy?.maximumHistoryRecords,
          }),
        maximumHistoryRecords: policy?.maximumHistoryRecords,
        maximumConcurrentChecks: policy?.maximumConcurrentChecks,
        hideGroups: bootstrap.configuration.hideGroups,
        hideCollectives: bootstrap.configuration.hideCollectives,
      },
      workers,
    );
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const service = yield* SocialHarness;
        const identity = yield* Identity;
        if (
          (yield* identity.me()).kind === "unregistered" &&
          bootstrap.configuration.registration !== undefined
        ) {
          yield* service.register(bootstrap.configuration.registration).pipe(
            Effect.catchTag("RegistrationError", (error) =>
              Effect.fail(
                new DaemonStartupError({
                  phase: "registration",
                  reason: error.reason,
                }),
              ),
            ),
          );
        }
        const app = yield* makeMcpHttpApp({
          implementation: { name: "sharnessd", version: packageVersion },
          credential: bootstrap.mcpCredential,
        });
        yield* serveMcp(bootstrap.configuration.mcpPort, app).pipe(
          Effect.catchTag("ServeError", () =>
            Effect.fail(new DaemonStartupError({ phase: "listener" })),
          ),
        );
        yield* Console.log(
          `sharnessd listening on http://${LOOPBACK_HOST}:${bootstrap.configuration.mcpPort}/mcp (package ${packageVersion}; WIRE_VERSION ${WIRE_VERSION})`,
        );
        return yield* Effect.never;
      }),
    ).pipe(
      Effect.provide(harness),
      Effect.raceFirst(FiberSet.join(workers)),
      Effect.catchAllCause(stopRuntime),
    );
  }).pipe(Effect.withSpan("runDaemonRuntime"));
}

function stopRuntime(
  cause: Cause.Cause<unknown>,
): Effect.Effect<never, DaemonStartupError> {
  if (Cause.isInterruptedOnly(cause)) {
    return Effect.failCause(Cause.stripFailures(cause));
  }
  const failure = Option.getOrUndefined(Cause.failureOption(cause));
  let error = new DaemonStartupError({ phase: "storage" });
  if (failure instanceof DaemonStartupError) {
    error = failure;
  } else if (
    failure instanceof CommunicationError &&
    (failure.reason === "outbox-too-old" ||
      failure.reason === "agent-not-admitted")
  ) {
    error = new DaemonStartupError({
      phase: "storage",
      reason: failure.reason,
    });
  }
  return Effect.logError("Daemon stopped", cause).pipe(
    Effect.zipRight(Effect.fail(error)),
  );
}

/** A process-format mismatch is rejected before any owner initializes or changes its tables. */
function databaseLayer(stateDirectory: string) {
  return Layer.scoped(
    SqlClient.SqlClient,
    Effect.gen(function* () {
      const filesystem = yield* FileSystem.FileSystem;
      yield* filesystem.makeDirectory(stateDirectory, {
        recursive: true,
        mode: 0o700,
      });
      yield* filesystem.chmod(stateDirectory, 0o700);
      const filename = join(stateDirectory, "sharnessd.sqlite3");
      const context = yield* Layer.build(
        SqliteClient.layer({ filename, disableWAL: true }),
      );
      const sql = Context.get(context, SqlClient.SqlClient);
      // eslint-disable-next-line @typescript-eslint/naming-convention -- SQLite names the PRAGMA result field.
      const [version] = yield* sql.unsafe<{ readonly user_version: number }>(
        "PRAGMA user_version",
      );
      const [tables] = yield* sql<{
        readonly count: number;
      }>`SELECT COUNT(*) AS count FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
      if (version?.user_version === 0 && tables?.count === 0) {
        yield* sql.unsafe("PRAGMA user_version = 2");
      } else if (version?.user_version !== 2) {
        return yield* new DaemonStartupError({
          phase: "storage",
          reason: "incompatible",
        });
      }
      yield* sql.unsafe("PRAGMA locking_mode = EXCLUSIVE");
      yield* sql.unsafe("PRAGMA foreign_keys = ON");
      yield* sql.unsafe("PRAGMA busy_timeout = 0");
      yield* sql.unsafe("PRAGMA journal_mode = WAL");
      yield* sql.unsafe("PRAGMA synchronous = FULL");
      yield* filesystem.chmod(filename, 0o600);
      return sql;
    }).pipe(
      Effect.provide(NodeFileSystem.layer),
      Effect.mapError(startupFailure),
    ),
  );
}

function networkLayer(configuration: DaemonProcessConfiguration) {
  return Layer.merge(
    Registry.layer({
      origin: configuration.registryOrigin,
      registrySignerPublicKey: configuration.registrySignerPublicKey,
      requestTimeout: Duration.seconds(30),
    }),
    Router.layer({
      origin: configuration.routerOrigin,
      sendTimeout: Duration.seconds(30),
      pollTimeout: Duration.seconds(35),
    }),
  ).pipe(Layer.provide(NodeHttpClient.layer));
}

function configurationFailure(error: unknown): DaemonStartupError {
  return new DaemonStartupError({
    phase: "configuration",
    ...(error instanceof DaemonConfigurationError ||
    error instanceof MessagePolicyConfigurationError
      ? { detail: error.message }
      : {}),
  });
}

function startupFailure(error: unknown): DaemonStartupError {
  if (error instanceof DaemonStartupError) {
    return error;
  }
  return new DaemonStartupError({ phase: "storage" });
}

/** Trusted embeddings use the same services as the process's MCP adapter. */
export {
  type ListConversationsRequest,
  type ListConversationsResult,
  type ReadHistoryRequest,
  type ReadHistoryResult,
  type SendRequest,
  SocialHarness,
} from "./social-harness.js";
/** Local identity and registration remain independent of delivery recovery. */
export {
  Identity,
  IdentityError,
  type IdentityState,
  RegistrationError,
  type RegistrationRequest,
} from "../identity/service.js";
/** Message transport owns requests, replies and verified history. */
export { Communication, CommunicationError } from "../communication/service.js";
/** Message policy produces permitted content and owns its decision history. */
export {
  Firewall,
  type FirewallOptions,
  FirewallRefusal,
} from "../firewall/service.js";
/** Network services expose complete calls through their owning layers. */
export { Registry, RegistryError } from "../identity/registry.js";
/** Authentication supplies verified attribution before operation decoding. */
export { Authentication } from "../identity/http-authentication.js";

/** Trusted policy providers receive typed assessment requests at construction. */
export type { DecisionProvider, DecisionRequest } from "../firewall/check.js";
