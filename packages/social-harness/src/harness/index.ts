/** @file Composes the per-agent process, its database and network clients. */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { Context, Data, Duration, Effect, Layer, Option } from "effect";
import { join } from "node:path";
import { Router } from "../communication/router/api.js";
import {
  loadMessagePolicy,
  MessagePolicyConfigurationError,
} from "../firewall/rules/config.js";
import { Firewall, type FirewallOptions } from "../firewall/service.js";
import { Registry } from "../identity/registry/index.js";
import { Identity, type RegistrationError } from "../identity/service.js";
import {
  DaemonConfigurationError,
  type DaemonProcessConfiguration,
  loadDaemonBootstrap,
  loadDaemonProcessConfiguration,
  processHelp,
  readCredential,
} from "./config.js";
import { DaemonRuntimeError, runDaemonRuntime } from "./lifecycle.js";
import { spanExport } from "./telemetry.js";

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
          ". Use a fresh state directory and register again. Docs: https://docs.moltzap.xyz/reference/spec/daemon/daemon#persistence";
        break;
      case "admission-refused":
        remedy =
          ". Supply the admission credential for this Registry. Docs: https://docs.moltzap.xyz/guides/run-sharnessd#run-one-agent";
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
        yield* sql.unsafe("PRAGMA user_version = 1");
      } else if (version?.user_version !== 1) {
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
  return error instanceof DaemonRuntimeError
    ? new DaemonStartupError({
        phase: error.phase,
        ...(error.reason === undefined ? {} : { reason: error.reason }),
      })
    : new DaemonStartupError({ phase: "storage" });
}

/** Trusted embeddings use the same services as the process's MCP adapter. */
export {
  type ListConversationsRequest,
  type ListConversationsResult,
  type ReadHistoryRequest,
  type ReadHistoryResult,
  type SendRequest,
  SocialHarness,
} from "./service.js";
/** Local identity and registration remain independent of delivery recovery. */
export {
  Identity,
  IdentityError,
  type IdentityState,
  RegistrationError,
  type RegistrationRequest,
} from "../identity/service.js";
/** Message transport owns requests, replies and verified history. */
export { Communication, CommunicationError } from "../communication/index.js";
/** Message policy produces permitted content and owns its decision history. */
export {
  Firewall,
  type FirewallOptions,
  FirewallRefusal,
} from "../firewall/service.js";
/** Network services expose complete calls through their owning layers. */
export { Registry, RegistryError } from "../identity/registry/index.js";
/** Authentication supplies verified attribution before operation decoding. */
export { Authentication } from "../identity/authentication/index.js";

/** Trusted policy providers receive typed assessment requests at construction. */
export type { DecisionProvider, DecisionRequest } from "../firewall/check.js";
