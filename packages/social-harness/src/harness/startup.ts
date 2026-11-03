/** @file Opens configured SQL storage and composes the shared daemon listener. */
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { Console, Context, Duration, Effect, Layer } from "effect";
import type { McpAuthentication } from "./mcp/authentication.js";
import { Router } from "../communication/router/client.js";
import { databaseConnection } from "../database/sql.js";
import { Registry } from "../identity/registry.js";
import { packageVersion, WIRE_VERSION } from "../version.js";
import {
  type DaemonSettings,
  databaseAccess,
  loadDaemonSettings,
  prepareDatabase,
  processHelp,
  spanExport,
} from "./config.js";
import { makeMcpHttpApp, serveMcp } from "./mcp/server.js";
import { RunningHarnesses } from "./running.js";

/** Usage documents configuration separately from ordinary SQL-backed startup. */
export const daemonHelp = processHelp;

/** All hosts use the same retained agent contexts and SQL-owned configuration. */
export const daemonLayer = Layer.scopedDiscard(runDaemon()).pipe(
  Layer.provide(spanExport),
);

function runDaemon() {
  return Effect.gen(function* () {
    const access = yield* databaseAccess;
    const context = yield* Layer.build(databaseConnection(access.locator));
    const sql = Context.get(context, SqlClient.SqlClient);
    return yield* Effect.gen(function* () {
      yield* prepareDatabase;
      const settings = yield* loadDaemonSettings(access.unlockKey);
      const running = yield* Layer.build(
        RunningHarnesses.layer(settings, access.unlockKey).pipe(
          Layer.provide(networkLayer(settings)),
          Layer.provide(NodeFileSystem.layer),
        ),
      );
      const agents = Context.get(running, RunningHarnesses);
      const authentication = authenticationOf(settings);
      const app = yield* makeMcpHttpApp(authentication, agents.get);
      yield* serveMcp(settings.port, app, settings.host);
      yield* Console.log(
        `sharnessd listening on http://${settings.host}:${settings.port}/mcp (package ${packageVersion}; WIRE_VERSION ${WIRE_VERSION})`,
      );
      return yield* Effect.never;
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
  }).pipe(Effect.withSpan("runDaemon"));
}

function authenticationOf(settings: DaemonSettings): McpAuthentication {
  const authentication = settings.authentication;
  return authentication.kind === "local"
    ? { credential: authentication.credential }
    : {
        oauth: {
          resource: authentication.resource,
          issuer: authentication.issuer,
          scope: authentication.scope,
          allowedOrigins: authentication.allowedOrigins.map(
            (value) => value.origin,
          ),
        },
      };
}

function networkLayer(settings: DaemonSettings) {
  return Layer.merge(
    Registry.layer({
      origin: settings.registryOrigin,
      registrySignerPublicKey: settings.registrySignerPublicKey,
      requestTimeout: Duration.seconds(30),
    }),
    Router.layer({
      origin: settings.routerOrigin,
      sendTimeout: Duration.seconds(30),
      pollTimeout: Duration.seconds(35),
    }),
  ).pipe(Layer.provide(NodeHttpClient.layer));
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
