/** @file Opens configured storage and owns agent resources independently of MCP connections. */
import { HttpClient } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  Cause,
  Console,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FiberSet,
  Layer,
  type Redacted,
  ScopedCache,
} from "effect";
import { Router } from "../communication/router/client.js";
import { Communication } from "../communication/service.js";
import { checkDatabase } from "../database/migrations.js";
import { databaseAccess, databaseConnection } from "../database/sql.js";
import { Firewall } from "../firewall/service.js";
import { Registry } from "../identity/registry.js";
import { Identity, IdentityError } from "../identity/service.js";
import {
  HeldNorms,
  type ServiceNorms,
  serviceNorms,
} from "../norms/configuration.js";
import { packageVersion, WIRE_VERSION } from "../version.js";
import {
  type DaemonSettings,
  loadDaemonSettings,
  processHelp,
  spanExport,
} from "./config.js";
import { makeAgentMcpApp, makeMcpHttpApp, serveMcp } from "./mcp/server.js";
import { webhookHttpClientLayer } from "./mcp/webhook-client.js";
import { makeWebhookEvents } from "./mcp/webhook.js";
import {
  getOrCreateOwnedAgent,
  listOwnedAgentKeys,
  loadAgentRegistration,
  localOwner,
  type OwnerAccount,
} from "./ownership.js";
import { SocialHarness } from "./social-harness.js";

/** Usage names the runtime inputs without opening storage or allocating an owner. */
export const daemonHelp = processHelp;

/** A daemon scope restores saved agents and serves authenticated requests to them. */
export const daemonLayer = Layer.scopedDiscard(runDaemon()).pipe(
  Layer.provide(spanExport),
);

function runDaemon() {
  return Effect.gen(function* () {
    const access = yield* databaseAccess;
    const { settings, agent } = yield* loadDaemonSettings();
    return yield* Effect.gen(function* () {
      yield* checkDatabase(access.unlockKey);
      if (settings.authentication.kind === "local") {
        yield* getOrCreateOwnedAgent(localOwner, access.unlockKey, agent);
      }
      const network = yield* Layer.build(networkLayer(settings));
      const selectApp = yield* makeAgentSelector(
        settings,
        access.unlockKey,
      ).pipe(Effect.provide(network));
      const app = yield* makeMcpHttpApp(settings.authentication, selectApp);
      yield* serveMcp(settings.port, app, settings.host);
      yield* Console.log(
        `sharnessd listening on http://${settings.host}:${settings.port}/mcp (package ${packageVersion}; WIRE_VERSION ${WIRE_VERSION})`,
      );
      return yield* Effect.never;
    }).pipe(Effect.scoped, Effect.provide(databaseConnection(access.locator)));
  }).pipe(Effect.provide(NodeFileSystem.layer), Effect.withSpan("runDaemon"));
}

/** Requests borrow a prepared app; their cancellation never owns its acquisition. */
function makeAgentSelector(
  settings: DaemonSettings,
  unlockKey: Redacted.Redacted<Uint8Array>,
) {
  return Effect.gen(function* () {
    const processScope = yield* Effect.scope;
    const sql = yield* SqlClient.SqlClient;
    const norms =
      settings.norms === undefined
        ? undefined
        : yield* serviceNorms(settings.norms);
    const cache = yield* ScopedCache.makeWith({
      capacity: Number.MAX_SAFE_INTEGER,
      lookup: (agentKey: string) =>
        makeAgentApp(settings, agentKey, unlockKey, norms),
      timeToLive: (exit) =>
        Exit.isSuccess(exit) ? Duration.infinity : Duration.zero,
    });
    const acquire = (agentKey: string) => Effect.scoped(cache.get(agentKey));
    yield* Effect.forEach(
      yield* listOwnedAgentKeys,
      (agentKey) =>
        acquire(agentKey).pipe(
          Effect.catchAll((error) =>
            Effect.logError("Agent restoration failed").pipe(
              Effect.annotateLogs({
                agentKey,
                failure: "_tag" in error ? error._tag : "ProtocolError",
              }),
            ),
          ),
        ),
      { concurrency: 4, discard: true },
    ).pipe(Effect.forkScoped);
    return (owner: OwnerAccount) =>
      getOrCreateOwnedAgent(owner, unlockKey).pipe(
        Effect.flatMap(acquire),
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.interruptible,
        Effect.forkIn(processScope),
        Effect.flatMap((fiber) => Fiber.join(fiber).pipe(Effect.interruptible)),
      );
  }).pipe(Effect.withSpan("makeAgentSelector"));
}

/**
 * One agent's retained services and MCP app. Its firewall applies the
 * service's policy and the norms the agent holds in its own editable policy.
 */
function makeAgentApp(
  settings: DaemonSettings,
  agentKey: string,
  unlockKey: Redacted.Redacted<Uint8Array>,
  norms?: ServiceNorms,
) {
  return Effect.gen(function* () {
    const registration = yield* loadAgentRegistration(agentKey, unlockKey);
    const workers = yield* FiberSet.make();
    const stopped = yield* Deferred.make<undefined>();
    const identity = Identity.layer({
      agentKey,
      signingAuthority: registration.signingAuthority,
      registrySignerPublicKey: settings.registrySignerPublicKey,
      admissionCredential:
        settings.admissionCredential === undefined
          ? Effect.fail(new IdentityError({ reason: "incompatible-daemon" }))
          : Effect.succeed(settings.admissionCredential),
    });
    const policy = settings.messagePolicy?.policy;
    const firewall = Firewall.layer(
      agentKey,
      settings.messagePolicy,
      norms?.chain,
      norms?.documents,
    ).pipe(Layer.provideMerge(identity));
    const services = yield* Layer.build(
      SocialHarness.layer(
        {
          agentKey,
          communication: (identityCard) =>
            Communication.layer({
              agentKey,
              identityCard,
              signingAuthority: registration.signingAuthority,
              registrySignerPublicKey: settings.registrySignerPublicKey,
              maximumHistoryRecords: policy?.maximumHistoryRecords,
            }),
          maximumHistoryRecords: policy?.maximumHistoryRecords,
          maximumConcurrentChecks: policy?.maximumConcurrentChecks,
          hideGroups: settings.hideGroups,
          hideCollectives: settings.hideCollectives,
        },
        workers,
      ).pipe(
        Layer.provideMerge(
          norms === undefined
            ? firewall
            : HeldNorms.layer(norms).pipe(Layer.provideMerge(firewall)),
        ),
      ),
    );
    if ((yield* Context.get(services, Identity).me()).kind === "unregistered") {
      yield* Context.get(services, SocialHarness).register(
        registration.registration,
      );
    }
    const callbackClient = yield* Layer.build(webhookHttpClientLayer);
    const webhook = yield* makeWebhookEvents(agentKey, unlockKey).pipe(
      Effect.provide(services),
      Effect.provideService(
        HttpClient.HttpClient,
        Context.get(callbackClient, HttpClient.HttpClient),
      ),
    );
    const app = yield* makeAgentMcpApp({
      webhook,
      stopped,
      implementation: { name: "sharnessd", version: packageVersion },
    }).pipe(Effect.provide(services));
    yield* FiberSet.join(workers).pipe(
      Effect.matchCauseEffect({
        onFailure: (cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.void
            : Effect.logError("Agent worker stopped").pipe(
                Effect.annotateLogs({ agentKey }),
              ),
        onSuccess: () => Effect.void,
      }),
      Effect.ensuring(Deferred.succeed(stopped, undefined)),
      Effect.forkScoped,
    );
    return app;
  }).pipe(Effect.withSpan("makeAgentApp"));
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

/** Deployment prepares schema and encryption metadata before the serving process starts. */
export { prepareDatabase } from "../database/migrations.js";

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
export {
  RegistrationId,
  Registry,
  RegistryError,
} from "../identity/registry.js";
/** Authentication supplies verified attribution before operation decoding. */
export { Authentication } from "../identity/http-authentication.js";

/** Trusted policy providers receive typed assessment requests at construction. */
export type { DecisionProvider, DecisionRequest } from "../firewall/check.js";
