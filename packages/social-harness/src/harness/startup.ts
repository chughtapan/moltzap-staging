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
  Option,
  type Redacted,
  Ref,
  ScopedCache,
} from "effect";
import { MessageAddressInput } from "../communication/messages.js";
import { Router } from "../communication/router/client.js";
import { Communication } from "../communication/service.js";
import { checkDatabase } from "../database/migrations.js";
import { databaseAccess, databaseConnection } from "../database/sql.js";
import { FirewallPolicyError } from "../firewall/rules.js";
import { Firewall } from "../firewall/service.js";
import { Registry } from "../identity/registry.js";
import { Identity } from "../identity/service.js";
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
  serverAgentOwner,
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
    const { settings, agentName, serverAgentCredential } =
      yield* loadDaemonSettings();
    return yield* Effect.gen(function* () {
      yield* checkDatabase(access.unlockKey);
      yield* Communication.configureTransport(settings.transport);
      if (settings.authentication.kind === "local") {
        yield* getOrCreateOwnedAgent(localOwner, access.unlockKey, agentName);
      }
      if (settings.transport.kind === "client-server") {
        yield* getOrCreateOwnedAgent(
          serverAgentOwner,
          access.unlockKey,
          settings.transport.serverAgentName,
        );
      }
      const network = yield* Layer.build(networkLayer(settings));
      const selectApp = yield* makeAgentSelector(
        settings,
        access.unlockKey,
      ).pipe(Effect.provide(network));
      const app = yield* makeMcpHttpApp(
        settings.authentication,
        selectApp,
        serverAgentCredential,
      );
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
    const serverAgentNorms = yield* Ref.make(
      Option.none<Context.Tag.Service<typeof HeldNorms>>(),
    );
    const cache = yield* ScopedCache.makeWith({
      capacity: Number.MAX_SAFE_INTEGER,
      lookup: (agentKey: string) =>
        makeAgentApp(
          { settings, unlockKey, serverAgentNorms, norms },
          agentKey,
        ),
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

/** What a message withheld by the server agent's firewall is answered with. */
const withheldReply = (hasNorms: boolean) =>
  hasNorms
    ? "Your message was not accepted: it breaks a rule this agent applies to incoming messages. Call read_norms to see which messages it accepts."
    : "Your message was not accepted: it breaks a rule this agent applies to incoming messages.";

/**
 * An agent's part: a peer in P2P mode, or in client/server mode the server
 * agent or one of its clients. The server agent is the one whose registered
 * name the settings give. A client knows the server agent's address, the only
 * one it exchanges messages with.
 */
const roleOf = (settings: DaemonSettings, agentName: string) => {
  if (settings.transport.kind !== "client-server") {
    return { kind: "peer" } as const;
  }
  const { serverAgentName } = settings.transport;
  return serverAgentName === agentName
    ? ({ kind: "server" } as const)
    : ({
        kind: "client",
        serverAgent: MessageAddressInput.make(`agent:${serverAgentName}`),
      } as const);
};

/** What every agent of the daemon is built from. */
interface DaemonResources {
  readonly settings: DaemonSettings;
  readonly unlockKey: Redacted.Redacted<Uint8Array>;
  /** The server agent's norms, once it has started, for its clients to read. */
  readonly serverAgentNorms: Ref.Ref<
    Option.Option<Context.Tag.Service<typeof HeldNorms>>
  >;
  readonly norms?: ServiceNorms;
}

/** Norms a client reads: the ones the server agent holds. */
const serverAgentsNorms = (daemon: DaemonResources) =>
  Layer.succeed(
    HeldNorms,
    HeldNorms.of({
      read: () =>
        Ref.get(daemon.serverAgentNorms).pipe(
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                ),
              onSome: (held) => held.read(),
            }),
          ),
        ),
    }),
  );

/**
 * The firewall and norms an agent runs with. A peer or the server agent
 * applies the service's policy and the norms it holds in its own editable
 * policy. A client applies none: the server agent's firewall checks what the
 * client sends it and what it sends the client, and the client reads the
 * server agent's norms.
 */
function agentChecks(
  daemon: DaemonResources,
  agentKey: string,
  role: ReturnType<typeof roleOf>,
) {
  const { settings, norms } = daemon;
  const firewall =
    role.kind === "client"
      ? Firewall.layer(agentKey)
      : Firewall.layer(
          agentKey,
          settings.messagePolicy,
          norms?.chain,
          norms?.documents,
        );
  if (norms === undefined) {
    return firewall;
  }
  return (
    role.kind === "client" ? serverAgentsNorms(daemon) : HeldNorms.layer(norms)
  ).pipe(Layer.provideMerge(firewall));
}

/** How an agent's messages travel: inside this daemon, or through the Router to peers. */
function agentCommunication(
  settings: DaemonSettings,
  options: Parameters<typeof Communication.layer>[0],
) {
  return settings.transport.kind === "client-server"
    ? Communication.clientServerLayer({
        ...options,
        serverAgentName: settings.transport.serverAgentName,
      })
    : Communication.layer(options).pipe(
        Layer.provide(
          Router.layer({
            origin: settings.transport.routerOrigin,
            sendTimeout: Duration.seconds(30),
            pollTimeout: Duration.seconds(35),
          }).pipe(Layer.provide(NodeHttpClient.layer)),
        ),
      );
}

/** One agent's retained services and MCP app. */
function makeAgentApp(daemon: DaemonResources, agentKey: string) {
  const { settings, unlockKey, norms } = daemon;
  return Effect.gen(function* () {
    const registration = yield* loadAgentRegistration(agentKey, unlockKey);
    const role = roleOf(settings, registration.registration.agentName);
    const workers = yield* FiberSet.make();
    const stopped = yield* Deferred.make<undefined>();
    const identity = Identity.layer({
      agentKey,
      signingAuthority: registration.signingAuthority,
      registrySignerPublicKey: settings.registrySignerPublicKey,
      registration: registration.registration,
    });
    const policy = settings.messagePolicy?.policy;
    const services = yield* Layer.build(
      SocialHarness.layer(
        {
          agentKey,
          communication: (identityCard) =>
            agentCommunication(settings, {
              agentKey,
              identityCard,
              signingAuthority: registration.signingAuthority,
              registrySignerPublicKey: settings.registrySignerPublicKey,
              maximumHistoryRecords: policy?.maximumHistoryRecords,
            }),
          maximumHistoryRecords: policy?.maximumHistoryRecords,
          maximumConcurrentChecks: policy?.maximumConcurrentChecks,
          hideGroups: role.kind !== "peer" || settings.hideGroups,
          ...(role.kind === "server"
            ? { withheldReply: withheldReply(norms !== undefined) }
            : {}),
        },
        workers,
      ).pipe(
        Layer.provideMerge(
          agentChecks(daemon, agentKey, role).pipe(
            Layer.provideMerge(identity),
          ),
        ),
      ),
    );
    if (role.kind === "server") {
      yield* Ref.set(
        daemon.serverAgentNorms,
        Context.getOption(services, HeldNorms),
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
      hideCollectives: role.kind !== "peer" || settings.hideCollectives,
      serverAgent:
        role.kind === "client" ? Option.some(role.serverAgent) : Option.none(),
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
  return Registry.layer({
    origin: settings.registryOrigin,
    registrySignerPublicKey: settings.registrySignerPublicKey,
    requestTimeout: Duration.seconds(30),
    admissionCredential: Option.fromNullable(settings.admissionCredential),
  }).pipe(Layer.provide(NodeHttpClient.layer));
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
