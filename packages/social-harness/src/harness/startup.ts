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
import { Router } from "../communication/p2p/router/client.js";
import { Communication } from "../communication/service.js";
import { checkDatabase } from "../database/migrations.js";
import { databaseAccess, databaseConnection } from "../database/sql.js";
import { Firewall, FirewallPolicyError } from "../firewall/index.js";
import { localRegistry } from "../identity/local-registry.js";
import { Registry } from "../identity/registry.js";
import { Identity } from "../identity/service.js";
import { HeldNorms, type ServiceNorms, serviceNorms } from "../norms/index.js";
import { packageVersion, WIRE_VERSION } from "../version.js";
import {
  type DaemonSettings,
  loadDaemonSettings,
  processHelp,
  spanExport,
} from "./config.js";
import { makeMcpHttpApp, serveMcp } from "./mcp/http.js";
import { makeAgentMcpApp } from "./mcp/server.js";
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
      if (settings.authentication.kind === "local") {
        yield* getOrCreateOwnedAgent(localOwner, access.unlockKey, agentName);
      }
      const norms =
        settings.norms === undefined
          ? undefined
          : yield* serviceNorms(settings.norms);
      const mode =
        settings.mode.kind === "p2p"
          ? yield* p2pMode(settings, settings.mode)
          : yield* clientServerMode(settings.mode, {
              unlockKey: access.unlockKey,
              hasNorms: norms !== undefined,
            });
      const selectApp = yield* makeAgentSelector({
        settings,
        unlockKey: access.unlockKey,
        mode,
        norms,
      }).pipe(Effect.provide(yield* Layer.build(mode.registry)));
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

/**
 * The peers' shared Registry names every agent, and each agent sends and
 * receives through the Router. Every agent is a peer: it runs the daemon's
 * own firewall and may address any other agent.
 */
function p2pMode(
  settings: DaemonSettings,
  mode: Extract<DaemonSettings["mode"], { readonly kind: "p2p" }>,
) {
  const router = Router.layer({
    origin: mode.routerOrigin,
    sendTimeout: Duration.seconds(30),
    pollTimeout: Duration.seconds(35),
  }).pipe(Layer.provide(NodeHttpClient.layer));
  return Effect.as(Communication.p2p.claim, {
    registrySignerPublicKey: mode.registrySignerPublicKey,
    registry: Registry.layer({
      origin: mode.registryOrigin,
      registrySignerPublicKey: mode.registrySignerPublicKey,
      requestTimeout: Duration.seconds(30),
      admissionCredential: Option.fromNullable(mode.admissionCredential),
    }).pipe(Layer.provide(NodeHttpClient.layer)),
    agent: () => ({
      communication: (options: Parameters<typeof Communication.p2p.layer>[0]) =>
        Communication.p2p.layer(options).pipe(Layer.provide(router)),
      checks: <Own>({ own }: { readonly own: Own }) => own,
      harness: { hideGroups: settings.hideGroups },
      app: {
        hideCollectives: settings.hideCollectives,
        serverAgent: Option.none(),
      },
      started: () => Effect.void,
    }),
  });
}

/**
 * The daemon names its own agents and delivers their messages itself. The
 * agent named in the settings is the server agent, created here if it does
 * not exist. It runs the daemon's firewall and holds its norms. Every other
 * agent is a client: it exchanges messages only with the server agent, runs
 * no check of its own, and reads the server agent's norms.
 */
function clientServerMode(
  mode: Extract<DaemonSettings["mode"], { readonly kind: "client-server" }>,
  input: {
    readonly unlockKey: Redacted.Redacted<Uint8Array>;
    readonly hasNorms: boolean;
  },
) {
  return Effect.gen(function* () {
    const { serverAgentName } = mode;
    yield* Communication.clientServer.claim(serverAgentName);
    yield* getOrCreateOwnedAgent(
      serverAgentOwner,
      input.unlockKey,
      serverAgentName,
    );
    const registry = yield* localRegistry(input.unlockKey);
    const serverNorms = yield* Ref.make(
      Option.none<Context.Tag.Service<typeof HeldNorms>>(),
    );
    const communication = (
      options: Parameters<typeof Communication.p2p.layer>[0],
    ) => Communication.clientServer.layer({ ...options, serverAgentName });
    const server = {
      communication,
      checks: <Own>({ own }: { readonly own: Own }) => own,
      harness: {
        hideGroups: true,
        withheldReply: input.hasNorms
          ? "Your message was not accepted: it breaks a rule this agent applies to incoming messages. Call read_norms to see which messages it accepts."
          : "Your message was not accepted: it breaks a rule this agent applies to incoming messages.",
      },
      app: { hideCollectives: true, serverAgent: Option.none() },
      started: (services: Context.Context<never>) =>
        Ref.set(serverNorms, Context.getOption(services, HeldNorms)),
    };
    const client = {
      communication,
      checks: ({ agentKey }: { readonly agentKey: string }) =>
        input.hasNorms
          ? readsNormsOf(serverNorms).pipe(
              Layer.provideMerge(Firewall.layer(agentKey)),
            )
          : Firewall.layer(agentKey),
      harness: { hideGroups: true },
      app: {
        hideCollectives: true,
        serverAgent: Option.some(
          MessageAddressInput.make(`agent:${serverAgentName}`),
        ),
      },
      started: () => Effect.void,
    };
    return {
      registrySignerPublicKey: registry.registrySignerPublicKey,
      registry: registry.layer,
      agent: (agentName: string) =>
        agentName === serverAgentName ? server : client,
    };
  }).pipe(Effect.withSpan("clientServerMode"));
}

/** Norms a client reads: the ones the server agent holds, once it has started. */
function readsNormsOf(
  serverNorms: Ref.Ref<Option.Option<Context.Tag.Service<typeof HeldNorms>>>,
) {
  return Layer.succeed(
    HeldNorms,
    HeldNorms.of({
      read: () =>
        Ref.get(serverNorms).pipe(
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
}

/** The one decision the daemon makes about how its agents are named, checked and reached. */
type Mode =
  | Effect.Effect.Success<ReturnType<typeof p2pMode>>
  | Effect.Effect.Success<ReturnType<typeof clientServerMode>>;

/** What every agent of the daemon is built from. */
interface DaemonResources {
  readonly settings: DaemonSettings;
  readonly unlockKey: Redacted.Redacted<Uint8Array>;
  readonly mode: Mode;
  readonly norms?: ServiceNorms;
}

/** Requests borrow a prepared app; their cancellation never owns its acquisition. */
function makeAgentSelector(daemon: DaemonResources) {
  const { unlockKey } = daemon;
  return Effect.gen(function* () {
    const processScope = yield* Effect.scope;
    const sql = yield* SqlClient.SqlClient;
    const cache = yield* ScopedCache.makeWith({
      capacity: Number.MAX_SAFE_INTEGER,
      lookup: (agentKey: string) => makeAgentApp(daemon, agentKey),
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

/** The firewall an agent runs when it checks its own messages, with the norms it holds. */
function ownChecks(daemon: DaemonResources, agentKey: string) {
  const { settings, norms } = daemon;
  const firewall = Firewall.layer(
    agentKey,
    settings.messagePolicy,
    norms?.chain,
    norms?.documents,
  );
  return norms === undefined
    ? firewall
    : HeldNorms.layer(norms).pipe(Layer.provideMerge(firewall));
}

/** One agent's retained services and MCP app. */
function makeAgentApp(daemon: DaemonResources, agentKey: string) {
  const { settings, unlockKey, mode } = daemon;
  return Effect.gen(function* () {
    const registration = yield* loadAgentRegistration(agentKey, unlockKey);
    const agent = mode.agent(registration.registration.agentName);
    const workers = yield* FiberSet.make();
    const stopped = yield* Deferred.make<undefined>();
    const identity = Identity.layer({
      agentKey,
      signingAuthority: registration.signingAuthority,
      registrySignerPublicKey: mode.registrySignerPublicKey,
      registration: registration.registration,
    });
    const policy = settings.messagePolicy?.policy;
    const services = yield* Layer.build(
      SocialHarness.layer(
        {
          ...agent.harness,
          agentKey,
          communication: (identityCard) =>
            agent.communication({
              agentKey,
              identityCard,
              signingAuthority: registration.signingAuthority,
              registrySignerPublicKey: mode.registrySignerPublicKey,
              maximumHistoryRecords: policy?.maximumHistoryRecords,
            }),
          maximumHistoryRecords: policy?.maximumHistoryRecords,
          maximumConcurrentChecks: policy?.maximumConcurrentChecks,
        },
        workers,
      ).pipe(
        Layer.provideMerge(
          agent
            .checks({ agentKey, own: ownChecks(daemon, agentKey) })
            .pipe(Layer.provideMerge(identity)),
        ),
      ),
    );
    yield* agent.started(services);
    const callbackClient = yield* Layer.build(webhookHttpClientLayer);
    const webhook = yield* makeWebhookEvents(agentKey, unlockKey).pipe(
      Effect.provide(services),
      Effect.provideService(
        HttpClient.HttpClient,
        Context.get(callbackClient, HttpClient.HttpClient),
      ),
    );
    const app = yield* makeAgentMcpApp({
      ...agent.app,
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

/** Deployment prepares schema and encryption metadata before the serving process starts. */
export { prepareDatabase } from "../database/migrations.js";

/** Trusted embeddings use the same services as the process's MCP adapter. */
export { type SendRequest, SocialHarness } from "./social-harness.js";
/** Local identity and registration remain independent of delivery recovery. */
export { Identity } from "../identity/service.js";
/** Message transport owns requests, replies and verified history. */
export { Communication } from "../communication/service.js";
/** Message policy produces permitted content and owns its decision history. */
export { Firewall, type FirewallOptions } from "../firewall/index.js";
/** Network services expose complete calls through their owning layers. */
export {
  RegistrationId,
  Registry,
  RegistryError,
} from "../identity/registry.js";

/** Trusted policy providers receive typed assessment requests at construction. */
export type { DecisionProvider, DecisionRequest } from "../firewall/index.js";
