/** @file Opens configured storage and owns agent resources independently of MCP connections. */
import { HttpClient } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
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
  Ref,
  ScopedCache,
} from "effect";
import { MessageAddressInput } from "../communication/index.js";
import { Communication } from "../communication/service.js";
import {
  checkDatabase,
  databaseAccess,
  databaseConnection,
} from "../database/index.js";
import { FirewallPolicyError } from "../firewall/index.js";
import { Firewall } from "../firewall/service.js";
import { type AgentCard, type AgentName, Identity } from "../identity.js";
import { HeldNorms, type ServiceNorms, serviceNorms } from "../norms/index.js";
import { packageVersion } from "../version.js";
import {
  DaemonConfigurationError,
  type DaemonSettings,
  loadDaemonSettings,
  spanExport,
} from "./config.js";
import { makeMcpHttpApp, serveMcp } from "./http.js";
import {
  getOrCreateOwnedAgent,
  listOwnedAgentKeys,
  localOwner,
  type OwnerAccount,
  serverAgentOwner,
} from "./ownership.js";
import { makeAgentMcpApp } from "./server.js";
import { SocialHarness } from "./social-harness.js";
import { makeWebhookEvents, webhookHttpClientLayer } from "./webhook.js";

/** A daemon scope restores saved agents and serves authenticated requests to them. */
export const daemonLayer = Layer.scopedDiscard(runDaemon()).pipe(
  Layer.provide(spanExport),
);

/** A startup refusal that names the setting to change. */
const refused = (message: string) =>
  new DaemonConfigurationError({ reason: "document", message });

function runDaemon() {
  return Effect.gen(function* () {
    const access = yield* databaseAccess;
    const { settings, agentName, serverAgentCredential } =
      yield* loadDaemonSettings();
    return yield* Effect.gen(function* () {
      yield* checkDatabase;
      if (settings.authentication.kind === "local") {
        yield* getOrCreateOwnedAgent(localOwner, agentName).pipe(
          Effect.catchTag("OwnershipError", () =>
            refused(
              "This database's agent has another name than agentName in the settings, or another agent has that name. Set agentName to the name the database was first started with, or prepare a new database.",
            ),
          ),
        );
      }
      const norms =
        settings.norms === undefined
          ? undefined
          : yield* serviceNorms(settings.norms);
      if (settings.serverAgentName !== undefined) {
        yield* getOrCreateOwnedAgent(
          serverAgentOwner,
          settings.serverAgentName,
        ).pipe(
          Effect.catchTag("OwnershipError", () =>
            refused(
              "This database's server agent has another name than serverAgentName in the settings, or another agent has that name. Set serverAgentName to the name the database was first started with, or prepare a new database.",
            ),
          ),
        );
      }
      const selectApp = yield* makeAgentSelector({
        settings,
        norms,
        serverNorms: yield* Ref.make(
          Option.none<Context.Tag.Service<typeof HeldNorms>>(),
        ),
        consumers: new Map(),
      });
      const app = yield* makeMcpHttpApp(
        settings.authentication,
        selectApp,
        serverAgentCredential,
      ).pipe(
        Effect.catchTag("AuthenticationRefused", () =>
          refused(
            "The OAuth issuer did not answer with its configuration. Check authentication.issuer and that the daemon can reach it.",
          ),
        ),
      );
      yield* serveMcp(settings.port, app, settings.host).pipe(
        Effect.catchTag("ServeError", () =>
          refused(
            `The daemon could not listen on ${settings.host}:${settings.port}. Stop the process that uses the port, or set another port.`,
          ),
        ),
      );
      yield* Console.log(
        `sharnessd listening on http://${settings.host}:${settings.port}/mcp (package ${packageVersion})`,
      );
      return yield* Effect.never;
    }).pipe(Effect.scoped, Effect.provide(databaseConnection(access.locator)));
  }).pipe(Effect.provide(NodeFileSystem.layer), Effect.withSpan("runDaemon"));
}

/**
 * How one agent takes part. Without a server agent every agent is a `peer`: it
 * messages any agent, alone or in a group, and checks its own messages. With
 * one, every conversation is the `server` agent and one `client`: the server
 * agent checks its messages and holds the norms, and a client checks nothing
 * of its own, reads the server agent's norms and talks only to it.
 */
function placeOf(
  settings: DaemonSettings,
  agentName: AgentName,
  hasNorms: boolean,
) {
  const { serverAgentName } = settings;
  if (serverAgentName === undefined) {
    return {
      role: "peer",
      hideGroups: settings.hideGroups,
      hideCollectives: settings.hideCollectives,
      serverAgent: Option.none(),
    } as const;
  }
  const server = agentName === serverAgentName;
  return {
    role: server ? "server" : "client",
    permits: serverAndOne(serverAgentName),
    hideGroups: true,
    hideCollectives: true,
    serverAgent: server
      ? Option.none()
      : Option.some(MessageAddressInput.make(`agent:${serverAgentName}`)),
    withheldReply: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${hasNorms ? " Call read_norms to see which messages it accepts." : ""}`,
  } as const;
}

/** Whether `members` are the server agent and one other agent. */
function serverAndOne(serverAgentName: AgentName) {
  return (members: readonly AgentCard[]) =>
    members.length === 2 &&
    members.some((member) => member.agentName === serverAgentName);
}

/** What every agent of the daemon is built from. */
interface DaemonResources {
  readonly settings: DaemonSettings;
  readonly norms?: ServiceNorms;
  /** The norms the server agent holds, once it has started; clients read them. */
  readonly serverNorms: Ref.Ref<
    Option.Option<Context.Tag.Service<typeof HeldNorms>>
  >;
  /** The latch each agent's consumer waits on, which a send to that agent opens. */
  readonly consumers: Map<string, Effect.Latch>;
}

/** Requests borrow a prepared app; their cancellation never owns its acquisition. */
function makeAgentSelector(daemon: DaemonResources) {
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
      getOrCreateOwnedAgent(owner).pipe(
        Effect.flatMap(acquire),
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.interruptible,
        Effect.forkIn(processScope),
        Effect.flatMap((fiber) => Fiber.join(fiber).pipe(Effect.interruptible)),
      );
  }).pipe(Effect.withSpan("makeAgentSelector"));
}

/**
 * The checks an agent runs. A peer and the server agent run the daemon's
 * message policy with the norms they hold. A client runs no policy of its own
 * and reads the server agent's norms.
 */
function checksOf(
  daemon: DaemonResources,
  role: ReturnType<typeof placeOf>["role"],
  agentKey: string,
) {
  const { settings, norms, serverNorms } = daemon;
  if (role === "client") {
    const unchecked = Firewall.layer(agentKey);
    return norms === undefined
      ? unchecked
      : readsNormsOf(serverNorms).pipe(Layer.provideMerge(unchecked));
  }
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

/** One agent's retained services and MCP app. */
function makeAgentApp(daemon: DaemonResources, agentKey: string) {
  const { settings, norms, serverNorms } = daemon;
  return Effect.gen(function* () {
    const identity = yield* Layer.build(Identity.layer(agentKey));
    const card = Context.get(identity, Identity).me;
    const place = placeOf(settings, card.agentName, norms !== undefined);
    const workers = yield* FiberSet.make();
    const stopped = yield* Deferred.make<undefined>();
    const policy = settings.messagePolicy?.policy;
    const services = yield* Layer.build(
      SocialHarness.layer(
        {
          agentKey,
          hideGroups: place.hideGroups,
          ...(place.role === "server"
            ? { withheldReply: place.withheldReply }
            : {}),
          maximumHistoryRecords: policy?.maximumHistoryRecords,
          maximumConcurrentChecks: policy?.maximumConcurrentChecks,
        },
        workers,
      ).pipe(
        Layer.provideMerge(
          Communication.layer({
            agentKey,
            card,
            consumers: daemon.consumers,
            maximumHistoryRecords: policy?.maximumHistoryRecords,
            ...(place.role === "peer" ? {} : { permits: place.permits }),
          }),
        ),
        Layer.provideMerge(
          checksOf(daemon, place.role, agentKey).pipe(
            Layer.provideMerge(Layer.succeedContext(identity)),
          ),
        ),
      ),
    );
    if (place.role === "server") {
      yield* Ref.set(serverNorms, Context.getOption(services, HeldNorms));
    }
    const callbackClient = yield* Layer.build(webhookHttpClientLayer);
    const webhook = yield* makeWebhookEvents(agentKey).pipe(
      Effect.provide(services),
      Effect.provideService(
        HttpClient.HttpClient,
        Context.get(callbackClient, HttpClient.HttpClient),
      ),
    );
    const app = yield* makeAgentMcpApp({
      hideCollectives: place.hideCollectives,
      serverAgent: place.serverAgent,
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
export { prepareDatabase } from "../database/index.js";

/** This agent's card and the other agents of its daemon by name. */
export { Identity } from "../identity.js";
/** Message policy produces permitted content and owns its decision history. */
export { Firewall, type FirewallOptions } from "../firewall/service.js";
/** Trusted policy providers receive typed assessment requests at construction. */
export type { DecisionProvider, DecisionRequest } from "../firewall/index.js";
