/** @file Opens the daemon's database and owns agent resources independently of MCP connections. */
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
import { type DaemonNorms, daemonNorms, HeldNorms } from "../norms/index.js";
import { packageVersion } from "../version.js";
import {
  type DaemonSettings,
  loadDaemonSettings,
  refused,
  spanExport,
} from "./config.js";
import { makeMcpHttpApp, serveMcp } from "./http.js";
import {
  getOrCreateOwnedAgentKey,
  listOwnedAgentKeys,
  localOwner,
  type Owner,
  serverAgentOwner,
} from "./ownership.js";
import { makeAgentMcpApp } from "./server.js";
import { SocialHarness } from "./social-harness.js";
import { makeWebhookEvents, webhookHttpClientLayer } from "./webhook.js";

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
      yield* checkDatabase;
      if (settings.authentication.kind === "local") {
        yield* getOrCreateOwnedAgentKey(localOwner, agentName).pipe(
          Effect.catchTag("OwnershipError", () =>
            refused(
              "database",
              "This database's agent has another name than agentName in the settings, or another agent has that name. Set agentName to the name the database was first started with, or prepare a new database.",
            ),
          ),
        );
      }
      const norms =
        settings.norms === undefined
          ? undefined
          : yield* daemonNorms(settings.norms);
      if (settings.serverAgentName !== undefined) {
        yield* getOrCreateOwnedAgentKey(
          serverAgentOwner,
          settings.serverAgentName,
        ).pipe(
          Effect.catchTag("OwnershipError", () =>
            refused(
              "database",
              "This database's server agent has another name than serverAgentName in the settings, or another agent has that name. Set serverAgentName to the name the database was first started with, or prepare a new database.",
            ),
          ),
        );
      }
      const appOfOwner = yield* makeAgentApps({
        settings,
        norms,
        serverAgentHeldNorms: yield* Ref.make(
          Option.none<Context.Tag.Service<typeof HeldNorms>>(),
        ),
        consumers: new Map(),
      });
      const app = yield* makeMcpHttpApp(
        settings.authentication,
        appOfOwner,
        serverAgentCredential,
      ).pipe(
        Effect.catchTag("AuthenticationRefused", () =>
          refused(
            "issuer",
            "The OAuth issuer did not answer with its configuration. Check authentication.issuer and that the daemon can reach it.",
          ),
        ),
      );
      yield* serveMcp(settings.port, app, settings.host).pipe(
        Effect.catchTag("ServeError", () =>
          refused(
            "listener",
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
 * One agent's role. In P2P mode every agent is a `peer`: it messages any
 * agent, alone or in a group, and checks its own messages. In client/server
 * mode every conversation is the `server-agent` and one `client`: the server
 * agent checks its messages and holds the norms, and a client checks nothing
 * of its own, reads the server agent's norms and talks only to it.
 */
function roleOf(
  settings: DaemonSettings,
  agentName: AgentName,
  hasNorms: boolean,
) {
  const { serverAgentName } = settings;
  if (serverAgentName === undefined) {
    return {
      kind: "peer",
      hideGroups: settings.hideGroups,
      hideCollectives: settings.hideCollectives,
      serverAgentAddress: Option.none(),
    } as const;
  }
  const isServerAgent = agentName === serverAgentName;
  return {
    kind: isServerAgent ? "server-agent" : "client",
    permits: serverAgentAndOne(serverAgentName),
    hideGroups: true,
    hideCollectives: true,
    serverAgentAddress: isServerAgent
      ? Option.none()
      : Option.some(MessageAddressInput.make(`agent:${serverAgentName}`)),
    withheldReply: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${hasNorms ? " Call read_norms to see which messages it accepts." : ""}`,
  } as const;
}

/** Whether `members` are the server agent and one other agent. */
function serverAgentAndOne(serverAgentName: AgentName) {
  return (members: readonly AgentCard[]) =>
    members.length === 2 &&
    members.some((member) => member.agentName === serverAgentName);
}

/** What every agent of the daemon is built from. */
interface DaemonResources {
  readonly settings: DaemonSettings;
  readonly norms?: DaemonNorms;
  /** The norms the server agent holds, once it has started; clients read them. */
  readonly serverAgentHeldNorms: Ref.Ref<
    Option.Option<Context.Tag.Service<typeof HeldNorms>>
  >;
  /** The latch each agent's consumer waits on, which a send to that agent opens. */
  readonly consumers: Map<string, Effect.Latch>;
}

/** Requests borrow a prepared app; their cancellation never owns its acquisition. */
function makeAgentApps(daemon: DaemonResources) {
  return Effect.gen(function* () {
    const daemonScope = yield* Effect.scope;
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
    return (owner: Owner) =>
      getOrCreateOwnedAgentKey(owner).pipe(
        Effect.flatMap(acquire),
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.interruptible,
        Effect.forkIn(daemonScope),
        Effect.flatMap((fiber) => Fiber.join(fiber).pipe(Effect.interruptible)),
      );
  }).pipe(Effect.withSpan("makeAgentApps"));
}

/**
 * The checks an agent runs. A peer and the server agent run the daemon's
 * message policy with the norms they hold. A client runs no policy of its own
 * and reads the server agent's norms.
 */
function firewallLayerFor(
  daemon: DaemonResources,
  roleKind: ReturnType<typeof roleOf>["kind"],
  agentKey: string,
) {
  const { settings, norms, serverAgentHeldNorms } = daemon;
  if (roleKind === "client") {
    const unchecked = Firewall.layer(agentKey);
    return norms === undefined
      ? unchecked
      : serverAgentNormsLayer(serverAgentHeldNorms).pipe(
          Layer.provideMerge(unchecked),
        );
  }
  const firewall = Firewall.layer(
    agentKey,
    settings.messagePolicy,
    norms?.chain,
    norms?.normDocuments,
  );
  return norms === undefined
    ? firewall
    : HeldNorms.layer(norms).pipe(Layer.provideMerge(firewall));
}

/** Norms a client reads: the ones the server agent holds, once it has started. */
function serverAgentNormsLayer(
  serverAgentHeldNorms: Ref.Ref<
    Option.Option<Context.Tag.Service<typeof HeldNorms>>
  >,
) {
  return Layer.succeed(
    HeldNorms,
    HeldNorms.of({
      read: () =>
        Ref.get(serverAgentHeldNorms).pipe(
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                ),
              onSome: (reader) => reader.read(),
            }),
          ),
        ),
    }),
  );
}

/** One agent's retained services and MCP app. */
function makeAgentApp(daemon: DaemonResources, agentKey: string) {
  const { settings, norms, serverAgentHeldNorms } = daemon;
  return Effect.gen(function* () {
    const identity = yield* Layer.build(Identity.layer(agentKey));
    const card = Context.get(identity, Identity).me;
    const role = roleOf(settings, card.agentName, norms !== undefined);
    const workers = yield* FiberSet.make();
    const stopped = yield* Deferred.make<undefined>();
    const policy = settings.messagePolicy?.policy;
    const services = yield* Layer.build(
      SocialHarness.layer(
        {
          agentKey,
          hideGroups: role.hideGroups,
          ...(role.kind === "server-agent"
            ? { withheldReply: role.withheldReply }
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
            ...(role.kind === "peer" ? {} : { permits: role.permits }),
          }),
        ),
        Layer.provideMerge(
          firewallLayerFor(daemon, role.kind, agentKey).pipe(
            Layer.provideMerge(Layer.succeedContext(identity)),
          ),
        ),
      ),
    );
    if (role.kind === "server-agent") {
      yield* Ref.set(
        serverAgentHeldNorms,
        Context.getOption(services, HeldNorms),
      );
    }
    const callbackHttp = yield* Layer.build(webhookHttpClientLayer);
    const subscription = yield* makeWebhookEvents(agentKey).pipe(
      Effect.provide(services),
      Effect.provideService(
        HttpClient.HttpClient,
        Context.get(callbackHttp, HttpClient.HttpClient),
      ),
    );
    const app = yield* makeAgentMcpApp({
      hideCollectives: role.hideCollectives,
      serverAgentAddress: role.serverAgentAddress,
      subscription,
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

/** Deployment prepares schema and encryption metadata before the daemon starts. */
export { prepareDatabase } from "../database/index.js";

/** This agent's card and the other agents of its daemon by name. */
export { Identity } from "../identity.js";
/** Message policy produces permitted content and owns its decision history. */
export { type ComposedMessagePolicy, Firewall } from "../firewall/service.js";
/** Trusted policy providers receive typed assessment requests at construction. */
export type { DecisionProvider, DecisionRequest } from "../firewall/index.js";
