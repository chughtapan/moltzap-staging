/** @file Opens the daemon's database and runs its agents independently of MCP connections. */
import { type FileSystem, HttpClient } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  Console,
  Context,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  ScopedCache,
} from "effect";
import { Communication } from "../communication/service.js";
import {
  checkDatabase,
  databaseConnection,
  databaseUrl,
} from "../database/index.js";
import { FirewallPolicyError } from "../firewall/index.js";
import { Firewall } from "../firewall/service.js";
import { type AgentName, Identity } from "../identity.js";
import { MessageAddressInput } from "../mcp/tools.js";
import { HeldNorms } from "../norms/index.js";
import { packageVersion } from "../version.js";
import { SocialHarness } from "./agent.js";
import { makeMcpHttpApp, serveMcp } from "./listener.js";
import {
  getOrCreateOwnedAgentKey,
  listOwnedAgentKeys,
  type Owner,
} from "./owners.js";
import {
  type DaemonSettings,
  loadDaemonSettings,
  refused,
  spanExport,
} from "./settings.js";
import { makeWebhookEvents, webhookHttpClientLayer } from "./subscriptions.js";
import { clientTools, peerTools } from "./tool-server.js";

/**
 * One agent's role, and everything that follows from it. In P2P mode every
 * agent is a peer: it messages any agent, alone or in a group, and checks its
 * own messages. In client/server mode every conversation is the server agent
 * and one client: the server agent checks its messages and holds the
 * contracts, and a client checks nothing of its own, reads the server agent's
 * norms and talks only to it.
 */
function roleOf(
  daemon: {
    readonly settings: Pick<
      DaemonSettings,
      "serverAgent" | "hideGroups" | "messagePolicy" | "norms"
    >;
    /** The norms the server agent holds, once it has started; clients read them. */
    readonly serverAgentHeldNorms: Ref.Ref<
      Option.Option<Context.Tag.Service<typeof HeldNorms>>
    >;
  },
  agentKey: string,
  agentName: AgentName,
) {
  const { settings, serverAgentHeldNorms } = daemon;
  const { norms } = settings;
  const serverAgentName = settings.serverAgent?.name;
  const checked = () => {
    const firewall = Firewall.layer(
      agentKey,
      settings.messagePolicy,
      norms?.contracts,
    );
    return norms === undefined
      ? firewall
      : HeldNorms.layer.pipe(Layer.provideMerge(firewall));
  };
  if (serverAgentName === undefined) {
    return {
      withheldReply: undefined,
      firewall: checked(),
      started: () => Effect.void,
      tools: peerTools(
        settings.messagePolicy !== undefined,
        !settings.hideGroups,
      ),
    };
  }
  if (agentName === serverAgentName) {
    return {
      withheldReply: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${norms === undefined ? "" : " Call read_norms to see which messages it accepts."}`,
      firewall: checked(),
      started: (services: Context.Context<never>) =>
        Ref.set(serverAgentHeldNorms, Context.getOption(services, HeldNorms)),
      tools: peerTools(settings.messagePolicy !== undefined, false),
    };
  }
  const unchecked = Firewall.layer(agentKey);
  return {
    withheldReply: undefined,
    firewall:
      norms === undefined
        ? unchecked
        : serverAgentNormsLayer(serverAgentHeldNorms).pipe(
            Layer.provideMerge(unchecked),
          ),
    started: () => Effect.void,
    tools: clientTools(MessageAddressInput.make(`agent:${serverAgentName}`)),
  };
}

/** The Node file system, typed by the interface the daemon reads files through. */
const fileSystem: Layer.Layer<FileSystem.FileSystem> = NodeFileSystem.layer;

/**
 * Runs the daemon until it is interrupted: restores saved agents and serves
 * authenticated requests to them. Fails if it cannot start.
 */
export const runDaemon = Effect.gen(function* () {
  const url = yield* databaseUrl;
  const settings = yield* loadDaemonSettings();
  return yield* Effect.gen(function* () {
    yield* checkDatabase;
    if (settings.serverAgent !== undefined) {
      yield* getOrCreateOwnedAgentKey(
        {
          issuer: settings.authentication.issuer,
          subject: settings.serverAgent.subject,
        },
        settings.serverAgent.name,
      ).pipe(
        Effect.catchTag("OwnershipError", () =>
          refused(
            "This database's server agent has another name than serverAgent.name in the settings, or another agent has that name. Set serverAgent.name to the name the database was first started with, or prepare a new database.",
          ),
        ),
      );
    }
    const agentOfOwner = yield* runningAgents(settings);
    const app = yield* makeMcpHttpApp(
      settings.authentication,
      agentOfOwner,
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
  }).pipe(Effect.scoped, Effect.provide(databaseConnection(url)));
}).pipe(
  Effect.withSpan("runDaemon"),
  Effect.provide(Layer.merge(fileSystem, spanExport)),
);

/** Norms a client reads: the ones the server agent holds, once it has started. */
function serverAgentNormsLayer(
  serverAgentHeldNorms: Parameters<typeof roleOf>[0]["serverAgentHeldNorms"],
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

/**
 * The daemon's running agents. Every saved agent starts with the daemon, and
 * the returned function gives an owner's agent, starting it on that owner's
 * first request.
 */
function runningAgents(settings: DaemonSettings) {
  return Effect.gen(function* () {
    const daemonScope = yield* Effect.scope;
    const sql = yield* SqlClient.SqlClient;
    const daemon = {
      settings,
      serverAgentHeldNorms: yield* Ref.make(
        Option.none<Context.Tag.Service<typeof HeldNorms>>(),
      ),
    };
    const consumers = new Map<string, Effect.Latch>();
    /** An agent that fails to start is not kept, so the next request for it starts it again. */
    const cache = yield* ScopedCache.makeWith({
      capacity: Number.MAX_SAFE_INTEGER,
      lookup: (agentKey: string) => startAgent(daemon, consumers, agentKey),
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
              Effect.annotateLogs({ agentKey, failure: error._tag }),
            ),
          ),
        ),
      { concurrency: 4, discard: true },
    ).pipe(Effect.forkScoped);
    /** The start runs in the daemon's scope and the request joins it, so a cancelled request cannot interrupt an agent that is starting. */
    const agentOfOwner = (owner: Owner) =>
      getOrCreateOwnedAgentKey(owner).pipe(
        Effect.flatMap(acquire),
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.interruptible,
        Effect.forkIn(daemonScope),
        Effect.flatMap((fiber) => Fiber.join(fiber).pipe(Effect.interruptible)),
      );
    return agentOfOwner;
  }).pipe(Effect.withSpan("runningAgents"));
}

/**
 * Starts one agent: its services, which run until the scope closes, and from
 * them its tools and its Events calls.
 * `consumers` holds the latch each agent's consumer waits on, which a send to
 * that agent opens.
 */
function startAgent(
  daemon: Parameters<typeof roleOf>[0],
  consumers: Map<string, Effect.Latch>,
  agentKey: string,
) {
  return Effect.gen(function* () {
    const identity = yield* Layer.build(Identity.layer(agentKey));
    const card = Context.get(identity, Identity).me;
    const role = roleOf(daemon, agentKey, card.agentName);
    const policy = daemon.settings.messagePolicy?.policy;
    const services = yield* Layer.build(
      SocialHarness.layer({
        agentKey,
        withheldReply: role.withheldReply,
        maximumHistoryRecords: policy?.maximumHistoryRecords,
        maximumConcurrentChecks: policy?.maximumConcurrentChecks,
      }).pipe(
        Layer.provideMerge(
          Communication.layer({
            agentKey,
            card,
            consumers,
          }),
        ),
        Layer.provideMerge(
          role.firewall.pipe(
            Layer.provideMerge(Layer.succeedContext(identity)),
          ),
        ),
      ),
    );
    yield* role.started(services);
    const callbackHttp = yield* Layer.build(webhookHttpClientLayer);
    const subscription = yield* makeWebhookEvents(agentKey).pipe(
      Effect.provide(services),
      Effect.provideService(
        HttpClient.HttpClient,
        Context.get(callbackHttp, HttpClient.HttpClient),
      ),
    );
    const tools = yield* role
      .tools(subscription.status)
      .pipe(Effect.provide(services));
    return { tools, events: subscription };
  }).pipe(Effect.withSpan("startAgent"));
}

/** Deployment prepares the schema before the daemon starts. */
export { prepareDatabase } from "../database/index.js";
