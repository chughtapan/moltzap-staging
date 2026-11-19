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
  ScopedCache,
} from "effect";
import type { CompiledContract } from "../firewall/index.js";
import { Communication } from "../communication/service.js";
import {
  databaseConnection,
  databaseUrl,
  prepareDatabase,
} from "../database.js";
import { Firewall } from "../firewall/service.js";
import { type AgentName, Identity } from "../identity.js";
import { AgentAddress } from "../mcp/tools.js";
import { packageVersion } from "../version.js";
import { SocialHarness } from "./agent.js";
import { makeMcpHttpApp, serveMcp } from "./listener.js";
import {
  getOrCreateOwnedAgentKey,
  identityLayer,
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
import { clientTools, serverAgentTools } from "./tool-server.js";

/**
 * One agent's role, and everything that follows from it. Every conversation
 * is the server agent and one client: the server agent checks its messages and holds the
 * contracts, and a client checks nothing of its own, reads the server agent's
 * norms through `contractsOf` and talks only to it.
 */
function roleOf(
  settings: Pick<
    DaemonSettings,
    "authentication" | "serverAgent" | "messagePolicy" | "norms"
  >,
  agentKey: string,
  agentName: AgentName,
  contractsOf: ContractsOf,
) {
  const { norms, serverAgent } = settings;
  if (agentName === serverAgent.name) {
    return {
      withheldReply: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${norms === undefined ? "" : " Call read_norms to see which messages it accepts."}`,
      firewall: Firewall.layer(
        agentKey,
        settings.messagePolicy,
        norms?.contracts,
      ),
      tools: serverAgentTools(norms !== undefined),
    };
  }
  return {
    withheldReply: undefined,
    firewall: Firewall.layer(agentKey),
    tools: clientTools(
      AgentAddress.make(`agent:${serverAgent.name}`),
      norms === undefined
        ? undefined
        : contractsOf({
            issuer: settings.authentication.issuer,
            subject: serverAgent.subject,
          }),
    ),
  };
}

/** The contracts an owner's running agent holds, starting the agent if it is not running. */
type ContractsOf = (owner: Owner) => Effect.Effect<readonly CompiledContract[]>;

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
    yield* prepareDatabase;
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
    const agentOfOwner = yield* runningAgents(settings);
    const app = yield* makeMcpHttpApp(
      settings.authentication,
      agentOfOwner,
    ).pipe(
      Effect.catchTag("AuthenticationRefused", () =>
        refused(
          `The OAuth issuer ${settings.authentication.issuer} did not answer with its configuration. Check authentication.issuer and that the daemon can reach it.`,
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

/**
 * The daemon's running agents. Every saved agent starts with the daemon, and
 * the returned function gives an owner's agent, starting it on that owner's
 * first request.
 */
function runningAgents(settings: DaemonSettings) {
  return Effect.gen(function* () {
    const daemonScope = yield* Effect.scope;
    const sql = yield* SqlClient.SqlClient;
    const consumers = new Map<string, Effect.Latch>();
    /** An agent that fails to start is not kept, so the next request for it starts it again. */
    const cache = yield* ScopedCache.makeWith({
      capacity: Number.MAX_SAFE_INTEGER,
      lookup: (agentKey: string) =>
        startAgent(settings, consumers, agentKey, contractsOf),
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
    /** An agent that cannot start ends the read as a call the daemon could not complete. */
    function contractsOf(
      owner: Owner,
    ): Effect.Effect<readonly CompiledContract[]> {
      return agentOfOwner(owner).pipe(
        Effect.flatMap((agent) => agent.readContracts()),
        Effect.orDie,
      );
    }
    return agentOfOwner;
  }).pipe(Effect.withSpan("runningAgents"));
}

/**
 * Starts one agent: its services, which run until the scope closes, and from
 * them its tools, its Events calls and the read of the contracts it holds.
 * `consumers` holds the latch each agent's consumer waits on, which a send to
 * that agent opens.
 */
function startAgent(
  settings: DaemonSettings,
  consumers: Map<string, Effect.Latch>,
  agentKey: string,
  contractsOf: ContractsOf,
) {
  return Effect.gen(function* () {
    const identity = yield* Layer.build(identityLayer(agentKey));
    const card = Context.get(identity, Identity).me;
    const role = roleOf(settings, agentKey, card.agentName, contractsOf);
    const services = yield* Layer.build(
      SocialHarness.layer({
        agentKey,
        withheldReply: role.withheldReply,
        maximumHistoryRecords: settings.maximumHistoryRecords,
        maximumConcurrentChecks:
          settings.messagePolicy?.policy.maximumConcurrentChecks,
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
    return {
      tools,
      events: subscription,
      readContracts: Context.get(services, Firewall).readContracts,
    };
  }).pipe(Effect.withSpan("startAgent"));
}
