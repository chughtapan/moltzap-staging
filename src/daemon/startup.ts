/** @file Opens the daemon's database and runs its agents independently of MCP connections. */
import { type FileSystem, HttpClient } from "@effect/platform";
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
  databaseConnection,
  databaseUrl,
} from "../database/index.js";
import { FirewallPolicyError } from "../firewall/index.js";
import { Firewall } from "../firewall/service.js";
import { type AgentCard, type AgentName, Identity } from "../identity.js";
import {
  clientToolDefinitions,
  textOnlyToolDefinitions,
  toolDefinitions,
} from "../mcp/tools.js";
import { type DaemonNorms, daemonNorms, HeldNorms } from "../norms/index.js";
import { packageVersion } from "../version.js";
import { SocialHarness } from "./agent.js";
import { makeMcpHttpApp, serveMcp } from "./listener.js";
import {
  getOrCreateOwnedAgentKey,
  listOwnedAgentKeys,
  localOwner,
  type Owner,
  serverAgentOwner,
} from "./owners.js";
import {
  type DaemonSettings,
  loadDaemonSettings,
  refused,
  spanExport,
} from "./settings.js";
import {
  type HarnessWebhookEvents,
  makeWebhookEvents,
  webhookHttpClientLayer,
} from "./subscriptions.js";
import { bind } from "./tool-server.js";

/**
 * One agent's role, and everything that follows from it. In P2P mode every
 * agent is a peer: it messages any agent, alone or in a group, and checks its
 * own messages. In client/server mode every conversation is the server agent
 * and one client: the server agent checks its messages and holds the norms,
 * and a client checks nothing of its own, reads the server agent's norms and
 * talks only to it.
 *
 * @internal
 */
export function roleOf(
  daemon: {
    readonly settings: Pick<
      DaemonSettings,
      "serverAgentName" | "hideGroups" | "hideCollectives" | "messagePolicy"
    >;
    readonly norms?: DaemonNorms;
    /** The norms the server agent holds, once it has started; clients read them. */
    readonly serverAgentHeldNorms: Ref.Ref<
      Option.Option<Context.Tag.Service<typeof HeldNorms>>
    >;
  },
  agentKey: string,
  agentName: AgentName,
) {
  const { settings, norms, serverAgentHeldNorms } = daemon;
  const { serverAgentName } = settings;
  const checked = () => {
    const firewall = Firewall.layer(
      agentKey,
      settings.messagePolicy,
      norms?.chain,
      norms?.normDocuments,
    );
    return norms === undefined
      ? firewall
      : HeldNorms.layer(norms).pipe(Layer.provideMerge(firewall));
  };
  if (serverAgentName === undefined) {
    return {
      hideGroups: settings.hideGroups,
      permits: undefined,
      withheldReply: undefined,
      firewall: checked(),
      started: () => Effect.void,
      tools: peerTools(settings.hideCollectives),
    };
  }
  const permits = serverAgentAndOne(serverAgentName);
  if (agentName === serverAgentName) {
    return {
      hideGroups: true,
      permits,
      withheldReply: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${norms === undefined ? "" : " Call read_norms to see which messages it accepts."}`,
      firewall: checked(),
      started: (services: Context.Context<never>) =>
        Ref.set(serverAgentHeldNorms, Context.getOption(services, HeldNorms)),
      tools: peerTools(true),
    };
  }
  const unchecked = Firewall.layer(agentKey);
  return {
    hideGroups: true,
    permits,
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
    const agentOfOwner = yield* runningAgents(settings, norms);
    const app = yield* makeMcpHttpApp(
      settings.authentication,
      agentOfOwner,
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
  }).pipe(Effect.scoped, Effect.provide(databaseConnection(url)));
}).pipe(
  Effect.withSpan("runDaemon"),
  Effect.provide(Layer.merge(fileSystem, spanExport)),
);

/** Whether `members` are the server agent and one other agent. */
function serverAgentAndOne(serverAgentName: AgentName) {
  return (members: readonly AgentCard[]) =>
    members.length === 2 &&
    members.some((member) => member.agentName === serverAgentName);
}

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
 * The tools of a peer and of the server agent: each addresses any agent and
 * owns its message policy. `hideCollectives` leaves collective requests and
 * responses out of send_message, which then sends text only.
 */
function peerTools(hideCollectives: boolean) {
  return Effect.fnUntraced(function* (subscription: HarnessWebhookEvents) {
    const harness = yield* SocialHarness;
    const firewall = yield* Firewall;
    const policyConfigured = firewall
      .readPolicy()
      .pipe(Effect.match({ onFailure: () => false, onSuccess: () => true }));
    return [
      yield* statusTool(subscription),
      hideCollectives
        ? bind(
            "send_message",
            textOnlyToolDefinitions.send_message,
            harness.send,
          )
        : bind("send_message", toolDefinitions.send_message, harness.send),
      bind(
        "receive_messages",
        toolDefinitions.receive_messages,
        harness.receive,
      ),
      bind(
        "list_conversations",
        toolDefinitions.list_conversations,
        harness.listConversations,
      ),
      bind(
        "read_conversation",
        toolDefinitions.read_conversation,
        harness.readHistory,
      ),
      bind(
        "read_policy",
        toolDefinitions.read_policy,
        () => firewall.readPolicy(),
        policyConfigured,
      ),
      bind(
        "update_policy",
        toolDefinitions.update_policy,
        firewall.updatePolicy,
        policyConfigured,
      ),
      bind(
        "read_firewall_decisions",
        toolDefinitions.read_firewall_decisions,
        firewall.readDecisions,
      ),
      ...(yield* normsTool(toolDefinitions.read_norms)),
    ];
  });
}

/**
 * The tools of a client in client/server mode. It sends to and reads its
 * conversation with the server agent, and names neither.
 */
function clientTools(serverAgentAddress: MessageAddressInput) {
  return Effect.fnUntraced(function* (subscription: HarnessWebhookEvents) {
    const harness = yield* SocialHarness;
    return [
      yield* statusTool(subscription),
      bind("send_message", clientToolDefinitions.send_message, ({ text }) =>
        harness.send({ to: serverAgentAddress, text }),
      ),
      bind(
        "receive_messages",
        toolDefinitions.receive_messages,
        harness.receive,
      ),
      bind(
        "read_conversation",
        clientToolDefinitions.read_conversation,
        (page) => harness.readHistory({ ...page, address: serverAgentAddress }),
      ),
      ...(yield* normsTool(clientToolDefinitions.read_norms)),
    ];
  });
}

function statusTool(subscription: HarnessWebhookEvents) {
  return Effect.map(Identity, (identity) =>
    bind("status", toolDefinitions.status, () =>
      Effect.map(subscription.status, (status) => ({
        agentId: identity.me.agentId,
        agentName: identity.me.agentName,
        subscription: status,
      })),
    ),
  );
}

/**
 * `read_norms` lists the norms held: this agent's, or for a client the server
 * agent's. An agent that holds none has no such tool.
 */
function normsTool(
  definition: (
    | typeof toolDefinitions
    | typeof clientToolDefinitions
  )["read_norms"],
) {
  return Effect.gen(function* () {
    const heldNorms = yield* Effect.serviceOption(HeldNorms);
    if (Option.isNone(heldNorms)) {
      return [];
    }
    const read = heldNorms.value.read().pipe(
      Effect.map((held) => ({
        norms: held.map((norm) => ({
          name: norm.name,
          description: norm.description,
          ...(norm.protocolName === undefined
            ? {}
            : { protocol: norm.protocolName }),
          document: norm.document,
        })),
      })),
    );
    return [bind("read_norms", definition, () => read)];
  });
}

/**
 * The daemon's running agents. Every saved agent starts with the daemon, and
 * the returned function gives an owner's agent, starting it on that owner's
 * first request.
 */
function runningAgents(settings: DaemonSettings, norms?: DaemonNorms) {
  return Effect.gen(function* () {
    const daemonScope = yield* Effect.scope;
    const sql = yield* SqlClient.SqlClient;
    const daemon = {
      settings,
      norms,
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
 * them its tools, its Events calls and what completes when its workers stop.
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
    const workers = yield* FiberSet.make();
    const stopped = yield* Deferred.make<undefined>();
    const policy = daemon.settings.messagePolicy?.policy;
    const services = yield* Layer.build(
      SocialHarness.layer(
        {
          agentKey,
          hideGroups: role.hideGroups,
          withheldReply: role.withheldReply,
          maximumHistoryRecords: policy?.maximumHistoryRecords,
          maximumConcurrentChecks: policy?.maximumConcurrentChecks,
        },
        workers,
      ).pipe(
        Layer.provideMerge(
          Communication.layer({
            agentKey,
            card,
            consumers,
            maximumHistoryRecords: policy?.maximumHistoryRecords,
            permits: role.permits,
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
      .tools(subscription)
      .pipe(Effect.provide(services));
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
    return { tools, events: subscription, stopped };
  }).pipe(Effect.withSpan("startAgent"));
}

/** Deployment prepares schema and encryption metadata before the daemon starts. */
export { prepareDatabase } from "../database/index.js";
