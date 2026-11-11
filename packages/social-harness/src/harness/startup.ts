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
  type Redacted,
  Ref,
  ScopedCache,
} from "effect";
import { MessageAddressInput } from "../communication/messages.js";
import { Communication } from "../communication/service.js";
import { checkDatabase } from "../database/migrations.js";
import { databaseAccess, databaseConnection } from "../database/sql.js";
import { Firewall, FirewallPolicyError } from "../firewall/index.js";
import { Identity } from "../identity/service.js";
import { HeldNorms, type ServiceNorms, serviceNorms } from "../norms/index.js";
import { packageVersion } from "../version.js";
import {
  DaemonConfigurationError,
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
      const mode =
        settings.mode.kind === "p2p"
          ? yield* p2pMode(settings)
          : yield* clientServerMode(settings.mode, norms !== undefined);
      const selectApp = yield* makeAgentSelector({
        settings,
        unlockKey: access.unlockKey,
        mode,
        norms,
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

/** A startup refusal that names the setting to change. */
const refused = (message: string) =>
  new DaemonConfigurationError({ reason: "document", message });

/** Marks the database as serving `mode`, or refuses a database that serves the other. */
function claimDatabase(mode: DaemonSettings["mode"]["kind"]) {
  return Communication.claim(mode).pipe(
    Effect.catchIf(
      (error) => error.reason === "membership-invalid",
      () =>
        refused(
          `This database belongs to a daemon in the other mode, not ${mode}. Set mode to the one the database was first started with, or prepare a new database.`,
        ),
    ),
  );
}

/**
 * Every agent is a peer: it runs the daemon's own firewall and may message
 * any other agent on the daemon, alone or in a group.
 */
function p2pMode(settings: DaemonSettings) {
  return Effect.as(claimDatabase("p2p"), {
    agent: () => ({
      communication: Communication.layer,
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
 * The agent named in the settings is the server agent, created here if it does
 * not exist. It runs the daemon's firewall and holds its norms. Every other
 * agent is a client: it exchanges messages only with the server agent, runs
 * no check of its own, and reads the server agent's norms.
 */
function clientServerMode(
  mode: Extract<DaemonSettings["mode"], { readonly kind: "client-server" }>,
  hasNorms: boolean,
) {
  return Effect.gen(function* () {
    const { serverAgentName } = mode;
    yield* claimDatabase("client-server");
    yield* getOrCreateOwnedAgent(serverAgentOwner, serverAgentName).pipe(
      Effect.catchTag("OwnershipError", () =>
        refused(
          `This database's server agent is not named ${serverAgentName}, or another agent has that name. Set serverAgentName to the name the database was first started with, or prepare a new database.`,
        ),
      ),
    );
    const serverNorms = yield* Ref.make(
      Option.none<Context.Tag.Service<typeof HeldNorms>>(),
    );
    const communication = (
      options: Parameters<typeof Communication.layer>[0],
    ) =>
      Communication.layer({
        ...options,
        permits: (members) =>
          members.length === 2 &&
          members.some((member) => member.agentName === serverAgentName),
      });
    const server = {
      communication,
      checks: <Own>({ own }: { readonly own: Own }) => own,
      harness: {
        hideGroups: true,
        withheldReply: hasNorms
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
        hasNorms
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
  /** Opens the webhook secrets sealed in the database. */
  readonly unlockKey: Redacted.Redacted<Uint8Array>;
  readonly mode: Mode;
  readonly norms?: ServiceNorms;
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
    const identity = yield* Layer.build(Identity.layer(agentKey));
    const agent = mode.agent(Context.get(identity, Identity).me.agentName);
    const workers = yield* FiberSet.make();
    const stopped = yield* Deferred.make<undefined>();
    const policy = settings.messagePolicy?.policy;
    const services = yield* Layer.build(
      SocialHarness.layer(
        {
          ...agent.harness,
          agentKey,
          communication: (card) =>
            agent.communication({
              agentKey,
              card,
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
            .pipe(Layer.provideMerge(Layer.succeedContext(identity))),
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
/** This agent's card and the other agents of its daemon by name. */
export { Identity } from "../identity/service.js";
/** Message delivery, questions and ordered history. */
export { Communication } from "../communication/service.js";
/** Message policy produces permitted content and owns its decision history. */
export { Firewall, type FirewallOptions } from "../firewall/index.js";
/** Trusted policy providers receive typed assessment requests at construction. */
export type { DecisionProvider, DecisionRequest } from "../firewall/index.js";
