/** @file Opens the daemon's database, installs the server agent's firewall and serves every owner's agent. */
import type { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { Console, Context, Effect, Layer, PubSub } from "effect";
import {
  databaseConnection,
  databaseUrl,
  prepareDatabase,
} from "../database.js";
import { Firewall } from "../firewall/service.js";
import { Identity } from "../identity.js";
import { structureCheck } from "../norms/index.js";
import { packageVersion } from "../version.js";
import { getOrCreateOwnedAgent } from "./agent.js";
import { makeMcpHttpApp, serveMcp } from "./server.js";
import { loadDaemonSettings, refused, spanExport } from "./settings.js";

/** The Node file system, typed by the interface the daemon reads files through. */
const fileSystem: Layer.Layer<FileSystem.FileSystem> = NodeFileSystem.layer;

/**
 * Runs the daemon until it is interrupted: installs the server agent's
 * firewall and answers each authenticated request from its owner's agent.
 * Fails if it cannot start.
 */
export const runDaemon = Effect.gen(function* () {
  const url = yield* databaseUrl;
  const settings = yield* loadDaemonSettings();
  return yield* Effect.gen(function* () {
    yield* prepareDatabase;
    const { authentication, norms } = settings;
    const serverAgent = yield* getOrCreateOwnedAgent(
      { issuer: authentication.issuer, subject: settings.serverAgent.subject },
      settings.serverAgent.name,
    ).pipe(
      Effect.catchTag("OwnershipError", () =>
        refused(
          "This database's server agent has another name than serverAgent.name in the settings, or another agent has that name. Set serverAgent.name to the name the database was first started with, or prepare a new database.",
        ),
      ),
    );
    const firewall = yield* Layer.build(
      Firewall.layer(
        serverAgent.agentKey,
        settings.messagePolicy,
        norms && structureCheck(norms),
      ).pipe(Layer.provide(Layer.succeed(Identity, { me: serverAgent.card }))),
    );
    const app = yield* makeMcpHttpApp(authentication, {
      serverAgent,
      firewall: Context.get(firewall, Firewall),
      ...(norms === undefined ? {} : { norms }),
      historyLimit: settings.maximumHistoryRecords ?? 256,
      withheldReply: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${norms === undefined ? "" : " Call read_norms to see which messages it accepts."}`,
      turns: new Map(),
      readable: yield* PubSub.unbounded<string>(),
    }).pipe(
      Effect.catchTag("AuthenticationRefused", () =>
        refused(
          `The OAuth issuer ${authentication.issuer} did not answer with its configuration. Check authentication.issuer and that the daemon can reach it.`,
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
