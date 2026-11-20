#!/usr/bin/env node
/**
 * @file The `sharnessd` executable. `--help` prints the usage, no argument runs
 * the daemon until it stops, and any other arguments print the usage to standard
 * error. Running the daemon opens its database, installs the server agent's
 * firewall and serves every owner's agent. A startup failure prints one line.
 * Every outcome but the usage and a stopped daemon exits unsuccessfully.
 */
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node";
import { Console, Context, Effect, Layer, PubSub } from "effect";
import {
  getOrCreateOwnedAgent,
  loadDaemonSettings,
  makeMcpHttpApp,
  refused,
  serveMcp,
  spanExport,
} from "./daemon/index.js";
import {
  databaseConnection,
  databaseUrl,
  prepareDatabase,
} from "./database.js";
import { Firewall } from "./firewall/service.js";
import { Identity } from "./identity.js";
import { packageVersion } from "./version.js";

/** What `--help` prints: the variables the daemon reads at every start. */
const HELP_MESSAGE = [
  "Usage: sharnessd [--help]",
  "",
  "SHARNESS_CONFIG_FILE: JSON daemon settings file",
  "SHARNESS_DATABASE_URL: PostgreSQL connection URL",
  "SHARNESS_DECISION_PROVIDER_KEY: API key of the message policy's decision provider",
  "Standard OTEL_* variables configure telemetry.",
].join("\n");

/** Printed for a startup failure that names no setting of its own. */
const UNEXPLAINED_FAILURE =
  "The daemon could not start. Check SHARNESS_CONFIG_FILE and SHARNESS_DATABASE_URL; run sharnessd --help for what each must hold.";

/** Prints one line to standard error and makes the daemon exit unsuccessfully. */
const exitUnsuccessfully = (line: string) =>
  Console.error(line).pipe(
    Effect.andThen(
      Effect.sync(() => {
        process.exitCode = 1;
      }),
    ),
  );

/**
 * Runs the daemon until it is interrupted: installs the server agent's
 * firewall and answers each authenticated request from its owner's agent.
 * Fails if it cannot start.
 */
const runDaemon = Effect.gen(function* () {
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
        norms?.contracts,
      ).pipe(Layer.provide(Layer.succeed(Identity, { me: serverAgent.card }))),
    ).pipe(
      Effect.catchTag("FirewallPolicyError", () =>
        refused(
          "The message policy and the norms could not be installed together. Check messagePolicyFile and normsDirectory in the settings.",
        ),
      ),
    );
    const app = yield* makeMcpHttpApp(authentication, {
      serverAgent,
      firewall: Context.get(firewall, Firewall),
      hasNorms: norms !== undefined,
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
  Effect.provide(Layer.merge(NodeFileSystem.layer, spanExport)),
);

const args = process.argv.slice(2);

NodeRuntime.runMain(
  Effect.suspend(() => {
    if (args.length === 1 && args[0] === "--help") {
      return Console.log(HELP_MESSAGE);
    }
    if (args.length === 0) {
      return runDaemon;
    }
    return exitUnsuccessfully(HELP_MESSAGE);
  }).pipe(
    Effect.catchTags({
      DaemonConfigurationError: ({ message }) => exitUnsuccessfully(message),
      DatabaseUrlMissingError: ({ message }) => exitUnsuccessfully(message),
      DatabasePreparationError: ({ message }) => exitUnsuccessfully(message),
    }),
    Effect.catchAll(() => exitUnsuccessfully(UNEXPLAINED_FAILURE)),
  ),
);
