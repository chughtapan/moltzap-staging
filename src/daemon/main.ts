#!/usr/bin/env node
/**
 * @file The `sharnessd` executable. `--help` prints the usage, no argument runs
 * the daemon until it stops, and any other arguments print the usage to standard
 * error. Running the daemon opens its database, installs the server agent's
 * firewall and serves every owner's agent. A startup failure prints one line.
 * Every outcome but the usage and a stopped daemon exits unsuccessfully.
 */
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node";
import { diag, DiagConsoleLogger, DiagLogLevel } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { Config, Console, Effect, Layer, Option } from "effect";
import { makeFirewall } from "../firewall.js";
import { structureCheck } from "../norms/index.js";
import { getOrCreateOwnedAgent, openDaemon } from "./agent.js";
import { makeMcpHttpApp, serveMcp } from "./server.js";
import { loadDaemonSettings, refused } from "./settings.js";
import { databaseConnection, databaseUrl, prepareDatabase } from "./store.js";
import { packageVersion } from "./tools.js";

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
 * Installs a process-global stderr logger for OpenTelemetry warnings and failures.
 * The logger remains installed after export closes.
 */
const exportDiagnostics = Layer.effectDiscard(
  Effect.sync(() =>
    diag.setLogger(new DiagConsoleLogger(), {
      logLevel: DiagLogLevel.WARN,
      suppressOverrideMessage: true,
    }),
  ),
);

/**
 * Exports spans as OTLP/HTTP protobuf when `OTEL_EXPORTER_OTLP_ENDPOINT` is
 * set, under the service name `OTEL_SERVICE_NAME` gives, or `sharnessd`.
 * OpenTelemetry reads the endpoint and its remaining exporter, batch and
 * resource settings from the standard variables, and warns of one it cannot use.
 */
const spanExport = Layer.unwrapEffect(
  Effect.gen(function* () {
    const endpoint = yield* Config.option(
      Config.nonEmptyString("OTEL_EXPORTER_OTLP_ENDPOINT"),
    );
    if (Option.isNone(endpoint)) {
      return Layer.empty;
    }
    const serviceName = yield* Config.nonEmptyString("OTEL_SERVICE_NAME").pipe(
      Config.withDefault("sharnessd"),
    );
    return NodeSdk.layer(() => ({
      resource: { serviceName, serviceVersion: packageVersion },
      spanProcessor: new BatchSpanProcessor(new OTLPTraceExporter()),
    })).pipe(Layer.discard, Layer.provide(exportDiagnostics));
  }).pipe(Effect.orDie),
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
    const daemon = yield* openDaemon({
      serverAgent,
      firewall: makeFirewall({
        ...(settings.messagePolicy === undefined
          ? {}
          : { source: settings.messagePolicy }),
        me: serverAgent,
        historyLimit: settings.maximumHistoryRecords ?? 256,
        notice: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${norms === undefined ? "" : " Call read_norms to see which messages it accepts."}`,
        ...(norms === undefined
          ? {}
          : { structureCheck: structureCheck(norms) }),
      }),
      ...(norms === undefined ? {} : { norms }),
    });
    const app = yield* makeMcpHttpApp(authentication, daemon).pipe(
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
