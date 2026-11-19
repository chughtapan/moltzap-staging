#!/usr/bin/env node
/**
 * @file The `sharnessd` executable. `--help` prints the usage, no argument runs
 * the daemon until it stops, and any other arguments print the usage to standard
 * error. A startup failure prints one line. Every outcome but the usage and a
 * stopped daemon exits unsuccessfully.
 */
import { NodeRuntime } from "@effect/platform-node";
import { Console, Effect } from "effect";
import { runDaemon } from "./daemon/startup.js";

/** What `--help` prints: the variables the daemon reads at every start. */
const HELP_MESSAGE = [
  "Usage: sharnessd [--help]",
  "",
  "SHARNESS_CONFIG_FILE: JSON daemon settings file",
  "SHARNESS_DATABASE_URL: PostgreSQL connection URL",
  "SHARNESS_DECISION_PROVIDER_KEY: API key of the message policy's decision provider",
  "Deployment prepares the database schema before startup.",
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
