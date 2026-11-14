#!/usr/bin/env node
/**
 * @file The `sharnessd` executable. `--help` prints the usage, no argument runs
 * the daemon until it stops, and any other arguments print the usage to standard
 * error. A startup failure prints one line. Every outcome but the usage and a
 * stopped daemon exits unsuccessfully.
 */
import { NodeRuntime } from "@effect/platform-node";
import { Console, Effect, Layer } from "effect";
import { processHelp } from "./harness/config.js";
import { daemonLayer } from "./harness/startup.js";

/** Printed for a startup failure that names no setting of its own. */
const UNEXPLAINED_FAILURE =
  "The daemon could not start. Check SHARNESS_CONFIG_FILE and SHARNESS_DATABASE_URL; run sharnessd --help for what each must hold.";

/** Prints one line to standard error and makes the process exit unsuccessfully. */
const exitUnsuccessfully = (line: string) =>
  Console.error(line).pipe(
    Effect.andThen(
      Effect.sync(() => {
        process.exitCode = 1;
      }),
    ),
  );

/** Reports a startup refusal whose message names the setting to change. */
const explained = (refusal: { readonly message: string }) =>
  exitUnsuccessfully(refusal.message);

const args = process.argv.slice(2);

NodeRuntime.runMain(
  Effect.suspend(() => {
    if (args.length === 1 && args[0] === "--help") {
      return Console.log(processHelp);
    }
    if (args.length === 0) {
      return Layer.launch(daemonLayer);
    }
    return exitUnsuccessfully(processHelp);
  }).pipe(
    Effect.catchTags({
      DaemonConfigurationError: explained,
      DatabaseAccessError: explained,
      DatabasePreparationError: explained,
    }),
    Effect.catchAll(() => exitUnsuccessfully(UNEXPLAINED_FAILURE)),
  ),
);
