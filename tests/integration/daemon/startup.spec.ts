/** @file What the installed `sharnessd` binary prints and how it exits before it serves. */

import { Command } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { expect } from "vitest";

const binary = fileURLToPath(new URL("../../../dist/main.js", import.meta.url));
const requiredVariable = "SHARNESS_DATABASE_URL";
const secret = "secret-path-value";

/** An empty environment makes missing-variable behavior independent of the test runner. */
function runProcess(
  processArguments: readonly string[],
  environment: Readonly<Record<string, string>>,
) {
  return Effect.gen(function* () {
    const child = yield* Command.start(
      Command.make(
        "env",
        "-i",
        `PATH=${dirname(process.execPath)}:/usr/bin:/bin`,
        ...Object.entries(environment).map(
          ([name, value]) => `${name}=${value}`,
        ),
        process.execPath,
        binary,
        ...processArguments,
      ),
    );
    return yield* Effect.all(
      {
        exitCode: child.exitCode,
        stdout: Stream.decodeText(child.stdout).pipe(
          Stream.runFold("", (output, chunk) => output + chunk),
        ),
        stderr: Stream.decodeText(child.stderr).pipe(
          Stream.runFold("", (output, chunk) => output + chunk),
        ),
      },
      { concurrency: 3 },
    );
  }).pipe(Effect.timeout("10 seconds"), Effect.provide(NodeContext.layer));
}

/** A run of the binary, whether it succeeds, and what it prints on standard output when it does and on standard error when it does not. */
const runs: ReadonlyArray<{
  readonly behavior: string;
  readonly processArguments: readonly string[];
  readonly environment: Readonly<Record<string, string>>;
  readonly succeeds: boolean;
  readonly says: readonly string[];
}> = [
  {
    behavior: "prints its usage and the variables it reads, and succeeds",
    processArguments: ["--help"],
    environment: { [requiredVariable]: secret },
    succeeds: true,
    says: [
      "Usage: sharnessd [--help]",
      `${requiredVariable}: PostgreSQL connection URL`,
      "SHARNESS_CONFIG_FILE: JSON daemon settings file",
    ],
  },
  {
    behavior: "names a missing variable and fails",
    processArguments: [],
    environment: {},
    succeeds: false,
    says: [`Set ${requiredVariable}`],
  },
  {
    behavior: "fails on invalid configuration",
    processArguments: [],
    environment: { [requiredVariable]: secret },
    succeeds: false,
    says: [],
  },
];

it.each(runs)(
  "sharnessd $behavior, without echoing a supplied value",
  ({ processArguments, environment, succeeds, says }) =>
    Effect.gen(function* () {
      const result = yield* runProcess(processArguments, environment);
      const [spoken, silent] = succeeds
        ? [result.stdout, result.stderr]
        : [result.stderr, result.stdout];

      expect(result.exitCode === 0).toBe(succeeds);
      expect(says.filter((text) => !spoken.includes(text))).toEqual([]);
      expect(silent).toBe("");
      expect(result.stdout + result.stderr).not.toContain(secret);
    }),
  15_000,
);
