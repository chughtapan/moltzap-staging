/** @file Operator feedback through the installed process interface. */

import { Command } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { WIRE_VERSION } from "@moltzap/social-harness";
import { Effect, Stream } from "effect";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { expect } from "vitest";

const binary = fileURLToPath(
  new URL("../../../packages/registry/bin/sharness-registry", import.meta.url),
);
const packageRoot = fileURLToPath(
  new URL("../../../packages/registry", import.meta.url),
);
const processName = "sharness-registry";
const requiredVariable = "SHARNESS_REGISTRY_POSTGRESQL_URL";
const defaultEntry = 'SHARNESS_REGISTRY_HOST: optional; default: "127.0.0.1"';
const secret = "not a valid secret-value";
const invalidVariable = "SHARNESS_REGISTRY_ADMISSION_CREDENTIAL";

/** An empty environment makes missing-variable behavior independent of the test runner; `nodeArguments` selects the entry Node runs. */
function runProcess(
  processArguments: readonly string[],
  environment: Readonly<Record<string, string>> = {},
  nodeArguments: readonly string[] = ["--conditions=sharness-internal", binary],
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
        ...nodeArguments,
        ...processArguments,
      ).pipe(Command.workingDirectory(packageRoot)),
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

// @agent-code-guard/regression-only: These process regressions verify help, closed configuration failures, secret redaction, and native Node conditions.
it(
  "prints environment requirements and defaults without starting",
  () =>
    Effect.gen(function* () {
      const result = yield* runProcess(["--help"], {
        [invalidVariable]: secret,
      });
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain(`Usage: ${processName} [--help]`);
      expect(result.stdout).toContain(
        `${requiredVariable}: required; default: none`,
      );
      expect(result.stdout).toContain(defaultEntry);
      expect(result.stdout).not.toContain(secret);
      expect(result.stderr).toBe("");
    }),
  15_000,
);

it(
  "names missing environment variables and exits unsuccessfully",
  () =>
    Effect.gen(function* () {
      const result = yield* runProcess([]);
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain(`${requiredVariable} is required`);
      expect(result.stdout).toBe("");
    }),
  15_000,
);

it(
  "reports invalid configuration without echoing supplied values",
  () =>
    Effect.gen(function* () {
      const result = yield* runProcess([], { [invalidVariable]: secret });
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain(invalidVariable);
      expect(result.stderr + result.stdout).not.toContain(secret);
      expect(result.stdout).toBe("");
    }),
  15_000,
);

it(
  "reports a test-composition startup failure and exits unsuccessfully",
  () =>
    Effect.gen(function* () {
      const result = yield* runProcess([], {}, [
        "--conditions=sharness-test",
        "--conditions=sharness-internal",
        "--input-type=module",
        "-e",
        `import { startRegistry } from "@moltzap/registry/test"; startRegistry(${JSON.stringify({ emitted: WIRE_VERSION, live: [WIRE_VERSION], history: [WIRE_VERSION] })});`,
      ]);
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain(`${requiredVariable} is required`);
    }),
  15_000,
);
