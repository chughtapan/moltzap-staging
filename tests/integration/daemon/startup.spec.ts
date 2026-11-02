/** @file Operator feedback through the installed process interface. */

import { Command } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { expect } from "vitest";
import { acquireOtlpReceiver } from "../observability/otlp-receiver.js";

const binary = fileURLToPath(
  new URL("../../../packages/social-harness/bin/sharnessd", import.meta.url),
);
const processName = "sharnessd";
const requiredVariable = "SHARNESS_STATE_DIRECTORY";
const defaultEntry = "SHARNESS_HISTORY_EXPORT: optional; default: unset";
const secret = "secret-path-value";
const invalidVariable = "SHARNESS_MCP_PORT";

/** An empty environment makes missing-variable behavior independent of the test runner. */
function runProcess(
  processArguments: readonly string[],
  environment: Readonly<Record<string, string>> = {},
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

// @agent-code-guard/regression-only: These process traces exercise the finite CLI help, validation and shutdown contracts.
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

const protocolRefusal =
  "The OTLP trace protocol, OTEL_EXPORTER_OTLP_TRACES_PROTOCOL or else OTEL_EXPORTER_OTLP_PROTOCOL, must be http/protobuf";

it(
  "refuses an OTLP trace protocol other than http/protobuf at startup",
  () =>
    Effect.gen(function* () {
      const receiver = yield* acquireOtlpReceiver;
      const result = yield* runProcess([], {
        OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint.origin,
        OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
      });
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain(protocolRefusal);
      expect(result.stdout).toBe("");
    }),
  15_000,
);

it(
  "refuses a traces-only OTLP protocol other than http/protobuf at startup",
  () =>
    Effect.gen(function* () {
      const receiver = yield* acquireOtlpReceiver;
      const result = yield* runProcess([], {
        OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint.origin,
        OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "grpc",
      });
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain(protocolRefusal);
    }),
  15_000,
);

it(
  "lets the traces protocol override the general one",
  () =>
    Effect.gen(function* () {
      const receiver = yield* acquireOtlpReceiver;
      const result = yield* runProcess([], {
        OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint.origin,
        OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "http/protobuf",
        OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
      });
      expect(result.stderr).not.toContain(protocolRefusal);
      expect(result.stderr).toContain(`${requiredVariable} is required`);
    }),
  15_000,
);

const endpointRefusal =
  "The OTLP traces URL, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or else OTEL_EXPORTER_OTLP_ENDPOINT, must be an http or https URL";

it(
  "refuses an OTLP endpoint that is not an http or https URL at startup",
  () =>
    Effect.gen(function* () {
      const result = yield* runProcess([], {
        OTEL_EXPORTER_OTLP_ENDPOINT: "collector:4318",
      });
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain(endpointRefusal);
    }),
  15_000,
);

it(
  "refuses a traces endpoint that is not an http or https URL at startup",
  () =>
    Effect.gen(function* () {
      const receiver = yield* acquireOtlpReceiver;
      const result = yield* runProcess([], {
        OTEL_EXPORTER_OTLP_ENDPOINT: receiver.endpoint.origin,
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "collector:4318/v1/traces",
      });
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain(endpointRefusal);
    }),
  15_000,
);

it(
  "treats a blank OTLP endpoint as unset",
  () =>
    Effect.gen(function* () {
      const result = yield* runProcess([], {
        OTEL_EXPORTER_OTLP_ENDPOINT: " ",
        OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
      });
      expect(result.stderr).not.toContain(protocolRefusal);
      expect(result.stderr).toContain(`${requiredVariable} is required`);
    }),
  15_000,
);
