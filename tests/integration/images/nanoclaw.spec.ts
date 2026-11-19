/** @file Exercises NanoClaw's native inbox and sends against a daemon container. */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { textContent } from "../daemon-fixture.ts";
import { type AgentAccess, openDaemon } from "./daemon-container.ts";

const IMAGE = process.env.NANOCLAW_INTEGRATION_IMAGE ?? "";
const SKIP =
  IMAGE.length === 0 || process.env.DAEMON_INTEGRATION_IMAGE === undefined
    ? "set NANOCLAW_INTEGRATION_IMAGE and DAEMON_INTEGRATION_IMAGE to built images"
    : false;
const CASE_TIMEOUT_MILLIS = 300_000;
const PROBE_TIMEOUT_MILLIS = 180_000;
const PROBE_OUTPUT_BYTES = 4 * 1024 * 1024;
/** The prefix of the probe's one report line on stdout. */
const PROBE_REPORT_PREFIX = "nanoclaw probe report: ";
const PROBE = fileURLToPath(new URL("nanoclaw-probe.ts", import.meta.url));
/** Where the probe is mounted inside the image's own file system. */
// eslint-disable-next-line sonarjs/publicly-writable-directories -- a path inside the disposable container, not on the host.
const PROBE_IN_IMAGE = "/tmp/integration-probe.ts";
const executeFile = promisify(execFile);
const TOOL_REPLY = "from NanoClaw send_message";
const FINAL_REPLY = "from NanoClaw final output";
const INBOUND = "hello through the native NanoClaw inbox";
/** The plain message the probe waits for in NanoClaw's native inbox. */
interface ExpectedInbound {
  readonly platformId: string;
  readonly sender: string;
  readonly text: string;
}

/** One send the probe queues; `final` sends through the final output. */
interface Destination {
  readonly to: string;
  readonly text: string;
  readonly final?: true;
}

/** What the probe saw of NanoClaw delivering the queued sends. */
interface ProbeReport {
  /** Rows the first delivery pass completed. */
  readonly delivered: number;
  /** Rows still due after that pass, which the second pass sends again. */
  readonly retried: number;
}

test(
  "the NanoClaw agent image routes inbound and outbound through NanoClaw's native host boundaries on a real daemon",
  { skip: SKIP, timeout: CASE_TIMEOUT_MILLIS },
  async () => {
    const daemon = await openDaemon();
    try {
      const target = await daemon.signIn();
      const caller = await daemon.signIn("caller");
      await caller.send({ text: INBOUND });
      const callerReceived = caller.next(2);
      await runProbe(
        target,
        daemon.container,
        [
          { to: caller.address, text: TOOL_REPLY },
          { to: caller.address, text: FINAL_REPLY, final: true },
        ],
        { platformId: caller.address, sender: caller.address, text: INBOUND },
      );

      const received = await callerReceived;
      for (const message of received) {
        assert.equal(message.author.agentId, target.agent.agentId);
      }
      assert.deepEqual(
        inAnyOrder(received.map((message) => message.content)),
        inAnyOrder([textContent(TOOL_REPLY), textContent(FINAL_REPLY)]),
      );
    } finally {
      await daemon.close();
    }
  },
);

/** The values as JSON text, sorted: the two replies arrive in either order. */
function inAnyOrder(values: readonly unknown[]): readonly string[] {
  return values
    .map((value) => JSON.stringify(value))
    .toSorted((left, right) => left.localeCompare(right));
}

/**
 * Run the probe in a container of the image under test, on the daemon
 * container's network and connected to the daemon at `mcp` as the agent whose
 * credential file it reads, and require that the first delivery pass
 * completed every send, leaving none for a retry.
 */
async function runProbe(
  daemon: Pick<AgentAccess, "mcp" | "mcpCredentialFile">,
  daemonContainer: string,
  destinations: readonly Destination[],
  inbound: ExpectedInbound,
): Promise<undefined> {
  const { stdout } = await executeFile(
    "docker",
    [
      "run",
      "--rm",
      "--pull=never",
      `--network=container:${daemonContainer}`,
      "--entrypoint=node",
      "--env",
      `SHARNESS_MCP_URL=${daemon.mcp.href}`,
      "--env",
      "SHARNESS_MCP_CREDENTIAL_FILE=/run/sharness-mcp-credential",
      "--volume",
      `${daemon.mcpCredentialFile}:/run/sharness-mcp-credential:ro`,
      "--env",
      `NANOCLAW_DESTINATIONS_JSON=${JSON.stringify(destinations)}`,
      "--env",
      `NANOCLAW_INBOUND_JSON=${JSON.stringify(inbound)}`,
      "--volume",
      `${PROBE}:${PROBE_IN_IMAGE}:ro`,
      IMAGE,
      PROBE_IN_IMAGE,
    ],
    { maxBuffer: PROBE_OUTPUT_BYTES, timeout: PROBE_TIMEOUT_MILLIS },
  );
  assert.deepEqual(decodeProbeReport(stdout), {
    delivered: destinations.length,
    retried: 0,
  });
}

/** The report on the probe's last report line, refusing any other shape. */
function decodeProbeReport(stdout: string): ProbeReport {
  const line = stdout
    .split("\n")
    .findLast((each) => each.startsWith(PROBE_REPORT_PREFIX));
  const { delivered, retried } = asRecord(
    JSON.parse(line?.slice(PROBE_REPORT_PREFIX.length) ?? "null"),
  );
  if (typeof delivered !== "number" || typeof retried !== "number") {
    throw new Error(`the probe printed no report:\n${stdout}`);
  }
  return { delivered, retried };
}

function asRecord(value: unknown): Readonly<Record<string, unknown>> {
  return isRecord(value) ? value : {};
}

/** Rejects null and arrays before reading a JSON object. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
