/** @file Exercises NanoClaw's native inbox and sends against a daemon container. */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod } from "node:fs/promises";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  type AgentAccess,
  type DaemonAccess,
  openDaemon,
} from "../promise-fixture.ts";
import { startDaemonContainer } from "./daemon-container.ts";

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
/** A client and the server agent NanoClaw runs as. */
interface Pair {
  readonly caller: AgentAccess;
  readonly target: AgentAccess;
}

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

describe("the NanoClaw agent image on a real daemon", { skip: SKIP }, () => {
  test(
    "routes NanoClaw inbound and outbound through its native host boundaries",
    { timeout: CASE_TIMEOUT_MILLIS },
    () => withPair(routesThroughNativeBoundaries),
  );
});

async function routesThroughNativeBoundaries({
  caller,
  target,
}: Pair): Promise<undefined> {
  await caller.send({ text: INBOUND });
  const callerReceived = caller.next(2);
  await runProbe(
    target,
    [
      { to: caller.address, text: TOOL_REPLY },
      { to: caller.address, text: FINAL_REPLY, final: true },
    ],
    { platformId: caller.address, sender: caller.address, text: INBOUND },
  );

  const replies = [textContent(TOOL_REPLY), textContent(FINAL_REPLY)];
  const received = await callerReceived;
  for (const item of received) {
    assert.equal(item.kind, "message");
    assert.deepEqual(reported(item.message.author), reported(target.agent));
  }
  assert.deepEqual(
    inAnyOrder(received.map((item) => item.message.content)),
    inAnyOrder(replies),
  );
  const exchange = inAnyOrder([textContent(INBOUND), ...replies]);
  assert.deepEqual(inAnyOrder(await caller.history()), exchange);
  assert.deepEqual(inAnyOrder(await target.history(caller)), exchange);
}

/** The values as JSON text, sorted: the two replies arrive in either order. */
function inAnyOrder(values: readonly unknown[]): readonly string[] {
  return values
    .map((value) => JSON.stringify(value))
    .toSorted((left, right) => left.localeCompare(right));
}

/** Ordinary messages retain their text parts unchanged. */
function textContent(text: string): readonly object[] {
  return [{ type: "text", text }];
}

/** The fields of an agent's card that `status` reports, which an endpoint holds. */
function reported({ agentId, agentName }: AgentAccess["agent"]) {
  return { agentId, agentName };
}

/**
 * Run the probe in a container of the image under test, connected to the
 * daemon at `mcp` as the agent whose credential file it reads, and require
 * that the first delivery pass completed every send, leaving none for a
 * retry.
 */
async function runProbe(
  daemon: Pick<AgentAccess, "mcp" | "mcpCredentialFile">,
  destinations: readonly Destination[],
  inbound: ExpectedInbound,
): Promise<undefined> {
  await chmod(daemon.mcpCredentialFile, 0o644);
  const { stdout } = await executeFile(
    "docker",
    [
      "run",
      "--rm",
      "--pull=never",
      `--network=container:sharness-daemon-${daemon.mcp.port}`,
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

function withPair(
  body: (endpoints: Pair) => Promise<undefined>,
): Promise<undefined> {
  return withDaemon(async (signIn) => {
    const [target, caller] = await Promise.all([signIn(), signIn("caller")]);
    await body({ caller, target });
    return undefined;
  });
}

/**
 * Start a daemon container, run `body` with a function that signs the server
 * agent's owner or a named client's in to it, and stop the daemon however
 * `body` ends.
 */
async function withDaemon(
  body: (signIn: DaemonAccess["signIn"]) => Promise<undefined>,
): Promise<undefined> {
  const daemon = await openDaemon(startDaemonContainer);
  try {
    await body(daemon.signIn);
  } finally {
    await daemon.close();
  }
}

function asRecord(value: unknown): Readonly<Record<string, unknown>> {
  return isRecord(value) ? value : {};
}

/** Rejects null and arrays before reading a JSON object. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
