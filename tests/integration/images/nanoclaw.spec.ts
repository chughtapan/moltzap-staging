/** @file Exercises NanoClaw's native inbox and sends against a daemon container. */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod } from "node:fs/promises";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { type AgentAccess, openDaemon } from "../promise-fixture.ts";
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
const DIRECT_REPLY = "direct from NanoClaw send_message";
const GROUP_REPLY = "group from NanoClaw final output";
const INBOUND = "hello through the native NanoClaw inbox";
/** Native image tests use the same public MCP fixture as other process consumers. */
type InboundItem = Awaited<ReturnType<AgentAccess["next"]>>[number];

/** A caller, the NanoClaw agent it talks to, and a third group member. */
interface Trio {
  readonly caller: AgentAccess;
  readonly target: AgentAccess;
  readonly peer: AgentAccess;
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
    () => withTrio(routesThroughNativeBoundaries),
  );
});

async function routesThroughNativeBoundaries({
  caller,
  target,
  peer,
}: Trio): Promise<undefined> {
  const shared = groupAddress([caller, target, peer]);
  await caller.send({ to: target.address, text: INBOUND });
  const callerReceived = caller.next(2);
  const peerReceived = peer.next(1);
  await runProbe(
    target,
    [
      { to: caller.address, text: DIRECT_REPLY },
      { to: shared, text: GROUP_REPLY, final: true },
    ],
    { platformId: caller.address, sender: caller.address, text: INBOUND },
  );

  const group = {
    kind: "message",
    messageKind: "group",
    address: shared,
    sender: target.agent,
    content: [{ type: "text", text: GROUP_REPLY }],
  };
  const callerItems = (await callerReceived).map(messageSummary);
  assert.equal(callerItems.length, 2);
  assert.deepEqual(
    callerItems.find(({ messageKind }) => messageKind === "direct"),
    {
      kind: "message",
      messageKind: "direct",
      address: target.address,
      sender: target.agent,
      content: [{ type: "text", text: DIRECT_REPLY }],
    },
  );
  assert.deepEqual(
    callerItems.find(({ messageKind }) => messageKind === "group"),
    group,
  );
  assert.deepEqual((await peerReceived).map(messageSummary), [group]);
  await assertRepliesRecorded({ caller, target, peer }, shared);
}

/**
 * Check each conversation's record: the inbound message and the direct
 * reply between caller and NanoClaw, and the group reply for every member.
 */
async function assertRepliesRecorded(
  { caller, target, peer }: Trio,
  shared: string,
): Promise<undefined> {
  const direct = [textContent(INBOUND), textContent(DIRECT_REPLY)];
  assert.deepEqual(await caller.history(target.address), direct);
  assert.deepEqual(await target.history(caller.address), direct);
  for (const endpoint of [caller, target, peer]) {
    assert.deepEqual(await endpoint.history(shared), [
      textContent(GROUP_REPLY),
    ]);
  }
}

/** Ordinary messages retain their text parts unchanged. */
function textContent(text: string): readonly object[] {
  return [{ type: "text", text }];
}

/** The group address of `members`, their names sorted. */
function groupAddress(members: readonly AgentAccess[]): string {
  return groupOf(members.map(({ agent }) => agent.agentName));
}

function compareCodeUnits(left: string, right: string): number {
  if (left === right) {
    return 0;
  }
  return left < right ? -1 : 1;
}

/** The kind of an inbound item and its message's kind, address, sender and content. */
function messageSummary(item: InboundItem): Readonly<Record<string, unknown>> {
  assert(item.kind === "message");
  return {
    kind: item.kind,
    messageKind: item.message.recipients.length === 1 ? "direct" : "group",
    address: conversationAddress(item.message),
    sender: reported(item.message.author),
    content: item.message.content,
  };
}

/** The address its recipient uses for the conversation `message` is in. */
function conversationAddress({
  author,
  recipients,
}: Pick<
  Extract<InboundItem, { readonly kind: "message" }>["message"],
  "author" | "recipients"
>): string {
  return recipients.length === 1
    ? `agent:${author.agentName}`
    : groupOf([author, ...recipients].map(({ agentName }) => agentName));
}

function groupOf(names: readonly string[]): string {
  return `group:${names.toSorted(compareCodeUnits).join(",")}`;
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

function withTrio(
  body: (endpoints: Trio) => Promise<undefined>,
): Promise<undefined> {
  return withDaemon(async (signIn) => {
    const [caller, target, peer] = await Promise.all([
      signIn("caller"),
      signIn("target"),
      signIn("peer"),
    ]);
    await body({ caller, target, peer });
    return undefined;
  });
}

/**
 * Start a daemon container, run `body` with a function that signs a named
 * owner's agent in to it, and stop the daemon however `body` ends.
 */
async function withDaemon(
  body: (signIn: (owner: string) => Promise<AgentAccess>) => Promise<undefined>,
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
