/** @file Exercises NanoClaw native inbox and sends against the disposable network. */
import { replyAddress } from "@moltzap/social-harness";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { describe, test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { inspect, isDeepStrictEqual, promisify } from "node:util";
import {
  type Network,
  type Endpoint as NetworkEndpoint,
  startNetwork,
} from "../process-network-fixture.ts";
import { startDaemonContainer } from "./daemon-container.ts";

const IMAGE = process.env.NANOCLAW_INTEGRATION_IMAGE ?? "";
const SKIP =
  IMAGE.length === 0 || process.env.DAEMON_INTEGRATION_IMAGE === undefined
    ? "set NANOCLAW_INTEGRATION_IMAGE and DAEMON_INTEGRATION_IMAGE to built images"
    : false;
const RECORD_TIMEOUT_MILLIS = 60_000;
const RECORD_POLL_MILLIS = 50;
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
const PENDING_SEND = "sent while the Router does not answer";
const FENCE = "sent after the Router answers again";
/** What the channel tells the model of a send that failed as `delivery-pending`. */
const PENDING_REPORT =
  "send failed: the message is pending and Social Harness will still deliver it";
const GATHER_QUESTION = "Which day works?";
const SLOT_SCHEMA = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
};

/** Native image tests use the same public MCP fixture as other process consumers. */
type Endpoint = NetworkEndpoint;
type InboundItem = Awaited<ReturnType<Endpoint["next"]>>[number];
type RequestItem = Extract<InboundItem, { readonly kind: "collectiveRequest" }>;

/** A caller and the NanoClaw agent it talks to. */
interface Pair {
  readonly caller: Endpoint;
  readonly target: Endpoint;
}

/** A caller, the NanoClaw agent, and a third group member. */
interface Trio extends Pair {
  readonly peer: Endpoint;
}

/** What the probe waits for in NanoClaw's native inbox. */
interface ExpectedInbound {
  readonly platformId: string;
  readonly sender: string;
  readonly text?: string;
  readonly textPrefix?: string;
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
  /** The text of each message Social Harness itself gave the model in the expected conversation. */
  readonly endpointMessages: readonly string[];
}

describe("the NanoClaw agent image on a real network", { skip: SKIP }, () => {
  test(
    "routes NanoClaw inbound and outbound through its native host boundaries",
    { timeout: CASE_TIMEOUT_MILLIS },
    () => withTrio(routesThroughNativeBoundaries),
  );

  test(
    "answers and starts a gather through NanoClaw's send_message text",
    { timeout: CASE_TIMEOUT_MILLIS },
    () => withPair(answersAndStartsGather),
  );

  test(
    "answers and starts an all_gather through NanoClaw's send_message text",
    { timeout: CASE_TIMEOUT_MILLIS },
    () => withTrio(answersAndStartsAllGather),
  );

  test(
    "completes a delivery-pending send once, tells the model, and posts it once",
    { timeout: CASE_TIMEOUT_MILLIS },
    () => withPair(completesPendingSendOnce),
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
    kind: "multicast",
    messageKind: "group",
    address: shared,
    sender: target.agent,
    content: [{ type: "text", text: GROUP_REPLY }],
  };
  const callerItems = (await callerReceived).map(multicastSummary);
  assert.equal(callerItems.length, 2);
  assert.deepEqual(
    callerItems.find(({ messageKind }) => messageKind === "direct"),
    {
      kind: "multicast",
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
  assert.deepEqual((await peerReceived).map(multicastSummary), [group]);
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
  assert.deepEqual(await caller.history(target.address), [
    textContent(INBOUND),
    [{ type: "text", text: DIRECT_REPLY }],
  ]);
  assert.deepEqual(await target.history(caller.address), [
    [{ type: "text", text: INBOUND }],
    textContent(DIRECT_REPLY),
  ]);
  for (const endpoint of [caller, target, peer]) {
    assert.deepEqual(await endpoint.history(shared), [
      endpoint === target
        ? textContent(GROUP_REPLY)
        : [{ type: "text", text: GROUP_REPLY }],
    ]);
  }
}

async function answersAndStartsGather({
  caller,
  target,
}: Pair): Promise<undefined> {
  const started = await caller.send({
    to: target.address,
    text: GATHER_QUESTION,
    collective: { op: "gather", deadline: 120, requestedSchema: SLOT_SCHEMA },
  });
  const operationId = started.id;
  const received = caller.next(1);
  await runProbe(
    target,
    [
      { to: caller.address, text: acceptText("tue", operationId) },
      { to: caller.address, text: gatherText("gather") },
    ],
    {
      platformId: caller.address,
      sender: caller.address,
      textPrefix: `gather from ${caller.address}, open until`,
    },
  );

  assert.match(operationId, /^col_/u);
  const results = (await received).map(asRecord);
  const [result] = results;
  assert.ok(result);
  assert.equal(typeof result.closedAt, "number");
  assert.deepEqual(results, [
    {
      kind: "collectiveResult",
      op: "gather",
      operationId,
      question: GATHER_QUESTION,
      closedAt: result.closedAt,
      closedBy: "settled",
      outcomes: [
        {
          member: target.agent,
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
      ],
    },
  ]);
  const request = requireRequest((await caller.next(1))[0]);
  assertGatherRequest(request, target);
}

function assertGatherRequest(request: RequestItem, target: Endpoint) {
  assert.equal(request.kind, "collectiveRequest");
  assert.equal(request.message.operation.op, "gather");
  assert.deepEqual(request.message.author, target.agent);
  assert.equal(replyAddress(request.message), target.address);
  assert.equal(request.message.operation.question, GATHER_QUESTION);
  assert.deepEqual(request.message.operation.requestedSchema, SLOT_SCHEMA);
}

/**
 * The caller starts an all_gather the peer declines and NanoClaw answers;
 * NanoClaw then starts its own, which the caller answers and the peer
 * declines.
 */
async function answersAndStartsAllGather(endpoints: Trio): Promise<undefined> {
  const { caller, target, peer } = endpoints;
  const group = groupAddress([caller, target, peer]);
  const operationId = await startDeclinedAllGather(endpoints, group);
  await runProbe(
    target,
    [
      { to: group, text: acceptText("tue", operationId) },
      { to: group, text: gatherText("all_gather") },
    ],
    {
      platformId: group,
      sender: caller.address,
      textPrefix: `all_gather from ${caller.address} to ${group}, open until`,
    },
  );
  const nanoClawRequest = await assertAllGatherAnswered(
    endpoints,
    group,
    operationId,
  );
  await assertNanoClawAllGather(endpoints, group, nanoClawRequest);
}

/** Start the caller's all_gather and have the peer decline it. */
async function startDeclinedAllGather(
  { caller, peer }: Trio,
  group: string,
): Promise<string> {
  const started = await caller.send({
    to: group,
    text: GATHER_QUESTION,
    collective: {
      op: "all_gather",
      deadline: 120,
      requestedSchema: SLOT_SCHEMA,
    },
  });
  const operationId = started.id;
  const peerRequest = requireRequest((await peer.next(1))[0]);
  assert.equal(peerRequest.message.operation.operationId, operationId);
  await peer.send({
    to: group,
    collectiveResponse: { action: "decline", operationId },
  });
  return operationId;
}

/**
 * Check the caller's all_gather result reached the caller and the peer, and
 * return NanoClaw's own request the caller received.
 */
async function assertAllGatherAnswered(
  { caller, target, peer }: Trio,
  group: string,
  operationId: string,
): Promise<RequestItem> {
  const callerItems = await caller.next(2);
  const result = callerItems.find((item) => item.kind === "collectiveResult");
  const nanoClawRequest = callerItems.find(
    (item) => item.kind === "collectiveRequest",
  );
  assert.ok(result, "the caller received no all_gather result");
  assert.equal(typeof result.closedAt, "number");
  assert.ok(nanoClawRequest, "the caller received no NanoClaw request");
  assert.deepEqual(result, {
    kind: "collectiveResult",
    op: "all_gather",
    operationId,
    question: GATHER_QUESTION,
    closedAt: result.closedAt,
    closedBy: "settled",
    outcomes: [
      { member: peer.agent, outcome: { kind: "declined" } },
      {
        member: target.agent,
        outcome: { kind: "answered", content: { slot: "tue" } },
      },
    ],
  });
  const peerItems = await peer.next(2);
  assert.ok(
    peerItems.some((item) => isDeepStrictEqual(item, result)),
    "the peer did not receive the all_gather result",
  );
  assert.ok(
    peerItems.some(
      (item) =>
        item.kind === "collectiveRequest" &&
        replyAddress(item.message) === group,
    ),
    "the peer did not receive NanoClaw's request",
  );
  return nanoClawRequest;
}

/** Answer NanoClaw's all_gather and check its result reaches every member. */
async function assertNanoClawAllGather(
  { caller, target, peer }: Trio,
  group: string,
  request: RequestItem,
): Promise<undefined> {
  assert.equal(request.message.operation.op, "all_gather");
  assert.deepEqual(request.message.author, target.agent);
  assert.equal(replyAddress(request.message), group);
  const operationId = request.message.operation.operationId;

  await caller.send({
    to: group,
    collectiveResponse: {
      action: "accept",
      operationId,
      content: { slot: "mon" },
    },
  });
  await peer.send({
    to: group,
    collectiveResponse: { action: "decline", operationId },
  });
  const [received] = await caller.next(1);
  const result = asRecord(received);
  assert.equal(result.kind, "collectiveResult");
  assert.equal(result.op, "all_gather");
  assert.equal(result.operationId, operationId);
  assert.equal(typeof result.closedAt, "number");
  assert.equal(result.closedBy, "settled");
  assert.deepEqual(result.outcomes, [
    {
      member: caller.agent,
      outcome: { kind: "answered", content: { slot: "mon" } },
    },
    { member: peer.agent, outcome: { kind: "declined" } },
  ]);
  assert.deepEqual(await peer.next(1), [received]);
}

/**
 * NanoClaw sends while the Router does not answer, so the daemon queues the
 * post, its own drain times out, and the send fails as `delivery-pending`.
 * The row completes on the first delivery pass, so the second sends nothing
 * again, and the model is told the message is pending. Once the Router
 * answers, the caller records the post once, then a fence the target sends
 * after it, so a second copy NanoClaw queued would be recorded before the
 * fence.
 */
async function completesPendingSendOnce(
  { caller, target }: Pair,
  network: Network,
): Promise<undefined> {
  await caller.send({ to: target.address, text: INBOUND });
  await awaitRecordedLast(target, caller.address, INBOUND);
  const delivery = await whileRouterHeld(network, () =>
    probeDelivery(target, [{ to: caller.address, text: PENDING_SEND }], {
      platformId: caller.address,
      sender: caller.address,
      text: INBOUND,
    }),
  );
  await target.send({ to: caller.address, text: FENCE });

  assert.deepEqual(
    {
      ...delivery,
      posts: await awaitRecordedLast(caller, target.address, FENCE),
    },
    {
      delivered: 1,
      retried: 0,
      endpointMessages: [PENDING_REPORT],
      posts: [
        textContent(INBOUND),
        [{ type: "text", text: PENDING_SEND }],
        [{ type: "text", text: FENCE }],
      ],
    },
  );
}

/** Run `body` with the network's Router held, releasing it however `body` ends. */
async function whileRouterHeld<Result>(
  network: Network,
  body: () => Promise<Result>,
): Promise<Result> {
  const release = await network.holdRouter();
  try {
    return await body();
  } finally {
    release();
  }
}

/**
 * Poll `endpoint`'s conversation at `address` until its last record is a
 * multicast of `text`, and return every record's content. A read the daemon
 * refuses while it still catches the conversation up, as `history-gap`,
 * counts as not yet recorded; the timeout names the last read.
 */
async function awaitRecordedLast(
  endpoint: Endpoint,
  address: string,
  text: string,
): Promise<readonly unknown[]> {
  const expected = [{ type: "text", text }];
  const deadline = Date.now() + RECORD_TIMEOUT_MILLIS;
  let lastRead: HistoryRead = { failure: "no read" };
  while (Date.now() < deadline) {
    lastRead = await readHistory(endpoint, address);
    if (
      "contents" in lastRead &&
      isDeepStrictEqual(lastRead.contents.at(-1), expected)
    ) {
      return lastRead.contents;
    }
    await delay(RECORD_POLL_MILLIS);
  }
  throw new Error(
    `${endpoint.address} did not record "${text}" in time; last read: ${inspect(lastRead, { depth: 6 })}`,
  );
}

/** One conversation read: its records' contents, or why the daemon refused it. */
type HistoryRead =
  | { readonly contents: readonly unknown[] }
  | { readonly failure: unknown };

async function readHistory(
  endpoint: Endpoint,
  address: string,
): Promise<HistoryRead> {
  try {
    return { contents: await endpoint.history(address) };
  } catch (failure) {
    return { failure };
  }
}

/** Ordinary messages retain their text parts unchanged. */
function textContent(text: string): readonly object[] {
  return [{ type: "text", text }];
}

/** The group address of `members`, their names sorted. */
function groupAddress(members: readonly Endpoint[]): string {
  const names = members.map(({ address }) => address.slice("agent:".length));
  return `group:${names.toSorted(compareCodeUnits).join(",")}`;
}

function compareCodeUnits(left: string, right: string): number {
  if (left === right) {
    return 0;
  }
  return left < right ? -1 : 1;
}

/** The kind of an inbound item and its message's kind, address, sender and content. */
function gatherText(operation: "gather" | "all_gather"): string {
  return JSON.stringify({
    [operation]: GATHER_QUESTION,
    deadline: 60,
    requestedSchema: SLOT_SCHEMA,
  });
}

/** The text a model sends to answer with `slot`. */
function acceptText(slot: string, operationId: string): string {
  return JSON.stringify({ action: "accept", operationId, content: { slot } });
}

/** The kind of an inbound item and its message's kind, address, sender and content. */
function multicastSummary(
  item: InboundItem,
): Readonly<Record<string, unknown>> {
  assert(item.kind === "multicast");
  return {
    kind: item.kind,
    messageKind: item.message.recipients.length === 1 ? "direct" : "group",
    address: replyAddress(item.message),
    sender: item.message.author,
    content: item.message.content,
  };
}

function requireRequest(item?: InboundItem): RequestItem {
  assert(item?.kind === "collectiveRequest", "expected a received request");
  return item;
}

/**
 * Run the probe against the daemon at `mcp` and require that the first
 * delivery pass completed every send, leaving none for a retry.
 */
async function runProbe(
  daemon: Pick<Endpoint, "mcp" | "mcpCredentialFile">,
  destinations: readonly Destination[],
  inbound: ExpectedInbound,
): Promise<undefined> {
  const { delivered, retried } = await probeDelivery(
    daemon,
    destinations,
    inbound,
  );
  assert.deepEqual(
    { delivered, retried },
    { delivered: destinations.length, retried: 0 },
  );
}

/**
 * Run the probe in a container of the image under test against the daemon
 * at `mcp`, and return its report once it exits 0.
 */
async function probeDelivery(
  daemon: Pick<Endpoint, "mcp" | "mcpCredentialFile">,
  destinations: readonly Destination[],
  inbound: ExpectedInbound,
): Promise<ProbeReport> {
  const { stdout } = await executeFile(
    "docker",
    [
      "run",
      "--rm",
      "--pull=never",
      `--network=container:${daemon.mcp.port ? "sharness-daemon-" + daemon.mcp.port : ""}`,
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
  return decodeProbeReport(stdout);
}

/** The report on the probe's last report line, refusing any other shape. */
function decodeProbeReport(stdout: string): ProbeReport {
  const line = stdout
    .split("\n")
    .findLast((each) => each.startsWith(PROBE_REPORT_PREFIX));
  const { delivered, retried, endpointMessages } = asRecord(
    JSON.parse(line?.slice(PROBE_REPORT_PREFIX.length) ?? "null"),
  );
  if (
    typeof delivered !== "number" ||
    typeof retried !== "number" ||
    !Array.isArray(endpointMessages) ||
    !endpointMessages.every((text) => typeof text === "string")
  ) {
    throw new Error(`the probe printed no report:\n${stdout}`);
  }
  return { delivered, retried, endpointMessages };
}

function withPair(
  body: (endpoints: Pair, network: Network) => Promise<undefined>,
): Promise<undefined> {
  return withNetwork(async (start, network) => {
    const [caller, target] = await Promise.all([
      start("caller"),
      start("target"),
    ]);
    await body({ caller, target }, network);
    return undefined;
  });
}

function withTrio(
  body: (endpoints: Trio) => Promise<undefined>,
): Promise<undefined> {
  return withNetwork(async (start) => {
    const [caller, target, peer] = await Promise.all([
      start("caller"),
      start("target"),
      start("peer"),
    ]);
    await body({ caller, target, peer });
    return undefined;
  });
}

/**
 * Start a network, run `body` with a starter for registered endpoints named
 * for this run and the network itself, and close the network however `body`
 * ends.
 */
async function withNetwork(
  body: (
    start: (role: string) => Promise<Endpoint>,
    network: Network,
  ) => Promise<undefined>,
): Promise<undefined> {
  const network = await startNetwork(startDaemonContainer);
  try {
    const run = randomBytes(4).toString("hex");
    await body((role) => network.endpoint(`nanoclaw-${run}-${role}`), network);
  } finally {
    await network.close();
  }
}

function asRecord(value: unknown): Readonly<Record<string, unknown>> {
  return isRecord(value) ? value : {};
}

/** Rejects null and arrays before reading a JSON object. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
