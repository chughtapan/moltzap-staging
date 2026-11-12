/** @file Exercises NanoClaw's native inbox and sends against a daemon container. */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod } from "node:fs/promises";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual, promisify } from "node:util";
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
const GATHER_QUESTION = "Which day works?";
const SLOT_SCHEMA = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
};

/** Native image tests use the same public MCP fixture as other process consumers. */
type InboundItem = Awaited<ReturnType<AgentAccess["next"]>>[number];
type RequestItem = Extract<InboundItem, { readonly kind: "collectiveRequest" }>;
type ResultItem = Extract<InboundItem, { readonly kind: "collectiveResult" }>;

/** A caller and the NanoClaw agent it talks to. */
interface Pair {
  readonly caller: AgentAccess;
  readonly target: AgentAccess;
}

/** A caller, the NanoClaw agent, and a third group member. */
interface Trio extends Pair {
  readonly peer: AgentAccess;
}

/**
 * What the probe waits for in NanoClaw's native inbox: a plain message with
 * `text`, or the inbox entry of the question `operationId` names.
 */
interface ExpectedInbound {
  readonly platformId: string;
  readonly sender: string;
  readonly text?: string;
  readonly operationId?: string;
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
  const direct = [textContent(INBOUND), textContent(DIRECT_REPLY)];
  assert.deepEqual(await caller.history(target.address), direct);
  assert.deepEqual(await target.history(caller.address), direct);
  for (const endpoint of [caller, target, peer]) {
    assert.deepEqual(await endpoint.history(shared), [
      textContent(GROUP_REPLY),
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
  const received = caller.next(2);
  await runProbe(
    target,
    [
      { to: caller.address, text: acceptText("tue", operationId) },
      { to: caller.address, text: gatherText("gather") },
    ],
    { platformId: caller.address, sender: caller.address, operationId },
  );

  assert.match(operationId, /^col_/u);
  const [first, second] = await received;
  const result = requireResult(first);
  assert.equal(typeof result.closedAt, "number");
  assert.deepEqual(withReportedMembers(result), {
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
  });
  assertGatherRequest(requireRequest(second), target);
}

function assertGatherRequest(request: RequestItem, target: AgentAccess) {
  assert.equal(request.kind, "collectiveRequest");
  assert.equal(request.message.operation.op, "gather");
  assert.deepEqual(reported(request.message.author), target.agent);
  assert.equal(conversationAddress(request.message), target.address);
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
    { platformId: group, sender: caller.address, operationId },
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
  const result = requireResult(
    callerItems.find((item) => item.kind === "collectiveResult"),
  );
  const nanoClawRequest = requireRequest(
    callerItems.find((item) => item.kind === "collectiveRequest"),
  );
  assert.equal(typeof result.closedAt, "number");
  assert.deepEqual(withReportedMembers(result), {
    kind: "collectiveResult",
    op: "all_gather",
    operationId,
    question: GATHER_QUESTION,
    closedAt: result.closedAt,
    closedBy: "settled",
    outcomes: inMemberNameOrder([
      { member: peer.agent, outcome: { kind: "declined" } },
      {
        member: target.agent,
        outcome: { kind: "answered", content: { slot: "tue" } },
      },
    ]),
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
        conversationAddress(item.message) === group,
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
  assert.deepEqual(reported(request.message.author), target.agent);
  assert.equal(conversationAddress(request.message), group);
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
  const result = requireResult((await caller.next(1))[0]);
  assert.equal(result.op, "all_gather");
  assert.equal(result.operationId, operationId);
  assert.equal(typeof result.closedAt, "number");
  assert.equal(result.closedBy, "settled");
  assert.deepEqual(
    withReportedMembers(result).outcomes,
    inMemberNameOrder([
      {
        member: caller.agent,
        outcome: { kind: "answered", content: { slot: "mon" } },
      },
      { member: peer.agent, outcome: { kind: "declined" } },
    ]),
  );
  assert.deepEqual(await peer.next(1), [result]);
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

/** The text a model sends to ask the question as a gather or an all_gather. */
function gatherText(op: "gather" | "all_gather"): string {
  return JSON.stringify({
    text: GATHER_QUESTION,
    collective: { op, deadline: 60, requestedSchema: SLOT_SCHEMA },
  });
}

/** The text a model sends to answer with `slot`. */
function acceptText(slot: string, operationId: string): string {
  return JSON.stringify({
    collectiveResponse: { action: "accept", operationId, content: { slot } },
  });
}

/** The kind of an inbound item and its message's kind, address, sender and content. */
function multicastSummary(
  item: InboundItem,
): Readonly<Record<string, unknown>> {
  assert(item.kind === "multicast");
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
}: Pick<RequestItem["message"], "author" | "recipients">): string {
  return recipients.length === 1
    ? `agent:${author.agentName}`
    : groupOf([author, ...recipients].map(({ agentName }) => agentName));
}

function groupOf(names: readonly string[]): string {
  return `group:${names.toSorted(compareCodeUnits).join(",")}`;
}

function requireRequest(item?: InboundItem): RequestItem {
  assert(item?.kind === "collectiveRequest", "expected a received request");
  return item;
}

function requireResult(item?: InboundItem): ResultItem {
  assert(item?.kind === "collectiveResult", "expected a received result");
  return item;
}

/** The fields of an agent's card that `status` reports, which an endpoint holds. */
function reported({ agentId, agentName }: AgentAccess["agent"]) {
  return { agentId, agentName };
}

/** A result with each outcome's member as an endpoint holds its agent. */
function withReportedMembers(result: ResultItem) {
  return {
    ...result,
    outcomes: result.outcomes.map(({ member, outcome }) => ({
      member: reported(member),
      outcome,
    })),
  };
}

/**
 * A result lists its outcomes in the order of the members' names, which the
 * daemon chooses.
 */
function inMemberNameOrder<
  Outcome extends { readonly member: AgentAccess["agent"] },
>(outcomes: readonly Outcome[]) {
  return outcomes.toSorted((left, right) =>
    compareCodeUnits(left.member.agentName, right.member.agentName),
  );
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
    const [caller, target] = await Promise.all([
      signIn("caller"),
      signIn("target"),
    ]);
    await body({ caller, target });
    return undefined;
  });
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
