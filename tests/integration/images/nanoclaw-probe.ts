/**
 * @file Drives NanoClaw's native inbox and outbound queue through its
 * registered Social Harness channel, inside the NanoClaw agent image.
 * `nanoclaw.spec.ts` mounts it into a container of the image under test
 * and runs it with `node`; it reads the daemon's URL from `SHARNESS_MCP_URL`,
 * the sends to queue from `NANOCLAW_DESTINATIONS_JSON` and the inbound
 * message to wait for from `NANOCLAW_INBOUND_JSON`. It delivers the queued
 * sends in two passes, as NanoClaw's delivery loop does when a pass leaves a
 * row due, and prints one report line for the test to check. NanoClaw's
 * compiled modules are known only at run time, so each export is checked
 * before use.
 */

import { type ChildProcess, spawn } from "node:child_process";
import { mkdir, mkdtemp, symlink, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const NANOCLAW_ROOT = "/opt/social-harness/nanoclaw";
const APP_ROOT = `${NANOCLAW_ROOT}/app`;
const APP_DIST = `${APP_ROOT}/dist`;
const RUNNER_ROOT =
  "/opt/social-harness/nanoclaw/app/container/agent-runner/src";
const CURRENT_WORKSPACE = "/var/lib/social-harness/nanoclaw/current-workspace";
const AGENT_GROUP_ID = "agent";
const INBOUND_TIMEOUT_MILLIS = 60_000;
const POLL_MILLIS = 50;
const INBOUND_HISTORY_LIMIT = 10;
/** The sender NanoClaw records for a message Social Harness itself gives the model. */
const ENDPOINT_SENDER = "Social Harness";
/** The prefix of the report line, which `nanoclaw.spec.ts` looks for. */
const REPORT_PREFIX = "nanoclaw probe report: ";
/**
 * The agent runner's script that queues each destination in order: one
 * marked `final` through the agent's final output, and any other as
 * `send_message` tool arguments `to` and `text`, whose text may state a
 * gather, all_gather or answer.
 */
const OUTBOUND_SOURCE = `
  import '${RUNNER_ROOT}/modules/index.ts';
  import { sendMessage } from '${RUNNER_ROOT}/mcp-tools/core.ts';
  import { dispatchResultText } from '${RUNNER_ROOT}/poll-loop.ts';
  const destinations = JSON.parse(process.env.NANOCLAW_DESTINATIONS_JSON);
  for (const { final, ...destination } of destinations) {
    if (final !== true) {
      const toolResult = await sendMessage.handler(destination);
      if (toolResult.isError === true) {
        throw new Error(toolResult.content.map(({ text }) => text).join('\\n'));
      }
      continue;
    }
    const finalResult = await dispatchResultText(
      '<message to="' + destination.to + '">' + destination.text + '</message>',
      {
        platformId: null,
        channelType: null,
        threadId: null,
        inReplyTo: null,
        taskRun: false,
      },
    );
    if (finalResult.sent !== 1) {
      throw new Error('NanoClaw final-output path did not queue exactly one message');
    }
  }
`;

type ModuleRecord = Readonly<Record<string, unknown>>;

/**
 * The message the probe waits for in the native inbox: its conversation,
 * sender, and either its exact text or a prefix of it.
 */
interface ExpectedInbound {
  readonly platformId: string;
  readonly sender: string;
  readonly text?: string;
  readonly textPrefix?: string;
}

/** The NanoClaw host modules the probe drives. */
interface HostModules {
  readonly config: ModuleRecord;
  readonly connection: ModuleRecord;
  readonly channels: ModuleRecord;
  readonly delivery: ModuleRecord;
  readonly router: ModuleRecord;
  readonly sessions: ModuleRecord;
  readonly sessionManager: ModuleRecord;
}

/** What the probe saw of NanoClaw delivering the queued sends. */
interface DeliveryReport {
  readonly delivered: number;
  readonly retried: number;
  readonly endpointMessages: readonly string[];
}

/** Whether the channel announced the expected conversation before content. */
interface MetadataState {
  observed: boolean;
}

await main();
process.exit(0);

async function main(): Promise<undefined> {
  required("SHARNESS_MCP_URL");
  const destinations = decodeDestinations(
    JSON.parse(required("NANOCLAW_DESTINATIONS_JSON")),
  );
  const expectedInbound = decodeInbound(
    JSON.parse(required("NANOCLAW_INBOUND_JSON")),
  );
  await provisionStateRoot();
  const modules = await importHostModules();
  await call(
    modules.connection,
    "initDb",
    stringExport(modules.config, "CENTRAL_DB_PATH"),
    {
      role: "tool",
    },
  );
  try {
    await exchangeMessages(modules, expectedInbound, destinations);
  } finally {
    await call(modules.channels, "teardownChannelAdapters");
    await call(modules.connection, "closeDb");
  }
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`missing ${name}`);
  }
  return value;
}

function decodeDestinations(value: unknown): readonly unknown[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error("NANOCLAW_DESTINATIONS_JSON must contain sends");
  }
  return value;
}

function decodeInbound(value: unknown): ExpectedInbound {
  const { platformId, sender, text, textPrefix } = asRecord(value);
  if (
    typeof platformId !== "string" ||
    typeof sender !== "string" ||
    !optionalString(text) ||
    !optionalString(textPrefix)
  ) {
    throw new Error("NANOCLAW_INBOUND_JSON must name a platformId and sender");
  }
  return {
    platformId,
    sender,
    ...(text === undefined ? {} : { text }),
    ...(textPrefix === undefined ? {} : { textPrefix }),
  };
}

/**
 * Lay out a fresh NanoClaw project directory, make it the working
 * directory, and provision the image's agent group in it with the image's
 * own `initialize-agent-group.ts`.
 */
async function provisionStateRoot(): Promise<undefined> {
  const stateRoot = await mkdtemp(join(tmpdir(), "nanoclaw-send-probe-"));
  const workspaceDirectory = join(stateRoot, "workspace-seed");
  await Promise.all(
    ["data", "groups", "store", "tmp"].map((directory) =>
      mkdir(join(stateRoot, directory), { recursive: true }),
    ),
  );
  await mkdir(workspaceDirectory, { recursive: true });
  await symlink(
    join(APP_ROOT, "package.json"),
    join(stateRoot, "package.json"),
  );
  process.chdir(stateRoot);

  const provision = await importModule(
    `${NANOCLAW_ROOT}/initialize-agent-group.ts`,
  );
  await call(
    provision,
    "provisionNanoClaw",
    {
      agentName: "NanoClaw integration",
      stateDirectory: stateRoot,
      workspaceDirectory,
      mcpServers: [],
    },
    { appRoot: APP_ROOT },
  );
}

/**
 * Import the host modules, after the mailbox composition and the Social Harness
 * channel register themselves.
 */
async function importHostModules(): Promise<HostModules> {
  await importModule(`${APP_DIST}/mailbox/compose.js`);
  await importModule(`${APP_DIST}/channels/sharness.js`);
  return {
    config: await importModule(`${APP_DIST}/config.js`),
    connection: await importModule(`${APP_DIST}/db/connection.js`),
    channels: await importModule(`${APP_DIST}/channels/channel-registry.js`),
    delivery: await importModule(`${APP_DIST}/delivery.js`),
    router: await importModule(`${APP_DIST}/router.js`),
    sessions: await importModule(`${APP_DIST}/db/sessions.js`),
    sessionManager: await importModule(`${APP_DIST}/session-manager.js`),
  };
}

/**
 * Start the channel adapters, wait for the Social Harness message in the agent's
 * inbox, queue the destinations from that session's workspace, deliver
 * them in two passes, and print what the first pass completed, what the
 * second sent again and what Social Harness told the model.
 */
async function exchangeMessages(
  modules: HostModules,
  expected: ExpectedInbound,
  destinations: readonly unknown[],
): Promise<undefined> {
  const metadata: MetadataState = { observed: false };
  await call(modules.channels, "initChannelAdapters", (adapter: unknown) =>
    adapterCallbacks(modules.router, expected, metadata, adapter),
  );
  if (!hasSocialHarnessAdapter(call(modules.channels, "getActiveAdapters"))) {
    throw new Error(
      "NanoClaw did not start the Social Harness channel adapter",
    );
  }
  call(
    modules.delivery,
    "setDeliveryAdapter",
    call(modules.channels, "createChannelDeliveryAdapter"),
  );

  const { session, sessionId } = await waitForInbound(modules, expected);
  await replaceWorkspace(
    stringResult(
      call(modules.sessionManager, "sessionDir", AGENT_GROUP_ID, sessionId),
      "sessionDir",
    ),
  );
  await invokeOutboundPaths(destinations);
  await call(modules.delivery, "deliverSessionMessages", session);
  const delivered = await deliveredIds(modules.sessionManager, sessionId);
  const retried = await dueCount(modules.sessionManager, sessionId, delivered);
  await call(modules.delivery, "deliverSessionMessages", session);
  const report: DeliveryReport = {
    delivered: delivered.size,
    retried,
    endpointMessages: await endpointMessages(
      modules.sessionManager,
      sessionId,
      expected.platformId,
    ),
  };
  process.stdout.write(`${REPORT_PREFIX}${JSON.stringify(report)}\n`);
}

/**
 * The host callbacks one channel adapter receives. Content that arrives
 * before the channel announces the expected conversation fails the probe.
 */
function adapterCallbacks(
  router: ModuleRecord,
  expected: ExpectedInbound,
  metadata: MetadataState,
  adapter: unknown,
): ModuleRecord {
  const { channelType, instance } = asRecord(adapter);
  return {
    onInbound: (platformId: unknown, threadId: unknown, message: unknown) => {
      if (!metadata.observed) {
        throw new Error(
          "NanoClaw received content before conversation metadata",
        );
      }
      return call(router, "routeInbound", {
        channelType,
        instance: instance ?? channelType,
        platformId,
        threadId,
        message: inboundMessage(message),
      });
    },
    onInboundEvent: (event: unknown) => call(router, "routeInbound", event),
    onMetadata: (platformId: unknown, name: unknown, isGroup: unknown) => {
      if (
        platformId === expected.platformId &&
        name === expected.platformId &&
        isGroup === false
      ) {
        metadata.observed = true;
      }
    },
    onAction: () => undefined,
  };
}

/** The router's form of a channel message: its content as JSON text. */
function inboundMessage(value: unknown): ModuleRecord {
  const message = asRecord(value);
  return {
    id: message.id,
    kind: message.kind,
    content: JSON.stringify(message.content),
    timestamp: message.timestamp,
    isMention: message.isMention,
    isGroup: message.isGroup,
  };
}

function hasSocialHarnessAdapter(adapters: unknown): boolean {
  return (
    Array.isArray(adapters) &&
    adapters.some(
      (adapter: unknown) => asRecord(adapter).channelType === "sharness",
    )
  );
}

/**
 * Poll the agent group's session until its inbound history holds the
 * expected message, and return that session.
 */
async function waitForInbound(
  modules: HostModules,
  expected: ExpectedInbound,
): Promise<{ readonly session: unknown; readonly sessionId: string }> {
  const deadline = Date.now() + INBOUND_TIMEOUT_MILLIS;
  while (Date.now() < deadline) {
    const session: unknown = await call(
      modules.sessions,
      "findSessionByAgentGroup",
      AGENT_GROUP_ID,
    );
    if (session !== undefined) {
      const sessionId = stringResult(asRecord(session).id, "session id");
      const history: unknown = await call(
        modules.sessionManager,
        "withMailboxSession",
        AGENT_GROUP_ID,
        sessionId,
        (mailbox: unknown) =>
          call(asRecord(mailbox), "getInboundHistory", INBOUND_HISTORY_LIMIT),
      );
      if (historyHolds(history, expected)) {
        return { session, sessionId };
      }
    }
    await delay(POLL_MILLIS);
  }
  throw new Error(
    "NanoClaw native inbox did not receive the Social Harness message",
  );
}

function historyHolds(history: unknown, expected: ExpectedInbound): boolean {
  return (
    Array.isArray(history) &&
    history.some((row: unknown) => {
      const { content } = asRecord(row);
      return (
        typeof content === "string" &&
        matchesInbound(JSON.parse(content), expected)
      );
    })
  );
}

function matchesInbound(value: unknown, expected: ExpectedInbound): boolean {
  const { text, address, sender, senderId } = asRecord(value);
  if (typeof text !== "string") {
    return false;
  }
  const textMatches =
    expected.textPrefix === undefined
      ? text === expected.text
      : text.startsWith(expected.textPrefix);
  return (
    textMatches &&
    address === expected.platformId &&
    sender === expected.sender &&
    senderId === expected.sender
  );
}

async function replaceWorkspace(target: string): Promise<undefined> {
  try {
    await unlink(CURRENT_WORKSPACE);
  } catch (error) {
    if (
      !(error instanceof Error && "code" in error && error.code === "ENOENT")
    ) {
      throw error;
    }
  }
  await symlink(target, CURRENT_WORKSPACE);
}

/** Queue every destination through the agent runner, under `bun`. */
async function invokeOutboundPaths(
  destinations: readonly unknown[],
): Promise<undefined> {
  // eslint-disable-next-line sonarjs/no-os-command-from-path -- the stock agent runner's bun is on the image's PATH.
  const child = spawn("bun", ["--eval", OUTBOUND_SOURCE], {
    env: {
      ...process.env,
      NANOCLAW_DESTINATIONS_JSON: JSON.stringify(destinations),
    },
    stdio: "inherit",
  });
  await waitForChild(child);
}

function waitForChild(child: ChildProcess): Promise<undefined> {
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) {
        resolve(undefined);
        return;
      }
      reject(
        new Error(
          `NanoClaw send_message probe exited with ${String(code ?? signal)}`,
        ),
      );
    });
  });
}

/** The ids of the outbound rows NanoClaw has marked delivered. */
async function deliveredIds(
  sessionManager: ModuleRecord,
  sessionId: string,
): Promise<ReadonlySet<unknown>> {
  const delivered: unknown = await call(
    sessionManager,
    "withMailboxSession",
    AGENT_GROUP_ID,
    sessionId,
    (mailbox: unknown) => call(asRecord(mailbox), "getDeliveredIds"),
  );
  return delivered instanceof Set ? delivered : new Set();
}

/** How many outbound rows are still due for NanoClaw's next delivery pass. */
async function dueCount(
  sessionManager: ModuleRecord,
  sessionId: string,
  delivered: ReadonlySet<unknown>,
): Promise<number> {
  const due: unknown = await call(
    sessionManager,
    "withMailboxSession",
    AGENT_GROUP_ID,
    sessionId,
    (mailbox: unknown) => call(asRecord(mailbox), "getDueMessages", delivered),
  );
  return Array.isArray(due) ? due.length : 0;
}

/**
 * The text of each message Social Harness itself gave the model in the
 * conversation at `address`, newest first.
 */
async function endpointMessages(
  sessionManager: ModuleRecord,
  sessionId: string,
  address: string,
): Promise<readonly string[]> {
  const history: unknown = await call(
    sessionManager,
    "withMailboxSession",
    AGENT_GROUP_ID,
    sessionId,
    (mailbox: unknown) =>
      call(asRecord(mailbox), "getInboundHistory", INBOUND_HISTORY_LIMIT),
  );
  if (!Array.isArray(history)) {
    return [];
  }
  return history.flatMap((row: unknown) => {
    const { content } = asRecord(row);
    const message = asRecord(
      typeof content === "string" ? JSON.parse(content) : undefined,
    );
    return message.sender === ENDPOINT_SENDER &&
      message.address === address &&
      typeof message.text === "string"
      ? [message.text]
      : [];
  });
}

async function importModule(specifier: string): Promise<ModuleRecord> {
  const loaded: unknown = await import(specifier);
  return asRecord(loaded);
}

function stringExport(module: ModuleRecord, name: string): string {
  return stringResult(module[name], name);
}

function stringResult(value: unknown, name: string): string {
  if (typeof value !== "string") {
    throw new Error(`NanoClaw returned no string ${name}`);
  }
  return value;
}

/** Call the function `target` has as `name`, with `target` as `this`. */
function call(target: ModuleRecord, name: string, ...args: unknown[]): unknown {
  const member = target[name];
  if (typeof member !== "function") {
    throw new Error(`NanoClaw has no function ${name}`);
  }
  const result: unknown = Reflect.apply(member, target, args);
  return result;
}

function optionalString(value: unknown): value is string | undefined {
  return value === undefined || typeof value === "string";
}

/** `value` as a record; anything else reads as one with no fields. */
function asRecord(value: unknown): ModuleRecord {
  return isRecord(value) ? value : {};
}

function isRecord(value: unknown): value is ModuleRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
