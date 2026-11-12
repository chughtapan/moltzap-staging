/** @file NanoClaw hands each inbox entry to its host and sends native text through a stock MCP client. */
import type * as SocialHarness from "@social-harness/social-harness";
import { Client, ProtocolError } from "@modelcontextprotocol/client";
import {
  AgentAddress,
  AgentCard,
  type connectFromEnvironment,
  DeliveryToken,
  GroupAddress,
  InboxEntry,
  type McpConnection,
  MsgId,
  SendError,
  type SendResult,
} from "@social-harness/social-harness";
import { Effect, Fiber, Queue, Schema, Scope, Stream } from "effect";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ChannelRegistration, ChannelSetup } from "./adapter.js";
import type { registerChannelAdapter } from "./channel-registry.js";

/* eslint-disable agent-code-guard/async-keyword -- NanoClaw's lifecycle and host callbacks are Promise-native. */
const boundary = vi.hoisted(() => ({
  register: vi.fn<typeof registerChannelAdapter>(),
  acquire: vi.fn<typeof connectFromEnvironment>(),
}));
vi.mock("./channel-registry.js", () => ({
  registerChannelAdapter: boundary.register,
}));
vi.mock("@social-harness/social-harness", async (original) => ({
  ...(await original<typeof SocialHarness>()),
  connectFromEnvironment: boundary.acquire,
}));

const messageText = "hello";
const deliveryToken = Schema.decodeUnknownSync(DeliveryToken)(
  `dlv_${"A".repeat(43)}`,
);
const msgId = Schema.decodeUnknownSync(MsgId)(`msg_${"A".repeat(43)}`);
const address = Schema.decodeUnknownSync(AgentAddress)("agent:alice");
const agent = (name: string, byte: number) =>
  Schema.decodeUnknownSync(AgentCard)({
    agentId: `agt_${Buffer.alloc(16, byte).toString("base64url")}`,
    agentName: name,
    principalId: `prn_${Buffer.alloc(16, byte).toString("base64url")}`,
  });
const alice = agent("alice", 1);
const bob = agent("bob", 2);
const carol = agent("carol", 3);
const group = Schema.decodeUnknownSync(GroupAddress)("group:alice,bob,carol");
/** Reads an entry as `receive_messages` returns it. */
const entry = Schema.decodeUnknownSync(Schema.typeSchema(InboxEntry));
const message = {
  kind: "multicast",
  message: {
    msgId,
    author: alice,
    recipients: [bob],
    content: [{ type: "text", text: messageText }],
  },
};
const plainEntry = entry({ deliveryToken, address, item: message });
const operationId = `col_${Buffer.concat([Buffer.alloc(16, 1), Buffer.alloc(32)]).toString("base64url")}`;
const slotForm = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
};
const question = {
  text: "Which day works?",
  collective: { op: "all_gather", deadline: 60, requestedSchema: slotForm },
};
const answer = {
  collectiveResponse: {
    action: "accept",
    operationId,
    content: { slot: "tue" },
  },
};
/** Alice's question to the group, in Bob's inbox. */
const questionEntry = entry({
  deliveryToken,
  address: group,
  item: {
    kind: "collectiveRequest",
    message: {
      msgId,
      author: alice,
      recipients: [bob, carol],
      content: [{ type: "text", text: question.text }],
      operation: {
        kind: "request",
        op: "all_gather",
        operationId,
        question: question.text,
        requestedSchema: slotForm,
        deadlineAt: 1_800_000_000_000,
      },
    },
  },
});
/** The result of that question. */
const resultEntry = entry({
  deliveryToken,
  address: group,
  item: {
    kind: "collectiveResult",
    op: "all_gather",
    operationId,
    closedAt: 1_800_000_000_000,
    closedBy: "settled",
    question: question.text,
    outcomes: [
      { member: bob, outcome: { kind: "answered", content: { slot: "tue" } } },
      { member: carol, outcome: { kind: "declined" } },
    ],
  },
});
/** Alice's message with a reminder from Bob's firewall. */
const remindedEntry = entry({
  deliveryToken,
  address,
  item: message,
  guidance: {
    checkId: `chk_${"a".repeat(64)}`,
    instructions: [
      {
        ruleId: "calendar",
        instruction: "Share calendar details only with the owner.",
        uncertain: { choice: "violation", probability: 0.5 },
      },
    ],
  },
});
type Adapter = NonNullable<Awaited<ReturnType<ChannelRegistration["factory"]>>>;
let registration: ChannelRegistration;
let adapter: Adapter | undefined;

const jsonObject = Schema.decodeUnknownSync(
  Schema.parseJson(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
);

/** Hands `delivered` to a connected channel and returns what its host receives. */
async function received(delivered: InboxEntry) {
  const fake = connection();
  const current = await channel();
  const callbacks = host();
  await current.setup(callbacks);
  Effect.runSync(Queue.offer(fake.inbox, delivered));
  await vi.waitFor(() => {
    expect(callbacks.onInboundEvent).toHaveBeenCalledTimes(1);
  });
  const event = callbacks.onInboundEvent.mock.calls[0]?.[0];
  return {
    metadata: callbacks.onMetadata.mock.calls[0],
    replyTo: event?.replyTo?.platformId,
    isGroup: event?.message.isGroup,
    content: jsonObject(event?.message.content),
  };
}

/** The entry in the text the host shows its model. */
function entryInText(content: Readonly<Record<string, unknown>>): unknown {
  return Schema.decodeUnknownSync(Schema.parseJson(Schema.Unknown))(
    content.text,
  );
}

function host() {
  return {
    onInbound: vi.fn<ChannelSetup["onInbound"]>(),
    onInboundEvent: vi.fn<ChannelSetup["onInboundEvent"]>(),
    onMetadata: vi.fn<ChannelSetup["onMetadata"]>(),
    onAction: vi.fn<ChannelSetup["onAction"]>(),
  };
}
function connection() {
  const client = new Client({ name: "nanoclaw-fixture", version: "1" });
  const call = vi.spyOn(client, "callTool").mockResolvedValue({
    content: [],
    structuredContent: { id: msgId, address } satisfies SendResult,
  });
  const inbox = Effect.runSync(Queue.unbounded<InboxEntry>());
  const released = vi.fn();
  boundary.acquire.mockReturnValue(
    Effect.gen(function* () {
      const scope = yield* Scope.Scope;
      yield* Effect.addFinalizer(() => Effect.sync(released));
      return {
        client,
        consumeInbox: (accept) =>
          // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- The adapter opens and closes this fixture in its activation scope.
          Effect.acquireUseRelease(
            Stream.fromQueue(inbox).pipe(
              Stream.runForEach((entry) => accept(entry)),
              Effect.interruptible,
              Effect.forkIn(scope),
            ),
            Fiber.join,
            Fiber.interrupt,
          ),
      } satisfies McpConnection;
    }),
  );
  return { call, inbox, released };
}
async function channel() {
  const created = await registration.factory();
  if (created === null) {
    throw new Error("Configured channel did not activate");
  }
  adapter = created;
  return created;
}

beforeEach(async () => {
  vi.resetModules();
  boundary.register.mockClear();
  boundary.acquire.mockReset();
  vi.stubEnv("SHARNESS_MCP_URL", "http://127.0.0.1:8321/mcp");
  await import("./sharness.js");
  const entry = boundary.register.mock.calls[0];
  if (entry === undefined) {
    throw new Error("Channel did not register");
  }
  registration = entry[1];
});
afterEach(async () => {
  const active = adapter;
  adapter = undefined;
  await active?.teardown();
  vi.unstubAllEnvs();
});

describe("NanoClaw lifecycle", () => {
  it("stays disabled without a configured daemon", async () => {
    vi.stubEnv("SHARNESS_MCP_URL", undefined);
    expect(await registration.factory()).toBeNull();
    expect(boundary.acquire).not.toHaveBeenCalled();
  });
  it("shares one scoped connection across repeated setup and teardown", async () => {
    const fake = connection();
    const current = await channel();
    const callbacks = host();
    await current.setup(callbacks);
    await current.setup(callbacks);
    expect(boundary.acquire).toHaveBeenCalledTimes(1);
    expect(current.isConnected()).toBe(true);
    await current.teardown();
    await current.teardown();
    expect(fake.released).toHaveBeenCalledTimes(1);
    expect(current.isConnected()).toBe(false);
  });
});

// @agent-code-guard/regression-only: Each inbox item kind has one fixed host form.
describe("NanoClaw incoming items", () => {
  it("hands a plain message to the host as its text, with its native reply route", async () => {
    const { metadata, replyTo, isGroup, content } = await received(plainEntry);
    expect(metadata).toEqual([address, address, false]);
    expect(replyTo).toBe(address);
    expect(isGroup).toBe(false);
    expect(content).toEqual({
      text: messageText,
      address,
      sender: address,
      senderId: address,
    });
  });
  it("hands a question to the host as its inbox entry, in its group", async () => {
    const { metadata, isGroup, content } = await received(questionEntry);
    expect(metadata).toEqual([group, group, true]);
    expect(isGroup).toBe(true);
    expect(content).toMatchObject({
      address: group,
      sender: "agent:alice",
      members: ["agent:alice", "agent:bob", "agent:carol"],
    });
    expect(entryInText(content)).toEqual(questionEntry);
  });
  it("hands a result to the host as its inbox entry, from Social Harness", async () => {
    const { isGroup, content } = await received(resultEntry);
    expect(isGroup).toBe(true);
    expect(content).toMatchObject({ sender: "Social Harness" });
    expect(entryInText(content)).toEqual(resultEntry);
  });
  it("hands a message with a firewall reminder to the host as its inbox entry", async () => {
    const { content } = await received(remindedEntry);
    expect(content.sender).toBe(address);
    expect(entryInText(content)).toEqual(remindedEntry);
  });
  it("disconnects when the native inbound callback fails", async () => {
    const fake = connection();
    const current = await channel();
    const callbacks = host();
    callbacks.onInboundEvent.mockRejectedValue(new Error("host refused"));
    await current.setup(callbacks);
    Effect.runSync(Queue.offer(fake.inbox, plainEntry));
    await vi.waitFor(() => {
      expect(current.isConnected()).toBe(false);
    });
  });
});

// @agent-code-guard/regression-only: Native callback completion and fixed send-failure classes define distinct lifecycle cases.
describe("NanoClaw sends", () => {
  it.each([
    { sends: "plain text", text: messageText, sent: { text: messageText } },
    { sends: "a question", text: JSON.stringify(question), sent: question },
    { sends: "an answer", text: JSON.stringify(answer), sent: answer },
    {
      sends: "JSON that is not send_message arguments",
      text: '{"note":"prose"}',
      sent: { text: '{"note":"prose"}' },
    },
  ])("sends $sends through send_message once", async ({ text, sent }) => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await current.deliver(address, null, { kind: "chat", content: text });
    expect(fake.call).toHaveBeenCalledTimes(1);
    expect(fake.call.mock.calls[0]?.[0]).toEqual({
      name: "send_message",
      arguments: { ...sent, to: address },
    });
  });
  it("tells the model which argument the daemon refused", async () => {
    const fake = connection();
    const current = await channel();
    const callbacks = host();
    const issues = [
      { path: ["collective", "deadline"], message: "Expected a number" },
    ];
    fake.call.mockRejectedValue(
      new ProtocolError(-32602, "Invalid tool arguments", { issues }),
    );
    await current.setup(callbacks);
    await expect(
      current.deliver(address, null, {
        kind: "chat",
        content: JSON.stringify({ ...question, collective: { op: "gather" } }),
      }),
    ).resolves.toBeUndefined();
    expect(
      jsonObject(callbacks.onInboundEvent.mock.calls[0]?.[0].message.content),
    ).toMatchObject({
      sender: "Social Harness",
      text: new SendError({
        reason: "content-invalid",
        detail: "the arguments are invalid",
        issues,
      }).message,
    });
  });
  it("reports an uncertain send without creating another send", async () => {
    const fake = connection();
    const current = await channel();
    const callbacks = host();
    fake.call.mockRejectedValue(
      new ProtocolError(-32603, "Uncertain", { reason: "outcome-unknown" }),
    );
    await current.setup(callbacks);
    await expect(
      current.deliver(address, null, { kind: "chat", content: messageText }),
    ).resolves.toBeUndefined();
    expect(
      callbacks.onInboundEvent.mock.calls[0]?.[0].message.content,
    ).toContain(new SendError({ reason: "outcome-unknown" }).message);
    expect(fake.call).toHaveBeenCalledTimes(1);
  });
  it("refuses an implicit destination before contacting MCP", async () => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await expect(
      current.deliver("Alice", null, { kind: "chat", content: messageText }),
    ).rejects.toThrow("explicit agent or group address");
    expect(fake.call).not.toHaveBeenCalled();
  });
});

/* eslint-enable agent-code-guard/async-keyword -- Restore Effect-first production boundaries. */
