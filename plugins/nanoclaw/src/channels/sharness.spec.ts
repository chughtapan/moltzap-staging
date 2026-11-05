/** @file NanoClaw routes incoming work and translates native sends through a stock MCP client. */
import { Client, ProtocolError } from "@modelcontextprotocol/client";
import {
  AgentAddress,
  AgentCard,
  DeliveryToken,
  MsgId,
  PermittedItem,
  SendError,
  type SendResult,
} from "@moltzap/social-harness";
import { Effect, Fiber, Queue, Schema, Scope, Stream } from "effect";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type * as McpModule from "#sharness-mcp/mcp-client";
import type {
  acquireMcpConnection,
  InboxEntry,
  McpConnection,
} from "#sharness-mcp/mcp-client";
import type { ChannelRegistration, ChannelSetup } from "./adapter.js";
import type { registerChannelAdapter } from "./channel-registry.js";
import { credentialFile } from "../__tests__/credential.js";

/* eslint-disable agent-code-guard/async-keyword -- NanoClaw's lifecycle and host callbacks are Promise-native. */
const boundary = vi.hoisted(() => ({
  register: vi.fn<typeof registerChannelAdapter>(),
  acquire: vi.fn<typeof acquireMcpConnection>(),
}));
vi.mock("./channel-registry.js", () => ({
  registerChannelAdapter: boundary.register,
}));
vi.mock("#sharness-mcp/mcp-client", async (original) => ({
  ...(await original<typeof McpModule>()),
  acquireMcpConnection: boundary.acquire,
}));

const messageText = "hello";
const deliveryToken = `dlv_${"A".repeat(43)}`;
const msgId = Schema.decodeUnknownSync(MsgId)(`msg_${"A".repeat(43)}`);
const address = Schema.decodeUnknownSync(AgentAddress)("agent:alice");
const agent = (name: string, byte: number) =>
  Schema.decodeUnknownSync(AgentCard)({
    agentId: `agt_${Buffer.alloc(16, byte).toString("base64url")}`,
    agentName: name,
    principalId: `prn_${Buffer.alloc(16, byte).toString("base64url")}`,
  });
const item = Schema.decodeUnknownSync(PermittedItem)({
  kind: "multicast",
  message: {
    msgId,
    author: agent("alice", 1),
    recipients: [agent("bob", 2)],
    content: [{ type: "text", text: messageText }],
  },
});
type Adapter = NonNullable<Awaited<ReturnType<ChannelRegistration["factory"]>>>;
let registration: ChannelRegistration;
let adapter: Adapter | undefined;

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
        consumeInbox: (options, accept) =>
          // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- The adapter opens and closes this fixture in its activation scope.
          Effect.acquireUseRelease(
            Stream.fromQueue(inbox).pipe(
              Stream.runForEach((entry) => accept(entry)),
              Effect.zipLeft(options.onReady ?? Effect.void),
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
  vi.stubEnv("SHARNESS_MCP_CREDENTIAL_FILE", credentialFile);
  vi.stubEnv("SHARNESS_MCP_ALLOW_HTTP", undefined);
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
  it.each([
    { value: undefined, expected: false },
    { value: "false", expected: false },
    { value: "true", expected: true },
  ])(
    "passes the host HTTP permission $value to MCP",
    async ({ value, expected }) => {
      vi.stubEnv("SHARNESS_MCP_ALLOW_HTTP", value);
      connection();
      const current = await channel();
      await current.setup(host());
      expect(boundary.acquire).toHaveBeenCalledWith(
        expect.objectContaining({ allowHttp: expected }),
        expect.anything(),
      );
    },
  );
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

// @agent-code-guard/regression-only: Native callback completion and fixed send-failure classes define distinct lifecycle cases.
describe("NanoClaw messages", () => {
  it("renders incoming content with its native reply route", async () => {
    const fake = connection();
    const current = await channel();
    const callbacks = host();
    await current.setup(callbacks);
    Effect.runSync(
      Queue.offer(fake.inbox, {
        deliveryToken: Schema.decodeUnknownSync(DeliveryToken)(deliveryToken),
        address,
        item,
      }),
    );
    await vi.waitFor(() => {
      expect(callbacks.onInboundEvent).toHaveBeenCalledTimes(1);
    });
    expect(callbacks.onMetadata).toHaveBeenCalledWith(address, address, false);
    const event = callbacks.onInboundEvent.mock.calls[0]?.[0];
    expect(event?.replyTo?.platformId).toBe(address);
    expect(event?.message.content).toContain(messageText);
  });
  it("disconnects when the native inbound callback fails", async () => {
    const fake = connection();
    const current = await channel();
    const callbacks = host();
    callbacks.onInboundEvent.mockRejectedValue(new Error("host refused"));
    await current.setup(callbacks);
    Effect.runSync(
      Queue.offer(fake.inbox, {
        deliveryToken: Schema.decodeUnknownSync(DeliveryToken)(deliveryToken),
        address,
        item,
      }),
    );
    await vi.waitFor(() => {
      expect(current.isConnected()).toBe(false);
    });
  });
  it("sends the explicitly addressed text through send_message once", async () => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await current.deliver(address, null, {
      kind: "chat",
      content: messageText,
    });
    expect(fake.call).toHaveBeenCalledTimes(1);
    expect(fake.call.mock.calls[0]?.[0]).toEqual({
      name: "send_message",
      arguments: { input: { to: address, text: messageText } },
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
