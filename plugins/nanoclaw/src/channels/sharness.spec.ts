/** @file NanoClaw hands each inbox entry to its host and sends native text through a stock MCP client. */
import type * as SocialHarness from "@social-harness/social-harness";
import { Client } from "@modelcontextprotocol/client";
import {
  AgentAddress,
  AgentCard,
  type connectFromEnvironment,
  DeliveryToken,
  InboxEntry,
  type McpConnection,
  MsgId,
  type SendResult,
} from "@social-harness/social-harness";
import {
  Effect,
  FastCheck as fc,
  Fiber,
  Queue,
  Schema,
  Scope,
  Stream,
} from "effect";
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
/** Reads an entry as `receive_messages` returns it. */
const readEntry = Schema.decodeUnknownSync(Schema.typeSchema(InboxEntry));
const message = {
  kind: "message",
  message: {
    msgId,
    author: alice,
    recipients: [bob],
    content: [{ type: "text", text: messageText }],
  },
};
const plainEntry = readEntry({ deliveryToken, address, item: message });
/** Alice's message in Bob's inbox with guidance. */
const guidedEntry = readEntry({
  deliveryToken,
  address,
  item: message,
  guidance: {
    checkId: `chk_${"a".repeat(64)}`,
    instructions: [
      {
        ruleId: "confirm-times",
        instruction: "Confirm the time before agreeing.",
        uncertain: { choice: "violation", probability: 0.6 },
      },
    ],
  },
});
const agentName = fc.stringMatching(/^[a-z0-9]{3,8}$/);
/** An explicit address of one agent. */
const addresses = agentName.map((name) => `agent:${name}`);
/** Fields a model may write beside a reply reference, an address among them. */
const besides = fc.dictionary(
  fc.constantFrom("text", "to", "note"),
  fc.string(),
);
/** A reply as a model writes it: a JSON object with a `replyToMsgId` property. */
const replies = besides.map((beside) => ({ ...beside, replyToMsgId: msgId }));
/** Text that is no such object: prose, or a JSON object without that property. */
const plainTexts = fc.oneof(
  fc.string(),
  besides.map((written) => JSON.stringify(written)),
);

type Adapter = NonNullable<Awaited<ReturnType<ChannelRegistration["factory"]>>>;
let registration: ChannelRegistration;
let adapter: Adapter | undefined;

const jsonObject = Schema.decodeUnknownSync(
  Schema.parseJson(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
);

/** Hands `entry` to a connected channel and returns what its host receives. */
async function received(entry: InboxEntry) {
  const fake = connection();
  const current = await channel();
  const callbacks = host();
  await current.setup(callbacks);
  Effect.runSync(Queue.offer(fake.inbox, entry));
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
  const mcp = new Client({ name: "nanoclaw-fixture", version: "1" });
  const call = vi.spyOn(mcp, "callTool").mockResolvedValue({
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
        mcp,
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
  const registered = boundary.register.mock.calls[0];
  if (registered === undefined) {
    throw new Error("Channel did not register");
  }
  registration = registered[1];
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

describe("NanoClaw inbox entries", () => {
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
  it("hands a message with guidance to the host as its inbox entry", async () => {
    const { metadata, isGroup, content } = await received(guidedEntry);
    expect(metadata).toEqual([address, address, false]);
    expect(isGroup).toBe(false);
    expect(content).toMatchObject({ address, sender: address });
    expect(entryInText(content)).toEqual(guidedEntry);
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

describe("NanoClaw sends", () => {
  it("sends text that is not a JSON reply as one message to its address", async () => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await fc.assert(
      fc.asyncProperty(addresses, plainTexts, async (to, text) => {
        fake.call.mockClear();
        await current.deliver(to, null, { kind: "chat", content: text });
        expect(fake.call.mock.calls.map(([called]) => called)).toEqual([
          { name: "send_message", arguments: { text, to } },
        ]);
      }),
    );
  });
  it("sends a JSON object that names a reply reference as those send_message arguments, to the row's address", async () => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await fc.assert(
      fc.asyncProperty(addresses, replies, async (to, written) => {
        fake.call.mockClear();
        await current.deliver(to, null, {
          kind: "chat",
          content: JSON.stringify(written),
        });
        expect(fake.call.mock.calls.map(([called]) => called)).toEqual([
          { name: "send_message", arguments: { ...written, to } },
        ]);
      }),
    );
  });
  it("refuses a name with no address scheme before contacting MCP", async () => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await fc.assert(
      fc.asyncProperty(agentName, async (name) => {
        await expect(
          current.deliver(name, null, { kind: "chat", content: messageText }),
        ).rejects.toThrow("explicit agent");
        expect(fake.call).not.toHaveBeenCalled();
      }),
    );
  });
  const invalidArguments = "Invalid tool arguments: text: Expected a string";
  it.each([
    {
      failure: "refuses an argument",
      answer: () =>
        Promise.resolve({
          isError: true,
          content: [{ type: "text" as const, text: invalidArguments }],
          structuredContent: { reason: "invalid-arguments" },
        }),
      told: invalidArguments,
    },
    {
      failure: "gives no answer",
      answer: () => Promise.reject(new Error("connection lost")),
      told: "send failed: the send did not complete; the message may or may not have been sent",
    },
  ])(
    "tells the model when the daemon $failure, and does not send again",
    async ({ answer, told }) => {
      const fake = connection();
      const current = await channel();
      const callbacks = host();
      fake.call.mockImplementation(answer);
      await current.setup(callbacks);
      await expect(
        current.deliver(address, null, { kind: "chat", content: messageText }),
      ).resolves.toBeUndefined();
      expect(
        jsonObject(callbacks.onInboundEvent.mock.calls[0]?.[0].message.content),
      ).toMatchObject({ sender: "Social Harness", text: told });
      expect(fake.call).toHaveBeenCalledTimes(1);
    },
  );
});

/* eslint-enable agent-code-guard/async-keyword -- Restore Effect-first production boundaries. */
