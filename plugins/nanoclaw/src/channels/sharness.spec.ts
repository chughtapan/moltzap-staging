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
/** The `send_message` arguments of a question, without their destination. */
const question = {
  text: "Which day works?",
  collective: { op: "all_gather", deadline: 60, requestedSchema: slotForm },
};
/** The `send_message` arguments of an answer, without their destination. */
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
const agentName = fc.stringMatching(/^[a-z0-9]{3,8}$/);
/** An explicit destination: one agent, or a group's members in any order. */
const destinations = fc.oneof(
  agentName.map((name) => `agent:${name}`),
  fc
    .uniqueArray(agentName, { minLength: 2, maxLength: 4 })
    .map((names) => `group:${names.join(",")}`),
);
/** Fields a model may write beside a question or an answer, a destination among them. */
const besides = fc.dictionary(
  fc.constantFrom("text", "to", "note"),
  fc.string(),
);
/** A question or an answer as a model writes it: a JSON object with a `collective` or `collectiveResponse` property. */
const operations = fc
  .tuple(besides, fc.constantFrom(question, answer))
  .map(([beside, named]) => ({ ...beside, ...named }));
/** Text that is no such object: prose, or a JSON object with neither property. */
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
  it("sends text that is not a question or an answer as one plain message to its address", async () => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await fc.assert(
      fc.asyncProperty(destinations, plainTexts, async (to, text) => {
        fake.call.mockClear();
        await current.deliver(to, null, { kind: "chat", content: text });
        expect(fake.call.mock.calls.map(([called]) => called)).toEqual([
          { name: "send_message", arguments: { text, to } },
        ]);
      }),
    );
  });
  it("sends a JSON object that names a question or an answer as those send_message arguments, to the row's address", async () => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await fc.assert(
      fc.asyncProperty(destinations, operations, async (to, written) => {
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
  it("refuses a name with no agent: or group: scheme before contacting MCP", async () => {
    const fake = connection();
    const current = await channel();
    await current.setup(host());
    await fc.assert(
      fc.asyncProperty(agentName, async (name) => {
        await expect(
          current.deliver(name, null, { kind: "chat", content: messageText }),
        ).rejects.toThrow("explicit agent or group address");
        expect(fake.call).not.toHaveBeenCalled();
      }),
    );
  });
  const issues = [
    { path: ["collective", "deadline"], message: "Expected a number" },
  ];
  it.each([
    {
      failure: "refuses an argument",
      error: new ProtocolError(-32602, "Invalid tool arguments", { issues }),
      told: new SendError({
        reason: "content-invalid",
        detail: "the arguments are invalid",
        issues,
      }),
    },
    {
      failure: "cannot say whether it sent",
      error: new ProtocolError(-32603, "Uncertain", {
        reason: "outcome-unknown",
      }),
      told: new SendError({ reason: "outcome-unknown" }),
    },
  ])(
    "tells the model when the daemon $failure, and does not send again",
    async ({ error, told }) => {
      const fake = connection();
      const current = await channel();
      const callbacks = host();
      fake.call.mockRejectedValue(error);
      await current.setup(callbacks);
      await expect(
        current.deliver(address, null, { kind: "chat", content: messageText }),
      ).resolves.toBeUndefined();
      expect(
        jsonObject(callbacks.onInboundEvent.mock.calls[0]?.[0].message.content),
      ).toMatchObject({ sender: "Social Harness", text: told.message });
      expect(fake.call).toHaveBeenCalledTimes(1);
    },
  );
});

/* eslint-enable agent-code-guard/async-keyword -- Restore Effect-first production boundaries. */
