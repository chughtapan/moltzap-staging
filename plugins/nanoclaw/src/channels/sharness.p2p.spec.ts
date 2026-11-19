/** @file NanoClaw hands a group message to its host in its group, and sends to a group address. */
import type * as SocialHarness from "@social-harness/social-harness";
import { Client } from "@modelcontextprotocol/client";
import {
  AgentCard,
  type connectFromEnvironment,
  GroupAddress,
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
import { afterEach, beforeEach, expect, it, vi } from "vitest";
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

const msgId = Schema.decodeUnknownSync(MsgId)(`msg_${"A".repeat(43)}`);
const address = Schema.decodeUnknownSync(GroupAddress)("group:alice,bob,carol");
const [alice, bob, carol] = ["alice", "bob", "carol"].map((name, index) =>
  Schema.decodeUnknownSync(AgentCard)({
    agentId: `agt_${Buffer.alloc(16, index + 1).toString("base64url")}`,
    agentName: name,
    principalId: `prn_${Buffer.alloc(16, index + 1).toString("base64url")}`,
  }),
);
/** Alice's message to the group, in Bob's inbox. */
const groupEntry = Schema.decodeUnknownSync(Schema.typeSchema(InboxEntry))({
  deliveryToken: `dlv_${"A".repeat(43)}`,
  address,
  item: {
    kind: "message",
    message: {
      msgId,
      author: alice,
      recipients: [bob, carol],
      content: [{ type: "text", text: "hello" }],
    },
  },
});
const agentName = fc.stringMatching(/^[a-z0-9]{3,8}$/);

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

it("hands a group message to the host in its group, naming its members", async () => {
  const fake = connection();
  const current = await channel();
  const callbacks = host();
  await current.setup(callbacks);
  Effect.runSync(Queue.offer(fake.inbox, groupEntry));
  await vi.waitFor(() => {
    expect(callbacks.onInboundEvent).toHaveBeenCalledTimes(1);
  });
  const event = callbacks.onInboundEvent.mock.calls[0]?.[0];
  expect(callbacks.onMetadata.mock.calls[0]).toEqual([address, address, true]);
  expect(event?.message.isGroup).toBe(true);
  expect(
    Schema.decodeUnknownSync(Schema.parseJson(Schema.Unknown))(
      event?.message.content,
    ),
  ).toMatchObject({
    address,
    sender: "agent:alice",
    members: ["agent:alice", "agent:bob", "agent:carol"],
  });
});

it("sends text to a group address, its members in any order", async () => {
  const fake = connection();
  const current = await channel();
  await current.setup(host());
  await fc.assert(
    fc.asyncProperty(
      fc
        .uniqueArray(agentName, { minLength: 2, maxLength: 4 })
        .map((names) => `group:${names.join(",")}`),
      fc.string(),
      async (to, text) => {
        fake.call.mockClear();
        await current.deliver(to, null, { kind: "chat", content: text });
        expect(fake.call.mock.calls.map(([called]) => called)).toEqual([
          { name: "send_message", arguments: { text, to } },
        ]);
      },
    ),
  );
});

/* eslint-enable agent-code-guard/async-keyword -- Restore Effect-first production boundaries. */
