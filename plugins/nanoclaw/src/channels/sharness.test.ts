/** @file Adapter behavior through NanoClaw's registry and public endpoint capability. */

import type * as ClientPublic from "@moltzap/social-harness";
import {
  type acquireHarnessEndpoint,
  AgentAddress,
  AgentView,
  CollectiveError,
  ConnectError,
  decodeHarnessSendErrorData,
  type HarnessEndpoint,
  type InboundDelivery,
  InboundItem,
  ListenError,
  type LocalGuidance,
  MsgId,
  renderCollectiveResult,
  renderLocalGuidance,
  SendError,
  type SendResult,
} from "@moltzap/social-harness";
import { Deferred, Effect, Encoding, Queue, Schema, Stream } from "effect";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { ChannelRegistration, ChannelSetup } from "./adapter.js";
import type { registerChannelAdapter } from "./channel-registry.js";
import { credentialFile } from "../__tests__/credential.js";

/* eslint-disable agent-code-guard/async-keyword -- NanoClaw's registry, lifecycle, and host callbacks are Promise-native; these tests exercise that public ABI directly. */
/* eslint-disable agent-code-guard/acquire-release-requires-scope -- The adapter owns and closes the acquisition scope; the fake finalizer verifies release through teardown and stream termination. */

const boundary = vi.hoisted(() => ({
  register: vi.fn<typeof registerChannelAdapter>(),
  acquire: vi.fn<typeof acquireHarnessEndpoint>(),
}));

vi.mock("./channel-registry.js", () => ({
  registerChannelAdapter: boundary.register,
}));
vi.mock("@moltzap/social-harness", async (importOriginal) => ({
  ...(await importOriginal<typeof ClientPublic>()),
  acquireHarnessEndpoint: boundary.acquire,
}));

const EVENT_ID = `dlv_${"A".repeat(43)}`;
const MESSAGE_ID = `msg_${"A".repeat(43)}`;
const OPERATION_ID = `col_${"A".repeat(64)}`;
const GROUP = "group:alice,bob,carol";
const DEADLINE_TEXT = "2026-09-30T12:00:00.000Z";
const QUESTION = "Which day?";
const FORM = {
  type: "object",
  properties: { slot: { type: "string" } },
} as const;

type Adapter = NonNullable<Awaited<ReturnType<ChannelRegistration["factory"]>>>;
type HostEvent = Parameters<ChannelSetup["onInboundEvent"]>[0];

function makeHost() {
  return {
    onInbound: vi.fn<ChannelSetup["onInbound"]>(),
    onInboundEvent: vi.fn<ChannelSetup["onInboundEvent"]>(),
    onMetadata: vi.fn<ChannelSetup["onMetadata"]>(),
    onAction: vi.fn<ChannelSetup["onAction"]>(),
  };
}

function makeEndpoint() {
  const inbox = Effect.runSync(Queue.unbounded<InboundDelivery>());
  const released = vi.fn<() => void>();
  const send = vi.fn<HarnessEndpoint["send"]>(() =>
    Effect.succeed({
      kind: "multicast",
      msgId: Schema.decodeUnknownSync(MsgId)(MESSAGE_ID),
      address: Schema.decodeUnknownSync(AgentAddress)("agent:alice"),
    } satisfies SendResult),
  );
  const endpoint: HarnessEndpoint = { send, messages: Stream.fromQueue(inbox) };
  boundary.acquire.mockReturnValue(
    Effect.acquireRelease(Effect.succeed(endpoint), () =>
      Effect.sync(() => {
        released();
      }),
    ),
  );
  return { inbox, send, released };
}

function directItem() {
  return Schema.decodeUnknownSync(InboundItem)({
    kind: "multicast",
    message: {
      msgId: MESSAGE_ID,
      content: [{ type: "text", text: "hello" }],
      author: testAgentView("alice"),
      recipients: [testAgentView("bob")],
    },
  });
}

function enqueue(
  fake: ReturnType<typeof makeEndpoint>,
  item: InboundItem,
  guidance?: LocalGuidance,
  eventId = EVENT_ID,
) {
  const acknowledged = vi.fn<() => void>();
  Effect.runSync(
    Queue.offer(fake.inbox, {
      item,
      eventId,
      ...(guidance === undefined ? {} : { guidance }),
      acknowledge: Effect.sync(() => {
        acknowledged();
      }),
    }),
  );
  return acknowledged;
}

function eventContent(event: HostEvent) {
  return Schema.decodeUnknownSync(
    Schema.parseJson(
      Schema.Struct({
        text: Schema.String,
        address: Schema.String,
        sender: Schema.String,
        senderId: Schema.String,
        members: Schema.optional(Schema.Array(Schema.String)),
      }),
    ),
  )(event.message.content);
}

let adapter: Adapter | undefined;
let registration: ChannelRegistration;

beforeEach(async () => {
  vi.resetModules();
  boundary.register.mockClear();
  boundary.acquire.mockReset();
  vi.stubEnv("SHARNESS_MCP_URL", "http://127.0.0.1:8321/mcp");
  vi.stubEnv("SHARNESS_MCP_CREDENTIAL_FILE", credentialFile);
  await import("./sharness.js");
  expect(boundary.register).toHaveBeenCalledWith("sharness", expect.anything());
  const entry = boundary.register.mock.calls[0];
  expect(entry).toBeDefined();
  if (entry === undefined) {
    throw new Error("Channel did not register");
  }
  registration = entry[1];
});

afterEach(async () => {
  const current = adapter;
  adapter = undefined;
  await current?.teardown();
  vi.unstubAllEnvs();
});

async function createAdapter() {
  const created = await registration.factory();
  if (created === null) {
    throw new Error("Configured channel did not activate");
  }
  adapter = created;
  return created;
}

// @agent-code-guard/regression-only: The suite pins NanoClaw host handoff and the closed endpoint outcomes; real-host persistence is qualified outside this repository.
it("stays disabled without an MCP URL", async () => {
  vi.stubEnv("SHARNESS_MCP_URL", undefined);
  expect(await registration.factory()).toBeNull();
  expect(boundary.acquire).not.toHaveBeenCalled();
});

it("refuses delivery before setup", async () => {
  const fake = makeEndpoint();
  const channel = await createAdapter();
  expect(channel.isConnected()).toBe(false);
  await expect(
    channel.deliver("agent:alice", null, { kind: "chat", content: "hi" }),
  ).rejects.toThrow("not connected");
  expect(fake.send).not.toHaveBeenCalled();
});

it("acquires one endpoint across repeated setup", async () => {
  makeEndpoint();
  const channel = await createAdapter();
  await channel.setup(makeHost());
  await channel.setup(makeHost());
  expect(channel.isConnected()).toBe(true);
  expect(boundary.acquire).toHaveBeenCalledTimes(1);
});

it("releases the endpoint once across repeated teardown", async () => {
  const fake = makeEndpoint();
  const channel = await createAdapter();
  await channel.setup(makeHost());
  await channel.teardown();
  await channel.teardown();
  expect(channel.isConnected()).toBe(false);
  expect(fake.released).toHaveBeenCalledTimes(1);
});

it("refuses delivery after teardown", async () => {
  const fake = makeEndpoint();
  const channel = await createAdapter();
  await channel.setup(makeHost());
  await channel.teardown();
  await expect(
    channel.deliver("agent:alice", null, { kind: "chat", content: "hi" }),
  ).rejects.toThrow("not connected");
  expect(fake.send).not.toHaveBeenCalled();
});

it("reactivates after teardown with a new endpoint acquisition", async () => {
  const fake = makeEndpoint();
  const channel = await createAdapter();
  await channel.setup(makeHost());
  await channel.teardown();
  await channel.setup(makeHost());
  expect(channel.isConnected()).toBe(true);
  expect(boundary.acquire).toHaveBeenCalledTimes(2);
  await channel.teardown();
  expect(fake.released).toHaveBeenCalledTimes(2);
});

it("propagates acquisition failure without becoming connected", async () => {
  boundary.acquire.mockReturnValue(
    Effect.fail(new ConnectError({ reason: "transport-failed" })),
  );
  const channel = await createAdapter();
  await expect(channel.setup(makeHost())).rejects.toThrow("transport-failed");
  expect(channel.isConnected()).toBe(false);
});

it.each([
  { ending: "completion", messages: Stream.empty },
  {
    ending: "failure",
    messages: Stream.fail(new ListenError({ reason: "transport-failed" })),
  },
])(
  "disconnects and releases the endpoint on stream $ending",
  async ({ messages }) => {
    const fake = makeEndpoint();
    const stop = Effect.runSync(Queue.unbounded<undefined>());
    boundary.acquire.mockReturnValue(
      Effect.acquireRelease(
        Effect.succeed({
          send: fake.send,
          messages: Stream.fromEffect(Queue.take(stop)).pipe(
            Stream.flatMap(() => messages),
          ),
        }),
        () =>
          Effect.sync(() => {
            fake.released();
          }),
      ),
    );
    const channel = await createAdapter();
    await channel.setup(makeHost());
    Effect.runSync(Queue.offer(stop, undefined));
    await vi.waitFor(() => {
      expect(channel.isConnected()).toBe(false);
    });
    await channel.teardown();
    expect(fake.released).toHaveBeenCalledTimes(1);
  },
);

it("projects direct metadata before routing and acknowledges only after host success", async () => {
  const fake = makeEndpoint();
  const host = makeHost();
  const accepted = Effect.runSync(Deferred.make<undefined>());
  host.onInboundEvent.mockImplementation(() => {
    expect(host.onMetadata).toHaveBeenCalledWith(
      "agent:alice",
      "agent:alice",
      false,
    );
    return Effect.runPromise(Deferred.await(accepted));
  });
  const channel = await createAdapter();
  await channel.setup(host);
  const acknowledged = enqueue(fake, directItem());

  await vi.waitFor(() => {
    expect(host.onInboundEvent).toHaveBeenCalledTimes(1);
  });
  expect(acknowledged).not.toHaveBeenCalled();
  const event = host.onInboundEvent.mock.calls[0]?.[0];
  expect(event).toMatchObject({
    channelType: "cli",
    instance: "cli",
    platformId: "local",
    threadId: null,
    replyTo: {
      channelType: "sharness",
      platformId: "agent:alice",
      threadId: null,
    },
    message: { id: EVENT_ID, kind: "chat", isMention: true, isGroup: false },
  });
  if (event === undefined) {
    throw new Error("Host did not receive a turn");
  }
  expect(eventContent(event)).toEqual({
    text: "hello",
    address: "agent:alice",
    sender: "agent:alice",
    senderId: "agent:alice",
  });
  Effect.runSync(Deferred.succeed(accepted, undefined));
  await vi.waitFor(() => {
    expect(acknowledged).toHaveBeenCalledTimes(1);
  });
  expect(fake.send).not.toHaveBeenCalled();
});

it("leaves delivery unacknowledged and disconnects when the host fails", async () => {
  const fake = makeEndpoint();
  const host = makeHost();
  host.onInboundEvent.mockRejectedValue(new Error("Host persistence failed"));
  const channel = await createAdapter();
  await channel.setup(host);
  const acknowledged = enqueue(fake, directItem());
  await vi.waitFor(() => {
    expect(fake.released).toHaveBeenCalledTimes(1);
  });
  expect(channel.isConnected()).toBe(false);
  expect(acknowledged).not.toHaveBeenCalled();
});

it("preserves canonical group address, sender, exact membership and native group flag", async () => {
  const fake = makeEndpoint();
  const host = makeHost();
  const channel = await createAdapter();
  await channel.setup(host);
  const acknowledged = enqueue(
    fake,
    Schema.decodeUnknownSync(InboundItem)({
      kind: "multicast",
      message: {
        msgId: MESSAGE_ID,
        content: [{ type: "text", text: "group hello" }],
        author: testAgentView("bob"),
        recipients: [testAgentView("alice"), testAgentView("carol")],
      },
    }),
  );
  await vi.waitFor(() => {
    expect(acknowledged).toHaveBeenCalledTimes(1);
  });
  expect(host.onMetadata).toHaveBeenCalledWith(GROUP, GROUP, true);
  const event = host.onInboundEvent.mock.calls[0]?.[0];
  expect(event).toMatchObject({
    replyTo: { platformId: GROUP },
    message: { isGroup: true },
  });
  if (event === undefined) {
    throw new Error("Host did not receive a turn");
  }
  expect(eventContent(event)).toEqual({
    text: "group hello",
    address: GROUP,
    sender: "agent:bob",
    senderId: "agent:bob",
    members: ["agent:alice", "agent:bob", "agent:carol"],
  });
});

it.each([
  { op: "gather", to: "agent:alice", isGroup: false },
  { op: "all_gather", to: GROUP, isGroup: true },
])(
  "renders a $op request with question, form, deadline and reply address",
  async ({ op, to, isGroup }) => {
    const fake = makeEndpoint();
    const host = makeHost();
    const channel = await createAdapter();
    await channel.setup(host);
    const acknowledged = enqueue(fake, requestItem(op, to));
    await vi.waitFor(() => {
      expect(acknowledged).toHaveBeenCalledTimes(1);
    });
    const event = host.onInboundEvent.mock.calls[0]?.[0];
    expect(event).toMatchObject({
      replyTo: { platformId: to },
      message: { id: EVENT_ID, isGroup },
    });
    if (event === undefined) {
      throw new Error("Host did not receive a turn");
    }
    expect(eventContent(event)).toMatchObject({
      sender: "agent:alice",
      address: to,
    });
    expect(eventContent(event).text).toContain(QUESTION);
    const content = eventContent(event);
    expect(content.text).toContain(op);
    expect(content.text).toContain(`Form: ${JSON.stringify(FORM)}`);
    expect(content.text).toContain(DEADLINE_TEXT);
    expect(content.text).toContain(`send_message to ${to}`);
  },
);

it("attributes a result to Social Harness and lists every member outcome in the addressed conversation", async () => {
  const fake = makeEndpoint();
  const host = makeHost();
  const channel = await createAdapter();
  await channel.setup(host);
  const item = Schema.decodeUnknownSync(InboundItem)({
    kind: "collectiveResult",
    closedAt: 0,
    closedBy: "deadline",
    op: "gather",
    operationId: OPERATION_ID,
    to: GROUP,
    question: QUESTION,
    outcomes: [
      {
        member: testAgentView("bob"),
        outcome: { kind: "answered", content: { slot: "mon" } },
      },
      { member: testAgentView("carol"), outcome: { kind: "no-answer" } },
    ],
  });
  if (item.kind !== "collectiveResult") {
    throw new Error("Expected a result fixture");
  }
  const acknowledged = enqueue(fake, item);
  await vi.waitFor(() => {
    expect(acknowledged).toHaveBeenCalledTimes(1);
  });
  const event = host.onInboundEvent.mock.calls[0]?.[0];
  expect(event).toMatchObject({ replyTo: { platformId: GROUP } });
  if (event === undefined) {
    throw new Error("Host did not receive a turn");
  }
  const content = eventContent(event);
  expect(content).toMatchObject({ sender: "Social Harness", address: GROUP });
  expect(content.text).toBe(renderCollectiveResult(item));
});

it("delivers an operation failure as a Social Harness turn and acknowledges host acceptance", async () => {
  const fake = makeEndpoint();
  const host = makeHost();
  const channel = await createAdapter();
  await channel.setup(host);
  const acknowledged = enqueue(
    fake,
    Schema.decodeUnknownSync(InboundItem)({
      kind: "operationFailed",
      operationId: OPERATION_ID,
      to: "agent:alice",
      error: "reply failed: no question is open in this conversation",
    }),
  );
  await vi.waitFor(() => {
    expect(acknowledged).toHaveBeenCalledTimes(1);
  });
  const event = host.onInboundEvent.mock.calls[0]?.[0];
  expect(event).toMatchObject({ replyTo: { platformId: "agent:alice" } });
  if (event === undefined) {
    throw new Error("Host did not receive a turn");
  }
  expect(eventContent(event)).toMatchObject({
    sender: "Social Harness",
    address: "agent:alice",
    text: "reply failed: no question is open in this conversation",
  });
});

it.each(["hello", { text: "hello" }])(
  "sends explicit agent text without prior discovery: %j",
  async (content) => {
    const fake = makeEndpoint();
    const channel = await createAdapter();
    await channel.setup(makeHost());
    await expect(
      channel.deliver("agent:alice", null, { kind: "chat", content }),
    ).resolves.toBeUndefined();
    expect(fake.send).toHaveBeenCalledExactlyOnceWith({
      to: "agent:alice",
      text: "hello",
    });
  },
);

it.each([
  { to: GROUP, text: "hello", input: { to: GROUP, text: "hello" } },
  {
    to: GROUP,
    text: JSON.stringify({
      gather: QUESTION,
      deadline: 60,
      requestedSchema: FORM,
    }),
    input: {
      to: GROUP,
      text: QUESTION,
      collective: { op: "gather", deadline: 60, requestedSchema: FORM },
    },
  },
  {
    to: GROUP,
    text: JSON.stringify({
      all_gather: QUESTION,
      deadline: 60,
      requestedSchema: FORM,
    }),
    input: {
      to: GROUP,
      text: QUESTION,
      collective: { op: "all_gather", deadline: 60, requestedSchema: FORM },
    },
  },
  {
    to: "agent:alice",
    text: JSON.stringify({ operationId: OPERATION_ID, action: "decline" }),
    input: {
      to: "agent:alice",
      collectiveResponse: { operationId: OPERATION_ID, action: "decline" },
    },
  },
])(
  "sends the operation its text states: $text",
  async ({ to, text, input }) => {
    const fake = makeEndpoint();
    const channel = await createAdapter();
    await channel.setup(makeHost());
    await channel.deliver(to, null, { kind: "chat", content: text });
    expect(fake.send).toHaveBeenCalledExactlyOnceWith(input);
  },
);

it("rejects a friendly alias before it reaches Client", async () => {
  const fake = makeEndpoint();
  const channel = await createAdapter();
  await channel.setup(makeHost());
  await expect(
    channel.deliver("Alice", null, { kind: "chat", content: "hello" }),
  ).rejects.toThrow("explicit agent or group address");
  expect(fake.send).not.toHaveBeenCalled();
});

it("hands a collective refusal to the model through NanoClaw's native callback", async () => {
  const fake = makeEndpoint();
  const data = Effect.runSync(
    decodeHarnessSendErrorData({
      reason: "collective-failed",
      operationId: OPERATION_ID,
      failure: {
        kind: "schema-invalid",
        detail: "The requested form is invalid",
      },
    }),
  );
  if (!("failure" in data)) {
    throw new Error("Expected collective refusal data");
  }
  const failure = new CollectiveError(data);
  fake.send.mockReturnValue(Effect.fail(failure));
  const host = makeHost();
  const channel = await createAdapter();
  await channel.setup(host);

  await expect(
    channel.deliver(GROUP, null, {
      kind: "chat",
      content: JSON.stringify({
        gather: QUESTION,
        deadline: 60,
        requestedSchema: FORM,
      }),
    }),
  ).resolves.toBeUndefined();

  const event = host.onInboundEvent.mock.calls[0]?.[0];
  expect(event).toBeDefined();
  if (event === undefined) {
    throw new Error("Host did not receive the refusal");
  }
  expect(eventContent(event)).toMatchObject({
    text: failure.message,
    sender: "Social Harness",
  });
  expect(fake.send).toHaveBeenCalledTimes(1);
});

it("reports parser refusals to the model without sending or failing the delivery", async () => {
  const fake = makeEndpoint();
  const host = makeHost();
  const channel = await createAdapter();
  await channel.setup(host);
  await expect(
    channel.deliver("agent:alice", null, {
      kind: "chat",
      content: '{"gather":"Which day?","deadline":0}',
    }),
  ).resolves.toBeUndefined();
  expect(fake.send).not.toHaveBeenCalled();
  const event = host.onInboundEvent.mock.calls[0]?.[0];
  if (event === undefined) {
    throw new Error("Host did not receive the refusal");
  }
  expect(eventContent(event)).toMatchObject({
    sender: "Social Harness",
    address: "agent:alice",
  });
  // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The parser refusal must identify the invalid input field; no public constant names JSON fields.
  expect(eventContent(event).text).toContain("deadline");
});

it.each([
  "invalid-address",
  "unknown-agent",
  "membership-invalid",
  "content-invalid",
  "delivery-pending",
  "outcome-unknown",
] as const)(
  "reports %s to the model and completes the delivery",
  async (reason) => {
    const fake = makeEndpoint();
    const error = new SendError({ reason });
    fake.send.mockReturnValue(Effect.fail(error));
    const host = makeHost();
    const channel = await createAdapter();
    await channel.setup(host);
    await expect(
      channel.deliver("agent:alice", null, {
        kind: "chat",
        content: "hello",
      }),
    ).resolves.toBeUndefined();
    expect(fake.send).toHaveBeenCalledTimes(1);
    const event = host.onInboundEvent.mock.calls[0]?.[0];
    expect(event).toMatchObject({ replyTo: { platformId: "agent:alice" } });
    if (event === undefined) {
      throw new Error("Host did not receive the refusal");
    }
    expect(eventContent(event)).toMatchObject({
      sender: "Social Harness",
      text: error.message,
    });
  },
);

it.each(["delivery-pending", "outcome-unknown"] as const)(
  "does not permit retry of %s even if reporting to the host fails",
  async (reason) => {
    const fake = makeEndpoint();
    fake.send.mockReturnValue(Effect.fail(new SendError({ reason })));
    const host = makeHost();
    host.onInboundEvent.mockRejectedValue(new Error("Host unavailable"));
    const channel = await createAdapter();
    await channel.setup(host);
    await expect(
      channel.deliver("agent:alice", null, {
        kind: "chat",
        content: "hello",
      }),
    ).resolves.toBeUndefined();
    expect(fake.send).toHaveBeenCalledTimes(1);
  },
);

it("propagates transient failure to NanoClaw's retry without retrying itself", async () => {
  const fake = makeEndpoint();
  fake.send.mockReturnValue(
    Effect.fail(new SendError({ reason: "network-unavailable" })),
  );
  const host = makeHost();
  const channel = await createAdapter();
  await channel.setup(host);
  await expect(
    channel.deliver("agent:alice", null, { kind: "chat", content: "hello" }),
  ).rejects.toThrow("network unavailable");
  expect(fake.send).toHaveBeenCalledTimes(1);
  expect(host.onInboundEvent).not.toHaveBeenCalled();
});

it("hands guidance and quoted peer text to the agent before acknowledging", async () => {
  const fake = makeEndpoint();
  const host = makeHost();
  const accepted = Effect.runSync(Deferred.make<undefined>());
  host.onInboundEvent.mockImplementation(() =>
    Effect.runPromise(Deferred.await(accepted)),
  );
  const channel = await createAdapter();
  await channel.setup(host);
  const guidance: LocalGuidance = {
    checkId: `chk_${"a".repeat(64)}`,
    subjects: [
      {
        id: "hello",
        sender: testAgentView("alice"),
        recipients: [testAgentView("bob")],
        text: "hello",
      },
    ],
    instructions: [
      {
        middlewareId: "norms",
        ruleId: "reply",
        expectation: "required",
        subjects: ["hello"],
        instruction: "Answer Alice.",
      },
    ],
  };
  const acknowledged = enqueue(fake, directItem(), guidance);
  await vi.waitFor(() => {
    expect(host.onInboundEvent).toHaveBeenCalledTimes(1);
  });
  expect(acknowledged).not.toHaveBeenCalled();
  const event = host.onInboundEvent.mock.calls[0]?.[0];
  if (event === undefined) {
    throw new Error("Host did not receive guidance");
  }
  expect(eventContent(event)).toMatchObject({
    sender: "agent:alice",
    address: "agent:alice",
    text: renderLocalGuidance(guidance, { text: "hello", origin: "peer" }),
  });
  Effect.runSync(Deferred.succeed(accepted, undefined));
  await vi.waitFor(() => {
    expect(acknowledged).toHaveBeenCalledTimes(1);
  });
});

it.each([
  {
    kind: "collectiveResult",
    sender: "Social Harness",
    label: "Social Harness item, quoting peer answers (quoted content):",
  },
  {
    kind: "collectiveRequest",
    sender: "agent:alice",
    label: "Peer message (quoted content):",
  },
] as const)(
  "hands $kind guidance to the host before acknowledging",
  async ({ kind, sender, label }) => {
    const fake = makeEndpoint();
    const host = makeHost();
    const accepted = Effect.runSync(Deferred.make<undefined>());
    host.onInboundEvent.mockImplementation(() =>
      Effect.runPromise(Deferred.await(accepted)),
    );
    const channel = await createAdapter();
    await channel.setup(host);
    const item = collectiveGuidanceItem(kind);
    const guidance: LocalGuidance = {
      checkId: `chk_${"a".repeat(64)}`,
      subjects: [],
      instructions: [],
    };
    const acknowledged = enqueue(fake, item, guidance);
    await vi.waitFor(() => {
      expect(host.onInboundEvent).toHaveBeenCalledTimes(1);
    });
    expect(acknowledged).not.toHaveBeenCalled();
    const event = host.onInboundEvent.mock.calls[0]?.[0];
    if (event === undefined) {
      throw new Error("Host did not receive a turn");
    }
    const content = eventContent(event);
    expect(content.sender).toBe(sender);
    expect(content.text).toContain(label);
    expect(event.replyTo?.platformId).toBe(
      kind === "collectiveResult" ? GROUP : "agent:alice",
    );
    Effect.runSync(Deferred.succeed(accepted, undefined));
    await vi.waitFor(() => {
      expect(acknowledged).toHaveBeenCalledTimes(1);
    });
  },
);

/** Both collective kinds enter through the public delivery schema. */
function collectiveGuidanceItem(
  kind: "collectiveResult" | "collectiveRequest",
) {
  return Schema.decodeUnknownSync(InboundItem)(
    kind === "collectiveResult"
      ? {
          kind,
          closedAt: 0,
          closedBy: "settled",
          op: "gather",
          operationId: OPERATION_ID,
          to: GROUP,
          question: QUESTION,
          outcomes: [
            {
              member: testAgentView("alice"),
              outcome: { kind: "answered", content: { slot: "mon" } },
            },
          ],
        }
      : requestItem("gather", "agent:alice"),
  );
}

/** A peer question keeps the same message envelope as other deliveries. */
function requestItem(op: string, to: string) {
  return Schema.decodeUnknownSync(InboundItem)({
    kind: "collectiveRequest",
    message: {
      msgId: MESSAGE_ID,
      author: testAgentView("alice"),
      recipients: (to.startsWith("group:")
        ? to.slice("group:".length).split(",")
        : ["bob"]
      )
        .map(testAgentView)
        .filter((member) => member.agentName !== "alice"),
      content: {
        op,
        operationId: OPERATION_ID,
        question: QUESTION,
        requestedSchema: FORM,
        deadlineAt: Date.parse(DEADLINE_TEXT),
      },
    },
  });
}

it("preserves distinct delivery identities when two group authors use the same message id", async () => {
  const fake = makeEndpoint();
  const host = makeHost();
  const channel = await createAdapter();
  await channel.setup(host);
  const secondEvent = `dlv_${"B".repeat(43)}`;
  const first = Schema.decodeUnknownSync(InboundItem)({
    kind: "multicast",
    message: {
      msgId: MESSAGE_ID,
      author: testAgentView("alice"),
      recipients: [testAgentView("bob"), testAgentView("carol")],
      content: [{ type: "text", text: "Alice's message" }],
    },
  });
  const second = Schema.decodeUnknownSync(InboundItem)({
    kind: "multicast",
    message: {
      msgId: MESSAGE_ID,
      author: testAgentView("bob"),
      recipients: [testAgentView("alice"), testAgentView("carol")],
      content: [{ type: "text", text: "Bob's message" }],
    },
  });
  enqueue(fake, first);
  enqueue(fake, second, undefined, secondEvent);
  await vi.waitFor(() => {
    expect(host.onInboundEvent).toHaveBeenCalledTimes(2);
  });
  expect(
    host.onInboundEvent.mock.calls.map(([event]) => event.message.id),
  ).toEqual([EVENT_ID, secondEvent]);
});

/* eslint-enable agent-code-guard/async-keyword -- Restore Effect-first defaults after the Promise-native host boundary tests. */
/* eslint-enable agent-code-guard/acquire-release-requires-scope -- Restore the scope guard after the adapter-owned acquisition fake. */

function testAgentView(name: string): AgentView {
  const byte = ["alice", "bob", "carol"].indexOf(name) + 1;
  const suffix = Encoding.encodeBase64Url(new Uint8Array(16).fill(byte));
  return Schema.decodeUnknownSync(AgentView)({
    agentId: `agt_${suffix}`,
    agentName: name,
    principalId: `prn_${suffix}`,
  });
}
