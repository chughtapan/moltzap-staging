/** @file A real daemon recovers a delivery held after Router ordering. */

import {
  AgentAddress,
  type AgentCard,
  type AgentName,
  type Content,
  type InboxEntry,
  type SendResult,
} from "@moltzap/social-harness";
import {
  Duration,
  Effect,
  Fiber,
  Queue,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { expect, it } from "vitest";
import {
  acquireMcpConnection,
  callTool,
  type McpConnection,
} from "../../../plugins/shared/mcp-client.js";
import { registerFixture } from "../agent-fixture.js";
import {
  acquireRouterHoldProxy,
  type RouterHoldProxy,
} from "../communication/router-hold-proxy.js";
import {
  acquireDaemonManagementClient,
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  configureDaemonFixture,
  type DaemonProcessFixture,
  type FirewallMode,
  makeDaemonProcessFixture,
} from "../daemon/daemon-fixture.js";
import { inboxStream } from "../mcp-inbox.js";
import { historyMessage } from "../saved-history.js";
import { processTrace } from "../scenario-fixture.js";

/**
 * The test starts PGlite, the Registry, the Router, a hold proxy and two
 * daemons, and certifies three messages between them. Beside a second full
 * Client suite (load average 30 to 47 on 8 cores) it has taken 114 to 127
 * seconds, most of it process startup. The limit is the only wall-clock bound
 * the trace sets itself, apart from its quiet windows.
 */
const HELD_DELIVERY_TEST_TIMEOUT_MS = 180_000;

const PARK_POLL_INTERVAL = Duration.millis(25);

/**
 * How long the held endpoint must stay silent while its ordered delivery is
 * parked, and how long it must stay silent afterwards to rule out a replay.
 */
const QUIET_WINDOW = Duration.seconds(2);

const heldText = "sent while the target link is held";

const replyText = "reply after the held delivery is released";

const followUpText = "follow-up in the recovered conversation";

/** Collects the endpoint's single inbound subscription for the whole scope. */
const collectDeliveries = <E>(messages: Stream.Stream<InboxEntry, E>) =>
  Effect.gen(function* () {
    const inbox = yield* Queue.unbounded<InboxEntry>();
    yield* Effect.forkScoped(
      Stream.runForEach(messages, (delivery) => Queue.offer(inbox, delivery)),
    );
    return inbox;
  });

const expectQuiet = (inbox: Queue.Queue<InboxEntry>) =>
  Effect.sleep(QUIET_WINDOW).pipe(
    Effect.zipRight(Queue.size(inbox)),
    Effect.tap((size) =>
      Effect.sync(() => {
        expect(size).toBe(0);
      }),
    ),
  );

/** Waits until the proxy parks a Router response for the held endpoint. */
const awaitParkedResponse = (parkedResponses: Effect.Effect<number>) =>
  parkedResponses.pipe(
    Effect.repeat({
      until: (count) => count > 0,
      schedule: Schedule.spaced(PARK_POLL_INTERVAL),
    }),
  );

const readHistory = (fixture: DaemonProcessFixture, address: AgentAddress) =>
  Effect.scoped(
    Effect.gen(function* () {
      const management = yield* acquireDaemonManagementClient(fixture);
      const history = yield* management.readConversation(address);
      return history;
    }),
  );

interface HeldPair {
  readonly proxy: RouterHoldProxy;
  readonly senderFixture: DaemonProcessFixture;
  readonly targetFixture: DaemonProcessFixture;
  readonly senderAgent: AgentCard;
  readonly targetAgent: AgentCard;
}

interface HeldDelivery {
  readonly proxy: RouterHoldProxy;
  readonly sender: McpConnection;
  readonly target: McpConnection;
  readonly senderInbox: Queue.Queue<InboxEntry>;
  readonly targetInbox: Queue.Queue<InboxEntry>;
  readonly senderAddress: AgentAddress;
  readonly targetAddress: AgentAddress;
  readonly senderAgent: AgentCard;
  readonly targetAgent: AgentCard;
  readonly heldSend: Fiber.RuntimeFiber<SendResult, unknown>;
}

const acquireHeldPair = (firewalls: {
  readonly sender?: FirewallMode;
  readonly target?: FirewallMode;
}) =>
  Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
    const proxy = yield* acquireRouterHoldProxy(infrastructure.routerOrigin);
    const [senderFixture, directFixture] = yield* Effect.all(
      [
        makeDaemonProcessFixture(
          infrastructure,
          "held-sender",
          firewalls.sender,
        ),
        makeDaemonProcessFixture(
          infrastructure,
          "held-target",
          firewalls.target,
        ),
      ] as const,
      { concurrency: 2 },
    );
    const targetFixture = yield* throughProxy(directFixture, proxy.origin);
    yield* Effect.all(
      [
        acquireDaemonProcess(senderFixture),
        acquireDaemonProcess(targetFixture),
      ],
      { concurrency: 2, discard: true },
    );

    return { proxy, senderFixture, targetFixture };
  });

/**
 * The held-delivery trace with each daemon's firewall, if any: a firewall
 * that passes every message changes nothing a peer or host sees.
 */
const heldDeliveryBehavior = (
  firewalls: {
    readonly sender?: FirewallMode;
    readonly target?: FirewallMode;
  } = {},
) =>
  Effect.gen(function* () {
    const started = yield* acquireHeldPair(firewalls);
    const sender = yield* registerFixture(started.senderFixture);
    const target = yield* registerFixture(started.targetFixture);
    const pair = {
      ...started,
      senderAgent: {
        agentId: sender.agentId,
        agentName: sender.agentName,
        principalId: sender.principalId,
      },
      targetAgent: {
        agentId: target.agentId,
        agentName: target.agentName,
        principalId: target.principalId,
      },
    };
    yield* Effect.scoped(
      waitForHeldDelivery(pair).pipe(
        Effect.flatMap(releaseHeldDelivery),
        Effect.flatMap(replyToReleasedDelivery),
        Effect.flatMap(followUpHeldDelivery),
      ),
    );
    yield* readHeldHistory(pair);
  }).pipe(Effect.scoped);

it(
  "recovers a delivery held after Router ordering between real daemons",
  processTrace(heldDeliveryBehavior()),
  HELD_DELIVERY_TEST_TIMEOUT_MS,
);

it(
  "recovers a held delivery the same way between daemons with firewalls in both modes",
  processTrace(
    heldDeliveryBehavior({ sender: "guidance", target: "enforcement" }),
  ),
  HELD_DELIVERY_TEST_TIMEOUT_MS,
);

/** Points one daemon's Router traffic at the hold proxy instead of the Router. */
function throughProxy(fixture: DaemonProcessFixture, routerOrigin: URL) {
  return configureDaemonFixture(fixture, { routerOrigin: routerOrigin.origin });
}

function waitForHeldDelivery(pair: HeldPair) {
  return Effect.gen(function* () {
    const { proxy, senderFixture, targetFixture, senderAgent, targetAgent } =
      pair;
    const senderAddress = directAddress(senderFixture.agentName);
    const targetAddress = directAddress(targetFixture.agentName);
    const sender = yield* acquireMcpConnection(
      {
        endpoint: senderFixture.endpoint,
        credential: senderFixture.mcpCredential,
      },
      { name: "process-test", version: "test" },
    );
    const target = yield* acquireMcpConnection(
      {
        endpoint: targetFixture.endpoint,
        credential: targetFixture.mcpCredential,
      },
      { name: "process-test", version: "test" },
    );
    const senderInbox = yield* collectDeliveries(inboxStream(sender));
    const targetInbox = yield* collectDeliveries(inboxStream(target));

    yield* proxy.hold;
    yield* Effect.addFinalizer(() => proxy.release);
    const heldSend = yield* Effect.forkScoped(
      callTool(sender.client, {
        name: "send_message",
        arguments: { input: { to: targetAddress, text: heldText } },
      }),
    );
    yield* awaitParkedResponse(proxy.parkedResponses);
    yield* expectQuiet(targetInbox);
    expect(yield* Queue.size(targetInbox)).toBe(0);

    return {
      proxy,
      sender,
      target,
      senderInbox,
      targetInbox,
      senderAddress,
      targetAddress,
      senderAgent,
      targetAgent,
      heldSend,
    };
  });
}

function releaseHeldDelivery(state: HeldDelivery) {
  return Effect.gen(function* () {
    const { proxy, targetInbox, heldSend, senderAgent } = state;
    yield* proxy.release;
    const released = yield* Queue.take(targetInbox);
    expect(released.item).toMatchObject({
      kind: "multicast",
      message: {
        author: senderAgent,
        recipients: [state.targetAgent],
        content: [{ type: "text", text: heldText }],
      },
    });

    yield* Fiber.join(heldSend);

    return state;
  });
}

function replyToReleasedDelivery(state: HeldDelivery) {
  return Effect.gen(function* () {
    const { target, senderAddress, targetAgent, senderInbox } = state;
    yield* callTool(target.client, {
      name: "send_message",
      arguments: { input: { to: senderAddress, text: replyText } },
    });
    const reply = yield* Queue.take(senderInbox);
    expect(reply.item).toMatchObject({
      kind: "multicast",
      message: {
        author: targetAgent,
        recipients: [state.senderAgent],
        content: [{ type: "text", text: replyText }],
      },
    });

    return state;
  });
}

function followUpHeldDelivery(state: HeldDelivery) {
  return Effect.gen(function* () {
    const {
      proxy,
      sender,
      targetAddress,
      targetInbox,
      senderAgent,
      senderInbox,
    } = state;
    yield* callTool(sender.client, {
      name: "send_message",
      arguments: { input: { to: targetAddress, text: followUpText } },
    });
    const followUp = yield* Queue.take(targetInbox);
    expect(followUp.item).toMatchObject({
      kind: "multicast",
      message: {
        author: senderAgent,
        recipients: [state.targetAgent],
        content: [{ type: "text", text: followUpText }],
      },
    });

    yield* expectQuiet(targetInbox);
    expect(yield* Queue.size(senderInbox)).toBe(0);
    expect(yield* proxy.parkedResponses).toBe(1);
  });
}

function readHeldHistory(pair: HeldPair) {
  return Effect.gen(function* () {
    const { senderFixture, targetFixture } = pair;
    const senderAddress = directAddress(senderFixture.agentName);
    const targetAddress = directAddress(targetFixture.agentName);
    const expectedContents = [
      multicastContent(heldText),
      multicastContent(replyText),
      multicastContent(followUpText),
    ];
    for (const history of [
      yield* readHistory(senderFixture, targetAddress),
      yield* readHistory(targetFixture, senderAddress),
    ]) {
      expect(history.nextCursor).toBeUndefined();
      expect(
        history.records.map((record) => historyMessage(record).content),
      ).toEqual(expectedContents);
    }
  });
}

/** Ordinary messages retain their text through certification and delivery. */
function multicastContent(text: string): Content {
  return [{ type: "text", text }];
}

function directAddress(agentName: AgentName): AgentAddress {
  return Schema.decodeUnknownSync(AgentAddress)(`agent:${agentName}`);
}
