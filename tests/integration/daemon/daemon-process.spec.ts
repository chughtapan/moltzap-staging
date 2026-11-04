/** @file Two real daemons certify, deliver, and recover multicast operations. */

import { FileSystem, HttpClient } from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { ProtocolErrorCode } from "@modelcontextprotocol/client";
import {
  AgentAddress,
  type AgentCard,
  type AgentName,
  type Content,
  type InboxEntry,
  SendError,
  type SendRequest,
  WIRE_VERSION,
} from "@moltzap/social-harness";
import { WireVersions } from "@moltzap/social-harness/test";
import {
  Effect,
  Fiber,
  Option,
  Redacted,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import {
  acquireMcpConnection,
  callTool,
  type McpConnection,
} from "../../../plugins/shared/mcp-client.js";
import { agentResetDocsLink } from "../expected-contracts.js";
import { inboxStream } from "../mcp-inbox.js";
import { historyMessage } from "../saved-history.js";
import { processTrace } from "../scenario-fixture.js";
import { arrayContaining } from "../test-matchers.js";
import {
  acquireDaemonManagementClient,
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  awaitDaemonStartupFailure,
  configureDaemonFixture,
  daemonListening,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  makeRegistrationRequest,
  managedProcess,
  type ProcessInfrastructure,
  ProcessTestError,
  type RunningProcess,
  stopProcess,
} from "./daemon-fixture.js";

interface RegisteredFixture extends DaemonProcessFixture {
  readonly agent: AgentCard;
}

const registerFixture = (fixture: DaemonProcessFixture) =>
  Effect.scoped(
    Effect.gen(function* () {
      const management = yield* acquireDaemonManagementClient(fixture);
      expect(yield* management.status()).toMatchObject({ kind: "registered" });

      const registered = yield* management.register(
        makeRegistrationRequest(fixture),
      );
      const agentCard = registered;
      expect(agentCard).toMatchObject({
        agentName: fixture.agentName,
      });
      expect({
        agentId: registered.agentId,
        agentName: registered.agentName,
        principalId: registered.principalId,
      }).toEqual({
        agentId: agentCard.agentId,
        agentName: agentCard.agentName,
        principalId: agentCard.principalId,
      });
      return {
        ...fixture,
        agent: {
          agentId: registered.agentId,
          agentName: registered.agentName,
          principalId: registered.principalId,
        },
      };
    }),
  );

/** Sends one addressed message with `send` and waits for the peer to receive it. */
const deliverDirect = (input: {
  readonly from: RegisteredFixture;
  readonly to: DaemonProcessFixture;
  readonly text: string;
  readonly send: typeof sendOrQueue;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sender = yield* acquireMcpConnection(
        { endpoint: input.from.endpoint, credential: input.from.mcpCredential },
        { name: "process-test", version: "test" },
      );
      const receiver = yield* acquireMcpConnection(
        { endpoint: input.to.endpoint, credential: input.to.mcpCredential },
        { name: "process-test", version: "test" },
      );
      const delivery = yield* Effect.forkScoped(
        nextDelivery(inboxStream(receiver)),
      );
      yield* input.send(sender, {
        to: directAddress(input.to.agentName),
        text: input.text,
      });
      const inbound = yield* Fiber.join(delivery);
      expect(inbound.item).toMatchObject({
        kind: "multicast",
        message: {
          author: input.from.agent,
          recipients: [{ agentName: input.to.agentName }],
          content: [{ type: "text", text: input.text }],
        },
      });
    }),
  );

const beforeRestartText = "sent before the daemon restarts";

/** Sends one message as a host does, failing on any send failure. */
const certifiedSend = (endpoint: McpConnection, input: SendRequest) =>
  callTool(endpoint.client, {
    name: "send_message",
    arguments: { input: input },
  }).pipe(Effect.asVoid);

const fromRestartedText = "sent by the restarted daemon";

interface RestartScenario {
  readonly restartedFixture: RegisteredFixture;
  readonly peerFixture: RegisteredFixture;
  readonly restartedDaemon: RunningProcess;
}

interface RouterRestartScenario {
  readonly infrastructure: ProcessInfrastructure;
  readonly memberFixture: RegisteredFixture;
  readonly conciergeFixture: RegisteredFixture;
}

interface ProcessScenario {
  readonly callerFixture: RegisteredFixture;
  readonly targetFixture: RegisteredFixture;
  readonly targetDaemon: RunningProcess;
  readonly callerAddress: AgentAddress;
  readonly targetAddress: AgentAddress;
}

const acquireRestartPair = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const [restartedFixture, peerFixture] = yield* Effect.all(
    [
      makeDaemonProcessFixture(infrastructure, "restart-subject"),
      makeDaemonProcessFixture(infrastructure, "restart-peer"),
    ] as const,
    { concurrency: 2 },
  );
  const [restartedDaemon] = yield* Effect.all(
    [
      acquireDaemonProcess(restartedFixture),
      acquireDaemonProcess(peerFixture),
    ] as const,
    { concurrency: 2 },
  );
  return {
    restartedFixture: yield* registerFixture(restartedFixture),
    peerFixture: yield* registerFixture(peerFixture),
    restartedDaemon,
  };
});

const acquireRouterRestartPair = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const [memberFixture, conciergeFixture] = yield* Effect.all(
    [
      makeDaemonProcessFixture(infrastructure, "router-restart-member"),
      makeDaemonProcessFixture(infrastructure, "router-restart-concierge"),
    ] as const,
    { concurrency: 2 },
  );
  yield* Effect.all(
    [
      acquireDaemonProcess({
        ...memberFixture,
        wireVersions: {
          emitted: WIRE_VERSION,
          live: [WIRE_VERSION, "2026.1019.3"],
          history: [WIRE_VERSION, "2026.1019.3"],
        },
      }),
      acquireDaemonProcess({
        ...conciergeFixture,
        wireVersions: {
          emitted: "2026.1019.3",
          live: [WIRE_VERSION, "2026.1019.3"],
          history: [WIRE_VERSION, "2026.1019.3"],
        },
      }),
    ] as const,
    { concurrency: 2 },
  );
  return {
    infrastructure,
    memberFixture: yield* registerFixture(memberFixture),
    conciergeFixture: yield* registerFixture(conciergeFixture),
  };
});

const acquireProcessPair = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const [callerFixture, targetFixture] = yield* Effect.all(
    [
      makeDaemonProcessFixture(infrastructure, "process-caller"),
      makeDaemonProcessFixture(infrastructure, "process-target"),
    ] as const,
    { concurrency: 2 },
  );
  const [, targetDaemon] = yield* Effect.all(
    [acquireDaemonProcess(callerFixture), acquireDaemonProcess(targetFixture)],
    { concurrency: 2 },
  );
  return {
    callerFixture: yield* registerFixture(callerFixture),
    targetFixture: yield* registerFixture(targetFixture),
    targetDaemon,
    callerAddress: directAddress(callerFixture.agentName),
    targetAddress: directAddress(targetFixture.agentName),
  };
});

function expectRetiredConversationIdentifierRefused(
  callerFixture: DaemonProcessFixture,
) {
  return Effect.gen(function* () {
    yield* Effect.scoped(
      Effect.gen(function* () {
        const management = yield* acquireDaemonManagementClient(callerFixture);
        const retiredIdentifier = "retired-public-identifier";
        const malformed = yield* management.callExpectingProtocolError(
          "read_conversation",
          { conversationId: retiredIdentifier },
        );
        expect(malformed.code).toBe(ProtocolErrorCode.InvalidParams);
        expect(malformed.data).toMatchObject({
          issues: arrayContaining([
            { path: ["conversationId"], message: "Unexpected field" },
          ]),
        });
        expect(JSON.stringify(malformed.data)).not.toContain(retiredIdentifier);
      }),
    );
  });
}

const readActiveStatus = (fixture: DaemonProcessFixture) =>
  Effect.scoped(
    Effect.gen(function* () {
      const management = yield* acquireDaemonManagementClient(fixture);
      return yield* management.status();
    }),
  );

/**
 * Each test starts PGlite, the Registry, the Router and two daemons; two of
 * them restart a daemon and one restarts the Router. Beside a second full Client suite and another
 * integration file (load average 17 to 32 on 8 cores) the restart test has
 * taken 62 to 163 seconds, most of it process startup. The limit is the only
 * wall-clock bound these traces set themselves.
 */
const PROCESS_TEST_TIMEOUT_MS = 180_000;

const initialText = "hello from the first real daemon";

const responseText = "addressed response from the second real daemon";

const initialContent = [
  { type: "text", text: initialText },
] as const satisfies Content;

const responseContent = [
  { type: "text", text: responseText },
] as const satisfies Content;

const readDurableHistory = (
  fixture: DaemonProcessFixture,
  address: AgentAddress,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const management = yield* acquireDaemonManagementClient(fixture);
      expect(yield* management.searchConversations()).toEqual({
        addresses: [address],
      });
      const history = yield* management.readConversation(address);
      return history;
    }),
  );

const toRestartedText = "sent to the restarted daemon";

/**
 * Either send after the restart may fail as queued, so each is confirmed by
 * its delivery. The restarted daemon's delivery of the peer's message shows it
 * stored that record, and an endpoint stores a record only after the one it
 * extends, so its durable history then holds all three messages.
 */
const singleRestartBehavior = Effect.gen(function* () {
  const scenario = yield* acquireRestartPair;
  yield* restartSenderAndExpectDelivery(scenario);
  yield* expectRestartedRecipientHistory(scenario);
}).pipe(Effect.scoped);

function restartSenderAndExpectDelivery({
  restartedFixture,
  peerFixture,
  restartedDaemon,
}: RestartScenario) {
  return Effect.gen(function* () {
    yield* deliverDirect({
      from: restartedFixture,
      to: peerFixture,
      text: beforeRestartText,
      send: certifiedSend,
    });
    yield* stopProcess(restartedDaemon);
    yield* acquireDaemonProcess(restartedFixture);
    yield* deliverDirect({
      from: restartedFixture,
      to: peerFixture,
      text: fromRestartedText,
      send: sendOrQueue,
    });
  });
}

const beforeRouterRestartText = "sent before the Router restarts";

const afterRouterRestartText = "sent after the Router restarts";

const replyAfterRouterRestartText = "reply after the Router restarts";

/**
 * The Router restarts with every daemon still up, as a network redeploy
 * replaces it. Both daemons must re-anchor their conversation under the new
 * Router instance and then deliver in both directions.
 */
const routerRestartBehavior = Effect.gen(function* () {
  const scenario = yield* acquireRouterRestartPair;
  yield* restartRouterAndExpectBidirectionalDelivery(scenario);
  yield* expectRecoveredRouterHistory(scenario);
}).pipe(Effect.scoped);

const processBehavior = Effect.gen(function* () {
  const scenario = yield* acquireProcessPair;
  yield* expectRetiredConversationIdentifierRefused(scenario.callerFixture);
  yield* exchangeInitialMessagesAndExpectDelivery(scenario);
  yield* expectInitialHistoriesMatch(scenario);
  yield* restartRecipientAndExpectRetainedHistory(scenario);
  yield* deliverDirect({
    from: scenario.callerFixture,
    to: scenario.targetFixture,
    text: "new message after the recipient restarts",
    send: sendOrQueue,
  });
  yield* deliverDirect({
    from: scenario.targetFixture,
    to: scenario.callerFixture,
    text: "reply from the restarted recipient",
    send: sendOrQueue,
  });
}).pipe(Effect.scoped);

/** Self-hosted recovery retains runtime settings while SQL owns the saved registration. */
const admissionLifetimeBehavior = Effect.scoped(
  Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
    const fixture = yield* makeDaemonProcessFixture(
      infrastructure,
      "saved-configuration",
    );
    const original = yield* acquireDaemonProcess(fixture);
    const before = yield* readActiveStatus(fixture);
    yield* stopProcess(original);
    const filesystem = yield* FileSystem.FileSystem;
    yield* filesystem.remove(fixture.agentPrivateKeyFile);
    yield* filesystem.remove(fixture.admissionCredentialFile);
    yield* filesystem.remove(fixture.mcpCredentialFile);
    yield* configureDaemonFixture(fixture, {
      agent: undefined,
      admissionCredential: undefined,
    });
    const recovered = yield* acquireDaemonProcess(fixture);
    expect(yield* readActiveStatus(fixture)).toEqual(before);
    yield* stopProcess(recovered);
    const failure = yield* awaitDaemonStartupFailure({
      ...fixture,
      environment: {
        ...fixture.environment,
        SHARNESS_DATABASE_KEY: "A".repeat(43),
      },
    });
    expect(failure.exitCode).not.toBe(0);
    expect(failure.logs).toContain(agentResetDocsLink);
  }),
);

/**
 * A release with an empty live window refuses this binary's retained outbound
 * bytes. The child composes only published Client service and test APIs.
 */
const retiredWindowDaemon = `
  import { NodeRuntime } from "@effect/platform-node";
  import { WIRE_VERSION } from "@moltzap/social-harness"; import { WireVersions } from "@moltzap/social-harness/test";
  import { daemonLayer } from "@moltzap/social-harness/service";
  import { ConfigProvider, Effect, Layer } from "effect";
  Layer.launch(daemonLayer).pipe(
    Effect.provideService(WireVersions, { live: [], history: [WIRE_VERSION] }),
    Effect.withConfigProvider(ConfigProvider.fromEnv()),
    NodeRuntime.runMain,
  );
`;

/** A retired version in a retained envelope must stop startup before any schema change. */
const refusesStaleOutbox = Effect.scoped(
  Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
    const local = yield* makeDaemonProcessFixture(
      infrastructure,
      "stale-outbox",
    );
    const peer = yield* makeDaemonProcessFixture(infrastructure, "stale-peer");
    const daemonProcess = yield* acquireDaemonProcess(local);
    yield* acquireDaemonProcess(peer);
    const registeredLocal = yield* registerFixture(local);
    yield* registerFixture(peer);
    yield* deliverDirect({
      from: registeredLocal,
      to: peer,
      text: "outbox baseline",
      send: sendOrQueue,
    });
    yield* stopProcess(daemonProcess);
    const before = yield* markStoredOutboundPending(local.stateDirectory);
    const failed = yield* managedProcess(
      process.execPath,
      [
        "--conditions=sharness-test",
        "--input-type=module",
        "-e",
        retiredWindowDaemon,
      ],
      local.environment,
      dirname(fileURLToPath(import.meta.url)),
    );
    yield* daemonListening(failed, local.endpoint).pipe(
      Effect.repeat({
        until: (ready) => ready,
        schedule: Schedule.spaced("25 millis"),
      }),
      Effect.timeout("10 seconds"),
    );
    yield* expectAgentUnavailable(local).pipe(
      Effect.retry(Schedule.spaced("50 millis")),
      Effect.timeout("20 seconds"),
    );
    expect(failed.child.exitCode).toBeNull();
    yield* stopProcess(failed);
    const after = yield* readStoredDatabase(local.stateDirectory);
    expect(after).toEqual(before);
  }),
);

it(
  "keeps the daemon listening when its network registration is revoked",
  processTrace(
    Effect.scoped(
      Effect.gen(function* () {
        const infrastructure = yield* acquireProcessInfrastructure;
        const local = yield* makeDaemonProcessFixture(
          infrastructure,
          "network-reset",
        );
        const daemon = yield* acquireDaemonProcess(local);
        yield* registerFixture(local);
        yield* infrastructure.resetNetwork;
        yield* expectAgentUnavailable(local).pipe(
          Effect.retry(Schedule.spaced("50 millis")),
          Effect.timeout("20 seconds"),
        );
        expect(daemon.child.exitCode).toBeNull();
      }),
    ),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "certifies fresh messages in both directions after one daemon restarts",
  processTrace(processBehavior),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "recovers a self-hosted agent without registration seed and refuses a different unlock key",
  processTrace(admissionLifetimeBehavior),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "delivers both ways after one real daemon restarts while its peer stays up",
  processTrace(singleRestartBehavior),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "certifies across adjacent emitted versions and re-anchors after the Router restarts",
  processTrace(
    routerRestartBehavior.pipe(
      Effect.provideService(WireVersions, {
        emitted: WIRE_VERSION,
        live: [WIRE_VERSION, "2026.1019.3"],
        history: [WIRE_VERSION, "2026.1019.3"],
      }),
    ),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "isolates a retired outbox from the listener and preserves retained state",
  processTrace(refusesStaleOutbox),
  PROCESS_TEST_TIMEOUT_MS,
);

function expectRestartedRecipientHistory(state: RestartScenario) {
  return Effect.gen(function* () {
    const { restartedFixture, peerFixture } = state;
    yield* deliverDirect({
      from: peerFixture,
      to: restartedFixture,
      text: toRestartedText,
      send: sendOrQueue,
    });
    const history = yield* readDurableHistory(
      restartedFixture,
      directAddress(peerFixture.agentName),
    );
    expect(
      history.records.map((record) => historyMessage(record).content[0]),
    ).toEqual([
      { type: "text", text: beforeRestartText },
      { type: "text", text: fromRestartedText },
      { type: "text", text: toRestartedText },
    ]);
  });
}

function restartRouterAndExpectBidirectionalDelivery(
  state: RouterRestartScenario,
) {
  return Effect.gen(function* () {
    const { infrastructure, memberFixture, conciergeFixture } = state;
    yield* Effect.all(
      [
        deliverDirect({
          from: memberFixture,
          to: conciergeFixture,
          text: beforeRouterRestartText,
          send: certifiedSend,
        }),
        deliverDirect({
          from: conciergeFixture,
          to: memberFixture,
          text: "successor-authored head before Router restart",
          send: certifiedSend,
        }),
      ],
      { concurrency: 2 },
    );
    yield* infrastructure.restartRouter;
    yield* deliverDirect({
      from: memberFixture,
      to: conciergeFixture,
      text: afterRouterRestartText,
      send: sendOrQueue,
    });
    yield* deliverDirect({
      from: conciergeFixture,
      to: memberFixture,
      text: replyAfterRouterRestartText,
      send: sendOrQueue,
    });
  });
}

function expectRecoveredRouterHistory(state: RouterRestartScenario) {
  return Effect.gen(function* () {
    const { memberFixture, conciergeFixture } = state;
    const history = yield* readDurableHistory(
      memberFixture,
      directAddress(conciergeFixture.agentName),
    );
    const texts = history.records.map(
      (record) => historyMessage(record).content[0],
    );
    expect(texts.slice(0, 2)).toEqual(
      arrayContaining([
        { type: "text", text: beforeRouterRestartText },
        { type: "text", text: "successor-authored head before Router restart" },
      ]),
    );
    expect(texts.slice(2)).toEqual([
      { type: "text", text: afterRouterRestartText },
      { type: "text", text: replyAfterRouterRestartText },
    ]);
    const peerHistory = yield* readDurableHistory(
      conciergeFixture,
      directAddress(memberFixture.agentName),
    );
    expect(peerHistory.records.map(historyMessage)).toEqual(
      history.records.map(historyMessage),
    );
  });
}

function exchangeInitialMessagesAndExpectDelivery(state: ProcessScenario) {
  return Effect.scoped(
    Effect.gen(function* () {
      const { callerFixture, targetFixture, callerAddress, targetAddress } =
        state;
      const caller = yield* acquireMcpConnection(
        {
          endpoint: callerFixture.endpoint,
          credential: callerFixture.mcpCredential,
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
      const callerDelivery = yield* Effect.forkScoped(
        nextDelivery(inboxStream(caller)),
      );
      const targetDelivery = yield* Effect.forkScoped(
        nextDelivery(inboxStream(target)),
      );

      const sent = yield* callTool(caller.client, {
        name: "send_message",
        arguments: {
          input: {
            to: targetAddress,
            text: initialText,
          },
        },
      });
      const targetInbound = yield* Fiber.join(targetDelivery);
      expect(sent).toMatchObject({ address: targetAddress });
      expect(targetInbound.item).toMatchObject({
        kind: "multicast",
        message: {
          msgId: sent.id,
          author: callerFixture.agent,
          recipients: [targetFixture.agent],
          content: [{ type: "text", text: initialText }],
        },
      });

      yield* callTool(target.client, {
        name: "send_message",
        arguments: { input: { to: callerAddress, text: responseText } },
      });
      const callerInbound = yield* Fiber.join(callerDelivery);
      expect(callerInbound.item).toMatchObject({
        kind: "multicast",
        message: {
          author: targetFixture.agent,
          recipients: [callerFixture.agent],
          content: [{ type: "text", text: responseText }],
        },
      });
    }),
  );
}

function expectInitialHistoriesMatch(state: ProcessScenario) {
  return Effect.gen(function* () {
    const { callerFixture, targetFixture, callerAddress, targetAddress } =
      state;
    const callerHistory = yield* readDurableHistory(
      callerFixture,
      targetAddress,
    );
    const targetHistory = yield* readDurableHistory(
      targetFixture,
      callerAddress,
    );
    expect(callerHistory.nextCursor).toBeUndefined();
    expect(callerHistory.records.map(({ kind }) => kind)).toEqual([
      "sent",
      "received",
    ]);
    expect(
      callerHistory.records.map((record) => historyMessage(record).content),
    ).toEqual([initialContent, [{ type: "text", text: responseText }]]);
    expect(targetHistory.nextCursor).toBeUndefined();
    expect(targetHistory.records.map(({ kind }) => kind)).toEqual([
      "received",
      "sent",
    ]);
    expect(
      targetHistory.records.map((record) => historyMessage(record).content),
    ).toEqual([[{ type: "text", text: initialText }], responseContent]);
  });
}

function restartRecipientAndExpectRetainedHistory({
  targetDaemon,
  targetFixture,
  callerAddress,
}: ProcessScenario) {
  return Effect.gen(function* () {
    yield* stopProcess(targetDaemon);
    yield* acquireDaemonProcess(targetFixture);
    const recovered = yield* readDurableHistory(targetFixture, callerAddress);
    expect(recovered.records).toHaveLength(2);
  });
}

function directAddress(agentName: AgentName): AgentAddress {
  return Schema.decodeUnknownSync(AgentAddress)(`agent:${agentName}`);
}

/** Accepts the next delivery before its owning subscription closes. */
function nextDelivery<E>(stream: Stream.Stream<InboxEntry, E>) {
  return Stream.runHead(stream).pipe(Effect.flatMap(requireDelivery));
}

function requireDelivery(
  delivery: Option.Option<InboxEntry>,
): Effect.Effect<InboxEntry, ProcessTestError> {
  return Option.match(delivery, {
    onNone: () =>
      Effect.fail(
        new ProcessTestError({ message: "message subscription ended" }),
      ),
    onSome: Effect.succeed,
  });
}

/**
 * Sends one message from `endpoint` after a daemon restart. A send whose own
 * outbox drain outlasts its bound or loses the Router fails with
 * `delivery-pending`: the message is durably queued, and the daemon delivers it
 * once its Router worker answers. Under load that happens while the restarted
 * daemon and its peer exchange recovery traffic, so the trace takes that
 * failure as queued and confirms the message by the peer's delivery. Any other
 * failure fails the trace.
 * @param endpoint Endpoint that sends.
 * @param input The message.
 * @returns Completion once the message is certified or queued.
 */
function sendOrQueue(endpoint: McpConnection, input: SendRequest) {
  return callTool(endpoint.client, {
    name: "send_message",
    arguments: { input: input },
  }).pipe(
    Effect.asVoid,
    Effect.catchIf(
      (error) =>
        error instanceof SendError && error.reason === "delivery-pending",
      () => Effect.void,
    ),
  );
}

/** Retry an existing signed envelope unchanged under the narrower release window. */
function markStoredOutboundPending(directory: string) {
  return Effect.try({
    try: () => {
      const database = new DatabaseSync(join(directory, "sharnessd.sqlite3"));
      try {
        const result = database
          .prepare(
            "UPDATE outbound_messages SET disposition = 'pending' WHERE envelope_id = (SELECT envelope_id FROM outbound_messages LIMIT 1)",
          )
          .run();
        if (result.changes !== 1) {
          throw new Error("expected an existing signed outbound envelope");
        }
      } finally {
        database.close();
      }
    },
    catch: (cause) =>
      new ProcessTestError({
        message: "could not prepare retired outbox",
        cause,
      }),
  }).pipe(Effect.zipRight(readStoredDatabase(directory)));
}

/** Reopening a database may checkpoint WAL pages, so compare retained records rather than file bytes. */
function readStoredDatabase(directory: string) {
  return Effect.try({
    try: () => {
      const database = new DatabaseSync(join(directory, "sharnessd.sqlite3"));
      try {
        return database
          .prepare("SELECT * FROM outbound_messages ORDER BY outbound_sequence")
          .all();
      } finally {
        database.close();
      }
    },
    catch: (cause) =>
      new ProcessTestError({
        message: "could not read retained outbound records",
        cause,
      }),
  });
}

/** Unavailable agent state refuses authenticated work without stopping the shared listener. */
function expectAgentUnavailable(fixture: DaemonProcessFixture) {
  return Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.post(fixture.endpoint.href, {
      headers: {
        authorization: `Bearer ${Redacted.value(fixture.mcpCredential)}`,
      },
    });
    if (response.status !== 503) {
      return yield* new ProcessTestError({
        message: "agent remains available",
      });
    }
  }).pipe(Effect.provide(NodeHttpClient.layer));
}
