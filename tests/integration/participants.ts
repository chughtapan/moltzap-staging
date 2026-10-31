/** @file Endpoints a collective-operation trace drives through real daemons. */

import {
  acquireHarnessEndpoint,
  AgentAddress,
  type AgentName,
  type AgentView,
  type HarnessEndpoint,
  type InboundDelivery,
  type InboundItem,
  SendInput,
} from "@moltzap/social-harness";
import { Duration, Effect, Queue, Schema, Stream } from "effect";
import {
  acquireDaemonManagementClient,
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  DAEMON_BOOT_CONCURRENCY,
  type DaemonProcessFixture,
  type FirewallMode,
  makeDaemonProcessFixture,
  makeRegistrationRequest,
  processTestError,
} from "./daemon/daemon-process-harness.js";
import { registrationResultKind } from "./expected-contracts.js";

/** One endpoint and the items its subscription has delivered, in order. */
export interface Participant {
  readonly address: AgentAddress;
  readonly agent: AgentView;
  readonly endpoint: HarnessEndpoint;
  readonly inbox: Queue.Queue<InboundDelivery>;
}

/**
 * Acquires the endpoint of `fixture`'s running daemon and queues every
 * delivery its subscription receives.
 */
const joinParticipant = (fixture: DaemonProcessFixture, agent: AgentView) =>
  Effect.gen(function* () {
    const endpoint = yield* acquireHarnessEndpoint(fixture.endpoint, {
      credential: fixture.mcpCredential,
      receiveMode: "push",
    });
    const inbox = yield* Queue.unbounded<InboundDelivery>();
    yield* endpoint.messages.pipe(
      Stream.runForEach((delivery) => Queue.offer(inbox, delivery)),
      Effect.forkScoped,
    );
    return {
      address: directAddress(fixture.agentName),
      agent,
      endpoint,
      inbox,
    };
  }).pipe(Effect.withSpan("joinParticipant"));

/** Registers one running daemon through its management endpoint. */
export const registerFixture = (fixture: DaemonProcessFixture) =>
  Effect.scoped(
    acquireDaemonManagementClient(fixture).pipe(
      Effect.flatMap((management) =>
        management.register(makeRegistrationRequest(fixture)).pipe(
          Effect.filterOrFail(
            (result) => result.kind === registrationResultKind,
            (result) =>
              processTestError(`fixture registration returned ${result.kind}`),
          ),
        ),
      ),
    ),
  );

/**
 * Starts one daemon per fixture, registers each, and joins each as a
 * participant.
 * @param fixtures The daemons to start.
 * @returns The participants, in `fixtures` order.
 */
export const startParticipants = (fixtures: readonly DaemonProcessFixture[]) =>
  Effect.gen(function* () {
    yield* Effect.forEach(
      fixtures,
      (fixture) => acquireDaemonProcess(fixture),
      {
        concurrency: DAEMON_BOOT_CONCURRENCY,
        discard: true,
      },
    );
    return yield* Effect.forEach(
      fixtures,
      (fixture) =>
        registerFixture(fixture).pipe(
          Effect.flatMap((registered) =>
            joinParticipant(fixture, registered.agent),
          ),
        ),
      { concurrency: 1 },
    );
  }).pipe(Effect.withSpan("startParticipants"));

/**
 * Starts PGlite, the Registry, the Router and one daemon per name, registers
 * each daemon, and joins each as a participant.
 * @param names The agent name of each daemon.
 * @param firewalls The firewall mode of each daemon that installs one, by name.
 * @returns The participants, in `names` order.
 */
export const acquireParticipants = (
  names: readonly string[],
  firewalls: Readonly<Record<string, FirewallMode>> = {},
) =>
  Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
    const fixtures = yield* Effect.all(
      names.map((name) =>
        makeDaemonProcessFixture(infrastructure, name, firewalls[name]),
      ),
      { concurrency: DAEMON_BOOT_CONCURRENCY },
    );
    return yield* startParticipants(fixtures);
  }).pipe(Effect.withSpan("acquireParticipants"));

/** Take the participant's next item and acknowledge its delivery. */
export const nextItem = (participant: Participant) =>
  Queue.take(participant.inbox).pipe(
    Effect.tap((delivery) => delivery.acknowledge),
    Effect.map((delivery): InboundItem => delivery.item),
  );

/** Sends `input`, decoded as a host's send input, from `participant`. */
export const send = (participant: Participant, input: unknown) =>
  participant.endpoint.send(Schema.decodeUnknownSync(SendInput)(input));

/** The fixed-member group address of `participants`. */
export const groupAddress = (participants: readonly Participant[]): string =>
  `group:${participants
    .map(({ address }) => address.slice("agent:".length))
    .join(",")}`;

/**
 * A message from `requester` to its group with `members`, received by each
 * member: the round `measuredDeadlineSeconds` times.
 */
export const groupRound = (
  requester: Participant,
  members: readonly Participant[],
) =>
  send(requester, {
    to: groupAddress([requester, ...members]),
    text: "one measured round",
  }).pipe(
    Effect.zipRight(Effect.forEach(members, nextItem, { concurrency: 1 })),
  );

/**
 * How many measured rounds a silent-member trace's collecting operation waits
 * before its deadline. Before it, the request messages certify and reach the
 * members and the answers certify; an all_gather's two answers extend one
 * group head and certify one after the other, so that work is several message
 * rounds. The remaining rounds absorb load that changes between the
 * measurement and the operation, and every round lengthens the test.
 */
const MEASURED_DEADLINE_ROUNDS = 4;

/**
 * The shortest deadline a measurement yields. 15 s has covered a
 * silent-member trace's pre-deadline work in CI, so a round measured in a
 * quiet moment cannot shrink the deadline below what that work needs.
 */
const MINIMUM_DEADLINE_SECONDS = 15;

/**
 * A collecting operation's deadline, in whole seconds: `MEASURED_DEADLINE_ROUNDS`
 * runs of `round` on this host as loaded now, and at least
 * `MINIMUM_DEADLINE_SECONDS`. It scales with the host's load, as the
 * certification work before the deadline does.
 * @param round One message that every asked member receives, as a trace sends it.
 * @returns The deadline in seconds, rounded up.
 */
export const measuredDeadlineSeconds = <E, R>(
  round: Effect.Effect<unknown, E, R>,
): Effect.Effect<number, E, R> =>
  Effect.timed(round).pipe(
    Effect.map(([elapsed]) =>
      Math.max(
        MINIMUM_DEADLINE_SECONDS,
        Math.ceil(
          Duration.toSeconds(Duration.times(elapsed, MEASURED_DEADLINE_ROUNDS)),
        ),
      ),
    ),
  );

function directAddress(agentName: AgentName): AgentAddress {
  return Schema.decodeUnknownSync(AgentAddress)(`agent:${agentName}`);
}
