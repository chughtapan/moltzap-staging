/** @file Agents of one daemon whose deliveries a trace takes in order. */

import {
  callTool,
  type InboxEntry,
  type PermittedItem,
  SendRequest,
} from "@social-harness/social-harness";
import { Duration, Effect, Queue, Schema, Stream } from "effect";
import {
  type Daemon,
  type DaemonOptions,
  sharedDaemon,
  type SignedIn,
} from "./daemon/daemon-fixture.js";
import { inboxStream } from "./mcp-inbox.js";

/** A signed-in owner and the items its agent has received, in order. */
export interface Participant extends SignedIn {
  readonly inbox: Queue.Queue<InboxEntry>;
}

/**
 * Signs in one owner per name on a running `daemon` and queues every delivery
 * each agent receives.
 * @param daemon The daemon the owners sign in to.
 * @param owners The owner of each participant.
 * @returns The participants, in `owners` order.
 */
export const joinParticipants = Effect.fn("joinParticipants")(
  (daemon: Daemon, owners: readonly string[]) =>
    Effect.forEach(
      owners,
      (owner) =>
        Effect.gen(function* () {
          const signedIn = yield* daemon.signInAs(owner);
          const inbox = yield* Queue.unbounded<InboxEntry>();
          yield* inboxStream(signedIn.connection).pipe(
            Stream.runForEach((delivery) => Queue.offer(inbox, delivery)),
            Effect.forkScoped,
          );
          return { ...signedIn, inbox };
        }),
      { concurrency: 1 },
    ),
);

/**
 * Starts one daemon and joins one participant per owner.
 * @param owners The owner of each participant.
 * @param options How the daemon differs from the fixture's defaults.
 * @returns The participants, in `owners` order.
 */
export const acquireParticipants = Effect.fn("acquireParticipants")(
  (owners: readonly string[], options: DaemonOptions = {}) =>
    sharedDaemon(options).pipe(
      Effect.tap((daemon) => daemon.start()),
      Effect.flatMap((daemon) => joinParticipants(daemon, owners)),
    ),
);

/** Takes the participant's next received item. */
export const nextItem = (participant: Participant) =>
  Queue.take(participant.inbox).pipe(
    Effect.map((delivery): PermittedItem => delivery.item),
  );

/**
 * Takes the participant's next received item, which must be a collective
 * request, and gives its message id: the `requestId` a response names.
 */
export const nextRequestId = (participant: Participant) =>
  nextItem(participant).pipe(
    Effect.flatMap((item) =>
      item.kind === "collectiveRequest"
        ? Effect.succeed(item.message.msgId)
        : Effect.dieMessage(
            `expected a collective request, received ${item.kind}`,
          ),
    ),
  );

/** Sends `input`, decoded as a host's send input, from `sender`. */
export const send = (sender: Pick<SignedIn, "connection">, input: unknown) =>
  callTool(sender.connection.client, {
    name: "send_message",
    arguments: Schema.decodeUnknownSync(SendRequest)(input),
  });

/** The fixed-member group address of `members`. */
export const groupAddress = (
  members: ReadonlyArray<Pick<SignedIn, "agent">>,
): string => `group:${members.map(({ agent }) => agent.agentName).join(",")}`;

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
 * How many measured rounds a silent-member trace's collective request waits
 * before its deadline. Before it, the request reaches its recipients and
 * their responses return, which is several message rounds. The remaining
 * rounds absorb load that changes between the measurement and the request,
 * and every round lengthens the test.
 */
const MEASURED_DEADLINE_ROUNDS = 4;

/**
 * The shortest deadline a measurement yields. 15 s has covered a
 * silent-member trace's pre-deadline work in CI, so a round measured in a
 * quiet moment cannot shrink the deadline below what that work needs.
 */
const MINIMUM_DEADLINE_SECONDS = 15;

/**
 * A collective request's deadline, in whole seconds: `MEASURED_DEADLINE_ROUNDS`
 * runs of `round` on this host as loaded now, and at least
 * `MINIMUM_DEADLINE_SECONDS`. It scales with the host's load, as the work
 * before the deadline does.
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
