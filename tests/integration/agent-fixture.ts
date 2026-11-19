/** @file Agents of one daemon whose deliveries a trace takes in order. */

import {
  type InboxEntry,
  MessageAddressInput,
  type PermittedItem,
  SendRequest,
} from "@social-harness/social-harness";
import { Effect, Queue, Schema, Stream } from "effect";
import type { Daemon, SignedIn } from "./daemon/daemon-fixture.js";
import { inboxStream } from "./mcp-inbox.js";
import { callTool } from "./tool-call.js";

/** A signed-in owner and the items its agent has received, in order. */
export interface Participant extends SignedIn {
  readonly inbox: Queue.Queue<InboxEntry>;
}

/**
 * Signs in one owner per name on a running `daemon` and queues every delivery
 * each agent receives.
 * @param daemon The daemon the owners sign in to.
 * @param owners The owner of each participant: `SERVER_AGENT_OWNER` for the
 *   server agent, any other name for a client.
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

/** Takes the participant's next received item. */
export const nextItem = (participant: Participant) =>
  Queue.take(participant.inbox).pipe(
    Effect.map((delivery): PermittedItem => delivery.item),
  );

/** A host's send input. The server agent names its recipient in `to`, and a client names none. */
const SendInput = Schema.Struct({
  ...SendRequest.fields,
  to: Schema.optional(MessageAddressInput),
});

/** Sends `input`, decoded as a host's send input, from `sender`. */
export const send = (sender: Pick<SignedIn, "connection">, input: unknown) =>
  callTool(sender.connection.mcp, {
    name: "send_message",
    arguments: Schema.decodeUnknownSync(SendInput)(input),
  });
