/** @file One P2P daemon process gives a group's members one order and keeps their unread messages across a restart. */

import { GroupAddress } from "@social-harness/social-harness";
import { Effect, Schema } from "effect";
import { expect, it } from "vitest";
import {
  joinParticipants,
  nextItem,
  type Participant,
  send,
} from "../agent-fixture.js";
import { groupAddress, P2P } from "../agent-fixture.p2p.js";
import { processTrace } from "../scenario-fixture.js";
import { callTool } from "../tool-call.js";
import { sharedDaemon, type SignedIn, stopProcess } from "./daemon-fixture.js";

/** Each trace starts one daemon, and one of them starts it twice. */
const PROCESS_TEST_TIMEOUT_MS = 120_000;

/** The identifiers of the messages in the agent's saved group conversation, in order. The traces do not page it. */
const savedOrder = (reader: SignedIn, address: string) =>
  callTool(reader.connection.mcp, {
    name: "read_conversation",
    arguments: { address: Schema.decodeUnknownSync(GroupAddress)(address) },
  }).pipe(
    Effect.map((page) => page.records.map((record) => record.message.msgId)),
  );

/** The identifiers of the next `count` messages the participant receives, in order. */
const receivedOrder = (participant: Participant, count: number) =>
  Effect.forEach(
    Array.from({ length: count }),
    () => nextItem(participant).pipe(Effect.map((item) => item.message.msgId)),
    { concurrency: 1 },
  );

it(
  "gives every member of a group of three the same order",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon({ settings: P2P });
      yield* daemon.start();
      const members = yield* joinParticipants(daemon, [
        "alice",
        "bob",
        "carol",
      ]);
      const rounds = 3;
      const authors = Array.from({ length: rounds }).flatMap(() => members);

      const sent = yield* Effect.forEach(
        authors,
        (author, index) =>
          send(author, {
            to: groupAddress(members),
            text: `concurrent ${index}`,
          }).pipe(Effect.map(({ id, address }) => ({ id, address, author }))),
        { concurrency: authors.length },
      );
      const received = yield* Effect.forEach(
        members,
        (participant) =>
          receivedOrder(participant, rounds * (members.length - 1)),
        { concurrency: members.length },
      );
      const saved = yield* Effect.forEach(
        members,
        (participant) => savedOrder(participant, sent[0]?.address ?? ""),
        { concurrency: 1 },
      );

      const order = saved[0] ?? [];
      expect(new Set(order)).toEqual(new Set(sent.map(({ id }) => id)));
      expect(saved).toEqual(members.map(() => order));
      expect(received).toEqual(
        members.map((participant) =>
          order.filter(
            (id) =>
              sent.find((message) => message.id === id)?.author !== participant,
          ),
        ),
      );
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

const firstUnread = "sent before the restart";
const secondUnread = "also sent before the restart";
const afterRestart = "sent after the restart";

it(
  "keeps messages a group's members have not read across a restart",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon({ settings: P2P });
      const running = yield* daemon.start();
      const unread = yield* Effect.scoped(
        Effect.gen(function* () {
          const alice = yield* daemon.signInAs("alice");
          const bob = yield* daemon.signInAs("bob");
          const carol = yield* daemon.signInAs("carol");
          const to = groupAddress([alice, bob, carol]);
          const first = yield* send(alice, { to, text: firstUnread });
          const second = yield* send(alice, { to, text: secondUnread });
          return { address: first.address, ids: [first.id, second.id] };
        }),
      );
      yield* stopProcess(running);
      yield* daemon.start();

      const [alice, bob, carol] = yield* joinParticipants(daemon, [
        "alice",
        "bob",
        "carol",
      ]);
      if (alice === undefined || bob === undefined || carol === undefined) {
        return yield* Effect.dieMessage("Expected three participants");
      }
      expect(yield* receivedOrder(bob, unread.ids.length)).toEqual(unread.ids);
      expect(yield* receivedOrder(carol, unread.ids.length)).toEqual(
        unread.ids,
      );
      const third = yield* send(carol, {
        to: unread.address,
        text: afterRestart,
      });
      expect(yield* receivedOrder(alice, 1)).toEqual([third.id]);
      expect(yield* receivedOrder(bob, 1)).toEqual([third.id]);
      for (const participant of [alice, bob, carol]) {
        expect(yield* savedOrder(participant, unread.address)).toEqual([
          ...unread.ids,
          third.id,
        ]);
      }
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);
