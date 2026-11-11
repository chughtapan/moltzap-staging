/** @file One daemon process delivers between its owners' agents, alone and in a group, and keeps their messages across restarts. */

import { AgentAddress, GroupAddress } from "@social-harness/social-harness";
import { Effect, Redacted, Schema } from "effect";
import { randomBytes } from "node:crypto";
import { expect, it } from "vitest";
import { callTool } from "../../../plugins/shared/mcp-client.js";
import {
  groupAddress,
  joinParticipants,
  nextItem,
  type Participant,
  send,
} from "../agent-fixture.js";
import { incompatibleDatabaseRemedy } from "../expected-contracts.js";
import { historyMessage } from "../saved-history.js";
import { processTrace } from "../scenario-fixture.js";
import { sharedDaemon, type SignedIn, stopProcess } from "./daemon-fixture.js";

/** Each trace starts one daemon, and some of them start it more than once. */
const PROCESS_TEST_TIMEOUT_MS = 120_000;

const textContent = (text: string) => [{ type: "text", text }];

/** The agent's saved conversation at `address`, which is not paged in these traces. */
const readConversation = (reader: SignedIn, address: string) =>
  callTool(reader.connection.client, {
    name: "read_conversation",
    arguments: {
      address: Schema.decodeUnknownSync(
        Schema.Union(AgentAddress, GroupAddress),
      )(address),
    },
  }).pipe(Effect.map((page) => page.records));

/** The identifiers of the messages in the agent's saved conversation, in order. */
const savedOrder = (reader: SignedIn, address: string) =>
  readConversation(reader, address).pipe(
    Effect.map((records) =>
      records.flatMap((record) =>
        record.kind === "result" ? [] : [record.message.msgId],
      ),
    ),
  );

/** The identifiers of the next `count` messages the participant receives, in order. */
const receivedOrder = (participant: Participant, count: number) =>
  Effect.forEach(
    Array.from({ length: count }),
    () =>
      nextItem(participant).pipe(
        Effect.map((item) =>
          item.kind === "multicast" ? item.message.msgId : item.kind,
        ),
      ),
    { concurrency: 1 },
  );

/** The settings a refused start tells the operator to change. */
const modeSetting = "mode";
const serverNameSetting = "serverAgentName";

const greeting = "hello from the first owner";
const reply = "reply from the second owner";

it(
  "delivers between two owners in both directions and saves one history for both",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
      yield* daemon.start();
      const [alice, bob] = yield* joinParticipants(daemon, ["alice", "bob"]);
      if (alice === undefined || bob === undefined) {
        return yield* Effect.dieMessage("Expected two participants");
      }

      const sent = yield* send(alice, { to: bob.address, text: greeting });
      expect(sent).toMatchObject({ address: bob.address });
      expect(yield* nextItem(bob)).toMatchObject({
        kind: "multicast",
        message: {
          msgId: sent.id,
          author: alice.agent,
          recipients: [bob.agent],
          content: textContent(greeting),
        },
      });
      yield* send(bob, { to: alice.address, text: reply });
      expect(yield* nextItem(alice)).toMatchObject({
        kind: "multicast",
        message: {
          author: bob.agent,
          recipients: [alice.agent],
          content: textContent(reply),
        },
      });

      const aliceHistory = yield* readConversation(alice, bob.address);
      const bobHistory = yield* readConversation(bob, alice.address);
      expect(aliceHistory.map(({ kind }) => kind)).toEqual([
        "sent",
        "received",
      ]);
      expect(bobHistory.map(({ kind }) => kind)).toEqual(["received", "sent"]);
      expect(
        aliceHistory.map((record) => historyMessage(record).content),
      ).toEqual([textContent(greeting), textContent(reply)]);
      expect(bobHistory.map(historyMessage)).toEqual(
        aliceHistory.map(historyMessage),
      );
      expect(
        yield* callTool(alice.connection.client, {
          name: "list_conversations",
          arguments: {},
        }),
      ).toEqual({ addresses: [bob.address] });
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "gives every member of a group of three the same order",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
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
      const daemon = yield* sharedDaemon();
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

const localAgentName = "solo";

it(
  "keeps a local daemon's agent across a restart and refuses another database key",
  processTrace(
    Effect.gen(function* () {
      const credential = randomBytes(32).toString("base64url");
      const daemon = yield* sharedDaemon({
        settings: {
          authentication: { kind: "local", credential },
          agentName: localAgentName,
        },
      });
      const signIn = Effect.scoped(
        daemon
          .signIn(Redacted.make(credential))
          .pipe(Effect.map(({ agent }) => agent)),
      );
      const original = yield* daemon.start();
      const before = yield* signIn;
      expect(before.agentName).toBe(localAgentName);
      yield* stopProcess(original);

      yield* daemon.configure({ agentName: undefined });
      const restarted = yield* daemon.start();
      expect(yield* signIn).toEqual(before);
      yield* stopProcess(restarted);

      const failure = yield* daemon.startupFailure({
        SHARNESS_DATABASE_KEY: randomBytes(32).toString("base64url"),
      });
      expect(failure.exitCode).not.toBe(0);
      expect(failure.logs).toContain(incompatibleDatabaseRemedy);
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "refuses another server agent name or the other mode on a started database, naming the setting",
  processTrace(
    Effect.gen(function* () {
      const named = (name: string) => ({
        [modeSetting]: { kind: "client-server", [serverNameSetting]: name },
      });
      const daemon = yield* sharedDaemon({ settings: named("front-desk") });
      yield* stopProcess(yield* daemon.start());

      yield* daemon.configure(named("back-office"));
      const renamed = yield* daemon.startupFailure();
      expect(renamed.exitCode).not.toBe(0);
      expect(renamed.logs).toContain(`Set ${serverNameSetting}`);

      yield* daemon.configure({ [modeSetting]: { kind: "p2p" } });
      const moved = yield* daemon.startupFailure();
      expect(moved.exitCode).not.toBe(0);
      expect(moved.logs).toContain(`Set ${modeSetting}`);
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);
