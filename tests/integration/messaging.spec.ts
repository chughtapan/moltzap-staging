/** @file One daemon process delivers between its server agent and a client, in one order for both. */

import { FileSystem } from "@effect/platform";
import { INBOX_EVENT } from "@social-harness/social-harness";
import { Effect, Stream } from "effect";
import { join } from "node:path";
import { expect, it } from "vitest";
import {
  callTool,
  inboxStream,
  joinParticipants,
  nextItem,
  processTrace,
  receivedOrder,
  send,
  SERVER_AGENT_OWNER,
  sharedDaemon,
  textContent,
} from "./daemon-fixture.js";

/** Each trace starts one daemon. */
const PROCESS_TEST_TIMEOUT_MS = 120_000;

/** The server agent and one client of a running daemon. */
const serverAndClient = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  yield* daemon.start();
  const [server, client] = yield* joinParticipants(daemon, [
    SERVER_AGENT_OWNER,
    "bob",
  ]);
  if (server === undefined || client === undefined) {
    return yield* Effect.dieMessage("Expected two participants");
  }
  return { server, client };
});

const greeting = "hello from the server agent";
const reply = "reply from the client";

it(
  "delivers between the server agent and a client in both directions and saves one history for both",
  processTrace(
    Effect.gen(function* () {
      const { server, client } = yield* serverAndClient;

      const sent = yield* send(server, { to: client.address, text: greeting });
      expect(sent).toMatchObject({ address: client.address });
      expect(yield* nextItem(client)).toMatchObject({
        msgId: sent.id,
        author: server.agent,
        recipients: [client.agent],
        content: textContent(greeting),
      });
      const replied = yield* send(client, { text: reply });
      expect(yield* nextItem(server)).toMatchObject({
        msgId: replied.id,
        author: client.agent,
        recipients: [server.agent],
        content: textContent(reply),
      });

      const serverHistory = (yield* callTool(server.connection.mcp, {
        name: "read_conversation",
        arguments: { address: client.address },
      })).records;
      const clientHistory = (yield* callTool(client.connection.mcp, {
        name: "read_conversation",
        arguments: {},
      })).records;
      expect(serverHistory.map(({ kind }) => kind)).toEqual([
        "sent",
        "received",
      ]);
      expect(clientHistory.map(({ kind }) => kind)).toEqual([
        "received",
        "sent",
      ]);
      for (const history of [serverHistory, clientHistory]) {
        expect(history.map((record) => record.message.msgId)).toEqual([
          sent.id,
          replied.id,
        ]);
      }
      expect(
        yield* callTool(server.connection.mcp, {
          name: "list_conversations",
          arguments: {},
        }),
      ).toEqual({ addresses: [client.address] });
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "offers the inbox event by poll, push and webhook, and delivers a polling host the entries after its cursor",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
      yield* daemon.start();
      const server = yield* daemon.signInAs(SERVER_AGENT_OWNER);
      const client = yield* daemon.signInAs("bob");
      const listed = yield* Effect.tryPromise(() =>
        client.connection.events.list(),
      );
      expect(listed.events).toMatchObject([
        { name: INBOX_EVENT, delivery: ["poll", "push", "webhook"] },
      ]);
      const first = yield* send(server, { to: client.address, text: greeting });
      const second = yield* send(server, { to: client.address, text: reply });
      const polled = yield* inboxStream(client.connection, "0", "poll").pipe(
        Stream.take(2),
        Stream.runCollect,
      );
      expect([...polled].map((entry) => entry.message.msgId)).toEqual([
        first.id,
        second.id,
      ]);
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

/** The asker opens with `ask`, and the answerer ends with `answer`. */
const exchangeNorm = `(norm exchange :initiator Asker :participant Answerer)
(content ask :by Asker :starts true :initiates asked
  (field question :type string :min-length 1))
(content answer :by Answerer :when asked :ends true
  (field reply :type string :min-length 1))
`;

it(
  "refuses a client's message the server agent's norm does not allow on the send itself, and stores it for neither agent",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon({
        settings: {
          messagePolicyFile: "message-policy.json",
          normsDirectory: "norms",
        },
      });
      const filesystem = yield* FileSystem.FileSystem;
      yield* filesystem.makeDirectory(join(daemon.directory, "norms"));
      yield* filesystem.writeFileString(
        join(daemon.directory, "norms", "exchange.lisp"),
        exchangeNorm,
      );
      yield* filesystem.writeFileString(
        join(daemon.directory, "message-policy.json"),
        JSON.stringify({}),
      );
      yield* daemon.start();
      const [server, client] = yield* joinParticipants(daemon, [
        SERVER_AGENT_OWNER,
        "bob",
      ]);
      if (server === undefined || client === undefined) {
        return yield* Effect.dieMessage("Expected two participants");
      }

      const plain = yield* send(client, { text: "Can we talk?" }).pipe(
        Effect.flip,
      );
      expect(plain).toMatchObject({
        reason: "firewall-refused",
        refusal: "rule-violation",
        rule: "norm-structural",
      });
      const outOfTurn = yield* send(client, {
        text: '(answer :ontology exchange :reply "yes")',
      }).pipe(Effect.flip);
      expect(outOfTurn).toMatchObject({
        reason: "firewall-refused",
        refusal: "rule-violation",
        rule: "norm-protocol",
      });
      expect(plain.message).toMatch(
        /^Error: Reply does not contain exactly one parenthesized speech act\. Check: chk_\w+, rule norm-structural; rule-violation\.$/u,
      );
      expect(outOfTurn.message).toMatch(
        /^Error: replyToMsgId is required for a continuation act\. Check: chk_\w+, rule norm-protocol; rule-violation\.$/u,
      );

      const asked = yield* send(client, {
        text: '(ask :ontology exchange :question "Is it open?")',
      });
      expect(yield* nextItem(server)).toMatchObject({ msgId: asked.id });
      for (const [participant, address] of [
        [server, { address: client.address }],
        [client, {}],
      ] as const) {
        const saved = yield* callTool(participant.connection.mcp, {
          name: "read_conversation",
          arguments: address,
        });
        expect(saved.records.map((record) => record.message.msgId)).toEqual([
          asked.id,
        ]);
      }
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

/** How many times the server agent and the client send each other two messages at once. */
const CROSSING_ROUNDS = 8;

it(
  "completes the messages the server agent and a client send each other at once and saves them in one order for both",
  processTrace(
    Effect.gen(function* () {
      const { server, client } = yield* serverAndClient;
      const text = "a crossing message";
      const toClient = send(server, { to: client.address, text });
      const toServer = send(client, { text });
      for (let round = 0; round < CROSSING_ROUNDS; round += 1) {
        yield* Effect.all([toClient, toServer, toClient, toServer], {
          concurrency: 4,
        });
      }
      for (const participant of [server, client]) {
        expect(
          new Set(yield* receivedOrder(participant, 2 * CROSSING_ROUNDS)).size,
        ).toBe(2 * CROSSING_ROUNDS);
      }
      const serverSaved = yield* callTool(server.connection.mcp, {
        name: "read_conversation",
        arguments: { address: client.address },
      });
      const clientSaved = yield* callTool(client.connection.mcp, {
        name: "read_conversation",
        arguments: {},
      });
      const order = serverSaved.records.map((record) => record.message.msgId);
      expect(order).toHaveLength(4 * CROSSING_ROUNDS);
      expect(clientSaved.records.map((record) => record.message.msgId)).toEqual(
        order,
      );
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);
