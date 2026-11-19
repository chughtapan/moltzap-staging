/** @file One daemon process delivers between its server agent and a client, in one order for both. */

import { Effect } from "effect";
import { expect, it } from "vitest";
import {
  callTool,
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
        kind: "message",
        message: {
          msgId: sent.id,
          author: server.agent,
          recipients: [client.agent],
          content: textContent(greeting),
        },
      });
      const replied = yield* send(client, { text: reply });
      expect(yield* nextItem(server)).toMatchObject({
        kind: "message",
        message: {
          msgId: replied.id,
          author: client.agent,
          recipients: [server.agent],
          content: textContent(reply),
        },
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
