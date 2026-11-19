/** @file One daemon process delivers between its server agent and a client, and keeps the name its database gave the server agent. */

import { Effect } from "effect";
import { expect, it } from "vitest";
import {
  joinParticipants,
  nextItem,
  type Participant,
  send,
} from "../agent-fixture.js";
import { historyMessage } from "../saved-history.js";
import { processTrace } from "../scenario-fixture.js";
import { callTool } from "../tool-call.js";
import {
  SERVER_AGENT_OWNER,
  sharedDaemon,
  stopProcess,
} from "./daemon-fixture.js";

/** Each trace starts one daemon, and some of them start it more than once. */
const PROCESS_TEST_TIMEOUT_MS = 120_000;

const textContent = (text: string) => [{ type: "text", text }];

/** The identifiers of the next `count` messages the participant receives, in order. */
const receivedOrder = (participant: Participant, count: number) =>
  Effect.forEach(
    Array.from({ length: count }),
    () => nextItem(participant).pipe(Effect.map((item) => item.message.msgId)),
    { concurrency: 1 },
  );

/** The settings a refused start tells the operator to change. */
const serverNameSetting = "serverAgent.name";

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
      yield* send(client, { text: reply });
      expect(yield* nextItem(server)).toMatchObject({
        kind: "message",
        message: {
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
      expect(
        serverHistory.map((record) => historyMessage(record).content),
      ).toEqual([textContent(greeting), textContent(reply)]);
      expect(clientHistory.map(historyMessage)).toEqual(
        serverHistory.map(historyMessage),
      );
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
  "completes the messages the server agent and a client send each other at once",
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
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "refuses another server agent name on a database started with one, naming the setting",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
      yield* stopProcess(yield* daemon.start());

      yield* daemon.configure({
        serverAgent: { name: "back-office", subject: SERVER_AGENT_OWNER },
      });
      const renamed = yield* daemon.startupFailure();
      expect(renamed.exitCode).not.toBe(0);
      expect(renamed.logs).toContain(`Set ${serverNameSetting}`);
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);
