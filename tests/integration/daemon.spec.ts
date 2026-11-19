/** @file How the `sharnessd` binary starts and refuses to start and what it keeps across a restart. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { Effect, Schedule } from "effect";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { runProcess } from "../../scripts/packed-workspace.js";
import {
  callTool,
  type Daemon,
  processTrace,
  send,
  SERVER_AGENT_NAME,
  SERVER_AGENT_OWNER,
  sharedDaemon,
  type SignedIn,
  stopProcess,
} from "./daemon-fixture.js";

const BINARY = fileURLToPath(new URL("../../dist/main.js", import.meta.url));
const DATABASE_VARIABLE = "SHARNESS_DATABASE_URL";
const SECRET = "secret-path-value";

/** Each trace starts one daemon, and some of them start it more than once. */
const PROCESS_TEST_TIMEOUT_MS = 180_000;

/**
 * A run of the binary, whether it succeeds, and what it prints on standard
 * output when it does and on standard error when it does not. Each runs in an
 * environment holding nothing else, so a missing variable is missing whatever
 * the test runner exports.
 */
const runs: ReadonlyArray<{
  readonly behavior: string;
  readonly processArguments: readonly string[];
  readonly environment: Readonly<Record<string, string>>;
  readonly succeeds: boolean;
  readonly says: readonly string[];
}> = [
  {
    behavior: "names the variables it reads when asked for help, and succeeds",
    processArguments: ["--help"],
    environment: { [DATABASE_VARIABLE]: SECRET },
    succeeds: true,
    says: [DATABASE_VARIABLE, "SHARNESS_CONFIG_FILE"],
  },
  {
    behavior: "names a missing variable and fails",
    processArguments: [],
    environment: {},
    succeeds: false,
    says: [DATABASE_VARIABLE],
  },
  {
    behavior: "fails on invalid configuration",
    processArguments: [],
    environment: { [DATABASE_VARIABLE]: SECRET },
    succeeds: false,
    says: [],
  },
];

it.for(runs)(
  "sharnessd $behavior, without echoing a supplied value",
  { timeout: 15_000 },
  ({ processArguments, environment, succeeds, says }, context) =>
    processTrace(
      Effect.gen(function* () {
        const result = yield* runProcess("env", [
          "-i",
          `PATH=${dirname(process.execPath)}:/usr/bin:/bin`,
          ...Object.entries(environment).map(
            ([name, value]) => `${name}=${value}`,
          ),
          process.execPath,
          BINARY,
          ...processArguments,
        ]).pipe(Effect.provide(NodeContext.layer));
        const [spoken, silent] = succeeds
          ? [result.stdout, result.stderr]
          : [result.stderr, result.stdout];

        expect(result.exitCode === 0).toBe(succeeds);
        expect(says.filter((text) => !spoken.includes(text))).toEqual([]);
        expect(silent).toBe("");
        expect(result.stdout + result.stderr).not.toContain(SECRET);
      }),
    )(context),
);

it(
  "rejects an invalid message policy file and starts again once the settings no longer name it",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
      const original = yield* daemon.start();
      const { agent } = yield* daemon.signInAs("carol");
      yield* stopProcess(original);
      const fs = yield* FileSystem.FileSystem;
      const messagePolicyFile = join(daemon.directory, "invalid-policy.json");
      yield* fs.writeFileString(
        messagePolicyFile,
        JSON.stringify({
          format: "org.social-harness/message-policy/v2",
          mode: "off",
        }),
      );
      yield* daemon.configure({ messagePolicyFile });
      expect((yield* daemon.startupFailure()).exitCode).not.toBe(0);
      yield* daemon.configure({ messagePolicyFile: undefined });
      yield* daemon.start();
      expect((yield* daemon.signInAs("carol")).agent).toEqual(agent);
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
      expect(renamed.logs).toContain("serverAgent.name");
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

/** The agent's inbox entries after `cursor`, read again until there is one. Without a cursor the read starts from now. */
const readInbox = (agent: SignedIn, cursor?: string) =>
  callTool(agent.connection.mcp, {
    name: "receive_messages",
    arguments: { cursor },
  }).pipe(
    Effect.filterOrFail(
      (page) => cursor === undefined || page.entries.length > 0,
    ),
    Effect.retry(Schedule.spaced("50 millis")),
  );

it(
  "keeps each owner's agent, history and inbox across a restart, where a cursor from before it reads the same entries",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
      const original = yield* daemon.start();
      const sent = yield* Effect.scoped(
        Effect.gen(function* () {
          const server = yield* daemon.signInAs(SERVER_AGENT_OWNER);
          const client = yield* daemon.signInAs("bob");
          expect(server.agent.agentId).not.toBe(client.agent.agentId);
          expect(server.agent.agentName).toBe(SERVER_AGENT_NAME);

          const start = yield* readInbox(client);
          expect(start.entries).toEqual([]);
          const first = yield* send(server, {
            to: client.address,
            text: "read before the restart",
          });
          const read = yield* readInbox(client, start.nextCursor);
          expect(read.entries).toMatchObject([
            { message: { msgId: first.id } },
          ]);
          const unread = yield* send(server, {
            to: client.address,
            text: "unread across the restart",
          });
          return {
            server: server.agent,
            client: client.agent,
            ids: [first.id, unread.id],
            cursors: [start.nextCursor, read.nextCursor],
          };
        }),
      );
      yield* stopProcess(original);
      yield* daemon.start();

      const client = yield* daemon.signInAs("bob");
      const server = yield* daemon.signInAs(SERVER_AGENT_OWNER);
      expect(client.agent).toEqual(sent.client);
      expect(server.agent).toEqual(sent.server);
      const [start, afterFirst] = sent.cursors;
      expect((yield* readInbox(client, afterFirst)).entries).toMatchObject([
        { message: { msgId: sent.ids[1] } },
      ]);
      expect(
        (yield* readInbox(client, start)).entries.map(
          (entry) => entry.message.msgId,
        ),
      ).toEqual(sent.ids);
      const history = yield* callTool(client.connection.mcp, {
        name: "read_conversation",
        arguments: {},
      });
      expect(history.records.map((record) => record.message.msgId)).toEqual(
        sent.ids,
      );
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);
