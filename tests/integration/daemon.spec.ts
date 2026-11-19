/** @file How the `sharnessd` binary starts and refuses to start, what it keeps across a restart, and how the operator script reaches it. */

import { FileSystem, HttpClient } from "@effect/platform";
import { NodeContext, NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  MAXIMUM_WAIT_SECONDS,
  toolDefinitions,
} from "@social-harness/social-harness";
import {
  databaseConnection,
  databaseUrl,
} from "@social-harness/social-harness/test";
import {
  ConfigProvider,
  Context,
  Effect,
  Layer,
  Redacted,
  Schema,
} from "effect";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { runProcess } from "../../scripts/test/packed-workspace.js";
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
const CLI = fileURLToPath(new URL("../../images/mcp.ts", import.meta.url));
const TEST_DIRECTORY = fileURLToPath(new URL(".cache/", import.meta.url));
const DATABASE_VARIABLE = "SHARNESS_DATABASE_URL";
const SECRET = "secret-path-value";
const AGENT_UNAVAILABLE = 503;
const CLEARTEXT_REFUSAL = "MCP URL must use HTTPS or literal HTTP loopback";

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

/** A page of the agent's pending inbox entries, waiting for one to arrive. `cursor` commits the page it came with first. */
const awaitInbox = (agent: SignedIn, cursor?: string) =>
  callTool(agent.connection.mcp, {
    name: "receive_messages",
    arguments: {
      wait: MAXIMUM_WAIT_SECONDS,
      ...(cursor === undefined ? {} : { cursor }),
    },
  });

/** The daemon's database, opened as the daemon opens it. */
function openDatabase(daemon: Daemon) {
  return Effect.gen(function* () {
    const url = yield* databaseUrl.pipe(
      Effect.withConfigProvider(
        ConfigProvider.fromMap(new Map(Object.entries(daemon.environment))),
      ),
    );
    return Context.get(
      yield* Layer.build(databaseConnection(url)),
      SqlClient.SqlClient,
    );
  });
}

/** The status the daemon answers a request that presents `bearer`. */
function requestStatus(daemon: Daemon, bearer: Redacted.Redacted) {
  return HttpClient.HttpClient.pipe(
    Effect.flatMap((client) =>
      client.post(daemon.endpoint.href, {
        headers: { authorization: `Bearer ${Redacted.value(bearer)}` },
      }),
    ),
    Effect.map((response) => response.status),
    Effect.provide(NodeHttpClient.layer),
  );
}

it(
  "keeps each owner's agent, history and the inbox entries it has not committed across a restart, and serves the server agent when a client's record is lost",
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

          const first = yield* send(server, {
            to: client.address,
            text: "read and committed before the restart",
          });
          const committed = yield* awaitInbox(client);
          expect(committed.entries).toMatchObject([
            { message: { msgId: first.id } },
          ]);
          const second = yield* send(server, {
            to: client.address,
            text: "read before the restart and not committed",
          });
          expect(
            (yield* awaitInbox(client, committed.nextCursor)).entries,
          ).toMatchObject([{ message: { msgId: second.id } }]);
          const unread = yield* send(server, {
            to: client.address,
            text: "unread across the restart",
          });
          return {
            server: server.agent,
            client: client.agent,
            ids: [first.id, second.id, unread.id],
          };
        }),
      );
      yield* stopProcess(original);
      const restarted = yield* daemon.start();

      const client = yield* daemon.signInAs("bob");
      const server = yield* daemon.signInAs(SERVER_AGENT_OWNER);
      expect(client.agent).toEqual(sent.client);
      expect(server.agent).toEqual(sent.server);
      expect((yield* awaitInbox(client)).entries).toMatchObject([
        { message: { msgId: sent.ids[1] } },
        { message: { msgId: sent.ids[2] } },
      ]);
      const history = yield* callTool(client.connection.mcp, {
        name: "read_conversation",
        arguments: {},
      });
      expect(history.records.map((record) => record.message.msgId)).toEqual(
        sent.ids,
      );

      const sql = yield* openDatabase(daemon);
      yield* sql`DELETE FROM agent_cards WHERE agent_name = ${client.agent.agentName}`;
      yield* stopProcess(restarted);
      const serving = yield* daemon.start();
      expect((yield* daemon.signInAs(SERVER_AGENT_OWNER)).agent).toEqual(
        sent.server,
      );
      expect(yield* requestStatus(daemon, yield* daemon.bearer("bob"))).toBe(
        AGENT_UNAVAILABLE,
      );
      expect(serving.child.exitCode).toBeNull();
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

/** Runs a copy of the script, which resolves the integration package's installed public dependencies. */
function operator(
  target: { readonly endpoint: URL; readonly credentialFile: string },
  args: readonly string[],
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      yield* fs.makeDirectory(TEST_DIRECTORY, { recursive: true });
      const directory = yield* fs.makeTempDirectoryScoped({
        directory: TEST_DIRECTORY,
        prefix: ".operator-cli-",
      });
      const script = join(directory, "mcp.ts");
      yield* fs.copyFile(CLI, script);
      return yield* runProcess(process.execPath, [script, ...args], {
        cwd: directory,
        env: {
          SHARNESS_MCP_URL: target.endpoint.href,
          SHARNESS_MCP_CREDENTIAL_FILE: target.credentialFile,
          NODE_PATH: "",
        },
      });
    }),
  ).pipe(Effect.provide(NodeContext.layer));
}

it(
  "lets the operator script report the agent of the owner whose credential it reads, and refuses to send one to an HTTP endpoint named by DNS",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
      yield* daemon.start();
      const alice = yield* daemon.signInAs("alice");
      const bob = yield* daemon.signInAs("bob");
      const status = (credentialFile: string) =>
        operator({ endpoint: daemon.endpoint, credentialFile }, [
          "call",
          "status",
        ]).pipe(
          Effect.flatMap(({ stdout }) =>
            Schema.decode(Schema.parseJson(toolDefinitions.status.output))(
              stdout,
            ),
          ),
        );
      expect((yield* status(alice.credentialFile)).agentName).toBe(
        alice.agent.agentName,
      );
      expect((yield* status(bob.credentialFile)).agentName).toBe(
        bob.agent.agentName,
      );

      const refused = yield* operator(
        {
          endpoint: new URL("http://localhost:1/mcp"),
          credentialFile: alice.credentialFile,
        },
        ["call", "status"],
      );
      expect(refused.exitCode).not.toBe(0);
      expect(refused.stderr).toContain(CLEARTEXT_REFUSAL);
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);
