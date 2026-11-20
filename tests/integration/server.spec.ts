/** @file How the `sharnessd` binary starts and refuses to start and what it keeps across a restart. */

import {
  Command,
  FetchHttpClient,
  FileSystem,
  HttpClient,
  HttpClientRequest,
} from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { Effect, identity, Redacted, Schedule, Stream } from "effect";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { callTool, send, type SignedIn } from "../../src/__tests__/server.js";
import { issuerFixture } from "../../src/__tests__/test.js";
import {
  inboxStream,
  processTrace,
  SERVER_AGENT_NAME,
  SERVER_AGENT_OWNER,
  sharedServer,
  stopProcess,
} from "./server-fixture.js";

const BINARY = fileURLToPath(new URL("../../dist/main.js", import.meta.url));
const DATABASE_VARIABLE = "SHARNESS_DATABASE_URL";
const SECRET = "secret-path-value";

/** Runs a command to its end and gives both of its output streams and its exit code. */
const runProcess = Effect.fn("runProcess")(function* (
  command: string,
  args: readonly string[],
) {
  const child = yield* Command.start(Command.make(command, ...args));
  const text = <E>(stream: Stream.Stream<Uint8Array, E>) =>
    stream.pipe(Stream.decodeText(), Stream.mkString);
  const [stdout, stderr, exitCode] = yield* Effect.all(
    [text(child.stdout), text(child.stderr), child.exitCode],
    { concurrency: 3 },
  );
  return { stdout, stderr, exitCode };
}, Effect.scoped);

/** Each trace starts one server, and some of them start it more than once. */
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

/** A norm document with no English rule. */
const unruledNorm = `(norm ping :initiator Asker :participant Answerer)
(content ask :by Asker :starts true :ends true)
`;

/** The same norm with an English rule, which needs a decision provider. */
const ruledNorm = `${unruledNorm}(rule :act ask "The question is polite.")
`;

it(
  "refuses each settings file it cannot serve and an occupied port, saying which, and starts with a policy and norms named relative to the settings file",
  processTrace(
    Effect.gen(function* () {
      const sharnessd = yield* sharedServer();
      const fs = yield* FileSystem.FileSystem;
      const write = (path: string, content: string) =>
        fs
          .makeDirectory(dirname(join(sharnessd.directory, path)), {
            recursive: true,
          })
          .pipe(
            Effect.zipRight(
              fs.writeFileString(join(sharnessd.directory, path), content),
            ),
            Effect.as(path),
          );
      const unjudged = yield* write("unjudged.json", "{}");
      const judged = yield* write(
        "nested/policy.json",
        JSON.stringify({ decisionProvider: { model: "configured-model" } }),
      );
      yield* write("nested/norms/ping.lisp", ruledNorm);
      yield* write("broken/ping.lisp", "(norm ping :initiator Asker)");
      const refusals: ReadonlyArray<
        readonly [Readonly<Record<string, unknown>>, string]
      > = [
        [{ port: 65_536 }, "settings file"],
        [{ port: 0.5 }, "settings file"],
        [{ unknownSetting: 1 }, "settings file"],
        [
          { serverAgent: { name: "Front Desk", subject: SERVER_AGENT_OWNER } },
          "settings file",
        ],
        [{ maximumHistoryRecords: 0 }, "settings file"],
        [{ maximumHistoryRecords: 1025 }, "settings file"],
        [{ messagePolicyFile: "absent.json" }, ""],
        [{ normsDirectory: "nested/norms" }, "configured norms"],
        [
          { normsDirectory: "nested/norms", messagePolicyFile: unjudged },
          "configured norms",
        ],
        [
          { normsDirectory: "broken", messagePolicyFile: unjudged },
          "norms directory",
        ],
      ];
      const settings = yield* fs.readFileString(
        join(sharnessd.directory, "settings.json"),
      );
      for (const [changes, said] of refusals) {
        yield* fs.writeFileString(
          join(sharnessd.directory, "settings.json"),
          JSON.stringify({ ...JSON.parse(settings), ...changes }),
        );
        const refused = yield* sharnessd.startupFailure();
        expect([changes, refused.exitCode]).toEqual([changes, 1]);
        expect(refused.logs).toContain(said);
      }

      yield* fs.writeFileString(
        join(sharnessd.directory, "settings.json"),
        settings,
      );
      yield* sharnessd.configure({
        maximumHistoryRecords: 1024,
        messagePolicyFile: judged,
        normsDirectory: "nested/norms",
      });
      yield* sharnessd.start();
      const server = yield* sharnessd.signInAs(SERVER_AGENT_OWNER);
      const shown = yield* Effect.tryPromise(() =>
        server.mcp.callTool({ name: "read_norms", arguments: {} }),
      );
      expect(shown.structuredContent).toMatchObject({
        norms: [{ name: "ping", document: ruledNorm }],
      });

      const occupied = yield* sharnessd.startupFailure();
      expect(occupied.exitCode).toBe(1);
      expect(occupied.logs).toContain(`could not listen on ${sharnessd.host}`);
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

it(
  "admits a token granted the scope as the agent of its subject, refuses every other request with the SDK's answer, and stops promptly with a stream open",
  processTrace(
    Effect.gen(function* () {
      const sharnessd = yield* sharedServer();
      const running = yield* sharnessd.start();
      const other = yield* issuerFixture();
      const client = yield* HttpClient.HttpClient;
      const statusOf = (path: string, bearer: string) =>
        HttpClientRequest.post(new URL(path, sharnessd.endpoint)).pipe(
          bearer === "" ? identity : HttpClientRequest.bearerToken(bearer),
          client.execute,
          Effect.map((response) => response.status),
        );
      const issued = (
        owner: string,
        claims: Parameters<typeof sharnessd.bearer>[1],
      ) => Effect.map(sharnessd.bearer(owner, claims), Redacted.value);

      expect(yield* statusOf("/mcp", "")).toBe(401);
      expect(yield* statusOf("/other", yield* issued("bob", {}))).toBe(404);
      const good = yield* issued("bob", {});
      for (const defective of [
        yield* other.token("bob", sharnessd.resource),
        `${good.slice(0, -4)}AAAA`,
        yield* issued("bob", { secondsLeft: -1 }),
        yield* issued("", {}),
      ]) {
        expect(yield* statusOf("/mcp", defective)).toBe(401);
      }
      expect(
        yield* statusOf("/mcp", yield* issued("bob", { scope: "other:scope" })),
      ).toBe(403);

      const server = yield* sharnessd.signInAs(SERVER_AGENT_OWNER);
      expect(server.name).toBe(SERVER_AGENT_NAME);
      const bob = yield* sharnessd.signInAs("bob");
      expect(bob.name).toMatch(/^agent-[0-9a-f]{24}$/u);
      expect((yield* sharnessd.signInAs("bob")).name).toBe(bob.name);

      yield* Effect.forkScoped(Stream.runDrain(inboxStream(bob, "0", "push")));
      yield* callTool(bob, "receive_messages");
      yield* stopProcess(running);
      expect(running.child.signalCode).toBeNull();
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);

/** The agent's inbox entries after `cursor`, read again until there is one. Without a cursor the read starts from now. */
const readInbox = (agent: SignedIn, cursor?: string) =>
  callTool(agent, "receive_messages", { cursor }).pipe(
    Effect.filterOrFail(
      (page) => cursor === undefined || page.entries.length > 0,
    ),
    Effect.retry(Schedule.spaced("50 millis")),
  );

it(
  "keeps each owner's agent name, history and inbox across a restart, where a cursor from before it reads the same entries",
  processTrace(
    Effect.gen(function* () {
      const sharnessd = yield* sharedServer();
      const original = yield* sharnessd.start();
      const sent = yield* Effect.scoped(
        Effect.gen(function* () {
          const server = yield* sharnessd.signInAs(SERVER_AGENT_OWNER);
          const client = yield* sharnessd.signInAs("bob");
          expect(server.name).toBe(SERVER_AGENT_NAME);

          const opening = yield* send(client, { text: "opening" });
          const start = yield* readInbox(client);
          expect(start.entries).toEqual([]);
          const first = yield* send(server, {
            to: client.name,
            text: "read before the restart",
          });
          const read = yield* readInbox(client, start.nextCursor);
          expect(read.entries).toMatchObject([
            { message: { msgId: first.id } },
          ]);
          const unread = yield* send(server, {
            to: client.name,
            text: "unread across the restart",
          });
          return {
            client: client.name,
            opening: opening.id,
            ids: [first.id, unread.id],
            cursors: [start.nextCursor, read.nextCursor],
          };
        }),
      );
      yield* stopProcess(original);
      yield* sharnessd.start();

      const client = yield* sharnessd.signInAs("bob");
      const server = yield* sharnessd.signInAs(SERVER_AGENT_OWNER);
      expect(client.name).toBe(sent.client);
      expect(server.name).toBe(SERVER_AGENT_NAME);
      const [start, afterFirst] = sent.cursors;
      expect((yield* readInbox(client, afterFirst)).entries).toMatchObject([
        { message: { msgId: sent.ids[1] } },
      ]);
      expect(
        (yield* readInbox(client, start)).entries.map(
          (entry) => entry.message.msgId,
        ),
      ).toEqual(sent.ids);
      const history = yield* callTool(client, "read_conversation");
      expect(history.records.map((record) => record.message.msgId)).toEqual([
        sent.opening,
        ...sent.ids,
      ]);
    }),
  ),
  PROCESS_TEST_TIMEOUT_MS,
);
