/**
 * @file The daemon as its executable runs it, against a test database and a
 * test issuer: which settings it starts with and which it refuses, with what
 * message; whom it admits; and when it ends a stream.
 */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  ConfigProvider,
  Effect,
  FastCheck as fc,
  Fiber,
  Redacted,
  Schedule,
} from "effect";
import { createServer } from "node:net";
import { join } from "node:path";
import { expect } from "vitest";
import { callTool } from "./__tests__/daemon.js";
import {
  issuerFixture,
  openTestDatabase,
  requiredScope,
} from "./__tests__/test.js";
import { runDaemon } from "./main.js";

const providerKey = "private-decision-provider-key";
const protocolVersion = "2026-07-28";

/** A free loopback port, released before the daemon binds it. */
const freePort = Effect.async<number>((resume) => {
  const server = createServer();
  server.listen(0, "127.0.0.1", () => {
    const address = server.address();
    server.close(() => {
      resume(
        address === null || typeof address === "string"
          ? Effect.dieMessage("Expected a TCP port")
          : Effect.succeed(address.port),
      );
    });
  });
});

/**
 * A daemon the executable's entry runs: valid settings in a directory of
 * their own, a test database and a test issuer. `write` replaces the
 * settings file, `refusal` is the message the daemon refuses to start with,
 * and `start` runs it until the test ends, once it listens.
 */
const fixture = Effect.gen(function* () {
  const filesystem = yield* FileSystem.FileSystem;
  const directory = yield* filesystem.makeTempDirectoryScoped({
    prefix: "sharness-settings-",
  });
  const database = yield* openTestDatabase();
  const issuer = yield* issuerFixture().pipe(Effect.orDie);
  const port = yield* freePort;
  const endpoint = `http://127.0.0.1:${port}/mcp`;
  const resource = `https://127.0.0.1:${port}/mcp`;
  const settings = {
    port,
    host: "127.0.0.1",
    authentication: { resource, issuer: issuer.issuer, scope: requiredScope },
    serverAgent: { name: "front-desk", subject: "server" },
  };
  const filename = join(directory, "settings.json");
  const write = (value: unknown) =>
    filesystem.writeFileString(filename, JSON.stringify(value));
  yield* write(settings);
  const run = runDaemon.pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(
        new Map([
          ["SHARNESS_CONFIG_FILE", filename],
          ["SHARNESS_DATABASE_URL", Redacted.value(database.url)],
          ["SHARNESS_DECISION_PROVIDER_KEY", providerKey],
        ]),
      ),
    ),
  );
  const start = Effect.gen(function* () {
    const running = yield* Effect.forkScoped(run);
    yield* Effect.raceFirst(
      Effect.tryPromise(() => fetch(endpoint)).pipe(
        Effect.retry(Schedule.spaced("25 millis")),
      ),
      Fiber.join(running),
    );
    return running;
  });
  /** One raw MCP request with the given bearer token, or with none. */
  const request = (method: string, bearer?: string) =>
    Effect.tryPromise(() =>
      fetch(endpoint, {
        method: "POST",
        headers: {
          ...(bearer === undefined
            ? {}
            : { authorization: `Bearer ${bearer}` }),
          "content-type": "application/json",
          accept: "application/json, text/event-stream",
          "mcp-protocol-version": protocolVersion,
          "mcp-method": method,
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method,
          params: {
            notifications: { toolsListChanged: true },
            _meta: {
              "io.modelcontextprotocol/protocolVersion": protocolVersion,
              "io.modelcontextprotocol/clientCapabilities": {},
              "io.modelcontextprotocol/clientInfo": {
                name: "daemon-test",
                version: "1",
              },
            },
          },
        }),
      }),
    ).pipe(Effect.orDie);
  return {
    filesystem,
    directory,
    settings,
    write,
    start,
    request,
    endpoint,
    /** The message the daemon refuses to start with. */
    refusal: run.pipe(
      Effect.flip,
      Effect.map((failure) => failure.message),
      Effect.timeout("20 seconds"),
    ),
    /** A token of the daemon's issuer for `subject`, as `claims` change it. */
    token: (subject: string, claims: Parameters<typeof issuer.token>[2] = {}) =>
      issuer.token(subject, resource, claims).pipe(Effect.orDie),
    /** The subject of every owner with an agent. */
    owners: database.sql<{
      subject: string;
    }>`SELECT subject FROM agent_cards ORDER BY subject`.pipe(
      Effect.map((rows) => rows.map((row) => row.subject)),
      Effect.orDie,
    ),
  };
});

type Fixture = Effect.Effect.Success<typeof fixture>;

/**
 * A message policy file in `beneath`, a directory under the settings file's
 * own, with a decision provider when `judged`. Returns the setting that
 * names it.
 */
function messagePolicy(test: Fixture, judged: boolean, beneath = "") {
  return Effect.gen(function* () {
    const directory = join(test.directory, beneath);
    yield* test.filesystem.makeDirectory(directory, { recursive: true });
    yield* test.filesystem.writeFileString(
      join(directory, "policy.json"),
      JSON.stringify({
        ...(judged ? { decisionProvider: { model: "configured-model" } } : {}),
      }),
    );
    return { messagePolicyFile: join(beneath, "policy.json") };
  });
}

/** A norm document with no English rule. */
const unruledNorm = `(norm ping :initiator Asker :participant Answerer)
(content ask :by Asker :starts true :ends true)
`;

/** The same norm with an English rule, which needs a decision provider. */
const ruledNorm = `${unruledNorm}(rule :act ask "The question is polite.")
`;

/** A norms directory in `beneath` holding `document` as its one norm document. */
function norms(test: Fixture, document: string, beneath = "") {
  return Effect.gen(function* () {
    const directory = join(test.directory, beneath, "norms");
    yield* test.filesystem.makeDirectory(directory, { recursive: true });
    yield* test.filesystem.writeFileString(
      join(directory, "ping.lisp"),
      document,
    );
    return { normsDirectory: join(beneath, "norms") };
  });
}

/** An SDK client connected to the daemon as the holder of `bearer`. The scope closes it. */
const connect = (test: Fixture, bearer: string) =>
  Effect.gen(function* () {
    const mcp = new Client(
      { name: "daemon-test", version: "1" },
      { versionNegotiation: { mode: { pin: protocolVersion } } },
    );
    const transport = new StreamableHTTPClientTransport(
      new URL(test.endpoint),
      { requestInit: { headers: { authorization: `Bearer ${bearer}` } } },
    );
    yield* Effect.tryPromise(() => mcp.connect(transport)).pipe(Effect.orDie);
    yield* Effect.addFinalizer(() =>
      Effect.tryPromise(() => mcp.close()).pipe(Effect.ignore),
    );
    return { mcp };
  });

it("names the missing settings-file variable", () =>
  runDaemon.pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(
        new Map([["SHARNESS_DATABASE_URL", "postgres://"]]),
      ),
    ),
    Effect.flip,
    Effect.tap((failure) => {
      expect(failure.message).toContain("SHARNESS_CONFIG_FILE");
    }),
  ));

it("refuses a settings file that does not exist", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.filesystem.remove(join(test.directory, "settings.json"));
    expect(yield* test.refusal).toContain("settings file");
  }).pipe(Effect.provide(NodeContext.layer)));

it.each(["", "nested/deeper"])(
  "starts with a policy, its provider key and a norms directory named relative to the settings file (beneath it: '%s'), and shows the norm to a signed-in agent",
  (beneath) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write({
        ...test.settings,
        ...(yield* messagePolicy(test, true, beneath)),
        ...(yield* norms(test, ruledNorm, beneath)),
      });

      yield* test.start;

      const { mcp } = yield* connect(test, yield* test.token("server"));
      const shown = yield* Effect.tryPromise(() =>
        mcp.callTool({ name: "read_norms", arguments: {} }),
      );
      expect(shown.structuredContent).toMatchObject({
        norms: [{ name: "ping", document: ruledNorm }],
      });
    }).pipe(Effect.provide(NodeContext.layer)),
);

it.each([
  { what: "no message policy", judged: undefined },
  { what: "a message policy without a decision provider", judged: false },
])("refuses norms with English rules under $what", ({ judged }) =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.write({
      ...test.settings,
      ...(judged === undefined ? {} : yield* messagePolicy(test, judged)),
      ...(yield* norms(test, ruledNorm)),
    });

    expect(yield* test.refusal).toContain("configured norms");
  }).pipe(Effect.provide(NodeContext.layer)),
);

it("refuses a norms directory whose document does not compile", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.write({
      ...test.settings,
      ...(yield* messagePolicy(test, false)),
      ...(yield* norms(test, "(norm ping :initiator Asker)")),
    });
    expect(yield* test.refusal).toContain("norms directory");
  }).pipe(Effect.provide(NodeContext.layer)));

it.each([
  { maximumHistoryRecords: 1, taken: true },
  { maximumHistoryRecords: 1024, taken: true },
  { maximumHistoryRecords: 0, taken: false },
  { maximumHistoryRecords: 1025, taken: false },
])(
  "takes a history limit of 1 to 1024 records and refuses any other ($maximumHistoryRecords)",
  ({ maximumHistoryRecords, taken }) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write({ ...test.settings, maximumHistoryRecords });

      if (taken) {
        yield* test.start;
        expect((yield* test.request("server/discover")).status).toBe(401);
      } else {
        expect(yield* test.refusal).toContain("settings file");
      }
    }).pipe(Effect.provide(NodeContext.layer)),
);

const knownSettings = new Set([
  "port",
  "host",
  "authentication",
  "serverAgent",
  "messagePolicyFile",
  "normsDirectory",
  "maximumHistoryRecords",
]);

/** One setting no daemon serves: a port outside 1 to 65535, a setting the document does not have, or a server agent name that is not an agent name. */
const unservable = fc.oneof(
  fc
    .oneof(
      fc.integer({ min: -65_535, max: 0 }),
      fc.integer({ min: 65_536, max: 1_000_000 }),
      fc.integer({ min: 1, max: 65_534 }).map((port) => port + 0.5),
    )
    .map((port) => ({ port })),
  fc
    .stringMatching(/^[a-zA-Z]{3,12}$/)
    .filter((name) => !knownSettings.has(name))
    .map((name) => ({ [name]: 4319 })),
  fc
    .oneof(
      fc.stringMatching(/^[A-Z ]{3,12}$/),
      fc.stringMatching(/^[a-z]{1,2}$/),
    )
    .map((name) => ({ serverAgent: { name, subject: "server" } })),
);

it("refuses a document with a setting it cannot serve", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(unservable, (setting) =>
          Effect.gen(function* () {
            yield* test.write({ ...test.settings, ...setting });
            expect(yield* test.refusal).toContain("settings file");
          }).pipe(Effect.runPromise),
        ),
        { numRuns: 40 },
      ),
    );
  }).pipe(Effect.provide(NodeContext.layer)));

it("refuses to start on a port another process holds, naming the port", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.start;
    expect(yield* test.refusal).toContain(
      `could not listen on 127.0.0.1:${test.settings.port}`,
    );
  }).pipe(Effect.provide(NodeContext.layer)));

/** A scope that begins with the configured one and goes on past it. */
const longerScope = fc
  .stringMatching(/^[a-z:]{1,3}$/u)
  .map((more) => `${requiredScope}${more}`);

/** Claims the daemon admits: any subject, and the configured scope anywhere among those granted. */
const admissible = fc.record({
  subject: fc.string({ minLength: 1 }),
  granted: fc
    .tuple(fc.array(longerScope), fc.array(longerScope))
    .map(([before, after]) => [...before, requiredScope, ...after]),
});

/** One claim that puts a token outside the daemon's authority. */
const defect = fc.oneof(
  fc.record({ subject: fc.constant("") }),
  fc.record({ granted: fc.array(longerScope) }),
  fc.record({ secondsLeft: fc.integer({ min: -3600, max: 0 }) }),
);

it("answers a token granted the scope from the agent of its subject, whose tools it can call", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.start;
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(admissible, ({ subject, granted }) =>
          Effect.gen(function* () {
            const response = yield* test.request(
              "server/discover",
              yield* test.token(subject, { scope: granted.join(" ") }),
            );
            expect(response.status).toBe(200);
            expect(yield* test.owners).toContain(subject);
          }).pipe(Effect.runPromise),
        ),
        { numRuns: 30 },
      ),
    );
    const status = yield* callTool(
      yield* connect(test, yield* test.token("server")),
      "status",
    );
    expect(status.agentName).toBe("front-desk");
  }).pipe(Effect.provide(NodeContext.layer)));

it("refuses a request with no token, a defective token or another issuer's token, and creates no agent for it", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.start;
    const other = yield* issuerFixture().pipe(Effect.orDie);
    const refused = (bearer?: string) =>
      Effect.gen(function* () {
        expect((yield* test.request("server/discover", bearer)).ok).toBe(false);
        expect(yield* test.owners).toEqual(["server"]);
      });
    yield* refused();
    yield* refused(
      yield* other
        .token("owner", test.settings.authentication.resource)
        .pipe(Effect.orDie),
    );
    yield* refused(
      yield* test
        .token("owner", {})
        .pipe(Effect.map((token) => `${token.slice(0, -4)}AAAA`)),
    );
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(defect, (claim) =>
          Effect.gen(function* () {
            yield* refused(
              yield* test.token("subject" in claim ? claim.subject : "owner", {
                ...("granted" in claim
                  ? { scope: claim.granted.join(" ") }
                  : {}),
                ...("secondsLeft" in claim
                  ? { secondsLeft: claim.secondsLeft }
                  : {}),
              }),
            );
          }).pipe(Effect.runPromise),
        ),
        { numRuns: 30 },
      ),
    );
  }).pipe(Effect.provide(NodeContext.layer)));

it(
  "ends an admitted stream when its token expires and when the daemon stops, and keeps answering other requests meanwhile",
  () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const running = yield* test.start;
      const listen = (secondsLeft: number) =>
        Effect.flatMap(test.token("server", { secondsLeft }), (token) =>
          test.request("subscriptions/listen", token),
        );

      const expiring = yield* listen(2);
      expect(expiring.status).toBe(200);
      expect(yield* Effect.tryPromise(() => expiring.text())).toContain(
        "notifications/subscriptions/acknowledged",
      );
      expect(
        (yield* test.request("server/discover", yield* test.token("server")))
          .status,
      ).toBe(200);

      const open = yield* listen(3600);
      yield* Fiber.interrupt(running);
      yield* Effect.tryPromise(() => open.text()).pipe(
        Effect.ignore,
        Effect.timeout("5 seconds"),
      );
      expect(
        yield* Effect.tryPromise(() => fetch(test.endpoint)).pipe(Effect.flip),
      ).toBeDefined();
    }).pipe(Effect.provide(NodeContext.layer)),
  20_000,
);
