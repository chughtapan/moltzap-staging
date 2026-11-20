/**
 * @file The executable's parts: it loads its settings from one file and
 * refuses a document it cannot serve; it admits owner-bound JWTs verified
 * against a real issuer key-set endpoint; and its listener reports a taken
 * port and closes open streams with its scope.
 */
import {
  FileSystem,
  HttpApp,
  HttpClient,
  HttpClientError,
  HttpClientResponse,
  HttpServerResponse,
} from "@effect/platform";
import {
  NodeContext,
  NodeHttpClient,
  NodeHttpServer,
} from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import {
  ConfigProvider,
  Effect,
  Either,
  Exit,
  FastCheck as fc,
  Redacted,
  Scope,
  Stream,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { join } from "node:path";
import { expect } from "vitest";
import { inProcessDaemon } from "./__tests__/daemon.js";
import { issuerFixture, requiredScope } from "./__tests__/test.js";
import { loadDaemonSettings, makeMcpHttpApp, serveMcp } from "./main.js";

const settingsVariable = "SHARNESS_CONFIG_FILE";
const providerKey = "private-decision-provider-key";
const settings = {
  port: 4319,
  host: "127.0.0.1",
  authentication: {
    resource: "https://mcp.example/mcp",
    issuer: "https://issuer.example/",
    scope: "use:mcp",
  },
  serverAgent: { name: "front-desk", subject: "server" },
};

/**
 * A valid settings file in a directory of its own, loaded through the variable
 * that names it.
 */
const fixture = Effect.gen(function* () {
  const filesystem = yield* FileSystem.FileSystem;
  const directory = yield* filesystem.makeTempDirectoryScoped({
    prefix: "sharness-settings-",
  });
  const filename = join(directory, "settings.json");
  const write = (value: unknown) =>
    filesystem.writeFileString(filename, JSON.stringify(value));
  yield* write(settings);
  const load = loadDaemonSettings().pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(
        new Map([
          [settingsVariable, filename],
          ["SHARNESS_DECISION_PROVIDER_KEY", providerKey],
        ]),
      ),
    ),
  );
  return { filesystem, directory, filename, write, load };
});

/**
 * A message policy file in `beneath`, a directory under the settings file's
 * own, with a decision provider when `judged`. Returns the setting that
 * names it.
 */
function messagePolicy(
  test: Effect.Effect.Success<typeof fixture>,
  judged: boolean,
  beneath = "",
) {
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
function norms(
  test: Effect.Effect.Success<typeof fixture>,
  document: string,
  beneath = "",
) {
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

it("names the missing settings-file variable", () =>
  loadDaemonSettings().pipe(
    Effect.withConfigProvider(ConfigProvider.fromMap(new Map())),
    Effect.flip,
    Effect.tap((failure) => {
      expect(failure.message).toContain(settingsVariable);
    }),
    Effect.provide(NodeContext.layer),
  ));

it("loads the settings file its variable names", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const loaded = yield* test.load;
    expect(loaded.port).toBe(settings.port);
    expect(loaded.authentication).toEqual({
      ...settings.authentication,
      resource: new URL(settings.authentication.resource),
    });
  }).pipe(Effect.provide(NodeContext.layer)));

it("refuses a settings file that does not exist", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.filesystem.remove(test.filename);
    expect((yield* test.load.pipe(Effect.flip)).message).toContain(
      "settings file",
    );
  }).pipe(Effect.provide(NodeContext.layer)));

it.each(["", "nested/deeper"])(
  "resolves the policy and the norms directory relative to the settings file (beneath it: '%s'), and reads the provider key from its variable",
  (beneath) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write({
        ...settings,
        ...(yield* messagePolicy(test, true, beneath)),
        ...(yield* norms(test, unruledNorm, beneath)),
      });

      const loaded = yield* test.load;

      expect(
        loaded.messagePolicy?.provider &&
          Redacted.value(loaded.messagePolicy.provider.apiKey),
      ).toBe(providerKey);
      expect(loaded.norms?.map((norm) => norm.source)).toEqual([unruledNorm]);
    }).pipe(Effect.provide(NodeContext.layer)),
);

it("loads the decision provider that judges initial norms' English rules, though the policy has no text rules", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.write({
      ...settings,
      ...(yield* messagePolicy(test, true)),
      ...(yield* norms(test, ruledNorm)),
    });

    const loaded = yield* test.load;

    expect(loaded.norms).toHaveLength(1);
    expect(loaded.messagePolicy?.policy.rules).toEqual([]);
    expect(loaded.messagePolicy?.provider?.model).toBe("configured-model");
  }).pipe(Effect.provide(NodeContext.layer)));

it.each([
  { what: "no message policy", policy: () => Effect.succeed({}) },
  {
    what: "a message policy without a decision provider",
    policy: (test: Effect.Effect.Success<typeof fixture>) =>
      messagePolicy(test, false),
  },
])("refuses initial norms with English rules under $what", ({ policy }) =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.write({
      ...settings,
      ...(yield* policy(test)),
      ...(yield* norms(test, ruledNorm)),
    });

    const failure = yield* test.load.pipe(Effect.flip);

    expect(failure.message).toContain("configured norms");
  }).pipe(Effect.provide(NodeContext.layer)),
);

it("refuses a norms directory whose document does not compile", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.write({
      ...settings,
      ...(yield* messagePolicy(test, false)),
      ...(yield* norms(test, "(norm ping :initiator Asker)")),
    });
    const failure = yield* test.load.pipe(Effect.flip);
    expect(failure.message).toContain("norms directory");
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
      yield* test.write({ ...settings, maximumHistoryRecords });

      const loaded = yield* test.load.pipe(Effect.either);

      expect(Either.getOrUndefined(loaded)?.maximumHistoryRecords).toBe(
        taken ? maximumHistoryRecords : undefined,
      );
    }).pipe(Effect.provide(NodeContext.layer)),
);

const knownSettings = new Set([
  ...Object.keys(settings),
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
    .map((name) => ({ [name]: settings.port })),
  fc
    .oneof(
      fc.stringMatching(/^[A-Z ]{3,12}$/),
      fc.stringMatching(/^[a-z]{1,2}$/),
    )
    .map((name) => ({ serverAgent: { name, subject: "server" } })),
);

it.prop(
  "refuses a document with a setting it cannot serve",
  [unservable],
  ([setting]) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write({ ...settings, ...setting });

      const failure = yield* test.load.pipe(Effect.flip);

      expect(failure.message).toContain("settings file");
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 40 } },
);

const resource = new URL("https://mcp.example.test/mcp");
const protocolVersion = "2026-07-28";

/**
 * A daemon's HTTP app behind a test issuer: `token` signs a token for a
 * subject, and `request` sends one raw MCP request with a bearer token.
 */
const admission = Effect.gen(function* () {
  const { daemon, sql } = yield* inProcessDaemon();
  const issuer = yield* issuerFixture().pipe(Effect.orDie);
  const handle = HttpApp.toWebHandler(
    yield* makeMcpHttpApp(
      { resource, issuer: issuer.issuer, scope: requiredScope },
      daemon,
    ).pipe(Effect.orDie),
  );
  return {
    sql,
    issuer,
    token: (subject: string, claims: Parameters<typeof issuer.token>[2] = {}) =>
      issuer.token(subject, resource.href, claims).pipe(Effect.orDie),
    request: (
      bearer: string,
      method: string,
      params: Readonly<Record<string, unknown>> = {},
    ) =>
      Effect.tryPromise(() =>
        handle(
          new Request(resource, {
            method: "POST",
            headers: {
              host: resource.host,
              authorization: `Bearer ${bearer}`,
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
                ...params,
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
        ),
      ).pipe(Effect.orDie),
  };
});

/** The subject of every owner with an agent on `daemon`. */
const ownersOf = (daemon: Effect.Effect.Success<typeof admission>) =>
  daemon.sql<{
    subject: string;
  }>`SELECT subject FROM agent_cards ORDER BY subject`.pipe(
    Effect.map((rows) => rows.map((row) => row.subject)),
    Effect.orDie,
  );

/** A scope that begins with the configured one and goes on past it. */
const longerScope = fc
  .stringMatching(/^[a-z:]{1,3}$/u)
  .map((more) => `${requiredScope}${more}`);

/** Claims the resource admits: any subject, and the configured scope anywhere among those granted. */
const admissible = fc.record({
  subject: fc.string({ minLength: 1 }),
  granted: fc
    .tuple(fc.array(longerScope), fc.array(longerScope))
    .map(([before, after]) => [...before, requiredScope, ...after]),
});

/** One claim that puts a token outside the resource's authority. */
const defect = fc.oneof(
  fc.record({ subject: fc.constant("") }),
  fc.record({
    audience: fc
      .webUrl()
      .filter((url) => url !== "https://mcp.example.test/mcp"),
  }),
  fc.record({ granted: fc.array(longerScope) }),
  fc.record({ secondsLeft: fc.integer({ min: -3600, max: 0 }) }),
);

/** Checks `rule` against generated claims. */
const forAll = <Claims>(
  claims: fc.Arbitrary<Claims>,
  rule: (claims: Claims) => Effect.Effect<void>,
) =>
  Effect.tryPromise(() =>
    fc.assert(
      fc.asyncProperty(claims, (each) => Effect.runPromise(rule(each))),
    ),
  );

it("answers a token granted the scope from the agent of its subject", () =>
  Effect.gen(function* () {
    const daemon = yield* admission;
    yield* forAll(admissible, ({ subject, granted }) =>
      Effect.gen(function* () {
        const response = yield* daemon.request(
          yield* daemon.token(subject, { scope: granted.join(" ") }),
          "server/discover",
        );
        expect(response.status).toBe(200);
        expect(yield* ownersOf(daemon)).toContain(subject);
      }),
    );
  }));

it("refuses a token with an empty subject, another audience, no grant of the scope or no time left", () =>
  Effect.gen(function* () {
    const daemon = yield* admission;
    yield* forAll(defect, (claim) =>
      Effect.gen(function* () {
        const token = yield* daemon.issuer
          .token(
            "subject" in claim ? claim.subject : "owner",
            "audience" in claim ? claim.audience : resource.href,
            {
              ...("granted" in claim ? { scope: claim.granted.join(" ") } : {}),
              ...("secondsLeft" in claim
                ? { secondsLeft: claim.secondsLeft }
                : {}),
            },
          )
          .pipe(Effect.orDie);
        const response = yield* daemon.request(token, "server/discover");
        expect(response.ok).toBe(false);
        expect(yield* ownersOf(daemon)).toEqual(["alice"]);
      }),
    );
  }));

it(
  "closes an admitted stream at token expiry without closing later requests",
  () =>
    Effect.gen(function* () {
      const daemon = yield* admission;
      const response = yield* daemon.request(
        yield* daemon.token("alice", { secondsLeft: 2 }),
        "subscriptions/listen",
        { notifications: { toolsListChanged: true } },
      );
      expect(response.status).toBe(200);
      const body = yield* Effect.tryPromise(() => response.text());
      expect(body).toContain("notifications/subscriptions/acknowledged");
      const later = yield* daemon.request(
        yield* daemon.token("alice"),
        "server/discover",
      );
      expect(later.status).toBe(200);
    }),
  15000,
);

const boundPort = Effect.gen(function* () {
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: 0,
  });
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected TCP listener");
  }
  return server.address.port;
});

it("reports an occupied port before declaring the listener ready", () =>
  Effect.gen(function* () {
    const port = yield* boundPort;
    expect(
      yield* serveMcp(
        port,
        Effect.succeed(HttpServerResponse.empty()),
        "127.0.0.1",
      ).pipe(Effect.flip),
    ).toMatchObject({ _tag: "ServeError" });
  }));

it("closes an active event stream with the listener's scope", () =>
  Effect.gen(function* () {
    const port = yield* Effect.scoped(boundPort);
    const scope = yield* Scope.make();
    const app = Effect.succeed(
      HttpServerResponse.stream(
        Stream.concat(Stream.make("active"), Stream.never).pipe(
          Stream.encodeText,
        ),
      ),
    );
    yield* serveMcp(port, app, "127.0.0.1").pipe(
      Effect.provideService(Scope.Scope, scope),
    );
    const url = `http://127.0.0.1:${port}`;
    yield* HttpClient.get(url).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.provide(NodeHttpClient.layer),
    );
    yield* Scope.close(scope, Exit.void).pipe(Effect.timeout("2 seconds"));
    expect(
      yield* HttpClient.get(url).pipe(
        Effect.provide(NodeHttpClient.layer),
        Effect.flip,
      ),
    ).toBeInstanceOf(HttpClientError.RequestError);
  }));
