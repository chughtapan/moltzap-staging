/** @file The daemon admits owner-bound JWTs verified against a real issuer key-set endpoint, and MCP clients discover the tools of their agent's role and read each call's result or failure. */

import {
  HttpClient,
  HttpClientError,
  HttpClientResponse,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpClient, NodeHttpServer } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Ajv } from "ajv";
import {
  Deferred,
  Effect,
  Exit,
  FastCheck as fc,
  Fiber,
  Queue,
  Schema,
  Scope,
  Stream,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { expect } from "vitest";
import {
  connectInProcessClient,
  mcpFixture,
  mcpRequest,
} from "../__tests__/mcp-fixtures.js";
import { AgentAddress, FirewallRefusal, SendError } from "./agent.js";
import { serveMcp } from "./server.js";

const response = Schema.parseJson(
  Schema.Struct({
    result: Schema.optional(
      Schema.Struct({
        isError: Schema.optional(Schema.Boolean),
        content: Schema.Array(Schema.Struct({ text: Schema.String })),
        structuredContent: Schema.optional(Schema.Unknown),
      }),
    ),
    error: Schema.optional(Schema.Unknown),
  }),
);
const json = (reply: Response) =>
  Effect.tryPromise(() => reply.text()).pipe(
    Effect.flatMap(Schema.decodeUnknown(response)),
  );
const bob = AgentAddress.make("agent:bob");
const send = {
  name: "send_message",
  arguments: { to: bob, text: "hello" },
};

const resource = new URL("https://mcp.example.test/mcp");
const owner = "owner-subject";
const scope = "use:mcp";

const admission = Effect.gen(function* () {
  const test = yield* mcpFixture({ resource });
  /** The subject of every owner with an agent. */
  const owners = test.sql<{
    subject: string;
  }>`SELECT subject FROM agent_cards ORDER BY subject`.pipe(
    Effect.map((rows) => rows.map((row) => row.subject)),
  );
  const token = (
    changes: {
      readonly audience?: string;
      readonly subject?: string;
      readonly granted?: readonly string[];
      readonly secondsLeft?: number;
    } = {},
  ) =>
    test.issuer.token(
      changes.subject ?? owner,
      changes.audience ?? resource.href,
      {
        scope: (changes.granted ?? [scope]).join(" "),
        secondsLeft: changes.secondsLeft ?? 300,
      },
    );
  const call = (bearer: string) =>
    Effect.tryPromise(() =>
      test.fetch(
        mcpRequest(
          "server/discover",
          {},
          {
            headers: {
              host: resource.host,
              authorization: `Bearer ${bearer}`,
            },
          },
        ),
      ),
    );
  return { ...test, owners, token, call };
});

/** A scope that begins with the configured one and goes on past it. */
const longerScope = fc
  .stringMatching(/^[a-z:]{1,3}$/u)
  .map((more) => `${scope}${more}`);

/** Claims the resource admits: any subject, and the configured scope anywhere among those granted. */
const admissible = fc.record({
  subject: fc.string({ minLength: 1 }),
  granted: fc
    .tuple(fc.array(longerScope), fc.array(longerScope))
    .map(([before, after]) => [...before, scope, ...after]),
});

/** One claim that puts a token outside the resource's authority. */
const defect = fc.oneof(
  fc.record({ subject: fc.constant("") }),
  fc.record({ audience: fc.webUrl().filter((url) => url !== resource.href) }),
  fc.record({ granted: fc.array(longerScope) }),
  fc.record({ secondsLeft: fc.integer({ min: -3600, max: 0 }) }),
);

it("answers a token granted the scope from the agent of its subject", () =>
  Effect.gen(function* () {
    const test = yield* admission;
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(admissible, (claims) =>
          Effect.gen(function* () {
            const response = yield* test.call(yield* test.token(claims));
            expect(response.status).toBe(200);
            expect(yield* test.owners).toContain(claims.subject);
          }).pipe(Effect.runPromise),
        ),
      ),
    );
  }));

it("refuses a token with an empty subject, another audience, no grant of the scope or no time left", () =>
  Effect.gen(function* () {
    const test = yield* admission;
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(defect, (claim) =>
          Effect.gen(function* () {
            const response = yield* test.call(yield* test.token(claim));
            expect(response.ok).toBe(false);
            expect(yield* test.owners).toEqual(["owner", "peer"]);
          }).pipe(Effect.runPromise),
        ),
      ),
    );
  }));

it(
  "closes an admitted stream at token expiry without closing later requests",
  () =>
    Effect.gen(function* () {
      const test = yield* admission;
      const token = yield* test.token({ secondsLeft: 2 });
      const response = yield* Effect.tryPromise(() =>
        test.fetch(
          mcpRequest(
            "subscriptions/listen",
            { notifications: { toolsListChanged: true } },
            {
              headers: {
                host: resource.host,
                authorization: `Bearer ${token}`,
              },
            },
          ),
        ),
      );
      expect(response.status).toBe(200);
      const body = yield* Effect.tryPromise(() => response.text());
      expect(body).toContain("notifications/subscriptions/acknowledged");
      const later = yield* test.call(yield* test.token());
      expect(later.status).toBe(200);
    }),
  15000,
);

type Fixture = Effect.Effect.Success<ReturnType<typeof mcpFixture>>;
/** Calls a tool on `fixture` and gives the reply's result. */
const toolCall = (
  fixture: Pick<Fixture, "fetch">,
  call: Readonly<Record<string, unknown>>,
) =>
  Effect.tryPromise(() => fixture.fetch(mcpRequest("tools/call", call))).pipe(
    Effect.flatMap(json),
    Effect.map((reply) => reply.result),
  );
const connect = (fixture: Pick<Fixture, "fetch">) =>
  connectInProcessClient((input, init) =>
    fixture.fetch(new Request(input, init)),
  );

it("gives the server agent its tools, and a status result that matches the output schema it lists", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture();
    const client = yield* connect(fixture);
    const { tools } = yield* Effect.tryPromise(() => client.listTools());
    expect(new Set(tools.map((tool) => tool.name))).toEqual(
      new Set([
        "list_conversations",
        "read_conversation",
        "read_firewall_decisions",
        "receive_messages",
        "send_message",
        "status",
      ]),
    );
    const reply = yield* Effect.tryPromise(() =>
      client.callTool({ name: "status", arguments: {} }),
    );
    const validate = new Ajv({
      strict: false,
      validateFormats: false,
      validateSchema: false,
    }).compile(
      tools.find((tool) => tool.name === "status")?.outputSchema ?? {},
    );
    expect(
      validate(reply.structuredContent),
      JSON.stringify(validate.errors),
    ).toBe(true);
    expect(reply.structuredContent).toEqual({
      agentId: fixture.card.agentId,
      agentName: fixture.card.agentName,
    });
    expect(reply.content).toEqual([
      { type: "text", text: JSON.stringify(reply.structuredContent) },
    ]);
  }));

it("gives a client tools that name no recipient and no conversation", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture({ client: true });
    const client = yield* connect(fixture);
    const { tools } = yield* Effect.tryPromise(() => client.listTools());
    expect(new Set(tools.map((tool) => tool.name))).toEqual(
      new Set([
        "read_conversation",
        "receive_messages",
        "send_message",
        "status",
      ]),
    );
    expect(
      Object.keys(
        tools.find((tool) => tool.name === "send_message")?.inputSchema
          .properties ?? {},
      ),
    ).toEqual(["text", "replyToMsgId"]);
    const sent = yield* Effect.tryPromise(() =>
      client.callTool({ name: "send_message", arguments: { text: "hello" } }),
    );
    expect(sent.structuredContent).toMatchObject({ address: bob });
    const history = yield* Effect.tryPromise(() =>
      client.callTool({ name: "read_conversation", arguments: {} }),
    );
    expect(history.structuredContent).toMatchObject({
      records: [
        {
          kind: "sent",
          message: {
            recipients: [{ agentName: "bob" }],
            content: [{ type: "text", text: "hello" }],
          },
        },
      ],
    });
  }));

it("refuses a call with a wrong or an unknown argument without running the tool", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture();
    for (const wrong of [{ text: 1 }, { extra_field: "x" }]) {
      const result = yield* toolCall(fixture, {
        ...send,
        arguments: { ...send.arguments, ...wrong },
      });
      expect(result?.isError).toBe(true);
      expect(result?.content[0]?.text).toContain(Object.keys(wrong)[0]);
    }
  }));

it("answers a failed call with the failure's message and its fields", () =>
  Effect.gen(function* () {
    const refused = yield* Queue.unbounded<FirewallRefusal>();
    const fixture = yield* mcpFixture({
      firewall: {
        checkOutgoing: () =>
          Effect.map(
            Queue.take(refused),
            ({ checkId, refusal, rule, feedback }) => ({
              kind: "refused" as const,
              checkId,
              reason: refusal,
              ...(rule === undefined ? {} : { rule }),
              feedback,
            }),
          ),
      },
    });
    const refusals = fc.record(
      {
        reason: fc.constant("firewall-refused" as const),
        checkId: fc.string({ minLength: 1 }),
        refusal: fc.constant("rule-violation" as const),
        rule: fc.string({ minLength: 1 }),
        feedback: fc.array(fc.string({ minLength: 1 })),
      },
      { requiredKeys: ["reason", "checkId", "refusal", "feedback"] },
    );
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(refusals, (fields) =>
          Effect.runPromise(
            Effect.gen(function* () {
              const failure = new FirewallRefusal(fields);
              yield* Queue.offer(refused, failure);
              expect(yield* toolCall(fixture, send)).toEqual({
                isError: true,
                content: [{ text: failure.message }],
                structuredContent: fields,
              });
            }),
          ),
        ),
        { numRuns: 25 },
      ),
    );
    for (const [to, fields] of [
      [
        "agent:nobody",
        {
          reason: "unknown-agent",
          detail:
            "This daemon has no agent named agent:nobody. Check the name with its owner.",
        },
      ],
      ["agent:alice", { reason: "membership-invalid" }],
    ] as const) {
      expect(
        yield* toolCall(fixture, { ...send, arguments: { to, text: "hello" } }),
      ).toEqual({
        isError: true,
        content: [{ text: new SendError(fields).message }],
        structuredContent: fields,
      });
    }
    expect(
      yield* toolCall(fixture, {
        name: "read_conversation",
        arguments: { address: "agent:nobody" },
      }),
    ).toEqual({
      isError: true,
      content: [
        {
          text: "reading failed: an agent the address names is not a known agent",
        },
      ],
      structuredContent: { reason: "unknown-agent" },
    });
    expect(
      yield* toolCall(fixture, {
        name: "read_conversation",
        arguments: { address: "agent:alice" },
      }),
    ).toMatchObject({
      isError: true,
      structuredContent: { reason: "invalid-address" },
    });
  }));

it("answers a call that dies without the defect's text, and a send that dies as one whose outcome is unknown", () =>
  Effect.gen(function* () {
    const dies = () => Effect.dieMessage("PRIVATE relation does not exist");
    const fixture = yield* mcpFixture({ firewall: { checkOutgoing: dies } });
    expect(yield* toolCall(fixture, send)).toEqual({
      isError: true,
      content: [{ text: new SendError({ reason: "outcome-unknown" }).message }],
      structuredContent: { reason: "outcome-unknown" },
    });
    yield* fixture.sql`DROP TABLE firewall_checks`;
    const result = yield* toolCall(fixture, {
      name: "read_firewall_decisions",
      arguments: {},
    });
    expect(result?.isError).toBe(true);
    expect(result?.structuredContent).toBeUndefined();
    expect(JSON.stringify(result)).not.toContain("firewall_checks");
  }));

it("interrupts only the aborted HTTP request", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const interrupted = yield* Deferred.make<undefined>();
    const fixture = yield* mcpFixture({
      firewall: {
        checkOutgoing: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
          ),
      },
    });
    const abort = new AbortController();
    const waiting = yield* Effect.tryPromise(() =>
      fixture.fetch(
        mcpRequest("tools/call", send, {
          id: "waiting",
          signal: abort.signal,
        }),
      ),
    ).pipe(Effect.either, Effect.forkScoped);
    yield* Deferred.await(entered);
    abort.abort();
    yield* Deferred.await(interrupted);
    yield* Fiber.join(waiting);
    const result = yield* toolCall(fixture, { name: "status", arguments: {} });
    expect(result?.isError).toBeUndefined();
  }));

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
