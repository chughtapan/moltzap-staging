/** @file MCP clients discover the tools of their agent's role and read each call's result or failure. */

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
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { expect } from "vitest";
import { digest, testAgentCard } from "../__tests__/agents.js";
import { FirewallRefusal } from "../firewall/index.js";
import {
  connectInProcessClient,
  mcpFixture,
  mcpRequest,
} from "./__tests__/mcp-fixtures.js";
import { serveMcp } from "./listener.js";
import { AgentAddress, HistoryError, SendError, SendResult } from "./values.js";

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
const serverAgent = AgentAddress.make("agent:bob");
const send = {
  name: "send_message",
  arguments: { to: serverAgent, text: "hello" },
};
const read = {
  name: "read_conversation",
  arguments: { address: serverAgent },
};
const sendReceipt = Schema.decodeUnknownSync(SendResult)({
  id: digest("msg_", 1),
  address: serverAgent,
});

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
    const sent: unknown[] = [];
    const read: unknown[] = [];
    const fixture = yield* mcpFixture({
      serverAgentName: testAgentCard("bob").agentName,
      harness: {
        send: (request) =>
          Effect.sync(() => {
            sent.push(request);
            return sendReceipt;
          }),
        readHistory: (request) =>
          Effect.sync(() => {
            read.push(request);
            return { records: [] };
          }),
      },
    });
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
    yield* Effect.tryPromise(() =>
      client.callTool({ name: "send_message", arguments: { text: "hello" } }),
    );
    yield* Effect.tryPromise(() =>
      client.callTool({ name: "read_conversation", arguments: {} }),
    );
    expect(sent).toEqual([{ to: serverAgent, text: "hello" }]);
    expect(read).toEqual([{ address: serverAgent }]);
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
    const failing = yield* Ref.make<
      Effect.Effect<never, SendError | FirewallRefusal>
    >(Effect.never);
    const history = new HistoryError({ reason: "unknown-agent" });
    const fixture = yield* mcpFixture({
      harness: {
        send: () => Effect.flatten(Ref.get(failing)),
        readHistory: () => Effect.fail(history),
      },
    });
    const sendErrors = fc
      .record(
        {
          reason: fc.constantFrom(
            "unknown-agent" as const,
            "content-invalid" as const,
          ),
          detail: fc.string(),
        },
        { requiredKeys: ["reason"] },
      )
      .map((fields) => ({ fields, failure: new SendError(fields) }));
    const refusals = fc
      .record(
        {
          reason: fc.constant("firewall-refused" as const),
          checkId: fc.string({ minLength: 1 }),
          refusal: fc.constant("rule-violation" as const),
          rule: fc.string({ minLength: 1 }),
          feedback: fc.array(fc.string({ minLength: 1 })),
        },
        { requiredKeys: ["reason", "checkId", "refusal", "feedback"] },
      )
      .map((fields) => ({ fields, failure: new FirewallRefusal(fields) }));
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(
          fc.oneof(sendErrors, refusals),
          ({ fields, failure }) =>
            Effect.runPromise(
              Effect.gen(function* () {
                yield* Ref.set(failing, Effect.fail(failure));
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
    expect(yield* toolCall(fixture, read)).toEqual({
      isError: true,
      content: [{ text: history.message }],
      structuredContent: { reason: "unknown-agent" },
    });
  }));

it("answers a call that dies without the defect's text, and a send that dies as one whose outcome is unknown", () =>
  Effect.gen(function* () {
    const dies = () => Effect.dieMessage("PRIVATE relation does not exist");
    const fixture = yield* mcpFixture({
      harness: { send: dies, readHistory: dies },
    });
    expect(yield* toolCall(fixture, send)).toEqual({
      isError: true,
      content: [{ text: new SendError({ reason: "outcome-unknown" }).message }],
      structuredContent: { reason: "outcome-unknown" },
    });
    const result = yield* toolCall(fixture, read);
    expect(result?.isError).toBe(true);
    expect(result?.structuredContent).toBeUndefined();
    expect(JSON.stringify(result)).not.toContain("PRIVATE");
  }));

it("interrupts only the aborted HTTP request", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const interrupted = yield* Deferred.make<undefined>();
    const fixture = yield* mcpFixture({
      harness: {
        receive: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
          ),
      },
    });
    const abort = new AbortController();
    const waiting = yield* Effect.tryPromise(() =>
      fixture.fetch(
        mcpRequest(
          "tools/call",
          { name: "receive_messages", arguments: {} },
          { id: "waiting", signal: abort.signal },
        ),
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
