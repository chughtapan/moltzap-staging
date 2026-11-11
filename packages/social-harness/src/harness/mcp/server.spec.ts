/** @file MCP clients authenticate, discover tools and use independent cancellable connections. */

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
  Fiber,
  Redacted,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import * as fc from "fast-check";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { describe, expect } from "vitest";
import {
  connectInProcessClient,
  mcpFixture,
  mcpRequest,
  mcpTestCredential,
} from "../__tests__/mcp-fixtures.js";
import { digest } from "../../__tests__/agents.js";
import { SendError } from "../../communication/errors.js";
import { MessageAddressInput } from "../../communication/messages.js";
import { localOwner, serverAgentOwner } from "../ownership.js";
import { HistoryError } from "../social-harness.js";
import { MAXIMUM_WAIT_SECONDS, toolDefinitions } from "./contracts.js";
import { serveMcp } from "./http.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Wire codes, notification names and private sentinels are the observable protocol contracts. */

const response = Schema.parseJson(
  Schema.Struct({
    id: Schema.Unknown,
    result: Schema.optional(Schema.Unknown),
    error: Schema.optional(
      Schema.Struct({
        code: Schema.Number,
        message: Schema.String,
        data: Schema.optional(Schema.Unknown),
      }),
    ),
  }),
);
const json = (reply: Response) =>
  Effect.tryPromise(() => reply.text()).pipe(
    Effect.flatMap(Schema.decodeUnknown(response)),
  );
const send = {
  name: "send_message",
  arguments: { to: "agent:bob", text: "hello" },
};

it("publishes signed identity schemas accepted by native draft-07 and 2020 clients", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture();
    const client = yield* connectInProcessClient((input, init) =>
      fixture.fetch(new Request(input, init)),
    );
    const listed = yield* Effect.tryPromise(() => client.listTools());
    const status = listed.tools.find((tool) => tool.name === "status");
    const reply = yield* Effect.tryPromise(() =>
      client.callTool({ name: "status", arguments: {} }),
    );
    if (status?.outputSchema === undefined) {
      return yield* Effect.dieMessage(
        "Status must publish its output contract",
      );
    }
    const validate = new Ajv({
      strict: false,
      validateFormats: false,
      validateSchema: false,
    }).compile(status.outputSchema);
    expect(
      validate(reply.structuredContent),
      JSON.stringify(validate.errors),
    ).toBe(true);
    expect(
      yield* Schema.decodeUnknown(toolDefinitions.status.output)(
        reply.structuredContent,
      ),
    ).toBeDefined();
  }));

// @agent-code-guard/regression-only: These cases exercise finite MCP authentication and wire-error contracts.
describe("MCP authentication and discovery", () => {
  it.each([{ authorization: "" }, { authorization: "Bearer wrong" }])(
    "refuses an invalid bearer before invoking a service",
    (headers) =>
      Effect.gen(function* () {
        const fixture = yield* mcpFixture();
        const reply = yield* Effect.tryPromise(() =>
          fixture.fetch(mcpRequest("tools/list", {}, { headers })),
        );
        expect(reply.status).toBe(401);
        expect(reply.headers.get("www-authenticate")).toBe("Bearer");
      }),
  );

  const rejectedOrigins: ReadonlyArray<Readonly<Record<string, string>>> = [
    { host: "remote.example" },
    { origin: "https://remote.example" },
  ];
  it.each(rejectedOrigins)("rejects a remote browser boundary", (headers) =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture();
      expect(
        (yield* Effect.tryPromise(() =>
          fixture.fetch(mcpRequest("tools/list", {}, { headers })),
        )).status,
      ).toBe(403);
    }),
  );

  it("works with an unmodified stock MCP client", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture();
      const client = yield* connectInProcessClient((input, init) =>
        fixture.fetch(new Request(input, init)),
      );
      const listed = yield* Effect.tryPromise(() => client.listTools());
      expect(listed.tools.map((tool) => tool.name)).toContain(
        "receive_messages",
      );
      expect(
        listed.tools.find((tool) => tool.name === "receive_messages")
          ?.outputSchema,
      ).toBeDefined();
      const reply = yield* Effect.tryPromise(() =>
        client.callTool({ name: "receive_messages", arguments: {} }),
      );
      expect(
        yield* Schema.decodeUnknown(toolDefinitions.receive_messages.output)(
          reply.structuredContent,
        ),
      ).toEqual({ items: [] });
    }));
});

// @agent-code-guard/regression-only: These cases exercise finite MCP authentication and wire-error contracts.
describe("MCP tool contracts", () => {
  it.each(["a-string-id", 9_007_199_254_740_990])(
    "preserves request id %s",
    (id) =>
      Effect.gen(function* () {
        const fixture = yield* mcpFixture();
        const reply = yield* Effect.tryPromise(() =>
          fixture.fetch(
            mcpRequest(
              "tools/call",
              { name: "receive_messages", arguments: {} },
              { id },
            ),
          ),
        );
        expect((yield* json(reply)).id).toBe(id);
      }),
  );

  it("reports argument paths without submitting invalid work", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture();
      const reply = yield* Effect.tryPromise(() =>
        fixture.fetch(
          mcpRequest("tools/call", {
            name: "read_conversation",
            arguments: { address: "private-invalid-token" },
          }),
        ),
      );
      const error = (yield* json(reply)).error;
      expect(error?.code).toBe(-32602);
      expect(JSON.stringify(error?.data)).toContain("address");
      expect(JSON.stringify(error)).not.toContain("private-invalid-token");
    }));

  it("preserves a closed send refusal without exposing internal failures", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture({
        harness: {
          send: () =>
            Effect.fail(new SendError({ reason: "delivery-pending" })),
        },
      });
      const reply = yield* Effect.tryPromise(() =>
        fixture.fetch(mcpRequest("tools/call", send)),
      );
      expect((yield* json(reply)).error?.data).toEqual({
        reason: "delivery-pending",
      });
    }));

  it("classifies an interrupted or defective send as uncertain", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture({
        harness: { send: () => Effect.dieMessage("private-stack-detail") },
      });
      const reply = yield* Effect.tryPromise(() =>
        fixture.fetch(mcpRequest("tools/call", send)),
      );
      const error = (yield* json(reply)).error;
      expect(error?.data).toEqual({ reason: "outcome-unknown" });
      expect(JSON.stringify(error)).not.toContain("private-stack-detail");
    }));

  it("publishes one receipt shape through a stock client", () =>
    Effect.gen(function* () {
      const receipt = yield* Schema.decodeUnknown(
        toolDefinitions.send_message.output,
      )({ id: digest("msg_", 1), address: "agent:bob" });
      const fixture = yield* mcpFixture({
        harness: { send: () => Effect.succeed(receipt) },
      });
      const client = yield* connectInProcessClient((input, init) =>
        fixture.fetch(new Request(input, init)),
      );
      const reply = yield* Effect.tryPromise(() => client.callTool(send));
      expect(reply.structuredContent).toEqual(receipt);
    }));

  it("interrupts only the aborted HTTP request", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<undefined>();
      const interrupted = yield* Deferred.make<undefined>();
      const fixture = yield* mcpFixture({
        harness: {
          listInbox: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.zipRight(Effect.never),
              Effect.onInterrupt(() =>
                Deferred.succeed(interrupted, undefined),
              ),
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
      const reply = yield* Effect.tryPromise(() =>
        fixture.fetch(
          mcpRequest(
            "tools/call",
            { name: "status", arguments: {} },
            { id: "unrelated" },
          ),
        ),
      );
      expect((yield* json(reply)).error).toBeUndefined();
    }));
});

it("ends an in-flight request and refuses later calls when its agent stops", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const fixture = yield* mcpFixture({
      harness: {
        send: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Effect.never),
          ),
      },
    });
    const waiting = yield* Effect.tryPromise(() =>
      fixture.fetch(mcpRequest("tools/call", send)),
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Deferred.succeed(fixture.stopped, undefined);
    expect((yield* Fiber.join(waiting)).status).toBe(503);
    const later = yield* Effect.tryPromise(() =>
      fixture.fetch(mcpRequest("tools/list")),
    );
    expect(later.status).toBe(503);
  }));

it("closes a retained stream when its agent stops", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture();
    const reply = yield* Effect.tryPromise(() =>
      fixture.fetch(
        mcpRequest("subscriptions/listen", {
          notifications: { toolsListChanged: true },
        }),
      ),
    );
    expect(reply.status).toBe(200);
    if (reply.body === null) {
      return yield* Effect.dieMessage("Expected a stream body");
    }
    const reader = reply.body.getReader();
    const next = Effect.tryPromise(() => reader.read());
    expect((yield* next).done).toBe(false);
    yield* Deferred.succeed(fixture.stopped, undefined);
    const ended = yield* Effect.iterate(yield* next, {
      while: (chunk) => !chunk.done,
      body: () => next,
    });
    expect(ended.done).toBe(true);
  }));

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore the default assertion policy. */

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
      yield* serveMcp(port, Effect.succeed(HttpServerResponse.empty())).pipe(
        Effect.flip,
      ),
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
    yield* serveMcp(port, app).pipe(Effect.provideService(Scope.Scope, scope));
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

const serverAgent = MessageAddressInput.make("agent:bob");
const sendReceipt = Schema.decodeUnknownSync(
  toolDefinitions.send_message.output,
)({ id: digest("msg_", 1), address: serverAgent });
const clientText = "hello";
const clientTools = [
  "read_conversation",
  "receive_messages",
  "send_message",
  "status",
];
const sendTool = "send_message";
const receiveTool = "receive_messages";
const missingField = "text";
const collectiveField = "collective";
const textFields = ["to", missingField];
const sendFields = ["to", missingField, "collective", "collectiveResponse"];

describe("send_message arguments", () => {
  const refusal = (sent: Readonly<Record<string, unknown>>) =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture();
      const reply = yield* Effect.tryPromise(() =>
        fixture.fetch(
          mcpRequest("tools/call", { name: "send_message", arguments: sent }),
        ),
      );
      return JSON.stringify((yield* json(reply)).error?.data);
    });

  it("names text when a send has neither text nor a collective response", () =>
    Effect.gen(function* () {
      expect(yield* refusal({ to: "agent:bob" })).toContain(missingField);
    }));

  it("offers only a recipient and text when collectives are turned off", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture({ hideCollectives: true });
      const client = yield* connectInProcessClient((input, init) =>
        fixture.fetch(new Request(input, init)),
      );
      const { tools } = yield* Effect.tryPromise(() => client.listTools());
      const send = tools.find((tool) => tool.name === sendTool);
      expect(Object.keys(send?.inputSchema.properties ?? {})).toEqual(
        textFields,
      );
      const reply = yield* Effect.tryPromise(() =>
        fixture.fetch(
          mcpRequest("tools/call", {
            name: sendTool,
            arguments: { to: "agent:bob", text: "hello", collective: {} },
          }),
        ),
      );
      expect(JSON.stringify((yield* json(reply)).error?.data)).toContain(
        collectiveField,
      );
    }));

  it("refuses a receive_messages wait longer than the limit, naming the limit", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture();
      const reply = yield* Effect.tryPromise(() =>
        fixture.fetch(
          mcpRequest("tools/call", {
            name: receiveTool,
            arguments: { wait: MAXIMUM_WAIT_SECONDS + 1 },
          }),
        ),
      );
      expect(JSON.stringify((yield* json(reply)).error?.data)).toContain(
        `at most ${String(MAXIMUM_WAIT_SECONDS)}`,
      );
    }));

  it("gives a client in client/server mode tools that name no recipient", () =>
    Effect.gen(function* () {
      const sent: unknown[] = [];
      const read: unknown[] = [];
      const fixture = yield* mcpFixture({
        serverAgent,
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
      const client = yield* connectInProcessClient((input, init) =>
        fixture.fetch(new Request(input, init)),
      );
      const { tools } = yield* Effect.tryPromise(() => client.listTools());
      expect(new Set(tools.map((tool) => tool.name))).toEqual(
        new Set(clientTools),
      );
      const send = tools.find((tool) => tool.name === sendTool);
      expect(Object.keys(send?.inputSchema.properties ?? {})).toEqual([
        missingField,
      ]);
      yield* Effect.tryPromise(() =>
        client.callTool({ name: sendTool, arguments: { text: clientText } }),
      );
      yield* Effect.tryPromise(() =>
        client.callTool({ name: "read_conversation", arguments: {} }),
      );
      expect(sent).toEqual([{ to: serverAgent, text: clientText }]);
      expect(read).toEqual([{ address: serverAgent }]);
    }));

  it("names any field it does not know", () =>
    Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(
          fc
            .stringMatching(/^[a-z]{4,12}$/u)
            .filter((name) => !sendFields.includes(name)),
          (name) =>
            refusal({ to: "agent:bob", text: "hello", [name]: true }).pipe(
              Effect.tap((data) =>
                Effect.sync(() => {
                  expect(data).toContain(name);
                }),
              ),
              Effect.asVoid,
              Effect.scoped,
              Effect.runPromise,
            ),
        ),
        { numRuns: 20 },
      ),
    ));
});

describe("a tool that fails", () => {
  it("answers with its error's own message and reason", () =>
    Effect.gen(function* () {
      const failure = new HistoryError({ reason: "unknown-agent" });
      const fixture = yield* mcpFixture({
        harness: { readHistory: () => Effect.fail(failure) },
      });
      const reply = yield* Effect.tryPromise(() =>
        fixture.fetch(
          mcpRequest("tools/call", {
            name: "read_conversation",
            arguments: { address: serverAgent },
          }),
        ),
      );
      expect((yield* json(reply)).error).toMatchObject({
        message: failure.message,
        data: { reason: failure.reason },
      });
    }));
});

describe("the server agent's credential", () => {
  const credential = Redacted.make("server-agent-credential-000000000001");
  const withBearer = (bearer: string) =>
    mcpRequest(
      "tools/list",
      {},
      { headers: { authorization: `Bearer ${bearer}` } },
    );

  it("admits its bearer as the server agent and leaves other bearers to the configured authentication", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture({ serverAgentCredential: credential });
      const asServerAgent = yield* Effect.tryPromise(() =>
        fixture.fetch(withBearer(Redacted.value(credential))),
      );
      const asLocalOwner = yield* Effect.tryPromise(() =>
        fixture.fetch(withBearer(Redacted.value(mcpTestCredential))),
      );
      const refused = yield* Effect.tryPromise(() =>
        fixture.fetch(withBearer(`${Redacted.value(credential)}x`)),
      );
      expect([asServerAgent.ok, asLocalOwner.ok, refused.ok]).toEqual([
        true,
        true,
        false,
      ]);
      expect(yield* Ref.get(fixture.owners)).toEqual([
        serverAgentOwner,
        localOwner,
      ]);
    }));
});
