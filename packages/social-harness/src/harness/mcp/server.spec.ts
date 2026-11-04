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
import { Deferred, Effect, Exit, Fiber, Schema, Scope, Stream } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { describe, expect } from "vitest";
import {
  connectInProcessClient,
  mcpFixture,
  mcpRequest,
} from "../__tests__/mcp-fixtures.js";
import { digest } from "../../__tests__/agents.js";
import { SendError } from "../../communication/errors.js";
import { INBOX_PENDING_EVENT, toolDefinitions } from "./contracts.js";
import { serveMcp } from "./server.js";

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
  arguments: { input: { to: "agent:bob", text: "hello" } },
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
      expect(listed.tools.map((tool) => tool.name)).toContain("read_inbox");
      expect(
        listed.tools.find((tool) => tool.name === "read_inbox")?.outputSchema,
      ).toBeDefined();
      const reply = yield* Effect.tryPromise(() =>
        client.callTool({ name: "read_inbox", arguments: {} }),
      );
      expect(
        yield* Schema.decodeUnknown(toolDefinitions.read_inbox.output)(
          reply.structuredContent,
        ),
      ).toEqual({ items: [] });
    }));

  it("hides active tools until registration is retained", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture({ registered: false });
      const client = yield* connectInProcessClient((input, init) =>
        fixture.fetch(new Request(input, init)),
      );
      const initial = yield* Effect.tryPromise(() => client.listTools());
      expect(initial.tools.map((tool) => tool.name)).toEqual([
        "status",
        "register",
      ]);
      yield* Effect.tryPromise(() =>
        client.callTool({
          name: "register",
          arguments: {
            operationId: `opn_${"A".repeat(22)}`,
            principalId: fixture.card.principalId,
            agentName: fixture.card.agentName,
          },
        }),
      );
      expect(
        (yield* Effect.tryPromise(() => client.listTools())).tools.map(
          (tool) => tool.name,
        ),
      ).toContain("send_message");
    }));
  it("delivers registration tool changes through a stock notification subscription", () =>
    Effect.gen(function* () {
      const fixture = yield* mcpFixture({ registered: false });
      const client = yield* connectInProcessClient((input, init) =>
        fixture.fetch(new Request(input, init)),
      );
      const changed = yield* Deferred.make<undefined>();
      client.setNotificationHandler("notifications/tools/list_changed", () => {
        Effect.runFork(Deferred.succeed(changed, undefined));
      });
      const subscription = yield* Effect.acquireRelease(
        Effect.tryPromise(() => client.listen({ toolsListChanged: true })),
        (listening) =>
          Effect.tryPromise(() => listening.close()).pipe(Effect.ignore),
      );
      expect(subscription.honoredFilter.toolsListChanged).toBe(true);
      yield* Effect.tryPromise(() =>
        client.callTool({
          name: "register",
          arguments: {
            operationId: `opn_${"A".repeat(22)}`,
            principalId: fixture.card.principalId,
            agentName: fixture.card.agentName,
          },
        }),
      );
      yield* Deferred.await(changed).pipe(Effect.timeout("2 seconds"));
      expect(
        (yield* Effect.tryPromise(() => client.listTools())).tools.map(
          (tool) => tool.name,
        ),
      ).toContain("send_message");
      yield* Effect.tryPromise(() => subscription.close());
      expect(yield* Effect.tryPromise(() => subscription.closed)).toBe("local");
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
              { name: "read_inbox", arguments: {} },
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
            name: "read_event",
            arguments: { eventId: "private-invalid-token" },
          }),
        ),
      );
      const error = (yield* json(reply)).error;
      expect(error?.code).toBe(-32602);
      expect(JSON.stringify(error?.data)).toContain("eventId");
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
            { name: "read_inbox", arguments: {} },
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

it("closes a retained Events response body when its agent stops", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture();
    const reply = yield* Effect.tryPromise(() =>
      fixture.fetch(
        mcpRequest("events/stream", {
          name: INBOX_PENDING_EVENT,
          arguments: {},
        }),
      ),
    );
    expect(reply.status).toBe(200);
    if (reply.body === null) {
      return yield* Effect.dieMessage("Expected an Events response body");
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
