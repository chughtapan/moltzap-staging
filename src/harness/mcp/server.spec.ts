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
  FastCheck as fc,
  Fiber,
  Record as Rec,
  Redacted,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { describe, expect } from "vitest";
import {
  connectInProcessClient,
  mcpFixture,
  mcpRequest,
  mcpTestCredential,
} from "../__tests__/mcp-fixtures.js";
import { digest, testAgentCard } from "../../__tests__/agents.js";
import { collectiveOperationId } from "../../communication/__tests__/request-ids.js";
import { SendError, sendFailureReasons } from "../../communication/errors.js";
import { MessageAddressInput } from "../../communication/messages.js";
import { localOwner, serverAgentOwner } from "../ownership.js";
import { HistoryError } from "../social-harness.js";
import { MAXIMUM_WAIT_SECONDS, toolDefinitions } from "./contracts.js";
import { serveMcp } from "./http.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Wire codes, notification names and private sentinels are the observable protocol contracts. */

const response = Schema.parseJson(
  Schema.Struct({
    id: Schema.Unknown,
    result: Schema.optional(
      Schema.Struct({ structuredContent: Schema.optional(Schema.Unknown) }),
    ),
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
const read = {
  name: "read_conversation",
  arguments: { address: "agent:bob" },
};
const serverAgent = MessageAddressInput.make("agent:bob");
const sendReceipt = Schema.decodeUnknownSync(
  toolDefinitions.send_message.output,
)({ id: digest("msg_", 1), address: serverAgent });

type Fixture = Effect.Effect.Success<ReturnType<typeof mcpFixture>>;
/** Sends one request to `fixture`. */
const exchange = (
  fixture: Pick<Fixture, "fetch">,
  ...request: Parameters<typeof mcpRequest>
) => Effect.tryPromise(() => fixture.fetch(mcpRequest(...request)));
/** Calls a tool on `fixture` and decodes the reply. */
const toolCall = (
  fixture: Pick<Fixture, "fetch">,
  call: Readonly<Record<string, unknown>>,
  id: string | number = 1,
) => exchange(fixture, "tools/call", call, { id }).pipe(Effect.flatMap(json));

/** Asserts `rule` for all generated `inputs`. The rule reads a fixture that the test around it built once. */
const forAll = <Ts extends [unknown, ...unknown[]]>(
  inputs: { [K in keyof Ts]: fc.Arbitrary<Ts[K]> },
  rule: (...values: Ts) => Effect.Effect<void, unknown>,
) =>
  Effect.tryPromise(() =>
    fc.assert(
      fc.asyncProperty<Ts>(...inputs, (...values) =>
        Effect.runPromise(rule(...values)),
      ),
    ),
  );
/** A value no argument accepts. A reply that contains its first word has repeated what was sent. */
const unacceptable = fc
  .stringMatching(/^[A-Z]{8}$/u)
  .map((letters) => `PRIVATE${letters}`);
/** Mostly `usual`, so that input made of several parts is often acceptable in all of them. */
const mostly = <A>(usual: fc.Arbitrary<A>, other: fc.Arbitrary<A>) =>
  fc.oneof({ arbitrary: usual, weight: 4 }, { arbitrary: other, weight: 1 });

it("reports the agent's identifier and name in a result native draft-07 and 2020 clients accept", () =>
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
    ).toMatchObject({
      agentId: fixture.card.agentId,
      agentName: fixture.card.agentName,
    });
  }));

const serverAgentCredential = "server-agent-credential-000000000001";
/** Whom each credential admits a request as. */
const owners = new Map([
  [Redacted.value(mcpTestCredential), localOwner],
  [serverAgentCredential, serverAgentOwner],
]);
/** How a Host header, then an Origin header, names the machine the daemon runs on. */
const localHost = /^(127\.0\.0\.1|localhost|\[::1\])(:\d+)?$/u;
const localOrigin = /^https?:\/\/(127\.0\.0\.1|localhost|\[::1\])(:\d+)?$/u;
const hosts = mostly(
  fc.constantFrom("127.0.0.1", "localhost:8080", "[::1]"),
  fc.oneof(fc.domain(), fc.ipV4()),
);
/** The Origin header of a request, which a request need not carry. */
const origins = fc.record(
  {
    origin: fc
      .tuple(
        mostly(fc.constantFrom("http", "https"), fc.constantFrom("ws", "ftp")),
        mostly(fc.constant("://"), fc.constant("://user@")),
        hosts,
        mostly(fc.constant(""), fc.constant("#part")),
      )
      .map((parts) => parts.join("")),
  },
  { requiredKeys: [] },
);
const bearerSchemes = mostly(
  fc.constantFrom("Bearer ", "bearer   ", "BEARER "),
  fc.constantFrom("Basic ", "Bearer", ""),
);
const tokens = fc
  .constantFrom(...owners.keys())
  .chain((known) =>
    mostly(fc.constant(known), fc.constantFrom(`${known}x`, known.slice(1))),
  );

it("answers 403 unless Host and Origin name this machine over plain http(s), then 401 unless the bearer is a credential, and otherwise serves that credential's owner", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture({
      serverAgentCredential: Redacted.make(serverAgentCredential),
    });
    yield* forAll(
      [hosts, origins, bearerSchemes, tokens],
      Effect.fnUntraced(function* (host, from, scheme, token) {
        const headers = { host, authorization: `${scheme}${token}`, ...from };
        const reply = yield* exchange(fixture, "tools/list", {}, { headers });
        const local =
          localHost.test(host) &&
          (from.origin === undefined || localOrigin.test(from.origin));
        const owner = /^bearer +$/iu.test(scheme)
          ? owners.get(token)
          : undefined;
        const refusal = local ? 401 : 403;
        const status = local && owner !== undefined ? 200 : refusal;
        expect(reply.status).toBe(status);
        expect(reply.headers.get("www-authenticate") === "Bearer").toBe(
          status === 401,
        );
        expect(yield* Ref.getAndSet(fixture.owners, [])).toEqual(
          status === 200 ? [owner] : [],
        );
      }),
    );
  }));

const slotForm = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
};
const filledForm = fc.constant({ slot: "mon" });
const operationId = collectiveOperationId(testAgentCard("agent:bob").agentId);
const thirtyDays = 30 * 24 * 60 * 60;
/** Seconds above `min`, up to `max`. */
const seconds = (min: number, max: number) =>
  fc.double({ min, max, minExcluded: true, noNaN: true });

/** The values one argument accepts and the values it refuses. An `absent` value leaves the argument out. */
interface Argument {
  readonly accepts: fc.Arbitrary<unknown>;
  readonly refuses: fc.Arbitrary<unknown>;
}
/** The arguments a call takes, each by its dotted path. */
type Arguments = Readonly<Record<string, Argument>>;
const absent = fc.constant(undefined);
const absentOrUnacceptable = fc.oneof(absent, unacceptable);

const recipient: Argument = {
  accepts: fc.stringMatching(/^agent:[a-z0-9]{3,8}$/u),
  refuses: absentOrUnacceptable,
};
const text: Argument = {
  accepts: fc.string(),
  refuses: fc.oneof(absent, fc.integer()),
};
/** A wait is optional, longer than no time and no longer than the limit, which is itself accepted. */
const wait: Argument = {
  accepts: fc.oneof(
    absent,
    fc.constant(MAXIMUM_WAIT_SECONDS),
    seconds(0, MAXIMUM_WAIT_SECONDS),
  ),
  refuses: fc.oneof(
    fc.constant(0),
    seconds(-MAXIMUM_WAIT_SECONDS, 0),
    seconds(MAXIMUM_WAIT_SECONDS, 2 * MAXIMUM_WAIT_SECONDS),
  ),
};
/**
 * What makes a message a question. Its deadline is whole seconds, from 1 to
 * thirty days: the refused ones are the nearest whole numbers outside that
 * range and a fraction inside it.
 */
const question: Arguments = {
  "collective.op": {
    accepts: fc.constantFrom("gather", "all_gather"),
    refuses: unacceptable,
  },
  "collective.deadline": {
    accepts: fc.integer({ min: 1, max: thirtyDays }),
    refuses: fc.oneof(
      absent,
      fc.constant(0),
      fc.constant(thirtyDays + 1),
      fc.integer({ min: 1, max: thirtyDays - 1 }).map((whole) => whole + 0.5),
    ),
  },
  "collective.requestedSchema": {
    accepts: fc.constant(slotForm),
    refuses: absentOrUnacceptable,
  },
};
/** An answer to a question takes no text. `content` says whether its action carries a filled form. */
const answer = (action: string, content: Argument): Arguments => ({
  to: recipient,
  text: { accepts: absent, refuses: fc.string() },
  "collectiveResponse.action": {
    accepts: fc.constant(action),
    refuses: unacceptable,
  },
  "collectiveResponse.operationId": {
    accepts: fc.constant(operationId),
    refuses: absentOrUnacceptable,
  },
  "collectiveResponse.content": content,
});
/** Each kind of call a peer makes with arguments. */
const calls: ReadonlyArray<{ name: string; takes: Arguments }> = [
  { name: "read_conversation", takes: { address: recipient } },
  { name: "receive_messages", takes: { wait } },
  { name: "send_message", takes: { to: recipient, text } },
  { name: "send_message", takes: { to: recipient, text, ...question } },
  {
    name: "send_message",
    takes: answer("accept", { accepts: filledForm, refuses: absent }),
  },
  {
    name: "send_message",
    takes: answer("decline", { accepts: absent, refuses: filledForm }),
  },
];
/** A call of any kind, with one value each of its arguments accepts and one value each refuses. */
const toolCalls = fc.constantFrom(...calls).chain(({ name, takes }) =>
  fc.record({
    name: fc.constant(name),
    accepted: fc.record(Rec.map(takes, (argument) => argument.accepts)),
    refused: fc.record(Rec.map(takes, (argument) => argument.refuses)),
  }),
);
/** The arguments that dotted paths stand for: `{ "a.b": 1, c: 2 }` is `{ a: { b: 1 }, c: 2 }`. */
const nested = (flat: Readonly<Record<string, unknown>>) => {
  const whole: Record<string, unknown> = {};
  const groups: Record<string, Record<string, unknown>> = {};
  for (const [path, value] of Object.entries(flat)) {
    const [head = path, leaf] = path.split(".");
    if (leaf === undefined) {
      whole[head] = value;
    } else {
      groups[head] = { ...groups[head], [leaf]: value };
      whole[head] = groups[head];
    }
  }
  return whole;
};

it("runs a call when every argument holds a value its tool accepts, and refuses it when any one argument does not, naming that argument without repeating its value", () =>
  Effect.gen(function* () {
    const returned = yield* Ref.make<readonly unknown[]>([]);
    const returns =
      <A>(result: A) =>
      () =>
        Ref.update(returned, (all) => [...all, result]).pipe(Effect.as(result));
    const fixture = yield* mcpFixture({
      harness: {
        send: returns(sendReceipt),
        listInbox: returns({ items: [] }),
        readHistory: returns({ records: [] }),
      },
    });
    yield* forAll(
      [toolCalls],
      Effect.fnUntraced(function* ({ name, accepted, refused }) {
        const reply = yield* toolCall(fixture, {
          name,
          arguments: nested(accepted),
        });
        expect(yield* Ref.getAndSet(returned, [])).toEqual([
          reply.result?.structuredContent,
        ]);
        for (const wrong of Object.keys(refused)) {
          const refusal = yield* toolCall(fixture, {
            name,
            arguments: nested({ ...accepted, [wrong]: refused[wrong] }),
          });
          expect(refusal.error?.code).toBe(-32602);
          expect(refusal.error?.message).toContain(`${wrong}: `);
          expect(yield* Ref.getAndSet(returned, [])).toEqual([]);
          expect(JSON.stringify(refusal)).not.toContain("PRIVATE");
        }
      }),
    );
  }));

/** A call each tool with no required argument accepts. */
const argumentless = ["status", "receive_messages", "list_conversations"].map(
  (name) => ({ name, arguments: {} }),
);

it("answers a call under the id it was sent with, whether or not the call succeeds", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture();
    yield* forAll(
      [
        fc.constantFrom(send, read, ...argumentless),
        fc.oneof(fc.string(), fc.maxSafeInteger()),
      ],
      Effect.fnUntraced(function* (call, id) {
        expect((yield* toolCall(fixture, call, id)).id).toBe(id);
      }),
    );
  }));

/** A field name no tool takes: none of theirs has an underscore. */
const unknownField = fc.stringMatching(/^[a-z]{3,8}_[a-z]{1,4}$/u);

it("refuses a call with a field its tool does not take, naming the field without repeating its value", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture();
    yield* forAll(
      [
        fc.constantFrom(send, read, ...argumentless),
        unknownField,
        unacceptable,
      ],
      Effect.fnUntraced(function* (call, name, value) {
        const reply = yield* toolCall(fixture, {
          ...call,
          arguments: { ...call.arguments, [name]: value },
        });
        expect(reply.error?.data).toEqual({
          issues: [{ path: [name], message: "Unexpected field" }],
        });
        expect(JSON.stringify(reply)).not.toContain("PRIVATE");
      }),
    );
  }));

describe("MCP tool contracts", () => {
  it("answers a send that fails with the failure's own message, reason and detail, and a send that dies as one whose outcome is unknown", () =>
    Effect.gen(function* () {
      const sending = yield* Ref.make<Effect.Effect<never, SendError>>(
        Effect.never,
      );
      const fixture = yield* mcpFixture({
        harness: { send: () => Effect.flatten(Ref.get(sending)) },
      });
      const refused = fc.record(
        { reason: fc.constantFrom(...sendFailureReasons), detail: fc.string() },
        { requiredKeys: ["reason"] },
      );
      const sendEndings = fc.oneof(
        refused.map((data) => ({
          data,
          ending: Effect.fail(new SendError(data)),
        })),
        unacceptable.map((defect) => ({
          data: { reason: "outcome-unknown" } as const,
          ending: Effect.dieMessage(defect),
        })),
      );
      yield* forAll(
        [sendEndings],
        Effect.fnUntraced(function* ({ data, ending }) {
          yield* Ref.set(sending, ending);
          expect((yield* toolCall(fixture, send)).error).toEqual({
            code: -32603,
            message: new SendError(data).message,
            data,
          });
        }),
      );
    }));

  it("answers a read that fails with the failure's own message and reason", () =>
    Effect.gen(function* () {
      const reading = yield* Ref.make(
        new HistoryError({ reason: "invalid-cursor" }),
      );
      const fixture = yield* mcpFixture({
        harness: { readHistory: () => Effect.flip(Ref.get(reading)) },
      });
      yield* forAll(
        [fc.constantFrom(...HistoryError.fields.reason.literals)],
        Effect.fnUntraced(function* (reason) {
          const failure = new HistoryError({ reason });
          yield* Ref.set(reading, failure);
          expect((yield* toolCall(fixture, read)).error).toEqual({
            code: -32603,
            message: failure.message,
            data: { reason },
          });
        }),
      );
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

const clientText = "hello";
const clientTools = [
  "read_conversation",
  "receive_messages",
  "send_message",
  "status",
];
const sendTool = "send_message";
const missingField = "text";
const collectiveField = "collective";
const textFields = ["to", missingField];

describe("send_message arguments", () => {
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
});
