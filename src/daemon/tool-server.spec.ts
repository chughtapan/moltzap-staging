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
  Struct,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { describe, expect } from "vitest";
import { digest, testAgentCard } from "../__tests__/agents.js";
import {
  HistoryError,
  MAXIMUM_WAIT_SECONDS,
  MessageAddressInput,
  SendError,
  toolDefinitions,
} from "../mcp/tools.js";
import {
  connectInProcessClient,
  mcpFixture,
  mcpRequest,
} from "./__tests__/mcp-fixtures.js";
import { serveMcp } from "./listener.js";

const response = Schema.parseJson(
  Schema.Struct({
    id: Schema.Unknown,
    result: Schema.optional(
      Schema.Struct({
        isError: Schema.optional(Schema.Boolean),
        content: Schema.optional(
          Schema.Array(Schema.Struct({ text: Schema.String })),
        ),
        structuredContent: Schema.optional(Schema.Unknown),
      }),
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
/** Every reason a send fails with; the type refuses a missing or an unknown one. */
const sendFailureReasons = Struct.keys({
  "unknown-agent": true,
  "membership-invalid": true,
  "content-invalid": true,
  "message-conflict": true,
  "network-unavailable": true,
  "outcome-unknown": true,
} satisfies Record<SendError["reason"], true>);
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

const bearerSchemes = mostly(
  fc.constantFrom("Bearer ", "bearer   ", "BEARER "),
  fc.constantFrom("Basic ", "Bearer", ""),
);

it("answers 401 unless the bearer is an owner's token, and otherwise serves that owner", () =>
  Effect.gen(function* () {
    const fixture = yield* mcpFixture();
    const known = Redacted.value(fixture.credential);
    const tokens = mostly(
      fc.constant(known),
      fc.constantFrom(`${known}x`, known.slice(1)),
    );
    yield* forAll(
      [bearerSchemes, tokens],
      Effect.fnUntraced(function* (scheme, token) {
        const headers = { authorization: `${scheme}${token}` };
        const reply = yield* exchange(fixture, "tools/list", {}, { headers });
        const owner =
          /^bearer +$/iu.test(scheme) && token === known
            ? fixture.owner
            : undefined;
        expect(reply.status).toBe(owner === undefined ? 401 : 200);
        expect(reply.headers.has("www-authenticate")).toBe(owner === undefined);
        expect(yield* Ref.getAndSet(fixture.owners, [])).toEqual(
          owner === undefined ? [] : [owner],
        );
      }),
    );
  }));

/** Seconds above `min`, up to `max`. */
const seconds = (min: number, max: number) =>
  fc.double({ min, max, minExcluded: true, noNaN: true });

/** The values one argument accepts and the values it refuses. An `absent` value leaves the argument out. */
interface Argument {
  readonly accepts: fc.Arbitrary<unknown>;
  readonly refuses: fc.Arbitrary<unknown>;
}
/** The arguments a call takes, each by its name. */
type Arguments = Readonly<Record<string, Argument>>;
const absent = fc.constant(undefined);
const absentOrUnacceptable = fc.oneof(absent, unacceptable);

/** The server agent's daemon offers no groups, so an address names one agent. */
const recipient: Argument = {
  accepts: fc.stringMatching(/^agent:[a-z0-9]{3,8}$/u),
  refuses: fc.oneof(
    absentOrUnacceptable,
    fc.stringMatching(/^group:[a-z]{3,6},[a-z]{3,6},[a-z]{3,6}$/u),
  ),
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
/** Each kind of call the server agent makes with arguments. */
const calls: ReadonlyArray<{ name: string; takes: Arguments }> = [
  { name: "read_conversation", takes: { address: recipient } },
  { name: "receive_messages", takes: { wait } },
  { name: "send_message", takes: { to: recipient, text } },
];
/** A call of any kind, with one value each of its arguments accepts and one value each refuses. */
const toolCalls = fc.constantFrom(...calls).chain(({ name, takes }) =>
  fc.record({
    name: fc.constant(name),
    accepted: fc.record(Rec.map(takes, (argument) => argument.accepts)),
    refused: fc.record(Rec.map(takes, (argument) => argument.refuses)),
  }),
);
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
        receive: returns({ entries: [], nextCursor: "start" }),
        readHistory: returns({ records: [] }),
      },
    });
    yield* forAll(
      [toolCalls],
      Effect.fnUntraced(function* ({ name, accepted, refused }) {
        const reply = yield* toolCall(fixture, {
          name,
          arguments: accepted,
        });
        expect(yield* Ref.getAndSet(returned, [])).toEqual([
          reply.result?.structuredContent,
        ]);
        for (const wrong of Object.keys(refused)) {
          const refusal = yield* toolCall(fixture, {
            name,
            arguments: { ...accepted, [wrong]: refused[wrong] },
          });
          expect(refusal.result?.isError).toBe(true);
          expect(refusal.result?.content?.[0]?.text).toContain(`${wrong}: `);
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
        expect(reply.result).toEqual({
          isError: true,
          content: [
            { text: `Invalid tool arguments: ${name}: Unexpected field` },
          ],
          structuredContent: {
            reason: "invalid-arguments",
            issues: [{ path: [name], message: "Unexpected field" }],
          },
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
          expect((yield* toolCall(fixture, send)).result).toEqual({
            isError: true,
            content: [{ text: new SendError(data).message }],
            structuredContent: data,
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
          expect((yield* toolCall(fixture, read)).result).toEqual({
            isError: true,
            content: [{ text: failure.message }],
            structuredContent: { reason },
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
          receive: () =>
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

const clientText = "hello";
const clientTools = [
  "read_conversation",
  "receive_messages",
  "send_message",
  "status",
];
const sendTool = "send_message";
const missingField = "text";
const replyField = "replyToMsgId";

describe("send_message arguments", () => {
  it("gives a client in client/server mode tools that name no recipient", () =>
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
        replyField,
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
