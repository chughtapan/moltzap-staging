/** @file The daemon admits owner-bound JWTs verified against a real issuer key-set endpoint, and MCP clients discover the tools of their agent's role and read each call's result or failure. */

import {
  HttpClient,
  HttpClientError,
  HttpClientResponse,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpClient, NodeHttpServer } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Exit,
  FastCheck as fc,
  Fiber,
  Scope,
  Stream,
  Struct,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { expect } from "vitest";
import { callTool, inProcessDaemon, send } from "../__tests__/daemon.js";
import { requiredScope } from "../__tests__/test.js";
import { AddressError, FirewallRefusal, SendError } from "./agent.js";
import { serveMcp } from "./server.js";

/** The subject of every owner with an agent on `daemon`. */
const ownersOf = (
  daemon: Effect.Effect.Success<ReturnType<typeof inProcessDaemon>>,
) =>
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
    const daemon = yield* inProcessDaemon();
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
    const daemon = yield* inProcessDaemon();
    yield* forAll(defect, (claim) =>
      Effect.gen(function* () {
        const token = yield* daemon.issuer
          .token(
            "subject" in claim ? claim.subject : "owner",
            "audience" in claim ? claim.audience : daemon.resource.href,
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
      const daemon = yield* inProcessDaemon();
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

it("gives the server agent its tools, and a status result that names the agent", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon();
    const alice = yield* daemon.signInAs("alice");
    const { tools } = yield* Effect.tryPromise(() => alice.mcp.listTools());
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
      alice.mcp.callTool({ name: "status", arguments: {} }),
    );
    expect(reply.structuredContent).toEqual({
      agentId: alice.agent.agentId,
      agentName: "alice",
    });
    expect(reply.content).toEqual([
      { type: "text", text: JSON.stringify(reply.structuredContent) },
    ]);
  }));

it("gives a client tools that name no recipient and no conversation", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon();
    const bob = yield* daemon.signInAs("bob");
    const { tools } = yield* Effect.tryPromise(() => bob.mcp.listTools());
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
    expect(yield* send(bob, { text: "hello" })).toMatchObject({
      address: "agent:alice",
    });
    expect((yield* callTool(bob, "read_conversation")).records).toMatchObject([
      {
        kind: "sent",
        message: { recipient: { agentName: "alice" }, text: "hello" },
      },
    ]);
  }));

it("refuses a call with a wrong or an unknown argument without running the tool", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon();
    const alice = yield* daemon.signInAs("alice");
    for (const wrong of [{ text: 1 }, { extra_field: "x" }]) {
      const failure = yield* callTool(alice, "send_message", {
        to: "agent:bob",
        text: "hello",
        ...wrong,
      }).pipe(Effect.flip);
      expect(failure.message).toContain(Object.keys(wrong)[0]);
    }
  }));

it("answers a failed call with the failure's message and its fields", () =>
  Effect.gen(function* () {
    const refusals = [
      {
        reason: "firewall-refused" as const,
        checkId: "chk_1",
        refusal: "rule-violation" as const,
        rule: "shape",
        feedback: ["Send a greeting first."],
      },
      {
        reason: "firewall-refused" as const,
        checkId: "chk_2",
        refusal: "unresolved-evidence" as const,
        feedback: [],
      },
    ];
    const pending = [...refusals];
    const daemon = yield* inProcessDaemon({
      firewall: {
        checkOutgoing: () =>
          Effect.map(Effect.fromNullable(pending.shift()), (next) => ({
            kind: "refused" as const,
            ...Struct.omit(next, "reason", "refusal"),
            reason: next.refusal,
          })).pipe(Effect.orDie),
      },
    });
    const alice = yield* daemon.signInAs("alice");
    const bob = yield* daemon.signInAs("bob");
    for (const fields of refusals) {
      expect(
        yield* send(alice, { to: bob.address, text: "hello" }).pipe(
          Effect.flip,
        ),
      ).toEqual({ ...fields, message: new FirewallRefusal(fields).message });
    }
    for (const [name, input] of [
      ["send_message", { to: "agent:nobody", text: "hello" }],
      ["send_message", { to: "agent:alice", text: "hello" }],
      ["read_conversation", { address: "agent:nobody" }],
      ["read_conversation", { address: "agent:alice" }],
    ] as const) {
      const address = "to" in input ? input.to : input.address;
      const failure = new AddressError({
        reason: address === "agent:alice" ? "invalid-address" : "unknown-agent",
        address,
      });
      expect(yield* callTool(alice, name, input).pipe(Effect.flip)).toEqual({
        reason: failure.reason,
        address,
        message: failure.message,
      });
    }
  }));

it("answers a call that dies without the defect's text, and a send that dies as one whose outcome is unknown", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon({
      firewall: {
        checkOutgoing: () =>
          Effect.dieMessage("PRIVATE relation does not exist"),
      },
    });
    const alice = yield* daemon.signInAs("alice");
    const bob = yield* daemon.signInAs("bob");
    expect(
      yield* send(alice, { to: bob.address, text: "hello" }).pipe(Effect.flip),
    ).toEqual({
      reason: "outcome-unknown",
      message: new SendError({ reason: "outcome-unknown" }).message,
    });
    yield* daemon.sql`DROP TABLE firewall_checks`;
    const failure = yield* callTool(alice, "read_firewall_decisions").pipe(
      Effect.flip,
    );
    expect(Object.keys(failure)).toEqual(["message"]);
    expect(failure.message).not.toContain("firewall_checks");
  }));

it("interrupts only the aborted call, whose conversation the next send then uses", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const interrupted = yield* Deferred.make<undefined>();
    let first = true;
    const daemon = yield* inProcessDaemon({
      firewall: {
        checkOutgoing: () =>
          Effect.suspend(() => {
            if (!first) {
              return Effect.succeed({ kind: "allowed" as const });
            }
            first = false;
            return Deferred.succeed(entered, undefined).pipe(
              Effect.zipRight(Effect.never),
              Effect.onInterrupt(() =>
                Deferred.succeed(interrupted, undefined),
              ),
            );
          }),
      },
    });
    const alice = yield* daemon.signInAs("alice");
    const bob = yield* daemon.signInAs("bob");
    const hello = { to: bob.address, text: "hello" };

    const waiting = yield* Effect.fork(send(alice, hello));
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(waiting);
    yield* Deferred.await(interrupted);

    expect(yield* send(alice, hello)).toMatchObject({ address: bob.address });
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
