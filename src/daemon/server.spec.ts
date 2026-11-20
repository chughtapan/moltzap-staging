/**
 * @file The daemon admits owner-bound JWTs verified against a real issuer
 * key-set endpoint; MCP clients discover the tools of their agent's role and
 * read each call's result or failure; and agents observe sends and their
 * checks, the inbox, history and conversations through those tools.
 */

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
  Either,
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
import type { DecisionProvider } from "../firewall.js";
import {
  callTool,
  decisionState,
  inboxOf,
  inProcessDaemon,
  send,
} from "../__tests__/daemon.js";
import { answersTo, type TestPolicy } from "../__tests__/firewall-fixtures.js";
import { requiredScope } from "../__tests__/test.js";
import { FirewallRefusal } from "../conversations.js";
import { compileNorm } from "../norms/compiler.js";
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
      const unknown = address === "agent:nobody";
      expect(yield* callTool(alice, name, input).pipe(Effect.flip)).toEqual({
        reason: unknown ? "unknown-agent" : "invalid-address",
        address,
        message: unknown
          ? "This daemon has no agent named agent:nobody. Check the name with its owner."
          : "The address agent:alice names you, not another agent.",
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
      message:
        "send failed: the send did not complete; the message may or may not have been sent",
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

/** The one rule of every policy here. */
const privacy = {
  id: "privacy",
  applies: "both" as const,
  text: "Protect the owner's private information.",
  feedback: "Keep private information private.",
};

/** A message policy of the rule `privacy`, judged by `provider`. With `action`, the rule replies to or silently withholds what breaks it. */
const policy = (
  provider: DecisionProvider,
  action?: "deny" | "reply",
): TestPolicy => ({
  provider,
  source: {
    policy: {
      rules: [{ ...privacy, ...(action === undefined ? {} : { action }) }],
      settings: { minimumProbability: 0.8, timeoutMilliseconds: 5_000 },
    },
  },
});

/** A provider that finds every message compliant. */
const compliant: DecisionProvider = {
  decide: (request) => Effect.succeed(answersTo(request)),
};

/** A provider that finds every message a client sends in violation, and no message of the server agent. */
const incomingViolation: DecisionProvider = {
  decide: (request) =>
    Effect.succeed(
      answersTo(
        request,
        {},
        decisionState(request.state).direction === "incoming"
          ? "violation"
          : "compliant",
      ),
    ),
};

/** A daemon with its server agent Alice and her client Bob signed in. */
const aliceAndBob = (options?: Parameters<typeof inProcessDaemon>[0]) =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon(options);
    const alice = yield* daemon.signInAs("alice");
    const bob = yield* daemon.signInAs("bob");
    return { daemon, alice, bob };
  });

/** The kind of each record on the first page of the agent's history with the agent at `address`, or with the server agent. */
const historyOf = (agent: Parameters<typeof callTool>[0], address?: string) =>
  Effect.map(
    callTool(agent, "read_conversation", address ? { address } : {}),
    ({ records }) => records.map((record) => record.kind),
  );

it("stores each send as a new message to the agent it names, with the reply reference it gives", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob();

    const first = yield* send(alice, { to: bob.address, text: "Hello" });
    const second = yield* send(alice, {
      to: bob.address,
      text: "Hello",
      replyToMsgId: first.id,
    });

    expect(first.address).toBe(bob.address);
    const [stored, reply] = yield* inboxOf(bob);
    expect(stored).toMatchObject({ msgId: first.id, recipient: bob.agent });
    expect(stored).not.toHaveProperty("replyToMsgId");
    expect(reply).toMatchObject({ msgId: second.id, replyToMsgId: first.id });
  }));

it("checks the messages of one conversation in order while another conversation progresses", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const assessed: string[] = [];
    const { daemon, alice, bob } = yield* aliceAndBob({
      policy: policy({
        decide: (request) =>
          Effect.gen(function* () {
            const { text } = decisionState(request.state).candidate;
            assessed.push(text);
            if (text === "First") {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return answersTo(request);
          }),
      }),
    });
    const carol = yield* daemon.signInAs("carol");
    const addresses = Effect.map(
      callTool(alice, "receive_messages", { cursor: "0" }),
      (page) => page.entries.map((entry) => entry.address),
    );

    const first = yield* Effect.forkScoped(send(bob, { text: "First" }));
    yield* Deferred.await(entered);
    const second = yield* Effect.forkScoped(send(bob, { text: "Second" }));
    yield* send(carol, { text: "Independent" });

    expect(yield* addresses).toEqual([carol.address]);
    expect(assessed).toEqual(["First", "Independent"]);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.joinAll([first, second]);
    expect(yield* addresses).toEqual([carol.address, bob.address, bob.address]);
    expect(assessed).toEqual(["First", "Independent", "Second"]);
  }));

it("reads the inbox a page of fifty at a time after a cursor, the same page again for the same cursor, and nothing earlier without one", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob();
    const receive = (cursor?: string) =>
      callTool(alice, "receive_messages", cursor ? { cursor } : {});
    const start = yield* receive();
    expect(start.entries).toEqual([]);
    const sent = yield* Effect.forEach(
      Array.from({ length: 57 }),
      () => Effect.map(send(bob, { text: "Hello" }), ({ id }) => id),
      { concurrency: 1 },
    );

    const first = yield* receive(start.nextCursor);
    const rest = yield* receive(first.nextCursor);

    expect(first.entries.map((entry) => entry.message.msgId)).toEqual(
      sent.slice(0, 50),
    );
    expect(rest.entries.map((entry) => entry.message.msgId)).toEqual(
      sent.slice(50),
    );
    expect(yield* receive(first.nextCursor)).toEqual(rest);
    expect(yield* receive()).toEqual({
      entries: [],
      nextCursor: rest.nextCursor,
    });
  }));

it("stores a withheld message for its sender, whose send succeeds, and keeps it from the recipient's inbox and history", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob({
      policy: policy(incomingViolation),
    });

    yield* send(bob, { text: "Private peer content" });

    expect(
      (yield* callTool(alice, "read_firewall_decisions")).decisions,
    ).toMatchObject([{ reason: "rule-violation", decision: "withheld" }]);
    expect(yield* inboxOf(alice)).toEqual([]);
    expect(yield* historyOf(alice, bob.address)).toEqual([]);
    expect(yield* historyOf(bob)).toEqual(["sent"]);
    expect(yield* inboxOf(bob)).toEqual([]);
  }));

it("fails a client's send and stores nothing when the check of its message could not run", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob({
      policy: policy({ decide: () => Effect.succeed({ answers: {} }) }),
    });

    expect(yield* send(bob, { text: "Hello" }).pipe(Effect.flip)).toMatchObject(
      { reason: "firewall-refused", refusal: "check-unavailable" },
    );

    expect(
      (yield* callTool(alice, "read_firewall_decisions")).decisions,
    ).toEqual([]);
    expect(yield* historyOf(bob)).toEqual([]);
  }));

it.each([
  { from: "the notice", action: undefined, text: "Not accepted." },
  { from: "a reply rule", action: "reply" as const, text: privacy.feedback },
])(
  "sends the firewall's reply to a withheld message, from $from, as an ordinary message that answers it",
  ({ action, text }) =>
    Effect.gen(function* () {
      const { alice, bob } = yield* aliceAndBob({
        notice: "Not accepted.",
        policy: policy(incomingViolation, action),
      });

      const withheld = yield* send(bob, { text: "Private peer content" });

      expect(yield* inboxOf(bob)).toMatchObject([
        {
          author: alice.agent,
          recipient: bob.agent,
          replyToMsgId: withheld.id,
          text,
        },
      ]);
      expect(yield* inboxOf(alice)).toEqual([]);
      expect(yield* historyOf(bob)).toEqual(["sent", "received"]);
    }),
);

/** `open` starts an instance and `close`, from the other agent, ends it. */
const exchange = Either.getOrThrow(
  compileNorm(`(norm exchange :initiator Sender :participant Receiver)
(content open :by Sender :starts true :initiates opened)
(content close :by Receiver :when opened :ends true)
`),
);

it("answers what an agent that holds norms withholds, plain text and an act out of turn alike, and the conversation goes on", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob({
      notice: "Not accepted.",
      norms: [exchange],
      policy: policy(compliant),
    });

    const plain = yield* send(bob, { text: "Hello, can you help?" });
    const outOfTurn = yield* send(bob, { text: "(close :ontology exchange)" });

    const replies = yield* inboxOf(bob);
    expect(replies).toMatchObject(
      [plain, outOfTurn].map((withheld) => ({ replyToMsgId: withheld.id })),
    );
    expect(
      replies.map(({ text }) => text.startsWith("Not accepted. ")),
    ).toEqual([true, true]);
    yield* send(bob, { text: "(open :ontology exchange)" });
    expect(yield* inboxOf(alice)).toHaveLength(1);
  }));

it("checks each act of a norm against the whole conversation, though the provider's history window holds one message", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob({
      historyLimit: 1,
      norms: [exchange],
      policy: policy(compliant),
    });

    for (let instance = 0; instance < 3; instance += 1) {
      const opened = yield* send(alice, {
        to: bob.address,
        text: "(open :ontology exchange)",
      });
      yield* send(bob, {
        text: "(close :ontology exchange)",
        replyToMsgId: opened.id,
      });
    }

    expect(yield* inboxOf(alice)).toHaveLength(3);
  }));

it("continues a conversation's history from the cursor a page returned, with what was sent since", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob();
    const say = (text: string) => send(alice, { to: bob.address, text });
    for (let index = 0; index < 51; index += 1) {
      yield* say(`Message ${index}`);
    }

    const first = yield* callTool(alice, "read_conversation", {
      address: bob.address,
    });
    yield* say("Later message");
    const rest = yield* callTool(alice, "read_conversation", {
      address: bob.address,
      cursor: first.nextCursor,
    });

    expect(first.records).toHaveLength(50);
    expect(rest.records.map((record) => record.message.text)).toEqual([
      "Message 50",
      "Later message",
    ]);
    expect(rest.nextCursor).toBeUndefined();
  }));

it("lists every conversation once, in address order", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon();
    const alice = yield* daemon.signInAs("alice");
    for (const name of ["peer-2", "peer-10", "peer-1", "peer-10"]) {
      yield* send(yield* daemon.signInAs(name), { text: "Hello" });
    }

    expect(yield* callTool(alice, "list_conversations")).toEqual({
      addresses: ["agent:peer-1", "agent:peer-10", "agent:peer-2"],
    });
  }));
