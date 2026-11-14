/** @file Messages, groups and questions between agents on one daemon, with and without a rule on who may share a conversation. */
import { SqlClient } from "@effect/sql/SqlClient";
import { scopedLive as it } from "@effect/vitest";
import {
  Array as Arr,
  Chunk,
  Context,
  Effect,
  FastCheck as fc,
  Layer,
  Option,
  Queue,
  Record,
  Schema,
} from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { conversationFixture } from "./__tests__/conversation-fixtures.js";
import { form } from "./__tests__/requests-fixtures.js";
import { Answer, Message, Request } from "./messages.js";
import { Communication, type Conversation, type Incoming } from "./service.js";

type Rule = Pick<Parameters<typeof Communication.layer>[0], "permits">;

/** Alice, Bob and Carol on one daemon, under `rule` on who may share a conversation when given one. */
const peers = Effect.fn("peers")(function* (rule: Rule = {}) {
  const { sql, ...agents } = yield* conversationFixture;
  const open = (agent: typeof agents.alice) =>
    Layer.build(Communication.layer({ ...agent, ...rule })).pipe(
      Effect.provideService(SqlClient, sql),
      Effect.map((context) => Context.get(context, Communication)),
    );
  const hosted = yield* Effect.all(Record.map(agents, open));
  const cards = Record.map(agents, (agent) => agent.card);
  const received = (agent: typeof hosted.alice) =>
    Effect.gen(function* () {
      const inbox = yield* Queue.unbounded<Incoming>();
      yield* agent
        .consume((item) => Queue.offer(inbox, item).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);
      return inbox;
    });
  /** The newest 1024 messages of a conversation as `agent` reads it, oldest first. */
  const history = (agent: typeof hosted.alice, members: Conversation) =>
    agent
      .readHistory({ members, order: "newest", limit: 1024 })
      .pipe(Effect.map((page) => page.entries.map((entry) => entry.message)));
  /** A conversation as each of its members on this daemon reads it. */
  const histories = (members: Conversation) =>
    Effect.all(
      Record.filterMap(hosted, (agent, name) =>
        members.some((member) => member.agentName === name)
          ? Option.some(history(agent, members))
          : Option.none(),
      ),
    );
  return {
    sql,
    agents,
    open,
    ...hosted,
    hosted,
    cards,
    received,
    history,
    histories,
  };
});

/** Every conversation is Alice and one other agent, as for a server agent and its clients. */
const fixture = Effect.gen(function* () {
  const f = yield* peers({
    permits: (members) =>
      members.length === 2 &&
      members.some((member) => member.agentName === "alice"),
  });
  const members: Conversation = [f.cards.alice, f.cards.bob];
  const message = (text: string) =>
    Message.create({
      author: f.cards.bob,
      recipients: [f.cards.alice],
      content: [{ type: "text", text }],
    });
  return { ...f, server: f.alice, client: f.bob, members, message };
});

it("retains a client/server exchange and delivers each message only to its recipient", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const request = yield* f.message("hello");
    yield* f.client.send(request);
    const inbox = yield* f.received(f.server);
    expect(yield* Queue.take(inbox)).toEqual({
      kind: "message",
      message: request,
    });
    const reply = yield* Message.create({
      author: request.recipients[0],
      recipients: [request.author],
      content: [{ type: "text", text: "reply" }],
    });
    yield* f.server.send(reply);
    expect((yield* f.client.listConversations()).conversations).toEqual([
      f.members,
    ]);
    const reopened = yield* f.open(f.agents.bob);
    expect(yield* f.history(reopened, f.members)).toEqual([request, reply]);
  }));

it("rolls back both participant views and serializes concurrent sends", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const cancelled = yield* f.message("rolled back");
    yield* f.sql
      .withTransaction(
        f.client.send(cancelled).pipe(Effect.zipRight(Effect.fail("cancel"))),
      )
      .pipe(Effect.either);
    for (const agent of [f.server, f.client]) {
      expect(yield* f.history(agent, f.members)).toEqual([]);
    }
    const first = yield* f.message("client");
    const second = yield* Message.create({
      author: first.recipients[0],
      recipients: [first.author],
      content: [{ type: "text", text: "server" }],
    });
    yield* Effect.all([f.client.send(first), f.server.send(second)], {
      concurrency: 2,
    });
    const server = yield* f.history(f.server, f.members);
    expect(server).toEqual(yield* f.history(f.client, f.members));
    expect(server).toHaveLength(2);
  }));

it("hands a message to the next consumer after one fails", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const message = yield* f.message("durable before delivery");
    yield* f.client.send(message);
    const consumerFailure = new Error("handler failed");
    expect(
      yield* f.server
        .consume(() => Effect.fail(consumerFailure))
        .pipe(Effect.flip),
    ).toBe(consumerFailure);
    const reopened = yield* f.open(f.agents.alice);
    expect(
      yield* reopened
        .consume((incoming) => Effect.fail(incoming))
        .pipe(Effect.flip),
    ).toEqual({ kind: "message", message });
  }));

it("delivers a group message to every member in one order, between any agents", () =>
  Effect.gen(function* () {
    const f = yield* peers();
    const bobInbox = yield* f.received(f.bob);
    const carolInbox = yield* f.received(f.carol);
    const first = yield* Message.create({
      author: f.cards.alice,
      recipients: [f.cards.bob, f.cards.carol],
      content: [{ type: "text", text: "to the group" }],
    });
    const second = yield* Message.create({
      author: f.cards.bob,
      recipients: [f.cards.alice, f.cards.carol],
      content: [{ type: "text", text: "from another member" }],
    });
    yield* f.alice.send(first);
    yield* f.bob.send(second);
    expect(yield* Queue.take(bobInbox)).toEqual({
      kind: "message",
      message: first,
    });
    expect(yield* Queue.takeN(carolInbox, 2)).toEqual(
      Chunk.make(
        { kind: "message", message: first },
        { kind: "message", message: second },
      ),
    );
    const direct = yield* Message.create({
      author: f.cards.bob,
      recipients: [f.cards.carol],
      content: [{ type: "text", text: "between two members" }],
    });
    yield* f.bob.send(direct);
    expect(yield* Queue.take(carolInbox)).toEqual({
      kind: "message",
      message: direct,
    });
  }));

it("carries a question to its recipient and the answer back as a result, before the answerer's next message", () =>
  Effect.gen(function* () {
    const f = yield* peers();
    const bobInbox = yield* f.received(f.bob);
    const answer = { slot: "tue" };
    const request = yield* Request.create({
      author: f.cards.alice,
      recipients: [f.cards.bob],
      question: "Which day works?",
      op: "gather",
      requestedSchema: form,
      deadline: 60,
    });
    yield* f.alice.send(request);
    const asked = yield* Queue.take(bobInbox);
    if (asked.kind !== "request") {
      return yield* Effect.dieMessage("Expected Bob to receive the question");
    }
    yield* f.bob.send(
      yield* Answer.create({
        author: f.cards.bob,
        request: asked.message,
        response: { action: "accept", content: answer },
      }),
    );
    const later = yield* Message.create({
      author: f.cards.bob,
      recipients: [f.cards.alice],
      content: [{ type: "text", text: "sent after the answer" }],
    });
    yield* f.bob.send(later);
    const aliceInbox = yield* f.received(f.alice);
    expect(yield* Queue.take(aliceInbox)).toMatchObject({
      kind: "result",
      result: {
        request,
        closedBy: "settled",
        outcomes: [
          {
            member: f.cards.bob,
            outcome: { kind: "answered", content: answer },
          },
        ],
      },
    });
    expect(yield* Queue.take(aliceInbox)).toEqual({
      kind: "message",
      message: later,
    });
  }));

it("appends a message once to the history of each member of its conversation, and of no one else, exactly when its author sends it to agents on the daemon that the rule lets share a conversation", () =>
  Effect.gen(function* () {
    let apart: readonly string[] = [];
    /** The rule in force: the agents `apart` names may not all share one conversation. */
    const permits = (members: ReadonlyArray<Conversation[number]>) =>
      !apart.every((name) => members.some((one) => one.agentName === name));
    const f = yield* peers({ permits });
    const people = { ...f.cards, dave: testAgentCard("dave") };
    const names = Record.keys(people);
    /** A send through `sender`'s service of a message it wrote, or one `author` wrote when that names an agent. */
    const sends = fc
      .record({
        sender: fc.constantFrom(...Record.keys(f.hosted)),
        author: fc.oneof(
          { weight: 3, arbitrary: fc.constant(undefined) },
          { weight: 1, arbitrary: fc.constantFrom(...names) },
        ),
        to: fc.subarray(names).filter(Arr.isNonEmptyArray),
      })
      .filter(({ sender, author = sender, to }) => !to.includes(author));
    const property = fc.asyncProperty(
      fc.subarray(names, { minLength: 2 }),
      sends,
      (rule, { sender, author = sender, to }) =>
        Effect.gen(function* () {
          apart = rule;
          const message = yield* Message.create({
            author: people[author],
            recipients: Arr.map(to, (name) => people[name]),
            content: [{ type: "text", text: "hello" }],
          });
          const members: Conversation = [message.author, ...message.recipients];
          const admitted =
            author === sender &&
            to.every((name) => name in f.hosted) &&
            permits(members);
          const before = yield* f.histories(members);
          const refusal = yield* f.hosted[sender].send(message).pipe(
            Effect.as("none"),
            Effect.catchTag("CommunicationError", (refused) =>
              Effect.succeed(refused.reason),
            ),
          );
          expect(refusal).toBe(admitted ? "none" : "membership-invalid");
          expect(yield* f.histories(members)).toEqual(
            Record.map(before, (earlier) =>
              admitted ? [...earlier, message] : earlier,
            ),
          );
          const outsiders = Record.keys(f.hosted).filter(
            (name) => name !== author && !to.includes(name),
          );
          for (const outsider of outsiders) {
            expect(
              yield* f.history(f.hosted[outsider], members).pipe(Effect.flip),
            ).toMatchObject({ reason: "membership-invalid" });
          }
        }).pipe(Effect.runPromise),
    );
    yield* Effect.tryPromise(() => fc.assert(property));
  }));

it("keeps one copy of a message its author sends again, and refuses a different message under the same identifier", () =>
  Effect.gen(function* () {
    const f = yield* peers();
    const names = Record.keys(f.hosted);
    type Name = (typeof names)[number];
    const write = (
      author: Name,
      recipients: Arr.NonEmptyReadonlyArray<Name>,
      text: string,
    ) =>
      Message.create({
        author: f.cards[author],
        recipients: Arr.map(recipients, (name) => f.cards[name]),
        content: [{ type: "text", text }],
      });
    /** An agent and two choices of the other agents to write to. */
    const sends = fc.constantFrom(...names).chain((sender) => {
      const others = fc
        .subarray(names.filter((name) => name !== sender))
        .filter(Arr.isNonEmptyArray);
      return fc.record({
        sender: fc.constant(sender),
        to: others,
        elsewhere: others,
      });
    });
    const property = fc.asyncProperty(sends, ({ sender, to, elsewhere }) =>
      Effect.gen(function* () {
        const message = yield* write(sender, to, "original");
        const different = yield* Schema.decodeUnknown(Message)({
          ...(yield* write(sender, elsewhere, "changed")),
          msgId: message.msgId,
        });
        const members: Conversation = [message.author, ...message.recipients];
        const before = yield* f.histories(members);
        yield* f.hosted[sender].send(message);
        yield* f.hosted[sender].send(message);
        expect(
          yield* f.hosted[sender].send(different).pipe(Effect.flip),
        ).toMatchObject({ reason: "message-conflict" });
        expect(yield* f.histories(members)).toEqual(
          Record.map(before, (earlier) => [...earlier, message]),
        );
        const reused = yield* f.histories([
          different.author,
          ...different.recipients,
        ]);
        for (const history of Record.values(reused)) {
          expect(history).not.toContainEqual(different);
        }
      }).pipe(Effect.runPromise),
    );
    /** Sender and recipients combine in 27 ways, and every run lengthens the histories the next one reads. */
    const numRuns = 30;
    yield* Effect.tryPromise(() => fc.assert(property, { numRuns }));
  }));
