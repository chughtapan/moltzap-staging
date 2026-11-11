/** @file Messages, groups and questions between agents on one daemon, in both modes. */
import { SqlClient } from "@effect/sql/SqlClient";
import { scopedLive as it } from "@effect/vitest";
import { Chunk, Context, Effect, Layer, Queue, Schema } from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { conversationFixture } from "./__tests__/conversation-fixtures.js";
import { form } from "./__tests__/requests-fixtures.js";
import { Answer, Message, Request } from "./messages.js";
import { Communication, type Conversation, type Incoming } from "./service.js";

type Agent = Effect.Effect.Success<typeof conversationFixture>["alice"];

/** Alice is the server agent; Bob and Carol are clients. */
const fixture = Effect.gen(function* () {
  const subject = yield* conversationFixture;
  const open = (agent: Agent) =>
    Layer.build(
      Communication.clientServer.layer({
        ...agent,
        serverAgentName: subject.alice.card.agentName,
      }),
    ).pipe(
      Effect.provideService(SqlClient, subject.sql),
      Effect.map((context) => Context.get(context, Communication)),
    );
  const server = yield* open(subject.alice);
  const client = yield* open(subject.bob);
  const other = yield* open(subject.carol);
  const members: Conversation = [subject.alice.card, subject.bob.card];
  const message = (text: string) =>
    Message.create({
      author: subject.bob.card,
      recipients: [subject.alice.card],
      content: [{ type: "text", text }],
    });
  return { subject, server, client, other, members, message, open };
});

// @agent-code-guard/regression-only: Real SQL exchanges cover atomic fanout, restart and incomplete retained state.
it("retains a client/server exchange and delivers each message only to its recipient", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const request = yield* f.message("hello");
    yield* f.client.send(request);
    yield* f.client.send(request);
    const inbox = yield* Queue.unbounded<Message>();
    yield* f.server
      .consume((item) =>
        item.kind === "message"
          ? Queue.offer(inbox, item.message).pipe(Effect.asVoid)
          : Effect.void,
      )
      .pipe(Effect.forkScoped);
    expect(yield* Queue.take(inbox)).toEqual(request);
    const reply = yield* Message.create({
      author: request.recipients[0],
      recipients: [request.author],
      content: [{ type: "text", text: "reply" }],
    });
    yield* f.server.send(reply);
    expect(
      (yield* f.client.readHistory({
        members: f.members,
        order: "newest",
        limit: 10,
      })).entries.map((entry) => entry.message),
    ).toEqual([request, reply]);
    expect(
      (yield* f.server.readHistory({ members: f.members })).entries.map(
        (entry) => entry.message,
      ),
    ).toEqual([request, reply]);
    expect(
      yield* f.client.waitForMessage({
        authorAgentId: request.author.agentId,
        msgId: request.msgId,
      }),
    ).toEqual(request);
    expect((yield* f.client.listConversations()).conversations).toEqual([
      f.members,
    ]);
    const reopened = yield* f.open(f.subject.bob);
    expect(
      (yield* reopened.readHistory({ members: f.members })).entries,
    ).toHaveLength(2);
  }));

it("rejects client-to-client messages, changed retries and another client's history", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const request = yield* f.message("original");
    yield* f.client.send(request);
    const changed = yield* Schema.decodeUnknown(Message)({
      ...request,
      content: [{ type: "text", text: "changed" }],
    });
    expect(yield* f.client.send(changed).pipe(Effect.flip)).toMatchObject({
      reason: "message-conflict",
    });
    const peer = yield* Message.create({
      author: request.author,
      recipients: [f.subject.carol.card],
      content: request.content,
    });
    expect(yield* f.client.send(peer).pipe(Effect.flip)).toMatchObject({
      reason: "membership-invalid",
    });
    expect(
      yield* f.other.readHistory({ members: f.members }).pipe(Effect.flip),
    ).toMatchObject({ reason: "membership-invalid" });
  }));

it("rolls back both participant views and serializes concurrent sends", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const cancelled = yield* f.message("rolled back");
    yield* f.subject.sql
      .withTransaction(
        f.client.send(cancelled).pipe(Effect.zipRight(Effect.fail("cancel"))),
      )
      .pipe(Effect.either);
    expect(
      (yield* f.server.readHistory({
        members: f.members,
        order: "newest",
        limit: 10,
      })).entries,
    ).toEqual([]);
    expect(
      (yield* f.client.readHistory({
        members: f.members,
        order: "newest",
        limit: 10,
      })).pendingOutgoing,
    ).toEqual([]);
    const first = yield* f.message("client");
    const second = yield* Message.create({
      author: first.recipients[0],
      recipients: [first.author],
      content: [{ type: "text", text: "server" }],
    });
    yield* Effect.all([f.client.send(first), f.server.send(second)], {
      concurrency: 2,
    });
    const server = yield* f.server.readHistory({ members: f.members });
    const client = yield* f.client.readHistory({ members: f.members });
    expect(server.entries).toEqual(client.entries);
    expect(server.entries).toHaveLength(2);
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
    const reopened = yield* f.open(f.subject.alice);
    expect(
      yield* reopened
        .consume((incoming) => Effect.fail(incoming))
        .pipe(Effect.flip),
    ).toEqual({ kind: "message", message });
  }));

it("keeps a database in the mode and with the server agent it has messages for", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.client.send(yield* f.message("client/server message"));
    expect(
      yield* Communication.p2p.claim.pipe(
        Effect.provideService(SqlClient, f.subject.sql),
        Effect.flip,
      ),
    ).toMatchObject({ reason: "membership-invalid" });
    expect(
      yield* Communication.clientServer
        .claim(f.subject.bob.card.agentName)
        .pipe(Effect.provideService(SqlClient, f.subject.sql), Effect.flip),
    ).toMatchObject({ reason: "membership-invalid" });
  }));

it("refuses a conversation with a missing position or a row that names another message", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const first = yield* f.message("first");
    const last = yield* f.message("last");
    yield* f.client.send(first);
    yield* f.client.send(last);
    yield* f.subject
      .sql`DELETE FROM message_log WHERE agent_key=${f.subject.alice.agentKey} AND msg_id=${last.msgId}`;
    expect(
      yield* f.server.readHistory({ members: f.members }).pipe(Effect.flip),
    ).toMatchObject({ reason: "history-gap" });
    expect(
      yield* f.server
        .consume(() => Effect.dieMessage("Incomplete history reached the host"))
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "history-gap" });
    expect(
      (yield* f.client.readHistory({ members: f.members })).entries.map(
        (entry) => entry.message,
      ),
    ).toEqual([first, last]);
    yield* f.subject
      .sql`UPDATE message_log SET msg_id='substituted' WHERE agent_key=${f.subject.bob.agentKey} AND msg_id=${first.msgId}`;
    expect(
      yield* f.client.readHistory({ members: f.members }).pipe(Effect.flip),
    ).toMatchObject({ reason: "history-gap" });
  }));

const peers = Effect.gen(function* () {
  const subject = yield* conversationFixture;
  const open = (agent: Agent) =>
    Layer.build(Communication.p2p.layer(agent)).pipe(
      Effect.provideService(SqlClient, subject.sql),
      Effect.map((context) => Context.get(context, Communication)),
    );
  const alice = yield* open(subject.alice);
  const bob = yield* open(subject.bob);
  const carol = yield* open(subject.carol);
  const cards = {
    alice: subject.alice.card,
    bob: subject.bob.card,
    carol: subject.carol.card,
  };
  const received = (agent: typeof alice) =>
    Effect.gen(function* () {
      const inbox = yield* Queue.unbounded<Incoming>();
      yield* agent
        .consume((item) => Queue.offer(inbox, item).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);
      return inbox;
    });
  return { alice, bob, carol, cards, received };
});

it("delivers a group message to every member in one order, between any agents", () =>
  Effect.gen(function* () {
    const f = yield* peers;
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
    const members: Conversation = [f.cards.alice, f.cards.bob, f.cards.carol];
    const histories = yield* Effect.forEach(
      [f.alice, f.bob, f.carol],
      (agent) => agent.readHistory({ members }),
      { concurrency: 1 },
    );
    for (const history of histories) {
      expect(history.entries.map((entry) => entry.message)).toEqual([
        first,
        second,
      ]);
    }
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

it("carries a question to its recipient and the answer back as a result", () =>
  Effect.gen(function* () {
    const f = yield* peers;
    const aliceInbox = yield* f.received(f.alice);
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
    const settled = yield* Queue.take(aliceInbox);
    expect(settled).toMatchObject({
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
  }));

it("refuses a message to an agent the daemon does not have", () =>
  Effect.gen(function* () {
    const f = yield* peers;
    const message = yield* Message.create({
      author: f.cards.alice,
      recipients: [f.cards.bob, testAgentCard("dave")],
      content: [{ type: "text", text: "to a stranger" }],
    });
    expect(yield* f.alice.send(message).pipe(Effect.flip)).toMatchObject({
      reason: "membership-invalid",
    });
    expect(
      (yield* f.bob.readHistory({
        members: [f.cards.alice, f.cards.bob, testAgentCard("dave")],
        order: "newest",
        limit: 10,
      })).entries,
    ).toEqual([]);
  }));
