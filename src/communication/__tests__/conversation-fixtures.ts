/** @file Alice, Bob and Carol on one daemon, each with its communication service. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Context, Effect, Layer, Option, Queue, Record } from "effect";
import type { AgentCard } from "../../identity.js";
import type { Message } from "../messages.js";
import { createTestAgent } from "../../__tests__/agents.js";
import { openTestDatabase } from "../../test.js";
import { Communication } from "../service.js";

/**
 * Alice, Bob and Carol in one database: each one's key, card and service,
 * the consumer latches the three share, and what a test reads of them.
 */
export const conversationFixture = Effect.gen(function* () {
  const { sql, run } = yield* openTestDatabase();
  const consumers = new Map<string, Effect.Latch>();
  const agent = (name: string) =>
    run(createTestAgent(name)).pipe(
      Effect.map((created) => ({ ...created, consumers })),
    );
  const agents = {
    alice: yield* agent("alice"),
    bob: yield* agent("bob"),
    carol: yield* agent("carol"),
  };
  const open = (agent: typeof agents.alice) =>
    Layer.build(Communication.layer(agent)).pipe(
      Effect.provideService(SqlClient, sql),
      Effect.map((context) => Context.get(context, Communication)),
    );
  const hosted = yield* Effect.all(Record.map(agents, open));
  const cards = Record.map(agents, (agent) => agent.card);
  const received = (agent: typeof hosted.alice) =>
    Effect.gen(function* () {
      const inbox = yield* Queue.unbounded<Message>();
      yield* agent
        .consume((message) => Queue.offer(inbox, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);
      return inbox;
    });
  /** The newest 1024 messages of a conversation as `agent` reads it, oldest first. */
  const history = (agent: typeof hosted.alice, members: readonly AgentCard[]) =>
    agent
      .readHistory({ members, order: "newest", limit: 1024 })
      .pipe(Effect.map((page) => page.entries.map((entry) => entry.message)));
  /** A conversation as each of its members on this daemon reads it. */
  const histories = (members: readonly AgentCard[]) =>
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
}).pipe(Effect.withSpan("conversationFixture"));
