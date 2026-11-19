/** @file Alice, Bob and Carol on one daemon, each with its communication service. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Context, Effect, Layer, Option, PubSub, Record } from "effect";
import type { AgentCard } from "../../identity.js";
import { createTestAgent } from "../../__tests__/agents.js";
import { openTestDatabase } from "../../test.js";
import { Communication, readInbox } from "../service.js";

/**
 * Alice, Bob and Carol in one database: each one's key, card and service,
 * and what a test reads of them.
 */
export const conversationFixture = Effect.gen(function* () {
  const { sql, run } = yield* openTestDatabase();
  const readable = yield* PubSub.unbounded<string>();
  const agent = (name: string) =>
    run(createTestAgent(name)).pipe(
      Effect.map((created) => ({ ...created, readable })),
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
  /** The ids of the messages `agent` may read after `cursor`, in read order, with the cursor to read on from. */
  const inbox = (agent: typeof agents.alice, cursor?: string) =>
    readInbox(agent.agentKey, { limit: 100, cursor }).pipe(
      Effect.provideService(SqlClient, sql),
      Effect.map(({ messages, nextCursor }) => ({
        ids: messages.map(({ message }) => message.msgId),
        nextCursor,
      })),
    );
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
    inbox,
    history,
    histories,
  };
}).pipe(Effect.withSpan("conversationFixture"));
