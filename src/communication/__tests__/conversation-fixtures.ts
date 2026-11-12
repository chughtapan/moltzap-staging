/** @file Three agents created in one database. */

import { Effect } from "effect";
import { createTestAgent } from "../../__tests__/agents.js";
import { openTestDatabase } from "./store-fixtures.js";

/**
 * Alice, Bob and Carol, each with its key, its card and the consumer latches
 * the three share, and the database they share.
 */
export const conversationFixture = Effect.gen(function* () {
  const { sql, run } = yield* openTestDatabase();
  const consumers = new Map<string, Effect.Latch>();
  const agent = (name: string) =>
    run(createTestAgent(name)).pipe(
      Effect.map((created) => ({ ...created, consumers })),
    );
  return {
    alice: yield* agent("alice"),
    bob: yield* agent("bob"),
    carol: yield* agent("carol"),
    sql,
  };
}).pipe(Effect.withSpan("conversationFixture"));
