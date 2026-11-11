/** @file Three agents created in one database. */

import { Effect } from "effect";
import { createTestAgent } from "../../__tests__/agents.js";
import { openTestDatabase } from "./store-fixtures.js";

/** Alice, Bob and Carol, each with its key and card, and the database they share. */
export const conversationFixture = Effect.gen(function* () {
  const { sql, run } = yield* openTestDatabase();
  return {
    alice: yield* run(createTestAgent("alice")),
    bob: yield* run(createTestAgent("bob")),
    carol: yield* run(createTestAgent("carol")),
    sql,
  };
}).pipe(Effect.withSpan("conversationFixture"));
