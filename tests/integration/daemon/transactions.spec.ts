/** @file Cross-agent fanout restores row authority after nested failure and cancellation. */
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import {
  databaseConnection,
  withAgentTransaction,
} from "@social-harness/social-harness/test";
import { Context, Deferred, Effect, Fiber, Layer } from "effect";
import { expect } from "vitest";
import { acquirePostgres } from "./postgres-fixture.js";

it(
  "restores the sender after a recipient query fails, succeeds or is interrupted",
  () =>
    Effect.gen(function* () {
      const database = yield* acquirePostgres;
      const sql = Context.get(
        yield* Layer.build(databaseConnection(database.runtimeLocator)),
        SqlClient.SqlClient,
      );
      const sender = "sender";
      const recipient = "recipient";
      const key = Effect.map(
        sql<{
          value: string;
        }>`SELECT current_setting('social_harness.agent_key',true) AS value`,
        (rows) => rows[0]?.value,
      );
      yield* withAgentTransaction(
        sender,
        Effect.gen(function* () {
          yield* withAgentTransaction(recipient, sql`SELECT 1/0`).pipe(
            Effect.either,
          );
          expect(yield* key).toBe(sender);
          expect(yield* withAgentTransaction(recipient, key)).toBe(recipient);
          expect(yield* key).toBe(sender);
          const entered = yield* Deferred.make<undefined>();
          const fiber = yield* withAgentTransaction(
            recipient,
            Deferred.succeed(entered, undefined).pipe(
              Effect.zipRight(Effect.never),
            ),
          ).pipe(Effect.fork);
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(fiber);
          expect(yield* key).toBe(sender);
          yield* sql`INSERT INTO message_log(agent_key,conversation_id,ordinal,author_agent_id,msg_id,canonical_message) VALUES('sender','conversation',0,'sender','message',${new Uint8Array([1])})`;
          expect(yield* sql`SELECT * FROM message_log`).toHaveLength(1);
        }),
      ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
      expect(yield* sql`SELECT * FROM message_log`).toHaveLength(0);
      expect(
        yield* withAgentTransaction(
          sender,
          sql`SELECT * FROM message_log`,
        ).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      ).toHaveLength(1);
    }),
  60_000,
);
