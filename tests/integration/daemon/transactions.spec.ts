/** @file Cross-agent fanout restores row authority after nested failure and cancellation. */
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { AgentName } from "@social-harness/social-harness";
import { Communication } from "@social-harness/social-harness/service";
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

it(
  "pins the server once a concurrent first delivery commits",
  () =>
    Effect.gen(function* () {
      const database = yield* acquirePostgres;
      const sql = Context.get(
        yield* Layer.build(databaseConnection(database.runtimeLocator)),
        SqlClient.SqlClient,
      );
      const firstServer = AgentName.make("first-server");
      const secondServer = AgentName.make("second-server");
      const bind = (serverAgentName: AgentName) =>
        Communication.configureTransport({
          kind: "client-server",
          serverAgentName,
        }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
      yield* bind(firstServer);
      const locked = yield* Deferred.make<undefined>();
      const release = yield* Deferred.make<undefined>();
      const first = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* bind(firstServer);
            yield* Deferred.succeed(locked, undefined);
            yield* Deferred.await(release);
            yield* sql`INSERT INTO message_sequences(conversation_id,next_ordinal) VALUES('first-delivery',1)`;
          }),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(locked);
      const second = yield* bind(secondServer).pipe(
        Effect.either,
        Effect.forkScoped,
      );
      yield* Effect.iterate(false, {
        while: (waiting) => !waiting,
        body: () =>
          database.admin`SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%communication_mode%'`.pipe(
            Effect.map((rows) => rows.length > 0),
            Effect.tap(() => Effect.sleep("10 millis")),
          ),
      }).pipe(Effect.timeout("10 seconds"));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      expect(
        yield* Fiber.join(second).pipe(
          Effect.flatMap((result) => result),
          Effect.flip,
        ),
      ).toMatchObject({ reason: "membership-invalid" });
      const [mode] = yield* sql<{
        server: string;
      }>`SELECT server_name AS server FROM communication_mode WHERE singleton=1`;
      expect(mode?.server).toBe(firstServer);
    }),
  60_000,
);
