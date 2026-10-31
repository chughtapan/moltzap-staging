/** @file Schedules local deadlines and observes accepted message certification. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Cause, Clock, Deferred, Effect, Queue } from "effect";
import type { Environment } from "./operations.js";
import type { StoredCollectiveMessage } from "./store.js";
import { transaction } from "./operations.js";
import { EndpointStoreError } from "../store/index.js";
import { CollectivesError } from "./contracts.js";
import { checkResultSize, finishLocal, finishRemote } from "./results.js";
import * as Store from "./store.js";

/** Startup restores observers once; the periodic pass visits only local collecting operations. */
export function runWorker(environment: Environment) {
  return resume(environment).pipe(
    Effect.zipRight(
      Effect.all(
        [
          localDeadlines(environment).pipe(
            Effect.zipRight(Effect.sleep("100 millis")),
            Effect.forever,
          ),
          Queue.take(environment.observations).pipe(
            Effect.flatMap((saved) =>
              start(environment, observeMessage(environment, saved)),
            ),
            Effect.forever,
          ),
        ],
        { concurrency: "unbounded", discard: true },
      ),
    ),
  );
}

/** Only service-owned workers launch observations, outside caller transaction contexts. */
export function start(
  environment: Environment,
  work: Effect.Effect<
    void,
    CollectivesError | EndpointStoreError,
    SqlClient.SqlClient
  >,
) {
  return work.pipe(
    Effect.provideService(SqlClient.SqlClient, environment.sql),
    Effect.catchTag("EndpointStoreError", () =>
      Deferred.fail(
        environment.failed,
        new CollectivesError({ reason: "persistence-failed" }),
      ),
    ),
    Effect.catchTag("CollectivesError", (error) =>
      Deferred.fail(environment.failed, error),
    ),
    Effect.catchAllCause((cause) =>
      Cause.isInterruptedOnly(cause)
        ? Effect.interrupt
        : Deferred.failCause(environment.failed, cause),
    ),
    Effect.asVoid,
    Effect.interruptible,
    Effect.forkIn(environment.scope),
    Effect.asVoid,
  );
}

/** A saved role observes Communication's retained Message without taking payload custody or resending. */
function observeMessage(
  environment: Environment,
  saved: StoredCollectiveMessage,
) {
  return Effect.gen(function* () {
    const operation = yield* Store.readCollectiveOperation(saved.operationId);
    if (
      operation === undefined ||
      operation.phase === "completed" ||
      !(yield* Store.readCollectiveMessages(saved.operationId)).some(
        (entry) =>
          entry.role === saved.role &&
          entry.destination === saved.destination &&
          entry.reference.authorAgentId === saved.reference.authorAgentId &&
          entry.reference.msgId === saved.reference.msgId,
      )
    ) {
      return;
    }
    const completion = yield* environment.communication
      .waitForMessage(saved.reference)
      .pipe(Effect.either);
    if (completion._tag === "Left") {
      if (saved.role === "close") {
        const item = {
          kind: "operationFailed" as const,
          operationId: saved.operationId,
          to: saved.destination,
          error: `The collective result could not be shared: ${completion.left.reason}`,
        };
        yield* checkResultSize({ item, sources: { answers: [] } });
        yield* transaction(Store.failCollectiveClose(saved.operationId, item));
      } else if (saved.role === "response") {
        yield* retireUnusableClose(saved);
      }
      return;
    }
    const message = completion.right;
    if (
      message.author.agentId !== saved.reference.authorAgentId ||
      message.msgId !== saved.reference.msgId
    ) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    yield* transaction(
      Effect.gen(function* () {
        if (saved.role === "response") {
          yield* Store.recordCollectiveAnswer(
            saved.operationId,
            message,
            yield* Clock.currentTimeMillis,
          );
          yield* finishRemote(environment, saved.operationId);
        } else if (saved.role === "close") {
          yield* Store.completeCollectiveOperation(saved.operationId);
        }
      }),
    );
  });
}

function resume(environment: Environment) {
  return Effect.gen(function* () {
    for (const saved of yield* Store.readCollectiveMessages()) {
      if (saved.role !== "request") {
        yield* Queue.offer(environment.observations, saved);
      }
    }
    for (const operation of yield* Store.readPendingRemoteCloses()) {
      yield* finishRemote(environment, operation.operationId);
    }
  });
}

function localDeadlines(environment: Environment) {
  return Effect.gen(function* () {
    for (const operation of yield* Store.readLocalCollectingOperations()) {
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const close = yield* finishLocal(environment, operation.operationId);
          if (close !== undefined) {
            yield* Queue.offer(environment.observations, close);
          }
        }),
      );
    }
  });
}

function retireUnusableClose(saved: StoredCollectiveMessage) {
  return transaction(
    Effect.gen(function* () {
      const operation = yield* Store.readCollectiveOperation(saved.operationId);
      if (
        operation?.origin === "remote" &&
        operation.phase === "finalizing" &&
        operation.close?.value.included.some(
          (reference) =>
            reference.authorAgentId === saved.reference.authorAgentId &&
            reference.msgId === saved.reference.msgId,
        )
      ) {
        yield* Store.retireCollectiveOperation(saved.operationId);
      }
    }),
  );
}
