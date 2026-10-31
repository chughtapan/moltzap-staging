/** @file Schedules local deadlines and observes accepted message certification. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Cause, Clock, Deferred, Effect, Queue } from "effect";
import type { EndpointStoreError } from "../store/index.js";
import type { StoredCollectiveMessage } from "./store.js";
import { CollectivesError } from "./contracts.js";
import { type Environment, transaction } from "./operations.js";
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
        { concurrency: 2, discard: true },
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
    const message = yield* environment.communication
      .waitForMessage(saved.reference)
      .pipe(
        Effect.catchTag("CommunicationError", (error) =>
          failOperationMessage(saved, error.reason).pipe(Effect.as(undefined)),
        ),
      );
    if (message === undefined) {
      return;
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

function failOperationMessage(saved: StoredCollectiveMessage, reason: string) {
  return Effect.gen(function* () {
    if (saved.role === "close") {
      const item = {
        kind: "operationFailed" as const,
        operationId: saved.operationId,
        to: saved.destination,
        error: `The collective result could not be shared: ${reason}`,
      };
      yield* checkResultSize({ item, sources: { answers: [] } });
      yield* transaction(Store.failCollectiveClose(saved.operationId, item));
    } else if (saved.role === "response") {
      yield* retireUnusableClose(saved);
    }
  });
}

function resume(environment: Environment) {
  return Effect.gen(function* () {
    for (const saved of yield* Store.readCollectiveMessages()) {
      yield* Queue.offer(environment.observations, saved);
    }
    for (const operationId of yield* Store.readCollectiveOperationIds(
      "remote",
      "finalizing",
    )) {
      yield* finishRemote(environment, operationId);
    }
  });
}

function localDeadlines(environment: Environment) {
  return Effect.gen(function* () {
    for (const operationId of yield* Store.readCollectiveOperationIds(
      "local",
      "collecting",
    )) {
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const close = yield* finishLocal(environment, operationId);
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
        operation.close?.included.some(
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
