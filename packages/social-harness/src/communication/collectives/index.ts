/** @file Collectives owns question lifetimes, first answers and durable result handoffs. */

import * as SqlClient from "@effect/sql/SqlClient";
import {
  Context,
  Deferred,
  Effect,
  Layer,
  type Option,
  Queue,
  Scope,
} from "effect";
import type { Message } from "../message.js";
import type { EndpointStoreError } from "../store/index.js";
import type { InboundItem as IncomingItem } from "./inbound.js";
import type { Question, ResultDelivery } from "./model.js";
import type { Environment } from "./operations.js";
import type { StoredCollectiveMessage } from "./store.js";
import { Identity } from "../../identity/local.js";
import { Communication } from "../index.js";
import { type CollectiveId, CollectivesError } from "./contracts.js";
import { receive } from "./receive.js";
import { answer, ask, getRequest } from "./requests.js";
import { consumeResults } from "./results.js";
import { runWorker, start } from "./worker.js";

/** Operation methods capture resources once and never expose protocol preparations. */
export class Collectives extends Context.Tag("@moltzap/Collectives")<
  Collectives,
  {
    /** Accepts the captured operation before its deadline. */
    readonly ask: (question: Question) => Effect.Effect<void, CollectivesError>;
    /** Returns the original recognized request even after it closes. */
    readonly getRequest: (
      operationId: CollectiveId,
    ) => Effect.Effect<
      Extract<IncomingItem, { readonly kind: "collectiveRequest" }>["message"],
      CollectivesError
    >;
    /** Rechecks the selected request and saves the exact outgoing message atomically. */
    readonly answer: (
      message: Message,
    ) => Effect.Effect<void, CollectivesError>;
    /** Interprets verified traffic without waiting for policy assessment. */
    readonly receive: (
      message: Message,
    ) => Effect.Effect<Option.Option<IncomingItem>, CollectivesError>;
    /** Retires a result only after its handler durably accepts it. */
    readonly consumeResults: <E, R>(
      handle: (result: ResultDelivery) => Effect.Effect<void, E, R>,
    ) => Effect.Effect<never, E | CollectivesError, R>;
  }
>() {
  /** The scope owns deadlines and certification observations through restart handoff. */
  static readonly layer = Layer.scoped(Collectives, makeCollectives());
}

/** Callers distinguish operation refusals from retained-message progress. */
export { CollectivesError } from "./contracts.js";

/** Shared operation values remain independent of service construction. */
export { Question, ResultDelivery, ResultSources } from "./model.js";
/** Only Collectives interprets operation content. */
export { type CollectiveValue, readCollectiveValue } from "./wire.js";

/** Host draft construction uses the same admitted operation representation. */
export { encodeCollectiveContent, mintCollectiveId } from "./wire.js";

/** Captures one SQL client and owns the workers that outlive individual calls. */
function makeCollectives() {
  return Effect.gen(function* () {
    const environment: Environment = {
      sql: yield* SqlClient.SqlClient,
      communication: yield* Communication,
      identity: yield* Identity,
      scope: yield* Scope.Scope,
      failed: yield* Deferred.make<never, CollectivesError>(),
      observations: yield* Queue.unbounded<StoredCollectiveMessage>(),
    };
    const capture = <A, E, R>(
      effect: Effect.Effect<A, E | EndpointStoreError, R>,
    ) =>
      effect.pipe(
        Effect.provideService(SqlClient.SqlClient, environment.sql),
        Effect.catchTag(
          "EndpointStoreError",
          () => new CollectivesError({ reason: "persistence-failed" }),
        ),
      );
    yield* start(environment, runWorker(environment));
    return {
      ask: (question) => capture(ask(environment, question)),
      getRequest: (operationId) => capture(getRequest(operationId)),
      answer: (message) => capture(answer(environment, message)),
      receive: (message) => capture(receive(environment, message)),
      consumeResults: (handle) => capture(consumeResults(environment, handle)),
    } satisfies Context.Tag.Service<typeof Collectives>;
  }).pipe(Effect.withSpan("makeCollectives"));
}
