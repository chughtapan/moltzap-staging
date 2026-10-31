/** @file Checks queued incoming items and records whether the host may receive them. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Cause, Clock, Deferred, Effect, Queue, type Scope } from "effect";
import type { IncomingEntry } from "./types.js";
import { ResultSources } from "../../communication/collectives/index.js";
import { transaction } from "../../communication/store/database/index.js";
import {
  decodeRuntimeValue,
  encodeRuntimeValue,
  EndpointStoreError,
} from "../../communication/store/index.js";
import { InboundItem } from "../../host-protocol/items.js";
import { saveMessageDecision, saveResultDecision } from "../store.js";
import {
  type FirewallEnvironment,
  prepareIncomingCheck,
} from "./firewall-context.js";
import { checkLocalItem, resultVisibility } from "./local-results.js";
import {
  markMessageVisibility,
  readNextItemsToCheck,
  requireNextItemToCheck,
  retireQueuedItem,
  settleQueuedItem,
} from "./pending.js";
import { putInboxItem } from "./store.js";

/** What the admission worker reads and changes in one service's delivery. */
export interface AdmissionContext {
  readonly sql: SqlClient.SqlClient;
  /** Local attribution becomes available after the daemon activates. */

  readonly firewallEnvironment: Effect.Effect<
    FirewallEnvironment | undefined,
    EndpointStoreError
  >;
  /** Service lifetime; each conversation's check runs as a fiber in it. */
  readonly scope: Scope.Scope;
  /** Offered after each pass and each finished check. */
  readonly admissionWake: Queue.Queue<undefined>;
  /** Completed by the first check that fails other than by interruption. */
  readonly admissionFailure: Deferred.Deferred<never, EndpointStoreError>;
  /** Conversations with a check in flight; at most one each. */
  readonly activeConversations: Set<string>;
}

/**
 * Run the worker until the service scope closes or a check fails: on each
 * wake, start the next pending check in each idle conversation,
 * up to the firewall's concurrency bound, and publish after each commit.
 * @param input The service's SQL client, local attribution and worker bookkeeping.
 * @param publish Runs after each committed disposition, so subscribers see it.
 * @returns Never succeeds; fails with the first store error a check returns.
 */
export function runAdmission(
  input: AdmissionContext,
  publish: Effect.Effect<void>,
): Effect.Effect<never, EndpointStoreError> {
  return Queue.take(input.admissionWake).pipe(
    Effect.zipRight(checkPendingMessages(input, publish)),
    Effect.forever,
    Effect.raceFirst(Deferred.await(input.admissionFailure)),
    Effect.provideService(SqlClient.SqlClient, input.sql),
  );
}
function checkPendingMessages(
  input: AdmissionContext,
  publish: Effect.Effect<void>,
) {
  return Effect.gen(function* () {
    const environment = yield* input.firewallEnvironment;
    const entries = yield* readNextItemsToCheck();
    if (environment === undefined) {
      yield* Effect.forEach(
        entries,
        (entry) => admitWithoutAssessment(entry).pipe(Effect.zipRight(publish)),
        { concurrency: 1, discard: true },
      );
      if (entries.length > 0) {
        yield* Queue.offer(input.admissionWake, undefined);
      }
      return;
    }
    for (const entry of entries) {
      if (
        input.activeConversations.size >=
        environment.firewall.configuration.maximumConcurrentChecks
      ) {
        return;
      }
      if (input.activeConversations.has(entry.conversation)) {
        continue;
      }
      input.activeConversations.add(entry.conversation);
      yield* checkPendingMessage(environment, entry).pipe(
        Effect.zipRight(publish),
        Effect.catchAllCause((cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.failCause(cause)
            : Deferred.failCause(input.admissionFailure, cause),
        ),
        Effect.ensuring(
          Effect.sync(() =>
            input.activeConversations.delete(entry.conversation),
          ).pipe(Effect.zipRight(Queue.offer(input.admissionWake, undefined))),
        ),
        Effect.forkIn(input.scope),
      );
    }
  });
}

/**
 * Check one queued item against one captured context and retain its disposition.
 * A request that expires before assessment retires without checking; expiry
 * during assessment withholds host presentation while preserving its result.
 */
function checkPendingMessage(
  environment: FirewallEnvironment,
  entry: IncomingEntry,
) {
  return Effect.gen(function* () {
    const item = yield* decodeRuntimeValue(InboundItem, entry.canonicalItem);
    switch (item.kind) {
      case "multicast":
        return yield* checkMessage(environment, entry, item);
      case "collectiveRequest": {
        if (yield* requestExpired(item)) {
          return yield* transaction(retireQueuedItem(entry.deliveryToken));
        }
        return yield* checkMessage(environment, entry, item);
      }
      case "collectiveResult":
      case "operationFailed":
        return yield* checkLocalItem(environment, { entry, item }).pipe(
          Effect.flatMap(saveResultDecision),
          Effect.asVoid,
        );
      default: {
        const exhaustive: never = item;
        return exhaustive;
      }
    }
  });
}

function checkMessage(
  environment: FirewallEnvironment,
  entry: IncomingEntry,
  item: Extract<
    InboundItem,
    { readonly kind: "multicast" | "collectiveRequest" }
  >,
) {
  return Effect.gen(function* () {
    const prepared = yield* prepareIncomingCheck(environment, entry, item);
    const [checked] = yield* environment.firewall.check([prepared]);
    if (checked === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const expired =
      item.kind === "collectiveRequest" &&
      (yield* Clock.currentTimeMillis) >= item.message.content.deadlineAt;
    yield* saveMessageDecision(
      {
        deliveryToken: entry.deliveryToken,
        canonicalItem: entry.canonicalItem,
        ...(checked.guidance === undefined || expired
          ? {}
          : { canonicalGuidance: yield* encodeRuntimeValue(checked.guidance) }),
      },
      expired ? { ...checked.completion, admit: false } : checked.completion,
    );
  });
}

/** Unconfigured assessment drains the same durable queue without producing policy decisions. */
function admitWithoutAssessment(entry: IncomingEntry) {
  return transaction(
    Effect.gen(function* () {
      yield* requireNextItemToCheck(entry.deliveryToken, entry.canonicalItem);
      const item = yield* decodeRuntimeValue(InboundItem, entry.canonicalItem);
      if (yield* requestExpired(item)) {
        return yield* retireQueuedItem(entry.deliveryToken);
      }
      yield* putInboxItem({
        deliveryToken: entry.deliveryToken,
        canonicalItem: entry.canonicalItem,
      });
      if (item.kind === "collectiveResult") {
        const sources = yield* decodeRuntimeValue(
          ResultSources,
          entry.canonicalSources ?? new Uint8Array(),
        );
        for (const visibility of resultVisibility(item, sources)) {
          yield* markMessageVisibility(
            visibility.message,
            visibility.admitted,
            entry.deliveryToken,
          );
        }
      }
      yield* settleQueuedItem(entry.deliveryToken, "admitted");
    }),
  );
}

function requestExpired(item: InboundItem) {
  return Clock.currentTimeMillis.pipe(
    Effect.map(
      (now) =>
        item.kind === "collectiveRequest" &&
        now >= item.message.content.deadlineAt,
    ),
  );
}
