/** @file Shares one SQLite lifetime and commits changes spanning communication, policy and delivery. */

import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as FileSystem from "@effect/platform/FileSystem";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import * as SqlClient from "@effect/sql/SqlClient";
import { Cause, Effect, Layer, Option } from "effect";
import { join } from "node:path";
import type { FirewallCompletion } from "../firewall/store/types.js";
import type { ItemProjection, LocalAdmission } from "./inbox/types.js";
import {
  createCommunicationTables,
  inspectStoreState,
  type StoreOpening,
  storeOperation,
  transaction,
} from "../communication/store/database/index.js";
import {
  EndpointStoreError,
  mapStoreFailure,
  OutboxTooOldError,
  readText,
  requireSameBytes,
} from "../communication/store/database/values.js";
import { saveCheckDecision } from "../firewall/store/firewall.js";
import { createFirewallTables } from "../firewall/store/schema.js";
import {
  requireNextItemToCheck,
  settleLocalItem,
  settleQueuedItem,
} from "./inbox/pending.js";
import { createInboxTables, putInboxItem } from "./inbox/store.js";
const DATABASE_NAME = "sharnessd.sqlite3";
/** Startup rejects incompatible state and expired pending bytes before accepting requests. */
export function layer(options: {
  readonly stateDirectory: string;
  readonly liveWireVersions: readonly string[];
}): Layer.Layer<SqlClient.SqlClient, EndpointStoreError | OutboxTooOldError> {
  const connection = SqliteClient.layer({
    filename: join(options.stateDirectory, DATABASE_NAME),
    disableWAL: true,
  });
  const initialized = Layer.effectDiscard(initializeDatabase(options));
  return Layer.unwrapEffect(
    Effect.gen(function* () {
      const filesystem = yield* FileSystem.FileSystem;
      yield* filesystem.makeDirectory(options.stateDirectory, {
        recursive: true,
        mode: 0o700,
      });
      yield* filesystem.chmod(options.stateDirectory, 0o700);
      return initialized.pipe(Layer.provideMerge(connection));
    }).pipe(Effect.withSpan("prepareDaemonStorage")),
  ).pipe(
    Layer.provide(NodeFileSystem.layer),
    Layer.catchAllCause(
      (cause): Layer.Layer<unknown, EndpointStoreError | OutboxTooOldError> => {
        if (Cause.isInterruptedOnly(cause)) {
          return Layer.failCause(Cause.stripFailures(cause));
        }
        const failure = Option.getOrUndefined(Cause.failureOption(cause));
        return failure instanceof OutboxTooOldError
          ? Layer.fail(failure)
          : Layer.fail(
              mapStoreFailure(
                failure ?? Option.getOrUndefined(Cause.dieOption(cause)),
                "persistence",
              ),
            );
      },
    ),
  );
}
/** Registration can distinguish a fresh directory without changing its saved state. */
export function inspectEndpointStore(
  stateDirectory: string,
): Effect.Effect<StoreOpening, EndpointStoreError> {
  return Effect.gen(function* () {
    const filesystem = yield* FileSystem.FileSystem;
    const filename = join(stateDirectory, DATABASE_NAME);
    if (!(yield* filesystem.exists(filename))) {
      return "create" as const;
    }
    return yield* inspectStoreState().pipe(
      Effect.provide(
        SqliteClient.layer({ filename, readonly: true, disableWAL: true }),
      ),
    );
  }).pipe(
    Effect.provide(NodeFileSystem.layer),
    storeOperation,
    Effect.withSpan("inspectEndpointStore"),
  );
}
/** A completed decision and the host's immutable message become durable together. */
export function saveMessageDecision(
  item: ItemProjection,
  completion: FirewallCompletion,
) {
  return transaction(
    Effect.gen(function* () {
      yield* requireNextItemToCheck(item.deliveryToken, item.canonicalItem);
      if (completion.direction !== "incoming") {
        return yield* new EndpointStoreError({ reason: "conflict" });
      }
      requireSameBytes(completion.canonicalCandidate, item.canonicalItem);
      const disposition = yield* saveCheckDecision(completion);
      if (disposition === "admitted") {
        yield* putInboxItem(item);
      }
      yield* settleQueuedItem(
        item.deliveryToken,
        disposition,
        completion.checkId,
      );
      return disposition;
    }),
  ).pipe(Effect.withSpan("saveMessageDecision"));
}
/** Result text, answer visibility and completed checks commit together. */
export function saveResultDecision(admission: LocalAdmission) {
  return transaction(
    Effect.gen(function* () {
      yield* settleLocalItem(admission);
      yield* putInboxItem(admission.item);
      return "admitted" as const;
    }),
  ).pipe(Effect.withSpan("saveResultDecision"));
}
function initializeDatabase(options: Parameters<typeof layer>[0]) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const opening = yield* inspectStoreState();
    if (opening === "reopen") {
      yield* requireLiveOutbox(options.liveWireVersions);
    }
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "PRAGMA locking_mode = EXCLUSIVE",
    );
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "PRAGMA foreign_keys = ON",
    );
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "PRAGMA busy_timeout = 0",
    );
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "PRAGMA journal_mode = WAL",
    );
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "PRAGMA synchronous = FULL",
    );
    yield* transaction(
      Effect.gen(function* () {
        if (opening === "create") {
          yield* createCommunicationTables();
          yield* createInboxTables();
          yield* createFirewallTables();
        }
      }),
    );
    const filesystem = yield* FileSystem.FileSystem;
    yield* filesystem.chmod(join(options.stateDirectory, DATABASE_NAME), 0o600);
  }).pipe(Effect.withSpan("initializeDaemonDatabase"));
}
function requireLiveOutbox(liveWireVersions: readonly string[]) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const pending = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT wire_version FROM outbound_messages WHERE disposition='pending'",
    );
    for (const row of pending) {
      if (!liveWireVersions.includes(readText(row, "wire_version"))) {
        return yield* new OutboxTooOldError();
      }
    }
  });
}
