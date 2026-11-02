/** @file Real scoped SQL for communication, independent of policy and host storage. */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { layer as sqliteLayer } from "@effect/sql-sqlite-node/SqliteClient";
import { SqlClient } from "@effect/sql/SqlClient";
import { Context, Effect, Layer } from "effect";
import { join } from "node:path";
import {
  acceptMessage,
  applyCatchUpReanchor,
  bindMessageIntent,
  completeReanchor,
  consumeRecord,
  failMessage,
  hasPendingMessages,
  lockFirstMessageProposal,
  lockProposal,
  putConversationFoundation,
  readAcceptedMessage,
  readMemberships,
  readRecentConversation,
  readStoredConversation,
  readUnconsumedRecords,
  readUnpreparedMessages,
  recoverStoredState,
  restartEmptyConversation,
  stageReanchor,
} from "../communication/store/conversations.js";
import {
  decodeRuntimeValue,
  encodeRuntimeValue,
  EndpointStoreError,
  initializeCommunication,
  isSemanticStoreRejection,
  OutboxTooOldError,
} from "../communication/store/database.js";
import {
  beginOutbound,
  completeOutbound,
  discardOutbound,
  enqueueOutbound,
  readRecordOutbound,
} from "../communication/store/outbound.js";
import {
  applyCertifiedRecord,
  mergeEvidence,
  promoteRecord,
  stageRecordAndOutbound,
  supersedeProposalLock,
} from "../communication/store/records.js";

/** Protocol fixtures use only the owning communication operations. */
export const protocolStore = {
  EndpointStoreError,
  isSemanticStoreRejection,
  decodeRuntimeValue,
  encodeRuntimeValue,
  OutboxTooOldError,
  applyCatchUpReanchor,
  bindMessageIntent,
  completeReanchor,
  lockFirstMessageProposal,
  lockProposal,
  putConversationFoundation,
  restartEmptyConversation,
  stageReanchor,
  beginOutbound,
  completeOutbound,
  discardOutbound,
  enqueueOutbound,
  readRecordOutbound,
  consumeRecord,
  readConversation: readStoredConversation,
  readMemberships,
  readRecentConversation,
  readUnconsumedRecords,
  recover: recoverStoredState,
  applyCertifiedRecord,
  mergeEvidence,
  promoteRecord,
  stageRecordAndOutbound,
  supersedeProposalLock,
  acceptMessage,
  failMessage,
  hasPendingMessages,
  readAcceptedMessage,
  readUnpreparedMessages,
  initializeCommunication,
};
/** The fixture exposes no policy, inbox or identity persistence operations. */
export type ProtocolStore = typeof protocolStore;

/** The scope closes SQLite before its temporary state directory is removed. */
export const openTestDatabase = (directory?: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const selected =
      directory ??
      (yield* fileSystem.makeTempDirectoryScoped({
        prefix: "sharness-communication-",
      }));
    const services = yield* Layer.build(
      sqliteLayer({
        filename: join(selected, "sharnessd.sqlite3"),
        disableWAL: true,
      }),
    );
    const sql = Context.get(services, SqlClient);
    return {
      directory: selected,
      sql,
      store: protocolStore,
      run: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(Effect.provideService(SqlClient, sql)),
    };
  }).pipe(
    Effect.provide(NodeFileSystem.layer),
    Effect.withSpan("openCommunicationTestDatabase"),
  );

/** Reopening the same database exercises the production recovery readers. */
export type TestDatabase = Effect.Effect.Success<
  ReturnType<typeof openTestDatabase>
>;
