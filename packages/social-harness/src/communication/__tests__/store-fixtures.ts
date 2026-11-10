/** @file Real scoped SQL for communication, independent of policy and host storage. */
import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { layer as sqliteLayer } from "@effect/sql-sqlite-node/SqliteClient";
import { SqlClient } from "@effect/sql/SqlClient";
import { Context, Effect, Layer } from "effect";
import { join } from "node:path";
import { migrateDaemon } from "../../database/migrations.js";
import * as StoreProtocol from "../conversations/store.js";
import * as StoreMessages from "../store.js";
const storeOperations = { ...StoreMessages, ...StoreProtocol };
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
    yield* migrateDaemon.pipe(Effect.provideService(SqlClient, sql));
    return {
      directory: selected,
      agentKey: selected,
      sql,
      store: testStore(selected),
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
function testStore(agentKey: string) {
  return {
    initializeCommunication: storeOperations.initializeCommunication.bind(
      undefined,
      agentKey,
    ),
    bindMessageIntent: storeOperations.bindMessageIntent.bind(
      undefined,
      agentKey,
    ),
    putConversationFoundation: storeOperations.putConversationFoundation.bind(
      undefined,
      agentKey,
    ),
    lockProposal: storeOperations.lockProposal.bind(undefined, agentKey),
    lockFirstMessageProposal: storeOperations.lockFirstMessageProposal.bind(
      undefined,
      agentKey,
    ),
    recoverStoredState: storeOperations.recoverStoredState.bind(
      undefined,
      agentKey,
    ),
    acceptMessage: storeOperations.acceptMessage.bind(undefined, agentKey),
    readAcceptedMessage: storeOperations.readAcceptedMessage.bind(
      undefined,
      agentKey,
    ),
    stageRecordAndOutbound: storeOperations.stageRecordAndOutbound.bind(
      undefined,
      agentKey,
    ),
    mergeEvidence: storeOperations.mergeEvidence.bind(undefined, agentKey),
    applyCertifiedRecord: storeOperations.applyCertifiedRecord.bind(
      undefined,
      agentKey,
    ),
    enqueueOutbound: storeOperations.enqueueOutbound.bind(undefined, agentKey),
    beginOutbound: storeOperations.beginOutbound.bind(undefined, agentKey),
    completeOutbound: storeOperations.completeOutbound.bind(
      undefined,
      agentKey,
    ),
    transaction: <A, E, R>(work: Effect.Effect<A, E, R>) =>
      storeOperations.transaction(agentKey, work),
  };
}
