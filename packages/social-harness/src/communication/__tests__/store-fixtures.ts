/** @file Real scoped SQL for communication, independent of policy and host storage. */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { layer as sqliteLayer } from "@effect/sql-sqlite-node/SqliteClient";
import { SqlClient } from "@effect/sql/SqlClient";
import { Context, Effect, Layer } from "effect";
import { join } from "node:path";
import * as Store from "../store.js";

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
      store: Store,
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
