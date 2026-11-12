/** @file Real scoped SQL for communication, independent of policy and host storage. */
import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { layer as sqliteLayer } from "@effect/sql-sqlite-node/SqliteClient";
import { SqlClient } from "@effect/sql/SqlClient";
import { Context, Effect, Layer } from "effect";
import { join } from "node:path";
import { migrateDaemon } from "../../database/migrations.js";
/** The scope closes SQLite before its temporary state directory is removed. */
export const openTestDatabase = Effect.fn("openTestDatabase")(function* (
  directory?: string,
) {
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
    run: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(Effect.provideService(SqlClient, sql)),
  };
}, Effect.provide(NodeFileSystem.layer));
/** An open test database. A fixture given one reopens it, as a restart does. */
export type TestDatabase = Effect.Effect.Success<
  ReturnType<typeof openTestDatabase>
>;
