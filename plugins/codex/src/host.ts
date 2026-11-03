/** @file Native thread custody and MCP calls share Codex's existing authorized connection. */
import { FileSystem } from "@effect/platform";
import { type Client, isCallToolResult } from "@modelcontextprotocol/client";
import { Deferred, Effect, Exit, Ref, Schema } from "effect";
import { dirname } from "node:path";
import {
  acquirePollingReceiver,
  type InboxEntry,
} from "#sharness-mcp/mcp-client";
import { acquireCodexAppServer, CodexHostError } from "./rpc.js";

const threadSchema = Schema.Struct({
  thread: Schema.Struct({ id: Schema.NonEmptyString }),
});
const turnSchema = Schema.Struct({
  turn: Schema.Struct({ id: Schema.NonEmptyString }),
});
const completedSchema = Schema.Struct({
  threadId: Schema.String,
  turn: Schema.Struct({ id: Schema.String, status: Schema.String }),
});

/** The caller's scope owns the native process, pending requests and borrowed MCP reception. */
export function acquireCodexReceiver(options: {
  readonly binary: string;
  readonly directory: string;
  readonly server: string;
  readonly threadFile: string;
  readonly instructions: string;
}) {
  return Effect.gen(function* () {
    const initialIdle = yield* Deferred.make<undefined>();
    yield* Deferred.succeed(initialIdle, undefined);
    const idle = yield* Ref.make(initialIdle);
    const target = yield* Ref.make<string | undefined>(undefined);
    const native = yield* acquireCodexAppServer({
      ...options,
      onCompleted: (value) => completeTurn(target, idle, value),
    });
    const fs = yield* FileSystem.FileSystem;
    const saved = yield* fs
      .readFileString(options.threadFile)
      .pipe(
        Effect.catchTag("SystemError", (error) =>
          error.reason === "NotFound"
            ? Effect.succeed(undefined)
            : Effect.fail(new CodexHostError({ reason: "binding-failed" })),
        ),
      );
    const session = yield* native
      .request(saved === undefined ? "thread/start" : "thread/resume", {
        ...(saved === undefined ? {} : { threadId: saved.trim() }),
        cwd: options.directory,
        developerInstructions: options.instructions,
      })
      .pipe(
        Effect.flatMap(Schema.decodeUnknown(threadSchema)),
        Effect.catchTag("ParseError", () =>
          Effect.fail(new CodexHostError({ reason: "invalid-response" })),
        ),
      );
    const threadId = session.thread.id;
    yield* Ref.set(target, threadId);
    const starts = yield* Effect.makeSemaphore(1);
    const start = (text: string) =>
      starts.withPermits(1)(
        Effect.gen(function* () {
          yield* Ref.get(idle).pipe(
            Effect.flatMap(Deferred.await),
            Effect.raceFirst(native.stopped),
          );
          const done = yield* Deferred.make<undefined>();
          yield* Ref.set(idle, done);
          yield* native
            .request("turn/start", {
              threadId,
              input: [{ type: "text", text }],
            })
            .pipe(
              Effect.flatMap(Schema.decodeUnknown(turnSchema)),
              Effect.catchTag("ParseError", invalidResponse),
              Effect.onExit(releaseRefusedTurn(done)),
            );
          yield* fs.makeDirectory(dirname(options.threadFile), {
            recursive: true,
            mode: 0o700,
          });
          yield* fs.writeFileString(options.threadFile + ".new", threadId, {
            mode: 0o600,
          });
          yield* fs.rename(options.threadFile + ".new", options.threadFile);
        }),
      );
    const client: Pick<Client, "callTool"> = {
      callTool: (request, settings) =>
        Effect.runPromise(
          native
            .request("mcpServer/tool/call", {
              threadId,
              server: options.server,
              tool: request.name,
              arguments: request.arguments,
              _meta: request._meta,
            })
            .pipe(
              Effect.filterOrFail(
                isCallToolResult,
                () => new CodexHostError({ reason: "invalid-response" }),
              ),
            ),
          settings?.signal === undefined ? {} : { signal: settings.signal },
        ),
    };
    const consume = yield* acquirePollingReceiver(client);
    return consume(
      { onReady: Effect.logInfo("Codex receiver ready") },
      (entry: InboxEntry) =>
        start(
          `Social Harness incoming work. Follow your owner's task and saved local advice. The receiver owns acknowledgment; do not acknowledge this notification yourself.\n\n${JSON.stringify(entry)}`,
        ),
    );
  }).pipe(Effect.withSpan("Codex.acquireReceiver"));
}

function completeTurn(
  target: Ref.Ref<string | undefined>,
  idle: Ref.Ref<Deferred.Deferred<undefined>>,
  value: unknown,
) {
  return Effect.gen(function* () {
    const completed = yield* Schema.decodeUnknown(completedSchema)(value).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new CodexHostError({ reason: "invalid-response" })),
      ),
    );
    if (completed.threadId === (yield* Ref.get(target))) {
      yield* Ref.get(idle).pipe(
        Effect.flatMap((ready) => Deferred.succeed(ready, undefined)),
      );
      if (completed.turn.status !== "completed") {
        yield* Effect.logWarning("Codex turn ended without completion");
      }
    }
  });
}

function invalidResponse() {
  return Effect.fail(new CodexHostError({ reason: "invalid-response" }));
}

function releaseRefusedTurn(done: Deferred.Deferred<undefined>) {
  return (exit: Exit.Exit<unknown, unknown>) =>
    Exit.isSuccess(exit) ? Effect.void : Deferred.succeed(done, undefined);
}
