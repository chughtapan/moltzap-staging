/** @file Scoped JSONL transport for Codex's documented app-server API. */
import { Command } from "@effect/platform";
import { packageVersion } from "@moltzap/social-harness";
import { Data, Deferred, Effect, Queue, Ref, Schema, Stream } from "effect";

/** Diagnostics exclude native RPC errors, tool arguments and connection secrets. */
export class CodexHostError extends Data.TaggedError("CodexHostError")<{
  readonly reason:
    | "host-refused"
    | "host-closed"
    | "invalid-response"
    | "binding-failed";
}> {
  override get message(): string {
    return `Codex receiver failed (${this.reason}). Docs: https://github.com/social-harness/moltzap/tree/main/plugins/codex#receiving-work`;
  }
}

const frameSchema = Schema.parseJson(
  Schema.Struct({
    id: Schema.optional(Schema.Union(Schema.Number, Schema.String)),
    method: Schema.optional(Schema.String),
    params: Schema.optional(Schema.Unknown),
    result: Schema.optional(Schema.Unknown),
    error: Schema.optional(Schema.Unknown),
  }),
);

type Reply = Deferred.Deferred<unknown, CodexHostError>;

/** Process exit fails every outstanding request and the host's idle waiter. */
export function acquireCodexAppServer(options: {
  readonly binary: string;
  readonly directory: string;
  readonly onCompleted: (value: unknown) => Effect.Effect<void, CodexHostError>;
}) {
  return Effect.gen(function* () {
    const outgoing = yield* Queue.unbounded<string>();
    yield* Effect.addFinalizer(() => Queue.shutdown(outgoing));
    const pending = new Map<number, Reply>();
    const closed = yield* Ref.make(false);
    const stopped = yield* Deferred.make<never, CodexHostError>();
    const sequence = yield* Ref.make(0);
    const write = (value: unknown) =>
      Queue.offer(outgoing, JSON.stringify(value) + "\n").pipe(Effect.asVoid);
    const failPending = Ref.set(closed, true).pipe(
      Effect.zipRight(
        Deferred.fail(stopped, new CodexHostError({ reason: "host-closed" })),
      ),
      Effect.zipRight(
        Effect.forEach(
          pending.values(),
          (reply) =>
            Deferred.fail(reply, new CodexHostError({ reason: "host-closed" })),
          { discard: true, concurrency: "inherit" },
        ),
      ),
      Effect.asVoid,
    );
    yield* Effect.addFinalizer(() => failPending);
    const process = yield* Command.make(
      options.binary,
      "app-server",
      "--listen",
      "stdio://",
    ).pipe(
      Command.workingDirectory(options.directory),
      Command.stdin(Stream.fromQueue(outgoing).pipe(Stream.encodeText)),
      Command.stderr("inherit"),
      Command.start,
      Effect.catchTags({
        SystemError: () =>
          Effect.fail(new CodexHostError({ reason: "host-closed" })),
        BadArgument: () =>
          Effect.fail(new CodexHostError({ reason: "host-closed" })),
      }),
    );
    const receive = (line: string) =>
      handleFrame({ pending, write, onCompleted: options.onCompleted }, line);
    yield* process.stdout.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.runForEach(receive),
      Effect.ensuring(failPending),
      Effect.forkScoped,
    );
    yield* process.exitCode.pipe(
      Effect.ensuring(failPending),
      Effect.forkScoped,
    );
    const request = (method: string, params: unknown) =>
      requestFrame({ pending, sequence, closed, write }, method, params);
    yield* request("initialize", {
      clientInfo: { name: "social_harness_receiver", version: packageVersion },
    });
    yield* write({ method: "initialized", params: {} });
    return { request, stopped: Deferred.await(stopped) };
  }).pipe(Effect.withSpan("Codex.acquireAppServer"));
}

function handleFrame(
  state: {
    readonly pending: Map<number, Reply>;
    readonly write: (value: unknown) => Effect.Effect<void>;
    readonly onCompleted: (
      value: unknown,
    ) => Effect.Effect<void, CodexHostError>;
  },
  line: string,
) {
  return Effect.gen(function* () {
    const frame = yield* Schema.decodeUnknown(frameSchema)(line).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new CodexHostError({ reason: "invalid-response" })),
      ),
    );
    if (frame.method !== undefined && frame.id !== undefined) {
      yield* Effect.logWarning("Codex requires owner interaction").pipe(
        Effect.annotateLogs({ method: frame.method }),
      );
      yield* state.write({
        id: frame.id,
        error: {
          code: -32601,
          message:
            "This receiver cannot approve interactive requests. Configure native permissions before unattended use. Docs: https://github.com/social-harness/moltzap/tree/main/plugins/codex#receiving-work",
        },
      });
    } else if (frame.method === "turn/completed") {
      yield* state.onCompleted(frame.params);
    } else if (typeof frame.id === "number") {
      const reply = state.pending.get(frame.id);
      if (reply !== undefined) {
        yield* frame.error === undefined
          ? Deferred.succeed(reply, frame.result)
          : Deferred.fail(
              reply,
              new CodexHostError({ reason: "host-refused" }),
            );
      }
    }
  });
}

function requestFrame(
  state: {
    readonly pending: Map<number, Reply>;
    readonly sequence: Ref.Ref<number>;
    readonly closed: Ref.Ref<boolean>;
    readonly write: (value: unknown) => Effect.Effect<void>;
  },
  method: string,
  params: unknown,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const ticket = yield* Effect.acquireRelease(
        Effect.gen(function* () {
          if (yield* Ref.get(state.closed)) {
            return yield* new CodexHostError({ reason: "host-closed" });
          }
          const id = yield* Ref.updateAndGet(
            state.sequence,
            (value) => value + 1,
          );
          const reply = yield* Deferred.make<unknown, CodexHostError>();
          state.pending.set(id, reply);
          return { id, reply };
        }),
        ({ id }) =>
          Effect.sync(() => {
            state.pending.delete(id);
          }),
      );
      yield* state.write({ id: ticket.id, method, params });
      return yield* Deferred.await(ticket.reply);
    }),
  );
}
