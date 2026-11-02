/** @file OpenCode queues admitted peer work in one persistent native session. */
import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { packageVersion } from "@moltzap/social-harness";
import { type Plugin, type PluginInput, tool } from "@opencode-ai/plugin";
import {
  Config,
  Data,
  Deferred,
  Effect,
  Exit,
  ManagedRuntime,
  Redacted,
  Scope,
} from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { acquireMcpConnection, type InboxEntry } from "#sharness-mcp/mcp";

/** Host failures expose neither credentials nor admitted peer text. */
class OpenCodeTriggerError extends Data.TaggedError("OpenCodeTriggerError")<{
  readonly reason: "binding-failed" | "host-refused";
}> {
  override get message(): string {
    return `OpenCode receive failed (${this.reason}). Docs: https://github.com/social-harness/moltzap/tree/main/plugins/opencode#receiving-work`;
  }
}

/** The native plugin owns one scoped connection and one persisted target session. */
export const socialHarness: Plugin = createPlugin;

/** Native initialization cleans up the connection if host setup fails. */
// eslint-disable-next-line agent-code-guard/async-keyword -- OpenCode loads a Promise-returning plugin and owns its async disposal boundary.
async function createPlugin(input: PluginInput): ReturnType<Plugin> {
  const runtime = ManagedRuntime.make(NodeFileSystem.layer);
  const scope = await runtime.runPromise(Scope.make());
  try {
    const receiver = await runtime.runPromise(
      makeReceiver(input.client, input.directory).pipe(Scope.extend(scope)),
    );
    return {
      // eslint-disable-next-line agent-code-guard/async-keyword -- Native disposal must close reception before disposing its runtime.
      dispose: async () => {
        try {
          await runtime.runPromise(Scope.close(scope, Exit.void));
        } finally {
          await runtime.dispose();
        }
      },
      config: (config) =>
        runtime.runPromise(
          Effect.sync(() => {
            config.mcp ??= {};
            config.mcp.social_harness = {
              type: "remote",
              url: receiver.endpoint.href,
              enabled: true,
              oauth: false,
              headers: {
                Authorization: `Bearer ${Redacted.value(receiver.credential)}`,
              },
            };
            config.instructions ??= [];
            config.instructions.push(
              fileURLToPath(new URL("./skills/*/SKILL.md", import.meta.url)),
            );
          }),
        ),
      tool: {
        social_harness_listen: tool({
          description:
            "Bind Social Harness reception to this OpenCode conversation. Repeat setup in the same conversation after reinstalling.",
          args: {},
          execute: (...[, context]) =>
            runtime.runPromise(
              receiver
                .bind(context.sessionID)
                .pipe(
                  Effect.as(
                    "Social Harness receives incoming work in this conversation.",
                  ),
                ),
            ),
        }),
      },
    };
  } catch (error) {
    try {
      await runtime.runPromise(Scope.close(scope, Exit.void));
    } finally {
      await runtime.dispose();
    }
    throw error;
  }
}

function makeReceiver(native: PluginInput["client"], directory: string) {
  return Effect.gen(function* () {
    const settings = yield* Config.all({
      endpoint: Config.url("SHARNESS_MCP_URL"),
      credential: Config.redacted("SHARNESS_MCP_TOKEN"),
    });
    const fs = yield* FileSystem.FileSystem;
    const target = yield* Deferred.make<string>();
    const binding = join(directory, ".opencode", "social-harness-session");
    const saved = yield* fs
      .readFileString(binding)
      .pipe(
        Effect.catchTag("SystemError", (error) =>
          error.reason === "NotFound"
            ? Effect.succeed(undefined)
            : Effect.fail(
                new OpenCodeTriggerError({ reason: "binding-failed" }),
              ),
        ),
      );
    if (saved !== undefined && saved.length > 0) {
      yield* Deferred.succeed(target, saved);
    }
    const connection = yield* acquireMcpConnection(settings, {
      name: "sharness-opencode",
      version: packageVersion,
    });
    const accept = (entry: InboxEntry) => queueEntry(native, target, entry);
    yield* connection.consumeInbox({}, accept).pipe(
      Effect.catchAll((error) => Effect.logError(error)),
      Effect.forkScoped,
    );
    const bindingLock = yield* Effect.makeSemaphore(1);
    const bind = (sessionID: string) =>
      Effect.gen(function* () {
        if (!(yield* Deferred.isDone(target))) {
          yield* fs.makeDirectory(join(directory, ".opencode"), {
            recursive: true,
            mode: 0o700,
          });
          yield* fs.writeFileString(binding, sessionID, { mode: 0o600 });
          yield* Deferred.succeed(target, sessionID);
        }
        if ((yield* Deferred.await(target)) !== sessionID) {
          return yield* new OpenCodeTriggerError({ reason: "binding-failed" });
        }
      });
    return {
      ...settings,
      bind: (sessionID: string) => bindingLock.withPermits(1)(bind(sessionID)),
    };
  });
}

/** The native completed response is the acceptance boundary for shared acknowledgment. */
function queueEntry(
  native: PluginInput["client"],
  target: Deferred.Deferred<string>,
  entry: InboxEntry,
) {
  return Effect.gen(function* () {
    const sessionID = yield* Deferred.await(target);
    const response = yield* Effect.tryPromise({
      try: (signal) =>
        native.session.prompt({
          path: { id: sessionID },
          body: {
            parts: [
              {
                type: "text",
                text: `Social Harness incoming work. Preserve the saved local advice and follow your owner's task. The receiver owns acknowledgment; do not call acknowledge_delivery for this notification.\n\n${JSON.stringify(entry)}`,
              },
            ],
          },
          signal,
          throwOnError: true,
        }),
      catch: () => new OpenCodeTriggerError({ reason: "host-refused" }),
    });
    if (response.data?.info.sessionID !== sessionID) {
      return yield* new OpenCodeTriggerError({ reason: "host-refused" });
    }
  });
}
