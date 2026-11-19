#!/usr/bin/env node
/**
 * @file The `sharness-mcp` executable. `call TOOL [JSON]` calls one tool of the
 * daemon at SHARNESS_MCP_URL with the JSON object as the tool's arguments, {}
 * when omitted, and prints the structured result. `--help` prints the usage,
 * and any other arguments print it to standard error. A failure exits 1 and
 * prints the reason to standard error.
 */
import { NodeRuntime } from "@effect/platform-node";
import { ProtocolError } from "@modelcontextprotocol/client";
import { Config, Console, Data, Effect, Schema } from "effect";
import { connectFromEnvironment, type McpConnection } from "./mcp/index.js";
import { packageVersion } from "./version.js";

/** What `--help` prints: the sub-command and the variables that select the daemon and the agent. */
const HELP_MESSAGE = [
  "Usage: sharness-mcp call TOOL [JSON]",
  "",
  "SHARNESS_MCP_URL: the daemon's MCP URL",
  "SHARNESS_MCP_CREDENTIAL_FILE: file holding the access token that selects the agent",
].join("\n");

/** A command that did not complete. `message` is what the reader sees. */
class CommandError extends Data.TaggedError("CommandError")<{
  readonly message: string;
}> {}

const toolArguments = Schema.parseJson(
  Schema.Record({ key: Schema.String, value: Schema.Unknown }),
);

const [command, first, second = "{}"] = process.argv.slice(2);

NodeRuntime.runMain(
  Effect.suspend(() => {
    if (command === "--help" && first === undefined) {
      return Console.log(HELP_MESSAGE);
    }
    if (command === "call" && first !== undefined) {
      return call(first, second);
    }
    return Effect.fail(commandError(HELP_MESSAGE));
  }).pipe(Effect.tapError((error) => Console.error(error.message))),
  { disableErrorReporting: true },
);

function call(tool: string, json: string): Effect.Effect<void, CommandError> {
  return Effect.gen(function* () {
    const parsed = yield* Schema.decode(toolArguments)(json).pipe(
      Effect.mapError(() =>
        commandError(
          `the arguments must be one JSON object, such as '{"to":"agent:bob","text":"Hi"}'`,
        ),
      ),
    );
    const { mcp } = yield* connect();
    const result = yield* callTool(mcp, tool, parsed);
    yield* Console.log(JSON.stringify(result, null, 2));
  }).pipe(Effect.scoped);
}

/** The host connection, with each failure worded for the person at the terminal. */
function connect() {
  return connectFromEnvironment({
    name: "sharness-mcp",
    version: packageVersion,
  }).pipe(
    Effect.catchTags({
      McpSettingsError: ({ message }) => Effect.fail(commandError(message)),
      McpConnectError: () =>
        Config.string("SHARNESS_MCP_URL").pipe(
          Effect.orElseSucceed(() => "the daemon"),
          Effect.flatMap((url) =>
            Effect.fail(
              commandError(
                `could not connect to ${url}; check that sharnessd is running and that SHARNESS_MCP_URL names its port. The MCP URL must use HTTPS or literal HTTP loopback without embedded credentials or fragments`,
              ),
            ),
          ),
        ),
    }),
  );
}

function callTool(
  mcp: McpConnection["mcp"],
  tool: string,
  args: Readonly<Record<string, unknown>>,
): Effect.Effect<unknown, CommandError> {
  return Effect.tryPromise({
    try: (signal) =>
      mcp.callTool({ name: tool, arguments: { ...args } }, { signal }),
    catch: (cause) => toolFailure(tool, cause),
  }).pipe(
    Effect.flatMap((result) =>
      result.isError === true
        ? Effect.fail(
            commandError(
              `${tool} failed: ${result.content
                .flatMap((part) => (part.type === "text" ? [part.text] : []))
                .join("\n")}`,
            ),
          )
        : Effect.succeed(result.structuredContent),
    ),
  );
}

/** The daemon's own message says what went wrong and what to do. */
function toolFailure(tool: string, cause: unknown): CommandError {
  return commandError(
    `${tool} failed: ${ProtocolError.isInstance(cause) ? cause.message : String(cause)}`,
  );
}

function commandError(message: string): CommandError {
  return new CommandError({ message });
}
