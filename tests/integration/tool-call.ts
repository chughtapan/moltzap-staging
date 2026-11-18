/** @file Calls a daemon tool as a host does, with the official MCP client. */

import type { Client } from "@modelcontextprotocol/client";
import {
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "@social-harness/social-harness";
import { Effect, Schema } from "effect";

const fields = Schema.decodeUnknownSync(
  Schema.Record({ key: Schema.String, value: Schema.Unknown }),
);

export function callTool<Name extends ToolName>(
  mcp: Pick<Client, "callTool">,
  request: ToolRequest & { readonly name: Name },
): Effect.Effect<ToolResult<Name>, Readonly<Record<string, unknown>>>;
/**
 * Calls the named tool once. A result is decoded with the tool's published
 * output schema, so a result that drifts from the contract fails the test. A
 * call the daemon answers as failed fails with the result's structured fields
 * and, as `message`, the text a model reads.
 */
export function callTool(
  mcp: Pick<Client, "callTool">,
  request: ToolRequest,
): Effect.Effect<unknown, Readonly<Record<string, unknown>>> {
  const output: Schema.Schema.AnyNoContext =
    toolDefinitions[request.name].output;
  return Effect.tryPromise((signal) =>
    mcp.callTool(
      { name: request.name, arguments: { ...request.arguments } },
      { signal },
    ),
  ).pipe(
    Effect.orDie,
    Effect.flatMap((result) =>
      result.isError === true
        ? Effect.fail({
            ...fields(result.structuredContent ?? {}),
            message: result.content
              .flatMap((part) => (part.type === "text" ? [part.text] : []))
              .join("\n"),
          })
        : Schema.decodeUnknown(output)(result.structuredContent).pipe(
            Effect.map((decoded): unknown => decoded),
            Effect.orDie,
          ),
    ),
  );
}
