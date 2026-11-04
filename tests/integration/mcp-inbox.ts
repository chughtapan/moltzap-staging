/** @file Process tests consume inbox entries through the public MCP interface. */
import { Effect, Stream } from "effect";
import {
  callTool,
  type McpConnection,
} from "../../plugins/shared/mcp-client.js";

/** Each successful inbox read records delivery before entries reach the test. */
export function inboxStream(connection: McpConnection) {
  return Stream.repeatEffect(
    callTool(connection.client, { name: "read_inbox", arguments: {} }),
  ).pipe(
    Stream.flatMap((page) =>
      page.items.length === 0
        ? Stream.fromEffect(Effect.sleep("50 millis")).pipe(Stream.drain)
        : Stream.fromIterable(page.items),
    ),
  );
}
