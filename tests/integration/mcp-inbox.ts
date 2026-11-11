/** @file Process tests consume inbox entries through the public MCP interface. */
import { callTool, type McpConnection } from "@social-harness/social-harness";
import { Effect, Stream } from "effect";

/** Each successful inbox read records delivery before entries reach the test. */
export function inboxStream(connection: McpConnection) {
  return Stream.repeatEffect(
    callTool(connection.client, { name: "receive_messages", arguments: {} }),
  ).pipe(
    Stream.flatMap((page) =>
      page.items.length === 0
        ? Stream.fromEffect(Effect.sleep("50 millis")).pipe(Stream.drain)
        : Stream.fromIterable(page.items),
    ),
  );
}
