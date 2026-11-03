/** @file Process tests accept inbox entries before making assertions on them. */
import { Effect, Stream } from "effect";
import {
  callTool,
  type McpConnection,
} from "../../plugins/shared/mcp-client.js";

/** Polls the public inbox and acknowledges each accepted entry before yielding it. */
export function inboxStream(connection: McpConnection) {
  return Stream.repeatEffect(
    callTool(connection.client, { name: "read_inbox", arguments: {} }),
  ).pipe(
    Stream.flatMap((page) =>
      page.items.length === 0
        ? Stream.fromEffect(Effect.sleep("50 millis")).pipe(Stream.drain)
        : Stream.fromIterable(page.items),
    ),
    Stream.mapEffect((entry) =>
      callTool(connection.client, {
        name: "acknowledge_delivery",
        arguments: { deliveryToken: entry.deliveryToken },
      }).pipe(Effect.as(entry)),
    ),
  );
}
