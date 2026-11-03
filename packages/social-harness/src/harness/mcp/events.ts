/** @file Optional MCP wake streams and durable signed webhook delivery use the saved inbox. */
import { type Context, Effect, Schema, Stream } from "effect";
import { randomUUID } from "node:crypto";
import { Identity } from "../../identity/service.js";
import {
  McpServerClient,
  ProtocolError,
} from "../../vendor/effect-mcp/McpSchema.js";
import { McpServer } from "../../vendor/effect-mcp/McpServer.js";
import { SocialHarness } from "../social-harness.js";
import {
  eventListInput,
  eventStreamInput,
  eventSubscribeInput,
  eventUnsubscribeInput,
  type HarnessWebhookEvents,
  INBOX_ITEM_EVENT,
  INBOX_PENDING_EVENT,
  itemEventDataJsonSchema,
} from "./contracts.js";
import { makeWebhookEvents } from "./webhook.js";

const decode = <A, I>(schema: Schema.Schema<A, I>, input: unknown) =>
  Schema.decodeUnknown(schema)(input).pipe(
    Effect.catchTag("ParseError", () =>
      Effect.fail(new ProtocolError(-32602, "Invalid event arguments")),
    ),
  );
const requireEvent = (
  name: string,
  expected: string,
  cursor?: string | null,
) => {
  if (name !== expected) {
    return Effect.fail(
      new ProtocolError(-32011, "Event not found", { kind: "event" }),
    );
  }
  return cursor == null
    ? Effect.void
    : Effect.fail(
        new ProtocolError(
          -32014,
          "Event replay is unavailable; read the inbox",
          { feature: "cursor" },
        ),
      );
};

/** Registers Events wake streams and webhook delivery with the protocol engine. */
export const installEvents = Effect.fn("installEvents")(function* () {
  const harness = yield* SocialHarness;
  const identity = yield* Identity;
  const server = yield* McpServer;
  const webhook = yield* makeWebhookEvents(yield* Effect.makeSemaphore(1));
  const registered = identity
    .me()
    .pipe(
      Effect.flatMap((status) =>
        status.kind === "registered"
          ? Effect.void
          : Effect.fail(
              new ProtocolError(-32012, "Endpoint is not registered"),
            ),
      ),
    );
  server.events.current = {
    list: (raw) =>
      decode(eventListInput, raw).pipe(
        Effect.flatMap((input) =>
          input.cursor === undefined
            ? Effect.succeed({
                events: [
                  {
                    name: INBOX_PENDING_EVENT,
                    description: "A wakeup to read the pending inbox.",
                    delivery: ["push"],
                    inputSchema: {
                      type: "object",
                      properties: {},
                      additionalProperties: false,
                    },
                    payloadSchema: {
                      type: "object",
                      properties: {
                        pendingCount: { type: "integer", minimum: 0 },
                      },
                      required: ["pendingCount"],
                    },
                  },
                  {
                    name: INBOX_ITEM_EVENT,
                    description:
                      "A saved inbox item or a reference retrieved through read_event.",
                    delivery: ["webhook"],
                    inputSchema: {
                      type: "object",
                      properties: {},
                      additionalProperties: false,
                    },
                    payloadSchema: itemEventDataJsonSchema,
                  },
                ],
              })
            : Effect.fail(
                new ProtocolError(-32602, "Invalid event catalog cursor"),
              ),
        ),
      ),
    stream: (raw) => streamPending(harness, registered, raw),
    subscribe: (raw) =>
      Effect.gen(function* () {
        const input = yield* decode(eventSubscribeInput, raw);
        yield* registered;
        yield* requireEvent(input.name, INBOX_ITEM_EVENT, input.cursor);
        if (input.delivery.mode !== "webhook") {
          return yield* Effect.fail(
            new ProtocolError(-32602, "Unsupported delivery mode"),
          );
        }
        return yield* webhook.subscribe(input, registered);
      }),
    unsubscribe: (raw) =>
      Effect.gen(function* () {
        const input = yield* decode(eventUnsubscribeInput, raw);
        yield* requireEvent(input.name, INBOX_ITEM_EVENT);
        yield* webhook.unsubscribe(input);
        return {};
      }),
  };
  yield* Stream.merge(
    Stream.make(undefined).pipe(Stream.concat(harness.inboxChanged)),
    Stream.tick("1 second"),
  ).pipe(
    Stream.runForEach(() =>
      webhook
        .observe()
        .pipe(
          Effect.catchAll(() => Effect.logWarning("Webhook delivery deferred")),
        ),
    ),
    Effect.forkScoped,
  );
  return webhook satisfies HarnessWebhookEvents;
}, Effect.withSpan("installEvents"));

/** Callback HTTP resources are scoped with the MCP server. */
export { webhookHttpClientLayer } from "./webhook-client.js";

function pendingInbox(harness: Context.Tag.Service<typeof SocialHarness>) {
  return Effect.gen(function* () {
    let total = 0;
    let cursor: string | undefined;
    do {
      const page = yield* harness.listInbox(
        cursor === undefined ? {} : { cursor },
      );
      total += page.items.length;
      cursor = page.nextCursor;
    } while (cursor !== undefined);
    return total;
  }).pipe(
    Effect.catchTag("InboxError", () =>
      Effect.fail(new ProtocolError(-32603, "Inbox unavailable")),
    ),
  );
}

function streamPending(
  harness: Context.Tag.Service<typeof SocialHarness>,
  registered: Effect.Effect<void, ProtocolError>,
  raw: unknown,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const input = yield* decode(eventStreamInput, raw);
      yield* registered;
      yield* requireEvent(input.name, INBOX_PENDING_EVENT, input.cursor);
      const context = yield* McpServerClient;
      const notify = context.notify;
      if (notify === undefined) {
        return yield* Effect.fail(
          new ProtocolError(-32603, "Event stream unavailable"),
        );
      }
      yield* notify("notifications/events/active", {
        cursor: null,
        truncated: false,
      });
      yield* Effect.sleep("20 seconds").pipe(
        Effect.zipRight(
          notify("notifications/events/heartbeat", { cursor: null }),
        ),
        Effect.forever,
        Effect.forkScoped,
      );
      yield* Stream.concat(Stream.make(undefined), harness.inboxChanged).pipe(
        Stream.runForEach(() =>
          pendingInbox(harness).pipe(
            Effect.flatMap((count) =>
              notify("notifications/events/event", {
                eventId: `wake_${randomUUID()}`,
                name: INBOX_PENDING_EVENT,
                timestamp: new Date().toISOString(),
                data: { pendingCount: count },
                cursor: null,
              }),
            ),
          ),
        ),
      );
      return {};
    }),
  );
}
