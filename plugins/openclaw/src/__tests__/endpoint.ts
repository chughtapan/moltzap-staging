/** @file A fake `HarnessEndpoint` and the inbound messages tests deliver through it. */

import {
  type CollectiveError,
  type FirewallSendError,
  type HarnessEndpoint,
  type InboundDelivery,
  type InboundItem,
  InboundMessage,
  type SendError,
  type SendInput,
  type SendResult,
} from "@moltzap/social-harness";
import { Effect, Schema, Stream } from "effect";
import { testAgentView } from "./guidance-fixtures.js";
import { endpointResult, postId } from "./send-results.js";

/** A fake endpoint and the sends it has recorded, in order. */
export interface FakeHarnessEndpoint {
  readonly endpoint: HarnessEndpoint;
  readonly sends: readonly SendInput[];
}

/** A direct message from alice whose content mixes text and data. */
export function directMessage(id = 1): InboundMessage {
  return Schema.decodeUnknownSync(InboundMessage)({
    kind: "direct",
    postId: postId(id),
    address: "agent:alice",
    sender: testAgentView("alice"),
    content: [
      { type: "text", text: "hello" },
      { type: "data", value: { count: 2 } },
    ],
  });
}

/** A group message from bob to alice, bob and carol. */
export function groupMessage(): InboundMessage {
  return Schema.decodeUnknownSync(InboundMessage)({
    kind: "group",
    postId: postId(2),
    address: "group:alice,bob,carol",
    sender: testAgentView("bob"),
    members: [
      testAgentView("alice"),
      testAgentView("bob"),
      testAgentView("carol"),
    ],
    content: [{ type: "text", text: "group message" }],
  });
}

/** The inbound item that delivers one message. */
export function multicast(message: InboundMessage): InboundItem {
  return { kind: "multicast", message };
}

/** A fake endpoint that delivers `items` once and logs each acknowledgment to `events`. */
export function makeInboundEndpoint(
  items: readonly InboundItem[],
  events: string[],
): FakeHarnessEndpoint {
  const deliveries = items.map(
    (item): InboundDelivery => ({
      item,
      eventId: "unused-event-reference",
      acknowledge: Effect.sync(() => {
        events.push(`ack:${itemTurnId(item)}`);
      }),
    }),
  );
  return makeEndpoint(Stream.fromIterable(deliveries), events);
}

/** A fake endpoint whose inbound stream never ends. */
export function makeListeningEndpoint(): FakeHarnessEndpoint {
  return makeEndpoint(Stream.never, []);
}

/**
 * A fake endpoint that records each send and answers it with `respond`, by
 * default the endpoint's own result for that send.
 */
export function makeEndpoint(
  messages: HarnessEndpoint["messages"],
  events: string[],
  respond: (
    input: SendInput,
    index: number,
  ) => Effect.Effect<
    SendResult,
    SendError | CollectiveError | FirewallSendError
  > = endpointResult,
): FakeHarnessEndpoint {
  const sends: SendInput[] = [];
  return {
    sends,
    endpoint: {
      send: (input) =>
        Effect.sync(() => {
          sends.push(input);
          events.push(`send:${JSON.stringify(input)}`);
          return sends.length - 1;
        }).pipe(Effect.flatMap((index) => respond(input, index))),
      messages,
    },
  };
}

/** The turn id OpenClaw sees for an item, which the fake acknowledgment logs. */
function itemTurnId(item: InboundItem): string {
  switch (item.kind) {
    case "multicast":
      return item.message.postId;
    case "collectiveRequest":
      return item.postId;
    case "collectiveResult":
      return `${item.id}:result`;
    case "operationFailed":
      return `${item.id}:failed`;
    default:
      return item satisfies never;
  }
}
