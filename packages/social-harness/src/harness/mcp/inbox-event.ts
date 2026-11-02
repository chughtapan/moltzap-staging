/** @file Builds host events from retained inbox items, using references for oversized payloads. */

import type { InboxEntry } from "../inbox.js";
import { maximumEventBytes } from "../../host-protocol/index.js";
import { INBOX_ITEM_EVENT } from "../../host-protocol/names.js";

/**
 * Keeps the original item and separate local guidance together. An envelope
 * exceeding the transport bound becomes a reference to the retained event;
 * neither content nor guidance is truncated.
 * @param entry The admitted, immutable inbox row.
 * @param timestamp The occurrence's Unix time in milliseconds.
 * @returns A bounded event whose identity remains stable across retries.
 */
export function makeItemEvent(entry: InboxEntry, timestamp: number) {
  const { address, item, guidance } = entry;
  const event = {
    eventId: entry.deliveryToken,
    name: INBOX_ITEM_EVENT,
    timestamp: new Date(timestamp).toISOString(),
    cursor: null,
    data: {
      kind: "item",
      address,
      item,
      ...(guidance === undefined ? {} : { guidance }),
    },
  } as const;
  return Buffer.byteLength(JSON.stringify(event), "utf8") <= maximumEventBytes
    ? event
    : {
        ...event,
        data: {
          kind: "reference" as const,
          itemKind: item.kind,
          bytes: Buffer.byteLength(
            JSON.stringify({ address, item, guidance }),
            "utf8",
          ),
        },
      };
}
