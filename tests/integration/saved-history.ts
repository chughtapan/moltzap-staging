/** @file Message content exposed by saved daemon history views. */

import type { HistoryRecord, Message } from "@moltzap/social-harness";
import assert from "node:assert/strict";

/** Direct-message scenarios require a saved multicast view for every received record. */
export function historyMessage(record: HistoryRecord): Message {
  if (record.kind === "sent") {
    return record.message;
  }
  assert(
    record.item.kind === "multicast",
    `Expected saved multicast history, received ${record.item.kind}`,
  );
  return record.item.message;
}
