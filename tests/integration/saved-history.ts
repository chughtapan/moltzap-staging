/** @file Message content exposed by saved daemon history views. */

import type { HistoryRecord, Message } from "@social-harness/social-harness";
import assert from "node:assert/strict";

/** Direct-message scenarios require a saved message for every received record. */
export function historyMessage(record: HistoryRecord): Message {
  if (record.kind === "sent") {
    return record.message;
  }
  assert(
    record.item.kind === "message",
    `Expected a saved message, received ${record.item.kind}`,
  );
  return record.item.message;
}
