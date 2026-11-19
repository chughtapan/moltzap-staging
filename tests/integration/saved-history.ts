/** @file Message content exposed by saved daemon history views. */

import type { HistoryRecord, Message } from "@social-harness/social-harness";

/** The message of a history record: the one sent, or the one its inbox entry holds. */
export function historyMessage(record: HistoryRecord): Message {
  return record.kind === "sent" ? record.message : record.item.message;
}
