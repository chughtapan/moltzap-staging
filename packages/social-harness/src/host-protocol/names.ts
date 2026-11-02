/** @file Event names identify inbox wakeups and signed callback payloads. */

/** A content-free wakeup to read the pending inbox. */
export const INBOX_PENDING_EVENT = "org.social-harness/inbox.pending";

/** Signed callbacks carry saved content or its lookup reference. */
export const INBOX_ITEM_EVENT = "org.social-harness/inbox.item";
