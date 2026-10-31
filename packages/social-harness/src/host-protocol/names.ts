/** @file The MCP tool, Events, and metadata names shared by hosts and the daemon. */

/** A content-free wakeup to read the classified runtime inbox. */
export const INBOX_PENDING_EVENT = "org.social-harness/inbox.pending";

/** Classified content shared by polling and webhook consumers. */
export const INBOX_ITEM_EVENT = "org.social-harness/inbox.item";

/** Ephemeral consumer binding, independent of request authentication. */
export const HARNESS_CONSUMER_META_KEY = "org.social-harness/consumer";

/** Semantic retrieval of content referenced by a large webhook event. */
export const HARNESS_READ_EVENT_TOOL = "read_event";

/** Adapter operation performing one collective operation. */
export const HARNESS_SEND_TOOL = "send_message";

/** Retires an inbox item after its host-specific acceptance contract is met. */
export const HARNESS_ACKNOWLEDGE_DELIVERY_TOOL = "acknowledge_delivery";

/** Runtime read of classified deliveries whose host acceptance is pending. */
export const HARNESS_READ_INBOX_TOOL = "read_inbox";
