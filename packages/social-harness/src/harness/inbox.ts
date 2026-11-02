/** @file Retains incoming work, the agent's permitted view and delivery acknowledgments. */

import { SqlClient, SqlSchema } from "@effect/sql";
import { absurd, Effect, Encoding, Schema } from "effect";
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { AgentAddress, GroupAddress } from "../communication/messages.js";
import {
  Incoming,
  type Message,
  type MessageReference,
  type PaginatedRequest,
  PaginatedResult,
} from "../communication/service.js";
import {
  decodeCanonical,
  encodeCanonical,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding/canonical-json.js";
import { canonicalIdentifier } from "../encoding/identifier.js";
import {
  type IncomingDecision,
  LocalGuidance,
  PermittedItem,
} from "../firewall/service.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Schemas and decoded values share their public names. */

/** Canonical host address of a direct or group conversation. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const MessageAddress = Schema.Union(AgentAddress, GroupAddress);
/** @useDeclaredType */
export type MessageAddress = typeof MessageAddress.Type;

/** Stable lookup and acknowledgment identity for one saved incoming item. */
export const DeliveryToken = canonicalIdentifier("DeliveryToken", "dlv_", 32);
/** @useDeclaredType */
export type DeliveryToken = typeof DeliveryToken.Type;

/** Canonical host conversation containing this permitted item. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxEvent = Schema.Struct({
  address: MessageAddress,
  item: PermittedItem,
  guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
});
/** @useDeclaredType */
export type InboxEvent = typeof InboxEvent.Type;

/** Pending deliveries retain their token after acknowledgment. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxEntry = Schema.Struct({
  ...InboxEvent.fields,
  deliveryToken: DeliveryToken,
});
/** @useDeclaredType */
export type InboxEntry = typeof InboxEntry.Type;

/** Reading leaves entries pending. Absence of nextCursor ends the page traversal. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxPage = Schema.Struct({
  ...PaginatedResult.fields,
  items: Schema.Array(InboxEntry),
});
/** @useDeclaredType */
export type InboxPage = typeof InboxPage.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after schemas. */

/** Inbox reads reject unusable cursors and unavailable retained items. */
export class InboxError extends Schema.TaggedError<InboxError>()("InboxError", {
  reason: Schema.Literal(
    "not-registered",
    "invalid-cursor",
    "invalid-event",
    "unknown-event",
    "persistence-failed",
  ),
}) {}

/** Acknowledgment changes pending delivery, not the saved content. */
export class DeliveryAcknowledgeError extends Schema.TaggedError<DeliveryAcknowledgeError>()(
  "DeliveryAcknowledgeError",
  {
    reason: Schema.Literal(
      "not-registered",
      "unknown-delivery",
      "delivery-conflict",
      "persistence-failed",
    ),
  },
) {}

const queuedRow = Schema.Struct({
  sequence: Schema.Number,
  token: DeliveryToken,
  address: MessageAddress,
  incoming: Schema.Uint8ArrayFromSelf,
});
const savedRow = Schema.Struct({
  sequence: Schema.Number,
  token: DeliveryToken,
  address: MessageAddress,
  view: Schema.Uint8ArrayFromSelf,
});
const inboxCursor = Schema.Struct({
  operation: Schema.Literal("inbox"),
  after: Schema.NonNegativeInt,
  through: Schema.NonNegativeInt,
});
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The harness composes page positions with this private owner's cursor grammar.
const historyCursor = Schema.Struct({
  operation: Schema.Literal("history"),
  phase: Schema.Literal("messages", "results"),
  address: MessageAddress,
  communication: Schema.optional(Schema.String),
  after: Schema.NonNegativeInt,
  through: Schema.NonNegativeInt,
});
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The harness composes page positions with this private owner's cursor grammar.
const catalogCursor = Schema.Struct({
  operation: Schema.Literal("conversations"),
  address: MessageAddress,
  communication: Schema.optional(Schema.String),
});

/** Private intake data keeps one immutable value until its check finishes. */
export interface PendingItem {
  readonly sequence: number;
  readonly token: DeliveryToken;
  readonly address: MessageAddress;
  readonly incoming: Incoming;
}

/** Captures the database and scopes opaque cursors to this inbox instance. */
export const makeInbox = Effect.fn("SocialHarness.inbox")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const cursorKey = randomBytes(32);
  const encodePage = <A, I>(schema: Schema.Schema<A, I>, value: NoInfer<A>) =>
    encodeCursor(schema, value, cursorKey);
  const decodePage = <A, I>(schema: Schema.Schema<A, I>, value: string) =>
    decodeCursor(schema, value, cursorKey);
  yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS harness_items (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    identity TEXT NOT NULL UNIQUE,
    token TEXT NOT NULL UNIQUE,
    address TEXT NOT NULL,
    kind TEXT NOT NULL,
    author TEXT,
    message_id TEXT,
    operation_id TEXT,
    incoming BLOB,
    view BLOB,
    status TEXT NOT NULL CHECK(status IN ('checking','withheld','pending','acknowledged'))
  ) STRICT`);
  yield* sql.unsafe(
    "CREATE INDEX IF NOT EXISTS harness_pending ON harness_items(status, sequence)",
  );
  yield* sql.unsafe(
    "CREATE INDEX IF NOT EXISTS harness_conversation ON harness_items(address, sequence)",
  );
  yield* sql.unsafe(
    "CREATE INDEX IF NOT EXISTS harness_message ON harness_items(author, message_id)",
  );
  yield* sql.unsafe(
    "CREATE INDEX IF NOT EXISTS harness_request ON harness_items(operation_id)",
  );
  yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS harness_visibility (
    author TEXT NOT NULL, message_id TEXT NOT NULL, visible INTEGER NOT NULL,
    PRIMARY KEY(author, message_id)
  ) STRICT`);
  const pendingRows = SqlSchema.findAll({
    Request: Schema.Number,
    Result: queuedRow,
    execute: (
      limit,
    ) => sql`SELECT sequence, token, address, incoming FROM harness_items item
      WHERE status='checking' AND NOT EXISTS (
        SELECT 1 FROM harness_items previous
        WHERE previous.address=item.address AND previous.status='checking' AND previous.sequence<item.sequence
      ) ORDER BY sequence LIMIT ${limit}`,
  });
  const queueIncoming = Effect.fn(function* (
    address: MessageAddress,
    incoming: Incoming,
  ) {
    const bytes = yield* encodeCanonical(Incoming, incoming);
    const message =
      incoming.kind === "message" || incoming.kind === "request"
        ? incoming.message
        : undefined;
    let operationId: string | undefined;
    switch (incoming.kind) {
      case "result":
        operationId = incoming.result.request.operationId;
        break;
      case "request-failed":
        operationId = incoming.request.operationId;
        break;
      case "request":
        operationId = incoming.message.operation.operationId;
        break;
      case "message":
        break;
      default:
        absurd(incoming);
    }
    const identity =
      message === undefined
        ? `result:${operationId}`
        : `message:${message.author.agentId}:${message.msgId}`;
    const token = DeliveryToken.make(
      `dlv_${randomBytes(32).toString("base64url")}`,
    );
    yield* sql`INSERT INTO harness_items(identity,token,address,kind,author,message_id,operation_id,incoming,status)
      VALUES (${identity},${token},${address},${incoming.kind},${message?.author.agentId ?? null},${message?.msgId ?? null},${operationId ?? null},${bytes},'checking') ON CONFLICT(identity) DO NOTHING`;
  });
  const listInbox = Effect.fn(function* (
    request: typeof PaginatedRequest.Type = {},
  ) {
    const position =
      request.cursor === undefined
        ? { after: 0, through: yield* head() }
        : yield* decodePage(inboxCursor, request.cursor);
    const rows = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: savedRow,
      execute: () => sql`SELECT sequence,token,address,view FROM harness_items
        WHERE status='pending' AND sequence>${position.after} AND sequence<=${position.through}
        ORDER BY sequence LIMIT 51`,
    })(undefined);
    const items: InboxEntry[] = [];
    let bytes = 0;
    let after = position.after;
    for (const row of rows.slice(0, 50)) {
      if (bytes + row.view.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES) {
        break;
      }
      const event = yield* decodeCanonical(InboxEvent, row.view);
      items.push({ ...event, deliveryToken: row.token });
      bytes += row.view.byteLength;
      after = row.sequence;
    }
    return {
      items,
      ...(rows.length > items.length
        ? {
            nextCursor: yield* encodePage(inboxCursor, {
              operation: "inbox",
              after,
              through: position.through,
            }),
          }
        : {}),
    } satisfies InboxPage;
  });
  const getInboxEvent = Effect.fn(function* (token: DeliveryToken) {
    const rows = yield* SqlSchema.findAll({
      Request: DeliveryToken,
      Result: savedRow,
      execute: (token) =>
        sql`SELECT sequence,token,address,view FROM harness_items WHERE token=${token} AND view IS NOT NULL`,
    })(token);
    const row = rows[0];
    return row === undefined
      ? yield* new InboxError({ reason: "unknown-event" })
      : yield* decodeCanonical(InboxEvent, row.view);
  });
  const publish = Effect.fn(function* (
    item: PendingItem,
    decision: IncomingDecision,
    expired: boolean,
  ) {
    let view: Uint8Array | undefined;
    let event: InboxEvent | undefined;
    if (decision.kind === "allowed" && !expired) {
      event = {
        address: item.address,
        item: decision.item,
        ...(decision.guidance === undefined
          ? {}
          : { guidance: decision.guidance }),
      };
      view = yield* encodeCanonical(InboxEvent, event);
      if (
        view.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES &&
        decision.item.kind === "collectiveResult"
      ) {
        event = { address: item.address, item: decision.item };
        view = yield* encodeCanonical(InboxEvent, event);
      }
      if (view.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES) {
        view = undefined;
        event = undefined;
      }
    }
    yield* sql`UPDATE harness_items SET incoming=NULL, view=${view ?? null}, status=${view === undefined ? "withheld" : "pending"}
      WHERE token=${item.token} AND status='checking'`;
    for (const permission of decision.visibility) {
      const visible =
        !expired && view !== undefined && permission.visibleToAgent;
      yield* sql`INSERT INTO harness_visibility(author,message_id,visible)
        VALUES (${permission.message.authorAgentId},${permission.message.msgId},${visible ? 1 : 0})
        ON CONFLICT(author,message_id) DO UPDATE SET visible=MAX(visible,excluded.visible)`;
    }
    return event;
  });
  const acknowledgeDelivery = Effect.fn(function* (token: DeliveryToken) {
    const rows = yield* sql<{
      readonly status: string;
    }>`SELECT status FROM harness_items WHERE token=${token} AND view IS NOT NULL`;
    if (rows[0] === undefined) {
      return yield* new DeliveryAcknowledgeError({
        reason: "unknown-delivery",
      });
    }
    yield* sql`UPDATE harness_items SET status='acknowledged' WHERE token=${token} AND status='pending'`;
  });
  function head() {
    return sql<{
      readonly head: number;
    }>`SELECT COALESCE(MAX(sequence),0) AS head FROM harness_items`.pipe(
      Effect.map((rows) => rows[0]?.head ?? 0),
    );
  }
  return {
    encodeCursor: encodePage,
    decodeCursor: decodePage,
    queueIncoming: (address: MessageAddress, incoming: Incoming) =>
      queueIncoming(address, incoming).pipe(
        Effect.mapError(persistenceFailure),
      ),
    listInbox: (request?: typeof PaginatedRequest.Type) =>
      listInbox(request ?? {}).pipe(
        Effect.catchAll((error) =>
          error instanceof InboxError
            ? Effect.fail(error)
            : Effect.fail(persistenceFailure()),
        ),
      ),
    getInboxEvent: (token: DeliveryToken) =>
      getInboxEvent(token).pipe(
        Effect.catchAll((error) =>
          error instanceof InboxError
            ? Effect.fail(error)
            : Effect.fail(persistenceFailure()),
        ),
      ),
    acknowledgeDelivery: (token: DeliveryToken) =>
      acknowledgeDelivery(token).pipe(
        Effect.catchAll((error) =>
          error instanceof DeliveryAcknowledgeError
            ? Effect.fail(error)
            : Effect.fail(
                new DeliveryAcknowledgeError({ reason: "persistence-failed" }),
              ),
        ),
      ),
    pending: (limit: number) =>
      pendingRows(limit).pipe(
        Effect.flatMap((rows) =>
          Effect.forEach(
            rows,
            (row) =>
              decodeCanonical(Incoming, row.incoming).pipe(
                Effect.map((incoming): PendingItem => ({ ...row, incoming })),
              ),
            { concurrency: 1 },
          ),
        ),
        Effect.mapError(persistenceFailure),
      ),
    publish: (item: PendingItem, decision: IncomingDecision, expired = false) =>
      publish(item, decision, expired).pipe(
        Effect.mapError(persistenceFailure),
      ),
    retire: (token: DeliveryToken) =>
      sql`UPDATE harness_items SET incoming=NULL, status='withheld' WHERE token=${token} AND status='checking'`.pipe(
        Effect.asVoid,
        Effect.mapError(persistenceFailure),
      ),
    isRequestVisible: (operationId: string) =>
      sql<{
        readonly found: number;
      }>`SELECT 1 AS found FROM harness_items WHERE kind='request' AND operation_id=${operationId} AND view IS NOT NULL LIMIT 1`.pipe(
        Effect.map((rows) => rows.length !== 0),
        Effect.mapError(persistenceFailure),
      ),
    visibleMessages: (messages: readonly Message[]) =>
      Effect.forEach(
        messages,
        (message) =>
          sql<{
            readonly visible: number;
          }>`SELECT visible FROM harness_visibility WHERE author=${message.author.agentId} AND message_id=${message.msgId}`.pipe(
            Effect.map((rows) => ({
              message,
              visibleToAgent: rows[0]?.visible === 1,
            })),
          ),
        { concurrency: 1 },
      ).pipe(Effect.mapError(persistenceFailure)),
    historyView: (reference: MessageReference) =>
      sql<{
        readonly view: Uint8Array;
      }>`SELECT view FROM harness_items WHERE author=${reference.authorAgentId} AND message_id=${reference.msgId} AND view IS NOT NULL LIMIT 1`.pipe(
        Effect.flatMap((rows) =>
          rows[0] === undefined
            ? Effect.succeed(undefined)
            : decodeCanonical(InboxEvent, rows[0].view),
        ),
        Effect.mapError(persistenceFailure),
      ),
    resultHead: (address: string) =>
      sql<{
        readonly head: number;
      }>`SELECT COALESCE(MAX(sequence),0) AS head FROM harness_items WHERE address=${address} AND kind IN ('result','request-failed') AND view IS NOT NULL`.pipe(
        Effect.map((rows) => rows[0]?.head ?? 0),
        Effect.mapError(persistenceFailure),
      ),
    resultPage: (
      address: string,
      after: number,
      through: number,
      limit: number,
    ) =>
      SqlSchema.findAll({
        Request: Schema.Void,
        Result: savedRow,
        execute: () =>
          sql`SELECT sequence,token,address,view FROM harness_items WHERE address=${address} AND sequence>${after} AND sequence<=${through} AND kind IN ('result','request-failed') AND view IS NOT NULL ORDER BY sequence LIMIT ${limit + 1}`,
      })(undefined).pipe(
        Effect.flatMap((rows) =>
          Effect.forEach(
            rows,
            (row) =>
              decodeCanonical(InboxEvent, row.view).pipe(
                Effect.map((event) => ({ ...row, event })),
              ),
            { concurrency: 1 },
          ),
        ),
        Effect.mapError(persistenceFailure),
      ),
    resultAddresses: (after = "") =>
      SqlSchema.findAll({
        Request: Schema.Void,
        Result: Schema.Struct({ address: MessageAddress }),
        execute: () =>
          sql`SELECT DISTINCT address FROM harness_items WHERE address>${after} AND kind IN ('result','request-failed') AND view IS NOT NULL ORDER BY address LIMIT 51`,
      })(undefined).pipe(
        Effect.map((rows) => rows.map((row) => row.address)),
        Effect.mapError(persistenceFailure),
      ),
  };
});

function persistenceFailure() {
  return new InboxError({ reason: "persistence-failed" });
}

/** Binds cursor integrity and lifetime to this inbox independently of access control. */
function encodeCursor<A, I>(
  schema: Schema.Schema<A, I>,
  value: A,
  key: Uint8Array,
) {
  return encodeCanonical(schema, value).pipe(
    Effect.map((bytes) => {
      const payload = Encoding.encodeBase64Url(bytes);
      const signature = createHmac("sha256", key)
        .update(payload)
        .digest("base64url");
      return `${payload}.${signature}`;
    }),
    Effect.mapError(persistenceFailure),
  );
}

function decodeCursor<A, I>(
  schema: Schema.Schema<A, I>,
  cursor: string,
  key: Uint8Array,
) {
  return Effect.gen(function* () {
    const [payload, signature, extra] = cursor.split(".");
    if (
      payload === undefined ||
      signature === undefined ||
      extra !== undefined
    ) {
      return yield* new InboxError({ reason: "invalid-cursor" });
    }
    const supplied = yield* Encoding.decodeBase64Url(signature);
    const expected = createHmac("sha256", key).update(payload).digest();
    if (
      supplied.byteLength !== expected.byteLength ||
      !timingSafeEqual(supplied, expected)
    ) {
      return yield* new InboxError({ reason: "invalid-cursor" });
    }
    const bytes = yield* Encoding.decodeBase64Url(payload);
    return yield* decodeCanonical(schema, bytes);
  }).pipe(
    // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- Malformed, expired and foreign cursor diagnostics share the public invalid-cursor failure.
    Effect.mapError(() => new InboxError({ reason: "invalid-cursor" })),
  );
}

/** History and catalog positions remain private to their owning process. */
export { catalogCursor, historyCursor };
