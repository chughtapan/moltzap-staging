/** @file Retains incoming work, the agent's permitted view and delivery acknowledgments. */

import { SqlClient, SqlSchema } from "@effect/sql";
import { Effect, Encoding, Schema } from "effect";
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import type {
  Message,
  MessageReference,
  MsgId,
  PaginatedRequest,
} from "../communication/index.js";
import type { IncomingDecision } from "../firewall/service.js";
import { Incoming } from "../communication/service.js";
import {
  type Compilable,
  executeQuery,
  type Generated,
  makeQueries,
  withAgentTransaction,
} from "../database/index.js";
import {
  decodeJson,
  encodeJson,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import {
  DeliveryAcknowledgeError,
  DeliveryToken,
  type InboxEntry,
  InboxError,
  InboxEvent,
  type InboxPage,
  MessageAddress,
} from "./message-format.js";

const queuedRow = Schema.Struct({
  sequence: Schema.Number,
  token: DeliveryToken,
  address: MessageAddress,
  incoming: Schema.String,
});
const savedRow = Schema.Struct({
  sequence: Schema.Number,
  token: DeliveryToken,
  address: MessageAddress,
  view: Schema.String,
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

type InboxTables = Record<
  "harness_items",
  Record<"agent_key" | "identity" | "token" | "address" | "kind", string> &
    Record<"author" | "message_id", string | null> &
    Record<"incoming" | "view", string | null> &
    Record<"sequence", Generated<number>> &
    Record<"status", "checking" | "withheld" | "pending" | "acknowledged">
> &
  Record<
    "harness_visibility",
    Record<"agent_key" | "author" | "message_id", string> &
      Record<"visible", number>
  >;

/** Captures the database and scopes opaque cursors to this inbox instance. */
export const makeInbox = Effect.fn("SocialHarness.inbox")(function* (
  agentKey: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const queries = yield* makeQueries<InboxTables>();
  const run = <Row extends object>(query: Compilable<Row>) =>
    withAgentTransaction(agentKey, executeQuery(query)).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
  const storedItems = () =>
    queries.selectFrom("harness_items").where("agent_key", "=", agentKey);
  const cursorKey = randomBytes(32);
  const encodePage = <A, I>(schema: Schema.Schema<A, I>, value: NoInfer<A>) =>
    encodeCursor(schema, value, cursorKey);
  const decodePage = <A, I>(schema: Schema.Schema<A, I>, value: string) =>
    decodeCursor(schema, value, cursorKey);
  const pendingRows = SqlSchema.findAll({
    Request: Schema.Number,
    Result: queuedRow,
    execute: (limit) =>
      run(
        queries
          .selectFrom("harness_items as item")
          .select([
            "item.sequence",
            "item.token",
            "item.address",
            "item.incoming",
          ])
          .where("item.agent_key", "=", agentKey)
          .where("item.status", "=", "checking")
          .where((eb) =>
            eb.not(
              eb.exists(
                eb
                  .selectFrom("harness_items as previous")
                  .select("previous.sequence")
                  .whereRef("previous.agent_key", "=", "item.agent_key")
                  .whereRef("previous.address", "=", "item.address")
                  .where("previous.status", "=", "checking")
                  .whereRef("previous.sequence", "<", "item.sequence"),
              ),
            ),
          )
          .orderBy("item.sequence")
          .limit(limit),
      ),
  });
  const queueIncoming = Effect.fn(function* (
    address: MessageAddress,
    incoming: Incoming,
  ) {
    const saved = yield* encodeJson(Incoming, incoming);
    const message =
      incoming.kind === "collectiveResult" ? undefined : incoming.message;
    const identity =
      incoming.kind === "collectiveResult"
        ? `result:${incoming.result.request.msgId}`
        : `message:${incoming.message.author.agentId}:${incoming.message.msgId}`;
    const token = DeliveryToken.make(
      `dlv_${randomBytes(32).toString("base64url")}`,
    );
    yield* run(
      queries
        .insertInto("harness_items")
        .values({
          agent_key: agentKey,
          identity,
          token,
          address,
          kind: incoming.kind,
          author: message?.author.agentId ?? null,
          message_id: message?.msgId ?? null,
          incoming: saved,
          view: null,
          status: "checking",
        })
        .onConflict((conflict) =>
          conflict.columns(["agent_key", "identity"]).doNothing(),
        ),
    );
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
      execute: () =>
        run(
          storedItems()
            .select(["sequence", "token", "address", "view"])
            .where("status", "=", "pending")
            .where("sequence", ">", position.after)
            .where("sequence", "<=", position.through)
            .orderBy("sequence")
            .limit(51),
        ),
    })(undefined);
    const items: InboxEntry[] = [];
    let bytes = 0;
    let after = position.after;
    for (const row of rows.slice(0, 50)) {
      const size = Buffer.byteLength(row.view);
      if (bytes + size > MAXIMUM_RUNTIME_ITEM_BYTES) {
        break;
      }
      const event = yield* decodeJson(InboxEvent, row.view);
      items.push({ ...event, deliveryToken: row.token });
      bytes += size;
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
  const publish = Effect.fn(function* (
    item: PendingItem,
    decision: IncomingDecision,
    expired: boolean,
  ) {
    let view: string | undefined;
    let event: InboxEvent | undefined;
    if (decision.kind === "allowed" && !expired) {
      event = {
        address: item.address,
        item: decision.item,
        ...(decision.guidance === undefined
          ? {}
          : { guidance: decision.guidance }),
      };
      view = yield* encodeJson(InboxEvent, event);
      if (
        Buffer.byteLength(view) > MAXIMUM_RUNTIME_ITEM_BYTES &&
        decision.item.kind === "collectiveResult"
      ) {
        event = { address: item.address, item: decision.item };
        view = yield* encodeJson(InboxEvent, event);
      }
      if (Buffer.byteLength(view) > MAXIMUM_RUNTIME_ITEM_BYTES) {
        view = undefined;
        event = undefined;
      }
    }
    yield* run(
      queries
        .updateTable("harness_items")
        .set({
          incoming: null,
          view: view ?? null,
          status: view === undefined ? "withheld" : "pending",
        })
        .where("agent_key", "=", agentKey)
        .where("token", "=", item.token)
        .where("status", "=", "checking"),
    );
    for (const permission of decision.visibility) {
      const visible =
        !expired && view !== undefined && permission.visibleToAgent;
      yield* run(
        queries
          .insertInto("harness_visibility")
          .values({
            agent_key: agentKey,
            author: permission.message.authorAgentId,
            message_id: permission.message.msgId,
            visible: visible ? 1 : 0,
          })
          .onConflict((conflict) =>
            conflict
              .columns(["agent_key", "author", "message_id"])
              .doUpdateSet((eb) => ({
                // eslint-disable-next-line agent-code-guard/then-chain -- Kysely CASE branches compile SQL; this is not Promise chaining.
                visible: eb
                  .case()
                  .when(
                    "excluded.visible",
                    ">",
                    eb.ref("harness_visibility.visible"),
                  )
                  .then(eb.ref("excluded.visible"))
                  .else(eb.ref("harness_visibility.visible"))
                  .end(),
              })),
          ),
      );
    }
    return event;
  });
  const acknowledgeDelivery = Effect.fn(function* (token: DeliveryToken) {
    const rows = yield* run(
      storedItems()
        .select("status")
        .where("token", "=", token)
        .where("view", "is not", null),
    );
    if (rows[0] === undefined) {
      return yield* new DeliveryAcknowledgeError({
        reason: "unknown-delivery",
      });
    }
    yield* run(
      queries
        .updateTable("harness_items")
        .set({ status: "acknowledged" })
        .where("agent_key", "=", agentKey)
        .where("token", "=", token)
        .where("status", "=", "pending"),
    );
  });
  function head() {
    return run(
      storedItems().select((eb) =>
        eb.fn.coalesce(eb.fn.max<number>("sequence"), eb.val(0)).as("head"),
      ),
    ).pipe(Effect.map((rows) => rows[0]?.head ?? 0));
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
              decodeJson(Incoming, row.incoming).pipe(
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
      run(
        queries
          .updateTable("harness_items")
          .set({ incoming: null, status: "withheld" })
          .where("agent_key", "=", agentKey)
          .where("token", "=", token)
          .where("status", "=", "checking"),
      ).pipe(Effect.asVoid, Effect.mapError(persistenceFailure)),
    isRequestVisible: (requestId: MsgId) =>
      run(
        storedItems()
          .select("sequence")
          .where("kind", "=", "collectiveRequest")
          .where("message_id", "=", requestId)
          .where("view", "is not", null)
          .limit(1),
      ).pipe(
        Effect.map((rows) => rows.length !== 0),
        Effect.mapError(persistenceFailure),
      ),
    visibleMessages: (messages: readonly Message[]) =>
      Effect.forEach(
        messages,
        (message) =>
          run(
            queries
              .selectFrom("harness_visibility")
              .select("visible")
              .where("agent_key", "=", agentKey)
              .where("author", "=", message.author.agentId)
              .where("message_id", "=", message.msgId),
          ).pipe(
            Effect.map((rows) => ({
              message,
              visibleToAgent: rows[0]?.visible === 1,
            })),
          ),
        { concurrency: 1 },
      ).pipe(Effect.mapError(persistenceFailure)),
    historyView: (reference: MessageReference) =>
      run(
        storedItems()
          .select("view")
          .where("author", "=", reference.authorAgentId)
          .where("message_id", "=", reference.msgId)
          .where("view", "is not", null)
          .limit(1),
      ).pipe(
        Effect.flatMap((rows) =>
          rows[0]?.view == null
            ? Effect.succeed(undefined)
            : decodeJson(InboxEvent, rows[0].view),
        ),
        Effect.mapError(persistenceFailure),
      ),
    resultHead: (address: string) =>
      run(
        storedItems()
          .select((eb) =>
            eb.fn.coalesce(eb.fn.max<number>("sequence"), eb.val(0)).as("head"),
          )
          .where("address", "=", address)
          .where("kind", "=", "collectiveResult")
          .where("view", "is not", null),
      ).pipe(
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
          run(
            storedItems()
              .select(["sequence", "token", "address", "view"])
              .where("address", "=", address)
              .where("sequence", ">", after)
              .where("sequence", "<=", through)
              .where("kind", "=", "collectiveResult")
              .where("view", "is not", null)
              .orderBy("sequence")
              .limit(limit + 1),
          ),
      })(undefined).pipe(
        Effect.flatMap((rows) =>
          Effect.forEach(
            rows,
            (row) =>
              decodeJson(InboxEvent, row.view).pipe(
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
          run(
            storedItems()
              .select("address")
              .distinct()
              .where("address", ">", after)
              .where("kind", "=", "collectiveResult")
              .where("view", "is not", null)
              .orderBy("address")
              .limit(51),
          ),
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
  return encodeJson(schema, value).pipe(
    Effect.map((text) => {
      const payload = Encoding.encodeBase64Url(text);
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
    const text = yield* Encoding.decodeBase64UrlString(payload);
    return yield* decodeJson(schema, text);
  }).pipe(
    // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- Malformed, expired and foreign cursor diagnostics share the public invalid-cursor failure.
    Effect.mapError(() => new InboxError({ reason: "invalid-cursor" })),
  );
}

/** History and catalog positions remain private to their owning process. */
export { catalogCursor, historyCursor };
