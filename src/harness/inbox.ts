/** @file Stores one agent's unchecked messages and collective results, its inbox entries and which of them are delivered. */

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
  type InboxPage,
  MessageAddress,
  PAGE_SIZE,
  StoredInboxEntry,
} from "./message-format.js";

const uncheckedRow = Schema.Struct({
  sequence: Schema.Number,
  token: DeliveryToken,
  address: MessageAddress,
  incoming: Schema.String,
});
const entryRow = Schema.Struct({
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
  phase: Schema.Literal("messages", "collectiveResults"),
  address: MessageAddress,
  communicationCursor: Schema.optional(Schema.String),
  after: Schema.NonNegativeInt,
  through: Schema.NonNegativeInt,
});
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The harness composes page positions with this private owner's cursor grammar.
const conversationsCursor = Schema.Struct({
  operation: Schema.Literal("conversations"),
  address: MessageAddress,
  communicationCursor: Schema.optional(Schema.String),
});

/**
 * A received message or collective result the firewall has not yet decided,
 * with the address and delivery token its inbox entry takes.
 */
export interface Unchecked {
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
    Record<"status", "unchecked" | "withheld" | "pending" | "delivered">
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
  const inboxRows = () =>
    queries.selectFrom("harness_items").where("agent_key", "=", agentKey);
  const cursorKey = randomBytes(32);
  const encodePage = <A, I>(
    schema: Schema.Schema<A, I>,
    position: NoInfer<A>,
  ) => encodeCursor(schema, position, cursorKey);
  const decodePage = <A, I>(schema: Schema.Schema<A, I>, cursor: string) =>
    decodeCursor(schema, cursor, cursorKey);
  const uncheckedRows = SqlSchema.findAll({
    Request: Schema.Number,
    Result: uncheckedRow,
    execute: (limit) =>
      run(
        queries
          .selectFrom("harness_items as candidate")
          .select([
            "candidate.sequence",
            "candidate.token",
            "candidate.address",
            "candidate.incoming",
          ])
          .where("candidate.agent_key", "=", agentKey)
          .where("candidate.status", "=", "unchecked")
          .where((eb) =>
            eb.not(
              eb.exists(
                eb
                  .selectFrom("harness_items as earlier")
                  .select("earlier.sequence")
                  .whereRef("earlier.agent_key", "=", "candidate.agent_key")
                  .whereRef("earlier.address", "=", "candidate.address")
                  .where("earlier.status", "=", "unchecked")
                  .whereRef("earlier.sequence", "<", "candidate.sequence"),
              ),
            ),
          )
          .orderBy("candidate.sequence")
          .limit(limit),
      ),
  });
  const storeUnchecked = Effect.fn(function* (
    address: MessageAddress,
    incoming: Incoming,
  ) {
    const encodedIncoming = yield* encodeJson(Incoming, incoming);
    const message =
      incoming.kind === "collectiveResult" ? undefined : incoming.message;
    const dedupeKey =
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
          identity: dedupeKey,
          token,
          address,
          kind: incoming.kind,
          author: message?.author.agentId ?? null,
          message_id: message?.msgId ?? null,
          incoming: encodedIncoming,
          view: null,
          status: "unchecked",
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
      Result: entryRow,
      execute: () =>
        run(
          inboxRows()
            .select(["sequence", "token", "address", "view"])
            .where("status", "=", "pending")
            .where("sequence", ">", position.after)
            .where("sequence", "<=", position.through)
            .orderBy("sequence")
            .limit(PAGE_SIZE + 1),
        ),
    })(undefined);
    const entries: InboxEntry[] = [];
    let bytes = 0;
    let after = position.after;
    for (const row of rows.slice(0, PAGE_SIZE)) {
      const size = Buffer.byteLength(row.view);
      if (bytes + size > MAXIMUM_RUNTIME_ITEM_BYTES) {
        break;
      }
      const stored = yield* decodeJson(StoredInboxEntry, row.view);
      entries.push({ ...stored, deliveryToken: row.token });
      bytes += size;
      after = row.sequence;
    }
    return {
      entries,
      ...(rows.length > entries.length
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
  const recordDecision = Effect.fn(function* (
    unchecked: Unchecked,
    decision: IncomingDecision,
    { expired }: { readonly expired: boolean },
  ) {
    let encodedEntry: string | undefined;
    if (decision.kind === "allowed" && !expired) {
      encodedEntry = yield* encodeJson(StoredInboxEntry, {
        address: unchecked.address,
        item: decision.item,
        ...(decision.guidance === undefined
          ? {}
          : { guidance: decision.guidance }),
      });
      if (
        Buffer.byteLength(encodedEntry) > MAXIMUM_RUNTIME_ITEM_BYTES &&
        decision.item.kind === "collectiveResult"
      ) {
        encodedEntry = yield* encodeJson(StoredInboxEntry, {
          address: unchecked.address,
          item: decision.item,
        });
      }
      if (Buffer.byteLength(encodedEntry) > MAXIMUM_RUNTIME_ITEM_BYTES) {
        encodedEntry = undefined;
      }
    }
    yield* run(
      queries
        .updateTable("harness_items")
        .set({
          incoming: null,
          view: encodedEntry ?? null,
          status: encodedEntry === undefined ? "withheld" : "pending",
        })
        .where("agent_key", "=", agentKey)
        .where("token", "=", unchecked.token)
        .where("status", "=", "unchecked"),
    );
    for (const permission of decision.visibility) {
      const visible =
        !expired && encodedEntry !== undefined && permission.visibleToAgent;
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
  });
  const acknowledgeDelivery = Effect.fn(function* (token: DeliveryToken) {
    const rows = yield* run(
      inboxRows()
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
        .set({ status: "delivered" })
        .where("agent_key", "=", agentKey)
        .where("token", "=", token)
        .where("status", "=", "pending"),
    );
  });
  function head() {
    return run(
      inboxRows().select((eb) =>
        eb.fn.coalesce(eb.fn.max<number>("sequence"), eb.val(0)).as("head"),
      ),
    ).pipe(Effect.map((rows) => rows[0]?.head ?? 0));
  }
  return {
    encodeCursor: encodePage,
    decodeCursor: decodePage,
    storeUnchecked: (address: MessageAddress, incoming: Incoming) =>
      storeUnchecked(address, incoming).pipe(
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
    unchecked: (limit: number) =>
      uncheckedRows(limit).pipe(
        Effect.flatMap((rows) =>
          Effect.forEach(
            rows,
            (row) =>
              decodeJson(Incoming, row.incoming).pipe(
                Effect.map((incoming): Unchecked => ({ ...row, incoming })),
              ),
            { concurrency: 1 },
          ),
        ),
        Effect.mapError(persistenceFailure),
      ),
    recordDecision: (
      unchecked: Unchecked,
      decision: IncomingDecision,
      { expired = false }: { readonly expired?: boolean } = {},
    ) =>
      recordDecision(unchecked, decision, { expired }).pipe(
        Effect.mapError(persistenceFailure),
      ),
    withholdExpired: (token: DeliveryToken) =>
      run(
        queries
          .updateTable("harness_items")
          .set({ incoming: null, status: "withheld" })
          .where("agent_key", "=", agentKey)
          .where("token", "=", token)
          .where("status", "=", "unchecked"),
      ).pipe(Effect.asVoid, Effect.mapError(persistenceFailure)),
    isCollectiveRequestVisible: (requestId: MsgId) =>
      run(
        inboxRows()
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
    storedEntry: (reference: MessageReference) =>
      run(
        inboxRows()
          .select("view")
          .where("author", "=", reference.authorAgentId)
          .where("message_id", "=", reference.msgId)
          .where("view", "is not", null)
          .limit(1),
      ).pipe(
        Effect.flatMap((rows) =>
          rows[0]?.view == null
            ? Effect.succeed(undefined)
            : decodeJson(StoredInboxEntry, rows[0].view),
        ),
        Effect.mapError(persistenceFailure),
      ),
    collectiveResultHead: (address: string) =>
      run(
        inboxRows()
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
    collectiveResultPage: (
      address: string,
      after: number,
      through: number,
      limit: number,
    ) =>
      SqlSchema.findAll({
        Request: Schema.Void,
        Result: entryRow,
        execute: () =>
          run(
            inboxRows()
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
              decodeJson(StoredInboxEntry, row.view).pipe(
                Effect.map((entry) => ({ ...row, entry })),
              ),
            { concurrency: 1 },
          ),
        ),
        Effect.mapError(persistenceFailure),
      ),
    collectiveResultAddresses: (after = "") =>
      SqlSchema.findAll({
        Request: Schema.Void,
        Result: Schema.Struct({ address: MessageAddress }),
        execute: () =>
          run(
            inboxRows()
              .select("address")
              .distinct()
              .where("address", ">", after)
              .where("kind", "=", "collectiveResult")
              .where("view", "is not", null)
              .orderBy("address")
              .limit(PAGE_SIZE + 1),
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
  position: A,
  key: Uint8Array,
) {
  return encodeJson(schema, position).pipe(
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

/** History and conversation-list positions remain private to their owning process. */
export { conversationsCursor, historyCursor };
