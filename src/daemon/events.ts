/**
 * @file Serves every agent's inbox through the MCP Events extension. One event
 * type carries the inbox: an occurrence for each entry, in the order entries
 * became pending, with the entry as its data. The SDK delivers it by poll,
 * push or webhook, and reads the inbox of the agent a subscription belongs to
 * through {@link InboxEvents.serve}.
 */

import { SqlClient } from "@effect/sql";
import { fromJsonSchema } from "@modelcontextprotocol/server";
import {
  EventsError,
  EventsExtension,
  type WebhookSubscriptionRecord,
  type WebhookSubscriptionStore,
} from "@modelcontextprotocol/server/ext/events";
import { nodeWebhookSender } from "@modelcontextprotocol/server/ext/events/node";
import { Duration, Effect, Runtime, Schema } from "effect";
import { PAGE_SIZE } from "../communication/index.js";
import { INBOX_PENDING_EVENT, InboxEntry } from "../mcp/tools.js";
import type { SocialHarness } from "./agent.js";
import { jsonSchema, objectSchema } from "./tool-server.js";

/** How long a push or webhook read of an empty inbox waits for an entry before it answers empty. */
const IDLE_WAIT = Duration.seconds(25);

/** The longest a webhook subscription lives without a refresh. */
const WEBHOOK_TTL_MILLISECONDS = 10 * 60_000;

type ReceiveAfter = SocialHarness["Type"]["receiveAfter"];

/** The daemon's Events extension and the way an agent's inbox is put behind it. */
export interface InboxEvents {
  readonly extension: EventsExtension;
  /** Answers the subscriptions of the agent `agentKey` names from `receiveAfter`. */
  readonly serve: (agentKey: string, receiveAfter: ReceiveAfter) => void;
}

/**
 * The agent a request is admitted as, which the listener puts in the request's
 * authentication info. It is the principal every subscription is kept under.
 */
export const AGENT_KEY_EXTRA = "agentKey";

/**
 * Builds the extension. A subscription's cursor is the position of the last
 * entry handed over on it. A host that subscribes without one resumes after
 * the last entry handed over to any of the agent's readers.
 * `trustedOrigins` are callback origins exempt from the HTTPS and
 * public-address rules.
 */
export const makeInboxEvents = Effect.fn("makeInboxEvents")(function* (
  trustedOrigins: readonly string[] = [],
) {
  const sql = yield* SqlClient.SqlClient;
  const run = Runtime.runPromise(yield* Effect.runtime());
  const inboxes = new Map<string, ReceiveAfter>();
  const extension = new EventsExtension({
    getPrincipal: (ctx) => {
      const agentKey = ctx.http?.authInfo?.extra?.[AGENT_KEY_EXTRA];
      return typeof agentKey === "string" ? agentKey : null;
    },
    webhookTtlMs: WEBHOOK_TTL_MILLISECONDS,
    webhookSender: nodeWebhookSender(),
    webhookStore: subscriptionStore(sql, run),
    trustedOrigins,
    checkIntervalMs: 250,
    maxSubscriptions: 100_000,
    onerror: (error) => {
      void run(Effect.logWarning("Events delivery", error.message));
    },
  });
  extension.registerEvent(INBOX_PENDING_EVENT, {
    description:
      "An inbox entry became pending. data is the entry: its conversation address, its message and any guidance.",
    inputSchema: fromJsonSchema<Record<string, never>>({
      type: "object",
      properties: {},
      additionalProperties: false,
    }),
    payloadSchema: fromJsonSchema<Record<string, unknown>>(
      objectSchema(jsonSchema(InboxEntry)),
    ),
    check: (ctx, _arguments, cursor) => {
      const receiveAfter =
        ctx.principal === null ? undefined : inboxes.get(ctx.principal);
      if (receiveAfter === undefined) {
        throw new EventsError("forbidden", "No agent answers this request");
      }
      const position = cursor === null ? null : Number(cursor);
      if (position !== null && !Number.isSafeInteger(position)) {
        throw new EventsError("invalidParams", "Unknown cursor");
      }
      return run(
        receiveAfter({
          position,
          limit: ctx.maxEvents ?? PAGE_SIZE,
          ...(ctx.mode === "poll" ? {} : { wait: IDLE_WAIT }),
        }).pipe(
          Effect.flatMap((page) =>
            Effect.forEach(page.entries, ({ entry }) =>
              Schema.encode(InboxEntry)(entry).pipe(
                Effect.map((data) => ({
                  eventId: `${entry.message.author.agentId}/${entry.message.msgId}`,
                  data,
                })),
              ),
            ).pipe(
              Effect.map((events) => ({
                events,
                cursor: String(page.position),
                hasMore: page.hasMore,
                nextPollMs: 1000,
              })),
            ),
          ),
        ),
        { signal: ctx.signal },
      );
    },
  });
  return {
    extension,
    serve: (agentKey, receiveAfter) => {
      inboxes.set(agentKey, receiveAfter);
    },
  } satisfies InboxEvents;
});

/** Webhook subscriptions in the `subscriptions` table, so they outlive the daemon. */
function subscriptionStore(
  sql: SqlClient.SqlClient,
  run: <A, E>(effect: Effect.Effect<A, E>) => Promise<A>,
): WebhookSubscriptionStore {
  const records = (rows: ReadonlyArray<{ readonly record: string }>) =>
    rows.map((row) => JSON.parse(row.record) as WebhookSubscriptionRecord);
  return {
    get: (id) =>
      run(
        sql<{
          record: string;
        }>`SELECT record FROM subscriptions WHERE id = ${id}`,
      ).then((rows) => records(rows)[0]),
    put: (record) =>
      run(
        sql`INSERT INTO subscriptions (id, name, record)
          VALUES (${record.id}, ${record.name}, ${JSON.stringify(record)})
          ON CONFLICT (id) DO UPDATE SET record = EXCLUDED.record`,
      ).then(() => undefined),
    delete: (id) =>
      run(sql`DELETE FROM subscriptions WHERE id = ${id} RETURNING id`).then(
        (rows) => rows.length > 0,
      ),
    async *listByName(name) {
      yield* records(
        await run(
          sql<{
            record: string;
          }>`SELECT record FROM subscriptions WHERE name = ${name}`,
        ),
      );
    },
  };
}
