/** @file Combines recent messages and retained local results for firewall assessment. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect } from "effect";
import type { Ed25519PublicKey } from "../../identity/keys.js";
import type {
  FirewallEvidenceRange,
  FirewallEvidenceSlices,
  FirewallHistoryEntry,
  FirewallLocalEntry,
} from "./types.js";
import {
  hasPendingMessages,
  readRecentMessages,
} from "../../communication/conversations/index.js";
import { messageMembers } from "../../communication/message.js";
import { deriveConversationId } from "../../communication/records/index.js";
import {
  EndpointStoreError,
  readBytes,
  readInteger,
  storeOperation,
  transaction,
} from "../../communication/store/database/index.js";
import { decodeRuntimeValue } from "../../communication/store/index.js";
import { compareAgentIds } from "../../encoding/identifier.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../../encoding/limits.js";
import { InboundItem } from "../../host-protocol/items.js";

interface HistoryWindow extends FirewallEvidenceRange {
  readonly maximumRecords: number;
  readonly registrySignerPublicKey: Ed25519PublicKey;
}
interface HistoryItem {
  readonly bytes: number;
  readonly entry: FirewallHistoryEntry | FirewallLocalEntry;
}

/** The candidate itself is excluded from its recent message history. */
export function readFirewallEvidence(input: HistoryWindow) {
  return readFirewallEvidenceSlices({ ...input, ranges: [input] }).pipe(
    Effect.flatMap(({ slices }) =>
      slices[0] === undefined
        ? new EndpointStoreError({ reason: "corrupt" })
        : Effect.succeed(slices[0]),
    ),
    Effect.withSpan("readFirewallEvidence"),
  );
}

/** All texts in a result see the same committed history. */
export function readFirewallEvidenceSlices(input: {
  readonly ranges: readonly FirewallEvidenceRange[];
  readonly maximumRecords: number;
  readonly registrySignerPublicKey: Ed25519PublicKey;
}) {
  return transaction(
    Effect.gen(function* () {
      if (
        !Number.isSafeInteger(input.maximumRecords) ||
        input.maximumRecords < 1 ||
        input.maximumRecords > 1024
      ) {
        return yield* new EndpointStoreError({ reason: "invalid-input" });
      }
      const slices = yield* Effect.forEach(
        input.ranges,
        (range) =>
          readSlice({
            ...range,
            maximumRecords: input.maximumRecords,
            registrySignerPublicKey: input.registrySignerPublicKey,
          }),
        { concurrency: 1 },
      );
      return { slices } satisfies FirewallEvidenceSlices;
    }),
  ).pipe(Effect.withSpan("readFirewallEvidenceSlices"));
}

function readSlice(window: HistoryWindow) {
  return Effect.gen(function* () {
    const conversationId =
      window.conversationId ??
      (window.message === undefined
        ? undefined
        : yield* conversationOf(window.message));
    const messages = yield* messageHistory({
      ...window,
      ...(conversationId === undefined ? {} : { conversationId }),
    });
    const results = yield* resultHistory(window);
    const resultCount = Math.min(
      results.length,
      Math.max(
        window.maximumRecords - messages.length,
        Math.floor(window.maximumRecords / 2),
      ),
    );
    const selected = [
      messages.slice(0, window.maximumRecords - resultCount),
      results.slice(0, resultCount),
    ];
    const history: Array<FirewallHistoryEntry | FirewallLocalEntry> = [];
    let bytes = 0;
    for (const stream of selected) {
      const entries: Array<FirewallHistoryEntry | FirewallLocalEntry> = [];
      for (const item of stream) {
        bytes += item.bytes;
        if (bytes > MAXIMUM_RUNTIME_ITEM_BYTES) {
          break;
        }
        entries.push(item.entry);
      }
      entries.reverse();
      history.push(...entries);
      if (bytes > MAXIMUM_RUNTIME_ITEM_BYTES) {
        break;
      }
    }
    return {
      history,
      overflow: bytes > MAXIMUM_RUNTIME_ITEM_BYTES,
      earlierHistoryOmitted:
        messages.length + results.length > selected.flat().length,
      pendingOutgoing:
        conversationId === undefined
          ? false
          : yield* hasPendingMessages(conversationId),
    };
  }).pipe(storeOperation);
}

function conversationOf(
  message: NonNullable<FirewallEvidenceRange["message"]>,
) {
  const [first, second, ...rest] = messageMembers(message)
    .map((member) => member.agentId)
    .sort(compareAgentIds);
  return first === undefined || second === undefined
    ? Effect.fail(new EndpointStoreError({ reason: "corrupt" }))
    : deriveConversationId([first, second, ...rest]).pipe(
        Effect.catchTag(
          "ClientRepresentationError",
          () => new EndpointStoreError({ reason: "corrupt" }),
        ),
      );
}

function messageHistory(window: HistoryWindow) {
  return Effect.gen(function* () {
    if (window.conversationId === undefined) {
      return [];
    }
    const messages = yield* readRecentMessages({
      registrySignerPublicKey: window.registrySignerPublicKey,
      conversationId: window.conversationId,
      maximumRecords: window.maximumRecords + 1,
    });
    const sql = yield* SqlClient.SqlClient;
    return yield* Effect.forEach(
      messages.filter(
        (entry) =>
          entry.message.author.agentId !== window.message?.author.agentId ||
          entry.message.msgId !== window.message?.msgId,
      ),
      (entry) =>
        Effect.gen(function* () {
          const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
            "SELECT admitted FROM message_visibility WHERE author_agent_id=? AND msg_id=?",
            [entry.message.author.agentId, entry.message.msgId],
          ))[0];
          return {
            bytes: entry.bytes,
            entry: {
              message: entry.message,
              visible: row !== undefined && readInteger(row, "admitted") === 1,
            },
          } satisfies HistoryItem;
        }),
      { concurrency: 1 },
    );
  });
}

function resultHistory(window: HistoryWindow) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT published_item FROM incoming_items WHERE conversation=? AND author_agent_id IS NULL AND published_item IS NOT NULL ORDER BY published_sequence DESC LIMIT ?",
      [window.conversation ?? null, window.maximumRecords + 1],
    );
    return yield* Effect.forEach(
      rows,
      (row) =>
        Effect.gen(function* () {
          const bytes = readBytes(row, "published_item");
          const item = yield* decodeRuntimeValue(InboundItem, bytes);
          if (
            item.kind !== "collectiveResult" &&
            item.kind !== "operationFailed"
          ) {
            return yield* new EndpointStoreError({ reason: "corrupt" });
          }
          return {
            bytes: bytes.byteLength,
            entry: { item },
          } satisfies HistoryItem;
        }),
      { concurrency: 1 },
    );
  }).pipe(storeOperation);
}
