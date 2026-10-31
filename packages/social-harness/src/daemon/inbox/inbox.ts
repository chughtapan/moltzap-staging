/** @file Classified inbox paging and stable runtime delivery identities. */

import type * as SqlClient from "@effect/sql/SqlClient";
import { Effect, Encoding, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type {
  HarnessReadInboxRequest,
  HarnessReadInboxResult,
} from "../../host-protocol/operations.js";
import {
  decodeRuntimeValue,
  DeliveryToken,
  encodeRuntimeValue,
  EndpointStoreError,
} from "../../communication/store/index.js";
import { exactStruct } from "../../encoding/exact.js";
import { localGuidanceSchema } from "../../host-protocol/advice.js";
import { InboundItem } from "../../host-protocol/items.js";
import { type InboxEntry, readInbox, readInboxItem } from "./store.js";

/**
 * Lookup retains the original content and guidance after acknowledgment.
 * @param token Validated event identity carried by a webhook.
 * @returns The original item or a closed lookup failure.
 */
export const readRuntimeEvent = (token: DeliveryToken) =>
  Effect.gen(function* () {
    const retained = yield* readInboxItem(token);
    if (retained === undefined) {
      return yield* Effect.fail({ reason: "unknown-event" });
    }
    return yield* decodeInboxEntry(retained);
  }).pipe(Effect.withSpan("readRuntimeEvent"));

/**
 * The retained item and the local guidance it was admitted with, if any.
 * @param retained One inbox entry's canonical item and guidance bytes.
 * @returns The decoded item and guidance, or a corrupt-store failure.
 */
export function decodeInboxEntry(
  retained: Pick<InboxEntry, "canonicalItem" | "canonicalGuidance">,
) {
  return Effect.gen(function* () {
    return {
      item: yield* decodeRuntimeValue(InboundItem, retained.canonicalItem),
      ...(retained.canonicalGuidance === undefined
        ? {}
        : {
            guidance: yield* decodeRuntimeValue(
              localGuidanceSchema,
              retained.canonicalGuidance,
            ),
          }),
    };
  }).pipe(Effect.withSpan("decodeInboxEntry"));
}

const cursorSchema = exactStruct({
  after: Schema.NonNegativeInt,
  through: Schema.NonNegativeInt,
});

const decodeCursor = (
  cursor?: string,
): Effect.Effect<
  { readonly after?: number; readonly through?: number },
  EndpointStoreError
> =>
  cursor === undefined
    ? Effect.succeed({})
    : Effect.suspend(() => Encoding.decodeBase64Url(cursor)).pipe(
        Effect.flatMap((bytes) => decodeRuntimeValue(cursorSchema, bytes)),
        Effect.catchTags({
          DecodeException: () =>
            Effect.fail(
              new EndpointStoreError({ reason: "invalid-continuation" }),
            ),
          EndpointStoreError: () =>
            Effect.fail(
              new EndpointStoreError({ reason: "invalid-continuation" }),
            ),
        }),
      );

/**
 * Read the classified projection, never the underlying collective protocol messages.
 * @param input Optional position within an earlier bounded snapshot.
 * @returns An inbox page and the continuation within that same snapshot.
 */
export const readRuntimeInbox = (
  input: HarnessReadInboxRequest,
): Effect.Effect<
  HarnessReadInboxResult,
  EndpointStoreError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const page = yield* decodeCursor(input.cursor).pipe(
      Effect.flatMap((bounds) => readInbox(bounds)),
    );
    const items = yield* Effect.forEach(
      page.items,
      (entry) =>
        Effect.gen(function* () {
          return {
            deliveryToken: entry.deliveryToken,
            ...(yield* decodeInboxEntry(entry)),
          };
        }),
      { concurrency: 1 },
    );
    if (page.nextAfter === undefined) {
      return { items };
    }
    const cursor = yield* encodeRuntimeValue({
      after: page.nextAfter,
      through: page.through,
    });
    return { items, nextCursor: Encoding.encodeBase64Url(cursor) };
  }).pipe(Effect.withSpan("readRuntimeInbox"));

/** A delivery token for a durable item emitted locally, without a Router message. */
export const mintLocalDeliveryToken = Effect.sync(() =>
  Schema.decodeUnknownSync(DeliveryToken)(
    `dlv_${randomBytes(32).toString("base64url")}`,
  ),
);
