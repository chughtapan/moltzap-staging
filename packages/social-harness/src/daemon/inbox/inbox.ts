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
import { readInbox, readInboxItem } from "./store.js";

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
    return {
      item: retained.item,
      ...(retained.guidance === undefined
        ? {}
        : { guidance: retained.guidance }),
    };
  }).pipe(Effect.withSpan("readRuntimeEvent"));

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
    const items = page.items.map((entry) => ({
      deliveryToken: entry.deliveryToken,
      item: entry.item,
      ...(entry.guidance === undefined ? {} : { guidance: entry.guidance }),
    }));
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
