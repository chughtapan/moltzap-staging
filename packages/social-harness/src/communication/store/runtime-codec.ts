/** @file Exact canonical serialization for daemon-owned runtime persistence. */

import { Effect, Schema } from "effect";
import { canonicalBytes } from "../../encoding/canonical-json.js";
import { EndpointStoreError } from "./database.js";
const decoder = new TextDecoder("utf-8", { fatal: true });
/**
 * Encode immutable runtime values before they enter the endpoint store.
 * @param value Validated runtime value without process resources.
 * @returns Canonical bytes or a closed persistence failure.
 */
export const encodeRuntimeValue = (
  value: unknown,
): Effect.Effect<Uint8Array, EndpointStoreError> =>
  canonicalBytes(value).pipe(
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.fail(new EndpointStoreError({ reason: "invalid-input" })),
    ),
  );
/**
 * Validate stored bytes at their owning runtime boundary before reuse.
 * @param schema Closed schema of the stored runtime value.
 * @param bytes Retained canonical UTF-8 bytes.
 * @returns The decoded value or a closed corruption failure.
 */
export const decodeRuntimeValue = <A, I>(
  schema: Schema.Schema<A, I>,
  bytes: Uint8Array,
): Effect.Effect<A, EndpointStoreError> =>
  Effect.try({
    try: () => decoder.decode(bytes),
    catch: () => new EndpointStoreError({ reason: "corrupt" }),
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(Schema.parseJson(schema))),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
    ),
  );
