/** @file Writes schema values as JSON text, reads them back and compares two texts. */

import { Data, Effect, Option, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import { exactParseOptions } from "./exact-struct.js";

/** Byte limit for a saved item or history window, including guidance. */
export const MAXIMUM_RUNTIME_ITEM_BYTES = 2 * 1024 * 1024;

/** A value did not encode to JSON text, or JSON text did not decode. */
export class ClientRepresentationError extends Data.TaggedError(
  "ClientRepresentationError",
) {}

/** Reports a representation failure without exposing decoder details. */
export const representationFailure = (): ClientRepresentationError =>
  new ClientRepresentationError();

/** The JSON text of a schema's encoding of a value. */
export const encodeJson = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  value: A,
): Effect.Effect<string, ClientRepresentationError, R> =>
  Schema.encode(Schema.parseJson(schema))(value).pipe(
    Effect.mapError(representationFailure),
  );

/** Reads JSON text through a schema, rejecting properties the schema does not name. */
export const decodeJson = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  text: string,
): Effect.Effect<A, ClientRepresentationError, R> =>
  Schema.decodeUnknown(
    Schema.parseJson(schema),
    exactParseOptions,
  )(text).pipe(
    Effect.mapError(representationFailure),
    Effect.withSpan("decodeJson"),
  );

const parseJson = Option.liftThrowable((text: string): unknown =>
  JSON.parse(text),
);

/**
 * Whether two JSON texts hold the same value. The order of an object's members
 * and the spelling of a number do not matter; text that is not JSON equals
 * nothing.
 */
export const sameJson = (left: string, right: string): boolean =>
  Option.zipWith(parseJson(left), parseJson(right), isDeepStrictEqual).pipe(
    Option.getOrElse(() => false),
  );
