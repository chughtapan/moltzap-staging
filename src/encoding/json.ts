/** @file Writes values as JSON text and reads JSON text back through a schema. */

import { Data, Effect, Option, Predicate, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import { exactParseOptions } from "./exact-struct.js";

const MAXIMUM_CONTAINER_DEPTH = 16;

/** Byte limit for a saved item or history window, including guidance. */
export const MAXIMUM_RUNTIME_ITEM_BYTES = 2 * 1024 * 1024;

/** A value could not be written as JSON text, or JSON text did not decode. */
export class ClientRepresentationError extends Data.TaggedError(
  "ClientRepresentationError",
) {}

/** Reports a representation failure without exposing decoder details. */
export const representationFailure = (): ClientRepresentationError =>
  new ClientRepresentationError();

/**
 * The JSON text of a value, leaving out undefined object members. Rejects
 * invalid Unicode, non-finite numbers, missing array elements and more than
 * sixteen nested containers.
 */
export const jsonText = (
  value: unknown,
): Effect.Effect<string, ClientRepresentationError> =>
  Effect.try({
    try: () => (isJsonValue(value, 0) ? JSON.stringify(value) : undefined),
    catch: representationFailure,
  }).pipe(Effect.filterOrFail(Predicate.isString, representationFailure));

/** The JSON text of a schema's encoding of a value. */
export const encodeJson = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  value: A,
): Effect.Effect<string, ClientRepresentationError, R> =>
  Schema.encode(schema)(value).pipe(
    Effect.mapError(representationFailure),
    Effect.flatMap(jsonText),
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

/** Whether a string has no lone surrogate, so that it survives UTF-8. */
export function hasWellFormedUnicode(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return false;
    }
    if (codeUnit < 0xd800 || codeUnit > 0xdbff) {
      continue;
    }
    index += 1;
    const next = value.charCodeAt(index);
    if (!(next >= 0xdc00 && next <= 0xdfff)) {
      return false;
    }
  }
  return true;
}

function isJsonValue(value: unknown, depth: number): boolean {
  if (value === null || typeof value === "boolean") {
    return true;
  }
  if (typeof value === "number") {
    return Number.isFinite(value);
  }
  if (typeof value === "string") {
    return hasWellFormedUnicode(value);
  }
  if (typeof value !== "object" || depth >= MAXIMUM_CONTAINER_DEPTH) {
    return false;
  }
  if (Array.isArray(value)) {
    if (Object.keys(value).length !== value.length) {
      return false;
    }
    for (let index = 0; index < value.length; index += 1) {
      if (
        !Object.hasOwn(value, index) ||
        !isJsonValue(value[index], depth + 1)
      ) {
        return false;
      }
    }
    return true;
  }
  return Object.entries(value).every(
    ([key, member]) =>
      hasWellFormedUnicode(key) &&
      (member === undefined || isJsonValue(member, depth + 1)),
  );
}
