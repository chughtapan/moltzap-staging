/** @file JSON text, exact structs, identifier checks and validation issue text shared by every layer that reads or writes external data. */

import type { ParseOptions } from "effect/SchemaAST";
import { Array, Data, Effect, Encoding, Schema } from "effect";

/** Rejects missing required fields and unknown properties. */
const exactParseOptions = {
  exact: true,
  onExcessProperty: "error",
} satisfies ParseOptions;

/** Rejects properties outside the supplied fields. */
export const exactStruct = <Fields extends Schema.Struct.Fields>(
  fields: Fields,
) => Schema.Struct(fields).annotations({ parseOptions: exactParseOptions });

/** Byte limit for a saved item or history window, including guidance. */
export const MAXIMUM_RUNTIME_ITEM_BYTES = 2 * 1024 * 1024;

/** A value did not encode to JSON text, or JSON text did not decode. */
export class JsonRepresentationError extends Data.TaggedError(
  "JsonRepresentationError",
) {}

/** Reports a representation failure without exposing decoder details. */
export const representationFailure = (): JsonRepresentationError =>
  new JsonRepresentationError();

/** The JSON text of a schema's encoding of a value. */
export const encodeJson = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  value: A,
): Effect.Effect<string, JsonRepresentationError, R> =>
  Schema.encode(Schema.parseJson(schema))(value).pipe(
    Effect.mapError(representationFailure),
  );

/** Reads JSON text through a schema, rejecting properties the schema does not name. */
export const decodeJson = Effect.fn("decodeJson")(
  <A, I, R>(schema: Schema.Schema<A, I, R>, text: string) =>
    Schema.decodeUnknown(
      Schema.parseJson(schema),
      exactParseOptions,
    )(text).pipe(Effect.mapError(representationFailure)),
);

/**
 * Compares ASCII names in the order used by group addresses.
 */
export const compareAscii = (left: string, right: string): number => {
  const sharedLength = Math.min(left.length, right.length);
  for (let index = 0; index < sharedLength; index += 1) {
    const difference = left.charCodeAt(index) - right.charCodeAt(index);
    if (difference !== 0) {
      return difference;
    }
  }
  return left.length - right.length;
};

/** Rejects duplicate, missing or out-of-order values. */
export function isStrictlyAscending<Value>(
  values: readonly Value[],
  compare: (left: Value, right: Value) => number,
): boolean {
  for (let index = 1; index < values.length; index += 1) {
    const previous = values[index - 1];
    const current = values[index];
    if (
      previous === undefined ||
      current === undefined ||
      compare(previous, current) >= 0
    ) {
      return false;
    }
  }
  return true;
}

/**
 * Keeps the prefix, decoded byte length and canonical base64url spelling identical
 * in runtime validation and generated JSON Schema.
 */
export const canonicalIdentifier = <const Name extends string>(
  name: Name,
  prefix: string,
  byteLength: number,
) => {
  const wholeCharacters = Math.floor((byteLength * 8) / 6);
  const remainingBits = (byteLength * 8) % 6;
  const finalCharacter =
    remainingBits === 0
      ? ""
      : `[${Array.makeBy(2 ** remainingBits, (value) =>
          Encoding.encodeBase64Url(
            Uint8Array.of(value << (8 - remainingBits)),
          ).charAt(0),
        ).join("")}]`;
  const pattern = new RegExp(
    `^${prefix}[A-Za-z0-9_-]{${wholeCharacters}}${finalCharacter}$`,
    "u",
  );
  const characters = Math.ceil((byteLength * 8) / 6);
  return Schema.String.pipe(
    Schema.length(prefix.length + characters),
    Schema.pattern(pattern),
    Schema.brand(name),
    Schema.annotations({
      identifier: name,
      description: `${name}: ${prefix} then ${String(characters)} base64url characters`,
    }),
  );
};

/**
 * The URL `value` names when it parses, uses one of `protocols` (each with its
 * colon) and carries no userinfo and no fragment; otherwise `undefined`.
 */
export const plainUrl = (
  value: string | URL,
  protocols: readonly string[],
): URL | undefined => {
  if (!URL.canParse(value)) {
    return undefined;
  }
  const url = new URL(value);
  return protocols.includes(url.protocol) &&
    [url.username, url.password, url.hash].every((part) => part === "")
    ? url
    : undefined;
};
