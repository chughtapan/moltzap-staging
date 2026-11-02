/** @file Encodes and validates canonical JSON bytes. */

import type { ParseOptions } from "effect/SchemaAST";
import canonicalize from "canonicalize";
import { Data, Effect, Schema } from "effect";

const MAXIMUM_CONTAINER_DEPTH = 16;
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });
const utf8Encoder = new TextEncoder();

/** Rejects missing required fields and unknown properties. */
export const exactParseOptions = {
  exact: true,
  onExcessProperty: "error",
} satisfies ParseOptions;

/** Rejects properties outside the supplied fields. */
export const exactStruct = <Fields extends Schema.Struct.Fields>(
  fields: Fields,
) => Schema.Struct(fields).annotations({ parseOptions: exactParseOptions });

/** Byte limit for a saved item or history window, including guidance. */
export const MAXIMUM_RUNTIME_ITEM_BYTES = 2 * 1024 * 1024;

/* eslint-disable agent-code-guard/no-nullish-type-aliases -- JSON null is a value, not an optional absence. */
type JsonValue =
  | null
  | boolean
  | number
  | string
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };
/* eslint-enable agent-code-guard/no-nullish-type-aliases -- Restore ordinary absence rules outside JSON values. */

const jsonValueSchema = Schema.Unknown.pipe(
  Schema.filter((value): value is JsonValue => isJsonValue(value, 0), {
    identifier: "CanonicalJsonValue",
    description:
      "JSON value with well-formed Unicode and bounded container depth",
  }),
);
const jsonTextSchema = Schema.parseJson(jsonValueSchema);

/** JSON could not be encoded or decoded as canonical UTF-8. */
export class CanonicalJsonError extends Data.TaggedError(
  "CanonicalJsonError",
) {}

/** A message representation, hash or verification check failed. */
export class ClientRepresentationError extends Data.TaggedError(
  "ClientRepresentationError",
) {}

/** Reports a representation failure without exposing decoder details. */
export const representationFailure = (): ClientRepresentationError =>
  new ClientRepresentationError();

/** Requires equal lengths and equal byte values. */
export const sameBytes = (left: Uint8Array, right: Uint8Array): boolean => {
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
};

/**
 * Parses RFC 8785 canonical UTF-8 JSON with finite numbers, valid Unicode
 * and at most sixteen nested containers.
 */
export const parseCanonicalJson = (bytes: Uint8Array) =>
  parseJsonBytes(bytes).pipe(
    Effect.flatMap((parsed) =>
      Effect.try({
        try: () => {
          if (!sameBytes(parsed.bytes, encodeJsonValue(parsed.value))) {
            throw new CanonicalJsonError();
          }
          return parsed.value;
        },
        catch: () => new CanonicalJsonError(),
      }),
    ),
    Effect.withSpan("parseCanonicalJson"),
  );

/**
 * Validates canonical JSON bytes, then decodes them with the schema.
 * Schema defaults affect the returned value, not the required input bytes.
 */
export const decodeCanonicalJson = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  bytes: Uint8Array,
): Effect.Effect<A, CanonicalJsonError, R> =>
  parseCanonicalJson(bytes).pipe(
    Effect.flatMap(Schema.decodeUnknown(schema, exactParseOptions)),
    Effect.catchTag("ParseError", () => Effect.fail(new CanonicalJsonError())),
  );

/**
 * Copies a JSON value into canonical UTF-8 bytes. Rejects invalid Unicode,
 * non-finite numbers and more than sixteen nested containers.
 */
export const encodeCanonicalJson = (
  value: unknown,
): Effect.Effect<Uint8Array, CanonicalJsonError> =>
  Effect.try({
    try: () => {
      if (!isJsonValue(value, 0)) {
        throw new CanonicalJsonError();
      }
      return encodeJsonValue(value);
    },
    catch: () => new CanonicalJsonError(),
  });

/** Accepts decoded schema values, including classes and undefined optional fields. */
export const canonicalBytes = (
  value: unknown,
): Effect.Effect<Uint8Array, ClientRepresentationError> =>
  Effect.try({
    try: () => {
      if (!isJsonValue(value, 0, true)) {
        throw new CanonicalJsonError();
      }
      return encodeJsonValue(value);
    },
    catch: representationFailure,
  });

/** Encodes a schema's representation as canonical UTF-8 bytes. */
export const encodeCanonical = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  value: A,
): Effect.Effect<Uint8Array, ClientRepresentationError, R> =>
  Schema.encode(schema)(value).pipe(
    Effect.mapError(representationFailure),
    Effect.flatMap(canonicalBytes),
  );

/**
 * Requires input bytes to match the schema's canonical encoding, including
 * its defaults and transformations.
 */
export const decodeCanonical = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  bytes: Uint8Array,
): Effect.Effect<A, ClientRepresentationError, R> =>
  Effect.gen(function* () {
    const parsed = yield* parseJsonBytes(bytes).pipe(
      Effect.mapError(representationFailure),
    );
    const decoded = yield* Schema.decodeUnknown(
      schema,
      exactParseOptions,
    )(parsed.value).pipe(Effect.mapError(representationFailure));
    const canonical = yield* encodeCanonical(schema, decoded);
    if (!sameBytes(parsed.bytes, canonical)) {
      return yield* representationFailure();
    }
    return decoded;
  }).pipe(Effect.withSpan("decodeCanonical"));

/** Whether two schema values have the same canonical encoded bytes. */
export const equalCanonical = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  left: A,
  right: A,
): Effect.Effect<boolean, ClientRepresentationError, R> =>
  Effect.all([
    encodeCanonical(schema, left),
    encodeCanonical(schema, right),
  ]).pipe(Effect.map(([a, b]) => sameBytes(a, b)));

function parseJsonBytes(bytes: Uint8Array) {
  return Effect.gen(function* () {
    const captured = yield* Effect.try({
      try: () => new Uint8Array(bytes),
      catch: () => new CanonicalJsonError(),
    });
    const text = yield* Effect.try({
      try: () => utf8Decoder.decode(captured),
      catch: () => new CanonicalJsonError(),
    });
    const value = yield* Schema.decodeUnknown(jsonTextSchema)(text).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new CanonicalJsonError()),
      ),
    );
    return { bytes: captured, value };
  });
}

function encodeJsonValue(value: unknown): Uint8Array {
  const text = canonicalize(value);
  if (text === undefined) {
    throw new CanonicalJsonError();
  }
  return utf8Encoder.encode(text);
}

function isJsonValue(
  value: unknown,
  depth: number,
  schemaOwned = false,
): value is JsonValue {
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
        !isJsonValue(value[index], depth + 1, schemaOwned)
      ) {
        return false;
      }
    }
    return true;
  }
  return isJsonObject(value, depth, schemaOwned);
}

function isJsonObject(
  value: object,
  depth: number,
  schemaOwned: boolean,
): boolean {
  const prototype: unknown = Object.getPrototypeOf(value);
  if (!schemaOwned && prototype !== Object.prototype && prototype !== null) {
    return false;
  }
  if (!schemaOwned && Object.getOwnPropertySymbols(value).length > 0) {
    return false;
  }
  return Object.entries(value).every(([key, member]) => {
    if (!hasWellFormedUnicode(key)) {
      return false;
    }
    return (
      (schemaOwned && member === undefined) ||
      isJsonValue(member, depth + 1, schemaOwned)
    );
  });
}

function hasWellFormedUnicode(value: string): boolean {
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
