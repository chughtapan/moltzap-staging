/** @file JSON text, exact structs, identifier checks, validation issue text and parenthesized data shared by every layer that reads or writes external data. */

import type { ParseOptions } from "effect/SchemaAST";
import { Array, Data, Effect, Either, Encoding, Option, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";

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

/**
 * One value of parenthesized data, as norm documents and acts are written: a
 * symbol, a quoted string or a list. It is inert: nothing in it is evaluated.
 */
export type SExpression =
  | { readonly kind: "symbol"; readonly value: string }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "list"; readonly values: readonly SExpression[] };

/** Text that is not parenthesized data, with the offset of the token at fault. */
export class SExpressionError extends Data.TaggedError("SExpressionError")<{
  readonly offset: number;
  readonly detail: string;
}> {}

/**
 * Reads every expression of `source`. A document may carry `;` comments; a
 * reader of a message passes `comments` false, so that every token of the
 * text is accounted for.
 */
export function readSExpressions(
  source: string,
  comments = true,
): Either.Either<readonly SExpression[], SExpressionError> {
  return Either.gen(function* () {
    const roots: SExpression[] = [];
    const stack: SExpression[][] = [roots];
    const tokens = /\s+|;[^\r\n]*|[()]|"(?:[^"\\]|\\[\s\S])*"|[^\s();"]+/uy;
    let offset = 0;
    while (offset < source.length) {
      tokens.lastIndex = offset;
      const token = tokens.exec(source)?.[0];
      if (token === undefined) {
        return yield* Either.left(
          new SExpressionError({
            offset,
            detail: "Expected a complete token.",
          }),
        );
      }
      yield* acceptToken(token, offset, stack, comments);
      offset = tokens.lastIndex;
    }
    if (stack.length !== 1) {
      return yield* Either.left(
        new SExpressionError({
          offset,
          detail: "An opening parenthesis is not closed.",
        }),
      );
    }
    return roots;
  });
}

/** The text of one expression, with strings quoted as JSON, which `readSExpressions` reads back. */
export function writeSExpression(value: SExpression): string {
  switch (value.kind) {
    case "symbol":
      return value.value;
    case "string":
      return JSON.stringify(value.value);
    case "list":
      return `(${value.values.map((item) => writeSExpression(item)).join(" ")})`;
    default: {
      const exhaustive: never = value;
      return exhaustive;
    }
  }
}

function acceptToken(
  token: string,
  offset: number,
  stack: SExpression[][],
  comments: boolean,
) {
  return Either.gen(function* () {
    const parent = stack.at(-1);
    if (parent === undefined) {
      return yield* Either.left(
        new SExpressionError({
          offset,
          detail: "Expected an enclosing expression.",
        }),
      );
    }
    if (/^\s/u.test(token)) {
      return;
    }
    if (token.startsWith(";")) {
      if (!comments) {
        return yield* Either.left(
          new SExpressionError({
            offset,
            detail:
              "A message cannot carry a comment outside its declared fields.",
          }),
        );
      }
      return;
    }
    if (token === "(") {
      const values: SExpression[] = [];
      parent.push({ kind: "list", values });
      stack.push(values);
    } else if (token === ")") {
      if (stack.length === 1) {
        return yield* Either.left(
          new SExpressionError({
            offset,
            detail: "Unexpected closing parenthesis.",
          }),
        );
      }
      stack.pop();
    } else {
      parent.push(yield* scalarToken(token, offset));
    }
  });
}

function scalarToken(
  token: string,
  offset: number,
): Either.Either<SExpression, SExpressionError> {
  if (!token.startsWith('"')) {
    return Either.right({ kind: "symbol", value: token });
  }
  return Either.gen(function* () {
    const value = yield* Either.try({
      try: (): unknown => JSON.parse(token),
      catch: () =>
        new SExpressionError({
          offset,
          detail: "Use JSON escaping inside quoted strings.",
        }),
    });
    if (typeof value !== "string" || /[\uD800-\uDFFF]/u.test(value)) {
      return yield* Either.left(
        new SExpressionError({
          offset,
          detail: "Expected a well-formed Unicode string.",
        }),
      );
    }
    return { kind: "string", value };
  });
}
