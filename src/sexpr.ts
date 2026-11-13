/** @file Reads inert parenthesized data with JSON string escaping. */

import { Data, Either } from "effect";

/** Symbols remain distinct from quoted strings and cannot execute code. */
export type SExpression =
  | { readonly kind: "symbol"; readonly value: string }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "list"; readonly values: readonly SExpression[] };

/** The source offset identifies the invalid lexical token. */
export class SExpressionError extends Data.TaggedError("SExpressionError")<{
  readonly offset: number;
  readonly detail: string;
}> {}

/** Documents allow comments; message readers account for every non-whitespace token. */
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

/** JSON escaping preserves strings through document, message and audit round trips. */
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
