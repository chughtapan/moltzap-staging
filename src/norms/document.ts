/** @file Reads norm declarations without interpreting their policy. */

import { Data, Effect, Either } from "effect";
import { readSExpressions, type SExpression } from "../sexpr.js";

/** Source diagnostics identify a declaration without echoing private document text. */
export class NormDocumentError extends Data.TaggedError("NormDocumentError")<{
  readonly detail: string;
  readonly declaration?: string;
}> {}

/** A declaration separates named attributes from its child forms and text. */
export interface Declaration {
  readonly kind: string;
  readonly name?: string;
  readonly attributes: ReadonlyMap<string, SExpression>;
  readonly children: readonly SExpression[];
}

/** Attribute names are checked by the declaration that consumes them. */
export const readDeclaration = Effect.fn("readDeclaration")(function* (
  node: SExpression,
) {
  if (node.kind !== "list") {
    return yield* new NormDocumentError({
      detail: "Expected a parenthesized declaration.",
    });
  }
  const [head, ...tail] = node.values;
  if (head?.kind !== "symbol") {
    return yield* new NormDocumentError({
      detail: "A declaration starts with a symbol.",
    });
  }
  const named = ["norm", "content", "field", "relation"].includes(head.value);
  const first = tail[0];
  if (named && (first?.kind !== "symbol" || first.value.startsWith(":"))) {
    return yield* new NormDocumentError({
      detail: "The declaration needs a name.",
      declaration: head.value,
    });
  }
  const fields = yield* declarationFields(
    named ? tail.slice(1) : tail,
    head.value,
  );
  return {
    kind: head.value,
    ...(named && first?.kind === "symbol" ? { name: first.value } : {}),
    ...fields,
  } satisfies Declaration;
});

/** Reject typos where their declaration owns the accepted vocabulary. */
export function checkAttributes(form: Declaration, allowed: readonly string[]) {
  const unknown = [...form.attributes.keys()].find(
    (key) => !allowed.includes(key),
  );
  return unknown === undefined
    ? Effect.void
    : Effect.fail(
        new NormDocumentError({
          detail: `Unknown attribute ${unknown}.`,
          declaration: form.kind,
        }),
      );
}

/** A scalar or list supplies names without comma or whitespace guessing. */
export function names(
  node?: SExpression,
): Effect.Effect<readonly string[], NormDocumentError> {
  if (node === undefined) {
    return Effect.succeed([]);
  }
  const values = node.kind === "list" ? node.values : [node];
  return Effect.forEach(
    values,
    (value) => {
      const name = atom(value);
      return name === undefined
        ? Effect.fail(
            new NormDocumentError({
              detail: "Expected a name or a list of names.",
            }),
          )
        : Effect.succeed(name);
    },
    { concurrency: 1 },
  );
}

/** Missing booleans default to false; other values are rejected. */
export function booleanAttribute(form: Declaration, key: string) {
  const node = form.attributes.get(key);
  const value = atom(node);
  if (node === undefined || value === "false") {
    return Effect.succeed(false);
  }
  return value === "true"
    ? Effect.succeed(true)
    : Effect.fail(
        new NormDocumentError({ detail: `${key} takes true or false.` }),
      );
}

/** Numeric constraints cannot carry nonfinite values. */
export function numberAttribute(form: Declaration, key: string) {
  const node = form.attributes.get(key);
  if (node === undefined) {
    return Effect.succeed(undefined);
  }
  const text = atom(node);
  const value =
    text === undefined || text.trim() === "" ? Number.NaN : Number(text);
  return Number.isFinite(value)
    ? Effect.succeed(value)
    : Effect.fail(
        new NormDocumentError({ detail: `${key} takes a finite number.` }),
      );
}

/** Symbols and strings both name configuration values; quoted prose stays unchanged. */
export function atom(node?: SExpression): string | undefined {
  return node?.kind === "symbol" || node?.kind === "string"
    ? node.value
    : undefined;
}
/** All declarations are data; a document has one leading norm header. */
export const readDocument = Effect.fn("readDocument")(function* (
  source: string,
) {
  const parsed = yield* readSExpressions(source).pipe(
    Either.mapLeft(
      (error) =>
        new NormDocumentError({
          detail: `${error.detail} Offset ${error.offset}.`,
        }),
    ),
  );
  const declarations = yield* Effect.forEach(
    parsed,
    (node) => readDeclaration(node),
    { concurrency: 1 },
  );
  const [header, ...body] = declarations;
  if (
    header?.kind !== "norm" ||
    body.some((item) => item.kind !== "content" && item.kind !== "rule")
  ) {
    return yield* new NormDocumentError({
      detail:
        "A document starts with one norm declaration followed by content and rule declarations.",
    });
  }
  return { header, body };
});

function declarationFields(items: readonly SExpression[], declaration: string) {
  return Effect.gen(function* () {
    const attributes = new Map<string, SExpression>();
    const children: SExpression[] = [];
    for (let index = 0; index < items.length; index += 1) {
      const item = items[index];
      if (item?.kind === "symbol" && item.value.startsWith(":")) {
        const value = items[index + 1];
        if (
          value === undefined ||
          attributes.has(item.value) ||
          (value.kind === "symbol" && value.value.startsWith(":"))
        ) {
          return yield* new NormDocumentError({
            detail: "Each attribute needs one value and must appear once.",
            declaration,
          });
        }
        attributes.set(item.value, value);
        index += 1;
      } else if (item !== undefined) {
        children.push(item);
      }
    }
    return { attributes, children };
  });
}
