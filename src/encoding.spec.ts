/** @file JSON text round trips through a schema, and comparison of two texts. */

import { Effect, Either, Schema } from "effect";
import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  decodeJson,
  encodeJson,
  plainUrl,
  readSExpressions,
  sameJson,
  type SExpression,
  writeSExpression,
} from "./encoding.js";

describe("JSON text of schema values", () => {
  it("round-trips generated JSON through one stable text", () => {
    fc.assert(
      fc.property(fc.jsonValue(), (value) => {
        const first = Effect.runSync(encodeJson(Schema.Unknown, value));
        const parsed = Effect.runSync(decodeJson(Schema.Unknown, first));
        expect(Effect.runSync(encodeJson(Schema.Unknown, parsed))).toBe(first);
      }),
    );
  });

  it("round trips class values with omitted optional fields", () => {
    class SavedValue extends Schema.Class<SavedValue>("SavedValue")({
      text: Schema.String,
      advice: Schema.optional(Schema.String),
    }) {}
    const value = new SavedValue({
      text: "See you Tuesday",
      advice: undefined,
    });
    const text = Effect.runSync(encodeJson(SavedValue, value));
    expect(Effect.runSync(decodeJson(SavedValue, text))).toEqual(value);
  });

  it("rejects a property the schema does not name", () => {
    const closed = Schema.Struct({ a: Schema.Number });
    expect(
      Either.match(
        Effect.runSync(Effect.either(decodeJson(closed, '{"a":1,"b":2}'))),
        { onLeft: () => false, onRight: () => true },
      ),
    ).toBe(false);
  });
});

describe("comparing two JSON texts", () => {
  it("ignores member order and number spelling", () => {
    expect(
      sameJson('{"a":1,"b":[true,null]}', '{"b":[true,null],"a":1.0}'),
    ).toBe(true);
  });

  it.each([
    { left: '{"a":1}', right: '{"a":2}' },
    { left: "[1,2]", right: "[2,1]" },
    { left: '{"a":1}', right: '{"a":1,"b":null}' },
    { left: "not JSON", right: "not JSON" },
  ])("tells $left from $right", ({ left, right }) => {
    expect(sameJson(left, right)).toBe(false);
  });
});

describe("a plain URL", () => {
  it("is one that parses, uses an allowed protocol and carries no userinfo or fragment", () => {
    const protocols = ["https:", "http:", "ftp:", "ws:"];
    fc.assert(
      fc.property(
        fc.record({
          protocol: fc.constantFrom(...protocols),
          allowed: fc.subarray(protocols),
          userinfo: fc.constantFrom("", "user@", ":secret@", "user:secret@"),
          host: fc.constantFrom(
            "example.com",
            "127.0.0.1",
            "[::1]:8443",
            "not a host",
          ),
          path: fc.constantFrom("", "/", "/mcp", "/a/b"),
          query: fc.constantFrom("", "?", "?a=1"),
          fragment: fc.constantFrom("", "#", "#part"),
        }),
        ({ protocol, allowed, userinfo, host, path, query, fragment }) => {
          const text = `${protocol}//${userinfo}${host}${path}${query}${fragment}`;
          const plain =
            URL.canParse(text) &&
            allowed.includes(protocol) &&
            userinfo === "" &&
            fragment !== "#part";
          const expected = plain ? new URL(text).href : undefined;
          expect(plainUrl(text, allowed)?.href).toBe(expected);
          expect(
            plainUrl(URL.canParse(text) ? new URL(text) : text, allowed)?.href,
          ).toBe(expected);
        },
      ),
    );
  });
});

describe("parenthesized data", () => {
  /** Any expression: a symbol free of delimiters, a string, or a list of expressions. */
  const { expression } = fc.letrec<{ expression: SExpression }>((tie) => ({
    expression: fc.oneof(
      { maxDepth: 3 },
      fc
        .stringMatching(/^[^\s();"]+$/u)
        .map((value): SExpression => ({ kind: "symbol", value })),
      fc.string().map((value): SExpression => ({ kind: "string", value })),
      fc
        .array(tie("expression"), { maxLength: 4 })
        .map((values): SExpression => ({ kind: "list", values })),
    ),
  }));
  const expressions = fc.array(expression, { maxLength: 4 });
  const written = (values: readonly SExpression[]) =>
    values.map((value) => writeSExpression(value)).join(" ");
  const refused = (text: string, comments: boolean) =>
    Either.match(readSExpressions(text, comments), {
      onLeft: () => true,
      onRight: () => false,
    });

  it("reads back every expression it writes", () => {
    fc.assert(
      fc.property(expressions, (values) => {
        expect(readSExpressions(written(values))).toEqual(Either.right(values));
      }),
    );
  });

  it("skips a comment in a document and refuses one in a message", () => {
    fc.assert(
      fc.property(
        expressions,
        fc.stringMatching(/^[^\r\n]*$/u),
        (values, comment) => {
          const text = `${written(values)} ;${comment}\n`;
          expect(readSExpressions(text)).toEqual(Either.right(values));
          expect(refused(text, false)).toBe(true);
        },
      ),
    );
  });

  it("refuses text whose parentheses do not balance", () => {
    fc.assert(
      fc.property(
        expressions,
        fc.constantFrom("(", ")"),
        fc.boolean(),
        (values, parenthesis, leading) => {
          const text = written(values);
          expect(
            refused(
              leading ? `${parenthesis} ${text}` : `${text} ${parenthesis}`,
              true,
            ),
          ).toBe(true);
        },
      ),
    );
  });
});
