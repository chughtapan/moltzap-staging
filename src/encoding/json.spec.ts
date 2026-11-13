/** @file JSON text round trips through a schema, and comparison of two texts. */

import { Effect, Either, Schema } from "effect";
import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import { decodeJson, encodeJson, sameJson } from "./index.js";

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
