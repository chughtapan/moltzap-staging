/** @file JSON text round trips, the values it refuses and comparison of two texts. */

import { Effect, Either, Schema } from "effect";
import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import { decodeJson, encodeJson, jsonText, sameJson } from "./json.js";

const succeeds = <A, E>(effect: Effect.Effect<A, E>): boolean =>
  Either.match(Effect.runSync(Effect.either(effect)), {
    onLeft: () => false,
    onRight: () => true,
  });

const encodes = (value: unknown): boolean => succeeds(jsonText(value));

const nestedArray = (depth: number): unknown =>
  JSON.parse(`${"[".repeat(depth)}0${"]".repeat(depth)}`);

const nestedObject = (depth: number): unknown => {
  let value = "0";
  for (let index = 0; index < depth; index += 1) {
    value = `{"a":${value}}`;
  }
  return JSON.parse(value);
};

describe("JSON text", () => {
  it("round-trips generated JSON through one stable text", () => {
    fc.assert(
      fc.property(fc.jsonValue({ maxDepth: 16 }), (value) => {
        const first = Effect.runSync(jsonText(value));
        const parsed = Effect.runSync(decodeJson(Schema.Unknown, first));
        expect(Effect.runSync(jsonText(parsed))).toBe(first);
      }),
    );
  });

  it.each([undefined, Number.NaN, Infinity, -Infinity])(
    "rejects a value outside the JSON data model: %s",
    (value) => {
      expect(encodes(value)).toBe(false);
    },
  );

  it("rejects cycles and missing array elements", () => {
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    const sparse: unknown[] = [];
    sparse.length = 1;
    expect(encodes(cyclic)).toBe(false);
    expect(encodes(sparse)).toBe(false);
  });

  it.each([nestedArray(16), nestedObject(16), 0, "scalar"])(
    "accepts the bounded container depth in %j",
    (value) => {
      expect(encodes(value)).toBe(true);
    },
  );

  it.each([nestedArray(17), nestedObject(17)])(
    "rejects excess container depth in %j",
    (value) => {
      expect(encodes(value)).toBe(false);
    },
  );

  it("rejects a property the schema does not name", () => {
    const closed = Schema.Struct({ a: Schema.Number });
    expect(succeeds(decodeJson(closed, '{"a":1,"b":2}'))).toBe(false);
  });
});

describe("saved schema values", () => {
  it("round trips class values with omitted optional fields", () => {
    class SavedValue extends Schema.Class<SavedValue>("SavedValue")({
      text: Schema.String,
      advice: Schema.optional(Schema.String),
    }) {}
    const value = new SavedValue({
      text: "See you Tuesday",
      advice: undefined,
    });
    const text = Effect.runSync(jsonText(value));
    expect(Effect.runSync(decodeJson(SavedValue, text))).toEqual(value);
  });

  it.each(["\uD800", "\uDC00"])(
    "refuses invalid Unicode before saving a schema string: %s",
    (text) => {
      const schema = Schema.Struct({ text: Schema.String });
      expect(succeeds(encodeJson(schema, { text }))).toBe(false);
      expect(encodes({ [text]: "value" })).toBe(false);
    },
  );
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
