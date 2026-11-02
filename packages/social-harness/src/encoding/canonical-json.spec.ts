/** @file Canonical JSON exactness, JSON value domain and representation bounds. */

import { Effect, Either, Schema } from "effect";
import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  canonicalBytes,
  decodeCanonical,
  decodeCanonicalJson,
  encodeCanonical,
  encodeCanonicalJson,
  parseCanonicalJson,
} from "./canonical-json.js";

const utf8 = (value: string): Uint8Array => new TextEncoder().encode(value);

const succeeds = <A, E>(effect: Effect.Effect<A, E>): boolean =>
  Either.match(Effect.runSync(Effect.either(effect)), {
    onLeft: () => false,
    onRight: () => true,
  });

const parses = (value: Uint8Array): boolean =>
  succeeds(parseCanonicalJson(value));

const encodes = (value: unknown): boolean =>
  succeeds(encodeCanonicalJson(value));

const nestedArray = (depth: number): string =>
  `${"[".repeat(depth)}0${"]".repeat(depth)}`;

const nestedObject = (depth: number): string => {
  let value = "0";
  for (let index = 0; index < depth; index += 1) {
    value = `{"a":${value}}`;
  }
  return value;
};

const nonCanonicalTexts = [
  { caseName: "leading whitespace", text: ' {"a":1}' },
  { caseName: "trailing whitespace", text: '{"a":1} ' },
  { caseName: "alternate member order", text: '{"b":2,"a":1}' },
  { caseName: "duplicate member", text: '{"a":1,"a":2}' },
  { caseName: "decimal suffix", text: '{"a":1.0}' },
  { caseName: "exponent spelling", text: '{"a":1e0}' },
  { caseName: "negative zero", text: '{"a":-0}' },
  {
    caseName: "escaped ordinary Unicode",
    text: String.raw`{"a":"\u0061"}`,
  },
  { caseName: "trailing JSON value", text: '{"a":1}null' },
  { caseName: "byte-order mark", text: `\uFEFF{"a":1}` },
  {
    caseName: "unpaired surrogate",
    text: String.raw`{"a":"\ud800"}`,
  },
] as const;

describe("canonical identity JSON encoding", () => {
  it("encodes member order, Unicode, and numbers as canonical UTF-8", () => {
    const encoded = Effect.runSync(
      encodeCanonicalJson({ z: 1.0, text: "€", negativeZero: -0, a: true }),
    );
    expect(new TextDecoder().decode(encoded)).toBe(
      // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- This independent oracle fixes the required RFC 8785 spelling.
      '{"a":true,"negativeZero":0,"text":"€","z":1}',
    );
  });

  it("round-trips generated JSON through one stable canonical spelling", () => {
    fc.assert(
      fc.property(fc.jsonValue({ maxDepth: 16 }), (value) => {
        const firstEncoding = Effect.runSync(encodeCanonicalJson(value));
        const parsed = Effect.runSync(parseCanonicalJson(firstEncoding));
        const secondEncoding = Effect.runSync(encodeCanonicalJson(parsed));
        expect(secondEncoding).toEqual(firstEncoding);
      }),
    );
  });
});

describe("canonical identity JSON parsing", () => {
  it.each(nonCanonicalTexts)("rejects $caseName", ({ text }) => {
    expect(parses(utf8(text))).toBe(false);
  });

  it("rejects malformed UTF-8", () => {
    expect(parses(Uint8Array.of(0xc3, 0x28))).toBe(false);
  });
});

describe("canonical identity JSON value domain", () => {
  it.each([
    undefined,
    Number.NaN,
    Infinity,
    -Infinity,
    new Date(0),
    { value: undefined },
  ])("rejects a value outside the JSON data model: %s", (value) => {
    expect(encodes(value)).toBe(false);
  });

  it("rejects cycles and missing array elements", () => {
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    const sparse: unknown[] = [];
    sparse.length = 1;
    expect(encodes(cyclic)).toBe(false);
    expect(encodes(sparse)).toBe(false);
  });
});

describe("canonical identity JSON bounds and domain decoding", () => {
  it.each([nestedArray(16), nestedObject(16), "0", '"scalar"'])(
    "accepts the bounded container depth in %s",
    (text) => {
      expect(parses(utf8(text))).toBe(true);
    },
  );

  it.each([nestedArray(17), nestedObject(17)])(
    "rejects excess container depth in %s",
    (text) => {
      expect(parses(utf8(text))).toBe(false);
    },
  );

  it("applies closed Schema decoding after canonical validation", () => {
    const closed = Schema.Struct({ a: Schema.Number });
    expect(succeeds(decodeCanonicalJson(closed, utf8('{"a":1,"b":2}')))).toBe(
      false,
    );
  });
});

describe("schema representation equality", () => {
  it("distinguishes canonical input from the schema's normalized representation", () => {
    const schema = Schema.Struct({
      value: Schema.optionalWith(Schema.Number, { default: () => 1 }),
    });
    const bytes = utf8("{}");
    expect(Effect.runSync(decodeCanonicalJson(schema, bytes))).toEqual({
      value: 1,
    });
    expect(succeeds(decodeCanonical(schema, bytes))).toBe(false);
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
    const bytes = Effect.runSync(canonicalBytes(value));
    expect(Effect.runSync(decodeCanonical(SavedValue, bytes))).toEqual(value);
  });

  it.each(["\uD800", "\uDC00"])(
    "refuses invalid Unicode before saving a schema string: %s",
    (text) => {
      const schema = Schema.Struct({ text: Schema.String });
      expect(succeeds(encodeCanonical(schema, { text }))).toBe(false);
      expect(succeeds(canonicalBytes({ [text]: "value" }))).toBe(false);
    },
  );
});
