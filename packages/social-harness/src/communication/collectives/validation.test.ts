/** @file Pins answer validation against a form-mode schema. */

import { Effect, Exit, Schema } from "effect";
import { describe, expect, it } from "vitest";
import type { AnswerContent } from "./contracts.js";
import { validateAnswer } from "./validation.js";
import { FormModeSchema } from "./wire.js";

const requestedSchema = Schema.decodeUnknownSync(FormModeSchema)({
  type: "object",
  properties: {
    slot: { type: "string", enum: ["mon", "tue"] },
    hours: { type: "number", minimum: 0 },
  },
  required: ["slot"],
});

const failingFields = (content: AnswerContent) =>
  Effect.runSync(Effect.flip(validateAnswer(requestedSchema, content))).map(
    ({ field, reason }) => ({ field, reason }),
  );

// @agent-code-guard/regression-only: examples pin which answer fields fail and how each failure is named.
describe("answer validation", () => {
  it("accepts an answer satisfying every field", () => {
    const content = { slot: "tue", hours: 2 };

    expect(Effect.runSync(validateAnswer(requestedSchema, content))).toBe(
      content,
    );
  });

  it("names a field whose value fails its property schema", () => {
    expect(failingFields({ slot: "wed" })).toEqual([
      { field: "slot", reason: "invalid" },
    ]);
  });

  it("names a missing required field", () => {
    expect(failingFields({ hours: 1 })).toEqual([
      { field: "slot", reason: "missing" },
    ]);
  });

  it("names a field the schema does not declare", () => {
    expect(failingFields({ slot: "mon", mood: "fine" })).toEqual([
      { field: "mood", reason: "unexpected" },
    ]);
  });

  it("names an undeclared field that shares an Object.prototype name", () => {
    expect(failingFields({ slot: "mon", constructor: "x" })).toEqual([
      { field: "constructor", reason: "unexpected" },
    ]);
  });

  it("names a missing required field that shares an Object.prototype name", () => {
    const toStringSchema = Schema.decodeUnknownSync(FormModeSchema)({
      type: "object",
      properties: { toString: { type: "string" } },
      required: ["toString"],
    });

    expect(
      Effect.runSync(Effect.flip(validateAnswer(toStringSchema, {}))).map(
        ({ field, reason }) => ({ field, reason }),
      ),
    ).toEqual([{ field: "toString", reason: "missing" }]);
  });

  it("names every failing field of one answer", () => {
    expect(failingFields({ slot: "wed", hours: -1 })).toEqual([
      { field: "slot", reason: "invalid" },
      { field: "hours", reason: "invalid" },
    ]);
  });
});

const fieldSchema = (field: object) =>
  Schema.decodeUnknownSync(FormModeSchema)({
    type: "object",
    properties: { field },
  });

/** The keyword each issue of an invalid field's detail names, in order. */
const keywordsOf = (field: object, value: AnswerContent[string]) =>
  Effect.runSync(
    Effect.flip(validateAnswer(fieldSchema(field), { field: value })),
  ).flatMap(({ reason, detail }) => [
    reason,
    ...(detail ?? "").split(", ").map((issue) => issue.split(":")[0]),
  ]);

const accepts = (field: object, value: AnswerContent[string]) =>
  Exit.isSuccess(
    Effect.runSyncExit(validateAnswer(fieldSchema(field), { field: value })),
  );

// @agent-code-guard/regression-only: examples pin which keyword an invalid string answer names.
describe("string answer keywords", () => {
  it("names both the type and the enum a mistyped selection fails", () => {
    expect(keywordsOf({ type: "string", enum: ["mon"] }, 3)).toEqual([
      "invalid",
      "type",
      "enum",
    ]);
  });

  it("counts a string's length in code points", () => {
    const field = { type: "string", minLength: 2 };

    expect(accepts(field, "\u{1F600}\u{1F600}")).toBe(true);
    expect(keywordsOf(field, "\u{1F600}")).toEqual(["invalid", "minLength"]);
  });

  it("names the format a string fails", () => {
    expect(keywordsOf({ type: "string", format: "email" }, "a@b")).toEqual([
      "invalid",
      "format",
    ]);
  });

  it("counts a string's maximum length in code points", () => {
    const field = { type: "string", maxLength: 2 };

    expect(accepts(field, "\u{1F600}\u{1F600}")).toBe(true);
    expect(keywordsOf(field, "abc")).toEqual(["invalid", "maxLength"]);
  });

  it("refuses a titled single-select value that two options share", () => {
    const field = {
      type: "string",
      oneOf: [
        { const: "a", title: "A" },
        { const: "a", title: "Also A" },
        { const: "b", title: "B" },
      ],
    };

    expect(accepts(field, "b")).toBe(true);
    expect(keywordsOf(field, "a")).toEqual(["invalid", "oneOf"]);
  });
});

// @agent-code-guard/regression-only: examples pin which keyword an invalid number or boolean names.
describe("number and boolean answer keywords", () => {
  it("accepts a fraction for a number and refuses a value above the maximum", () => {
    const field = { type: "number", maximum: 2 };

    expect(accepts(field, 1.5)).toBe(true);
    expect(accepts(field, 2)).toBe(true);
    expect(keywordsOf(field, 2.5)).toEqual(["invalid", "maximum"]);
    expect(keywordsOf(field, "1")).toEqual(["invalid", "type"]);
  });

  it("refuses a string for a boolean", () => {
    expect(accepts({ type: "boolean" }, false)).toBe(true);
    expect(keywordsOf({ type: "boolean" }, "true")).toEqual([
      "invalid",
      "type",
    ]);
  });

  it("refuses a fraction for an integer and a value below the minimum", () => {
    const field = { type: "integer", minimum: 1 };

    expect(keywordsOf(field, 1.5)).toEqual(["invalid", "type"]);
    expect(keywordsOf(field, 0)).toEqual(["invalid", "minimum"]);
  });
});

// @agent-code-guard/regression-only: examples pin which keyword an invalid multi-select names.
describe("multi-select answer keywords", () => {
  it("names each selected item outside a multi-select's options", () => {
    const field = {
      type: "array",
      maxItems: 1,
      items: { type: "string", enum: ["a"] },
    };

    expect(keywordsOf(field, ["a", "b"])).toEqual([
      "invalid",
      "maxItems",
      "items[1].enum",
    ]);
  });

  it("refuses fewer selections than the minimum", () => {
    const field = {
      type: "array",
      minItems: 1,
      items: { type: "string", enum: ["a"] },
    };

    expect(accepts(field, ["a"])).toBe(true);
    expect(keywordsOf(field, [])).toEqual(["invalid", "minItems"]);
  });

  it("checks a titled multi-select's items against its option consts", () => {
    const field = {
      type: "array",
      items: { anyOf: [{ const: "a", title: "A" }] },
    };

    expect(accepts(field, ["a"])).toBe(true);
    expect(keywordsOf(field, ["b"])).toEqual(["invalid", "items[0].anyOf"]);
  });
});
