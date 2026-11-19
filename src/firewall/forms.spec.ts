/** @file A filled form is held to the fields its form declares and to each field's type and bounds. */

import { Effect, Either, FastCheck as fc, Schema, Struct } from "effect";
import { expect, it } from "vitest";
import { RequestedSchema, validateResponseContent } from "./forms.js";

const fieldSchema = (field: object) =>
  Schema.decodeUnknownSync(RequestedSchema)({
    type: "object",
    properties: { field },
    required: [],
  });

/** Whether a field accepts a value. */
const accepted = (field: object, value: unknown) =>
  Either.isRight(
    Effect.runSync(
      Effect.either(
        validateResponseContent(fieldSchema(field), { field: value }),
      ),
    ),
  );

const fieldNames = ["slot", "hours", "note", "constructor", "toString"];

it("names exactly the fields a response is missing, adds or gets wrong", () => {
  const field = fc.record({
    declared: fc.boolean(),
    required: fc.boolean(),
    response: fc.constantFrom("absent", "valid", "invalid"),
  });
  fc.assert(
    fc.property(fc.tuple(field, field, field, field, field), (fields) => {
      const named = fields.map((state, index) => ({
        ...state,
        name: fieldNames[index] ?? "",
      }));
      const declared = named.filter((state) => state.declared);
      const schema = Schema.decodeUnknownSync(RequestedSchema)({
        type: "object",
        properties: Object.fromEntries(
          declared.map(({ name }) => [name, { type: "string", maxLength: 3 }]),
        ),
        required: declared
          .filter((state) => state.required)
          .map(({ name }) => name),
      });
      const content = Object.fromEntries(
        named
          .filter((state) => state.response !== "absent")
          .map(({ name, response }) => [
            name,
            response === "valid" ? "yes" : "never",
          ]),
      );
      const reasonOf = (state: (typeof named)[number]) => {
        if (!state.declared) {
          return state.response === "absent" ? [] : ["unexpected"];
        }
        if (state.response === "absent") {
          return state.required ? ["missing"] : [];
        }
        return state.response === "valid" ? [] : ["invalid"];
      };
      const expected = named.flatMap((state) =>
        reasonOf(state).map((reason) => `${state.name} ${reason}`),
      );
      const failures = Either.match(
        Effect.runSync(Effect.either(validateResponseContent(schema, content))),
        {
          onRight: () => [],
          onLeft: (fields) =>
            fields.map(({ field, reason }) => `${field} ${reason}`),
        },
      );

      expect(failures).toHaveLength(expected.length);
      expect(new Set(failures)).toEqual(new Set(expected));
    }),
  );
});

it("refuses a value another type of field is given", () => {
  const kinds = {
    string: { field: { type: "string" }, value: "a" },
    integer: { field: { type: "integer" }, value: 1 },
    array: {
      field: { type: "array", items: { type: "string", enum: ["a"] } },
      value: ["a"],
    },
  } as const;
  const kind = fc.constantFrom(...Struct.keys(kinds));
  fc.assert(
    fc.property(kind, kind, (asked, given) => {
      expect(accepted(kinds[asked].field, kinds[given].value)).toBe(
        asked === given,
      );
    }),
  );
});

it("holds an integer to its bounds and to whole values", () => {
  fc.assert(
    fc.property(
      fc.integer({ min: -100, max: 100 }),
      fc.integer({ min: 0, max: 50 }),
      fc.oneof(
        fc.integer({ min: -200, max: 200 }),
        fc.double({ min: -200, max: 200, noNaN: true }),
      ),
      (minimum, span, value) => {
        const maximum = minimum + span;

        expect(accepted({ type: "integer", minimum, maximum }, value)).toBe(
          Number.isInteger(value) && value >= minimum && value <= maximum,
        );
      },
    ),
  );
});

it("counts a string's length in code points against its bounds", () => {
  fc.assert(
    fc.property(
      fc.string({ unit: "binary", maxLength: 8 }),
      fc.integer({ min: 0, max: 4 }),
      fc.integer({ min: 0, max: 4 }),
      (text, minLength, span) => {
        const maxLength = minLength + span;
        const length = Array.from(text).length;

        expect(accepted({ type: "string", minLength, maxLength }, text)).toBe(
          length >= minLength && length <= maxLength,
        );
      },
    ),
  );
});

it("checks each selected item against the options and their count against its bounds", () => {
  fc.assert(
    fc.property(
      fc.array(fc.constantFrom("a", "b", "x"), { maxLength: 5 }),
      fc.integer({ min: 0, max: 3 }),
      fc.integer({ min: 0, max: 3 }),
      (selected, minItems, span) => {
        const maxItems = minItems + span;
        const items = { type: "string", enum: ["a", "b"] };

        expect(
          accepted({ type: "array", minItems, maxItems, items }, selected),
        ).toBe(
          selected.length >= minItems &&
            selected.length <= maxItems &&
            !selected.includes("x"),
        );
      },
    ),
  );
});
