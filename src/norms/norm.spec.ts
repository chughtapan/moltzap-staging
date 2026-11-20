/** @file A direct message reads as the one act its text states, an act fits a form exactly when its fields do, and parenthesized data reads back as written. */

import { Effect, Either, FastCheck as fc, Struct } from "effect";
import { expect, it } from "vitest";
import { Message } from "../communication/index.js";
import { peer } from "../firewall/__tests__/firewall-fixtures.js";
import { form, normOf } from "./__tests__/norms-fixtures.js";
import {
  type Act,
  checkStructure,
  formatAct,
  readAct,
  readSExpressions,
  type SExpression,
  writeSExpression,
} from "./norm.js";

const alice = peer("alice", 1);
const bob = peer("bob", 2);

const name = fc.stringMatching(/^[a-z][a-z0-9-]{0,7}$/u);

/** Any act: a performative, a norm and fields of every type a form admits. */
const act = fc.record({
  name,
  norm: name,
  fields: fc.dictionary(
    name.filter((field) => field !== "ontology"),
    fc.oneof(
      fc.string(),
      fc
        .double({ noNaN: true, noDefaultInfinity: true })
        .map((value) => (value === 0 ? 0 : value)),
      fc.array(fc.string(), { maxLength: 3 }),
    ),
    { maxKeys: 4 },
  ),
});

/** A message from Alice to Bob with the given text parts. */
function message([first, ...rest]: readonly [string, ...string[]]) {
  const part = (text: string) => ({ type: "text" as const, text });
  return Effect.runSync(
    Message.create({
      author: alice,
      recipients: [bob],
      content: [part(first), ...rest.map(part)],
    }),
  );
}

it("reads back every act it writes", () => {
  fc.assert(
    fc.property(act, (written) => {
      expect(readAct(message([formatAct(written)]))).toEqual(
        Either.right(written),
      );
    }),
  );
});

it("refuses a message that is anything but one act: more text, a comment or a data part", () => {
  fc.assert(
    fc.property(
      act,
      fc.constantFrom<(text: string) => Message>(
        (text) => message([text, text]),
        (text) => message([`${text} trailing`]),
        (text) => message([`${text} ; a comment`]),
        (text) => message([text.slice(0, -1)]),
        (text) =>
          Effect.runSync(
            Message.create({
              author: alice,
              recipients: [bob],
              content: [
                { type: "text", text },
                { type: "data", value: {} },
              ],
            }),
          ),
      ),
      (written, spoiled) => {
        expect(readAct(spoiled(formatAct(written)))).toMatchObject(
          Either.left({ stage: "structural", reason: "rule-violation" }),
        );
      },
    ),
  );
});

/** A norm whose one act, `open`, has the given fields. */
const sampleOf = (fields: readonly object[]) =>
  normOf("sample", [
    { ...form, act: "open", by: ["Sender"], fields, starts: true, ends: true },
  ]);

/** How `open` with `fields` fits the form that declares `declared`. */
const fitted = (declared: readonly object[], fields: Act["fields"]) =>
  checkStructure(sampleOf(declared), { name: "open", norm: "sample", fields });

/** Whether a field accepts a value. */
const accepted = (field: object, value: Act["fields"][string]) =>
  Either.match(
    fitted([{ name: "field", optional: false, ...field }], { field: value }),
    { onLeft: () => false, onRight: () => true },
  );

const fieldNames = ["slot", "hours", "note", "constructor", "toString"];

it("names exactly the fields an act is missing, adds or gets wrong", () => {
  const field = fc.record({
    declared: fc.boolean(),
    required: fc.boolean(),
    stated: fc.constantFrom("absent", "valid", "invalid"),
  });
  fc.assert(
    fc.property(fc.tuple(field, field, field, field, field), (fields) => {
      const named = fields.map((state, index) => ({
        ...state,
        name: fieldNames[index] ?? "",
      }));
      const checked = fitted(
        named
          .filter((state) => state.declared)
          .map((state) => ({
            name: state.name,
            optional: !state.required,
            type: "string",
            maxLength: 3,
          })),
        Object.fromEntries(
          named
            .filter((state) => state.stated !== "absent")
            .map((state) => [
              state.name,
              state.stated === "valid" ? "yes" : "never",
            ]),
        ),
      );
      const failureOf = (state: (typeof named)[number]) => {
        if (!state.declared) {
          return state.stated === "absent" ? [] : ["is not a valid field of"];
        }
        if (state.stated === "absent") {
          return state.required ? ["is required for"] : [];
        }
        return state.stated === "valid" ? [] : ["is not valid for"];
      };
      const expected = named.flatMap((state) =>
        failureOf(state).map(
          (failure) => `Error: ${state.name} ${failure} open in sample.`,
        ),
      );
      const stated = Either.match(checked, {
        onRight: () => [],
        onLeft: (problem) => problem.detail.split(/(?<=\.) (?=Error: )/u),
      });
      expect(new Set(stated)).toEqual(new Set(expected));
      expect(stated).toHaveLength(expected.length);
    }),
  );
});

it("refuses a value another type of field is given", () => {
  const kinds = {
    string: { field: { type: "string" }, value: "a" },
    integer: { field: { type: "integer" }, value: 1 },
    "multi-select": {
      field: { type: "multi-select", choices: ["a"] },
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

it("checks each selected item against the choices and their count against its bounds", () => {
  fc.assert(
    fc.property(
      fc.array(fc.constantFrom("a", "b", "x"), { maxLength: 5 }),
      fc.integer({ min: 0, max: 3 }),
      fc.integer({ min: 0, max: 3 }),
      (selected, minItems, span) => {
        const maxItems = minItems + span;
        const field = {
          type: "multi-select",
          choices: ["a", "b"],
          minItems,
          maxItems,
        };
        expect(accepted(field, selected)).toBe(
          selected.length >= minItems &&
            selected.length <= maxItems &&
            !selected.includes("x"),
        );
      },
    ),
  );
});

it("refuses an act of a performative the norm lacks, and an act that names another norm", () => {
  const sample = sampleOf([]);
  fc.assert(
    fc.property(
      name.filter((performative) => performative !== "open"),
      name.filter((norm) => norm !== "sample"),
      (performative, norm) => {
        expect(
          checkStructure(sample, {
            name: performative,
            norm: "sample",
            fields: {},
          }),
        ).toMatchObject(Either.left({ stage: "structural" }));
        expect(
          checkStructure(sample, { name: "open", norm, fields: {} }),
        ).toMatchObject(Either.left({ stage: "protocol" }));
      },
    ),
  );
});

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
