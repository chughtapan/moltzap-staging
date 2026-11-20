/** @file A direct message reads as the one act its text states, and an act fits a form exactly when its fields do. */

import { Effect, Either, FastCheck as fc, Schema } from "effect";
import { expect, it } from "vitest";
import { Message } from "../communication/index.js";
import { peer } from "../firewall/__tests__/firewall-fixtures.js";
import {
  type Act,
  checkStructure,
  compiledContractSchema,
  formatAct,
  readAct,
} from "./contract.js";

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

const read = (text: string) =>
  Effect.runSync(Effect.either(readAct(message([text]))));

it("reads back every act it writes", () => {
  fc.assert(
    fc.property(act, (written) => {
      expect(read(formatAct(written))).toEqual(Either.right(written));
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
        const problem = Effect.runSync(
          Effect.flip(readAct(spoiled(formatAct(written)))),
        );
        expect(problem).toMatchObject({
          stage: "structural",
          reason: "rule-violation",
        });
      },
    ),
  );
});

const contract = Schema.decodeUnknownSync(compiledContractSchema)({
  name: "sample",
  digest: "0".repeat(64),
  source: "",
  description: "",
  roles: { initiator: "Asker", participant: "Answerer" },
  forms: [
    {
      act: "open",
      by: ["Asker"],
      fields: {
        type: "object",
        properties: {
          topic: { type: "string", minLength: 1 },
          count: { type: "integer" },
          day: {
            type: "array",
            items: { type: "string", enum: ["mon", "tue"] },
          },
        },
        required: ["topic", "day"],
      },
      starts: true,
      ends: true,
      when: [],
      initiates: [],
      terminates: [],
      relations: [],
    },
  ],
  textRules: [],
});

/** How an act states one declared field: not at all, with a value of its type, or with one that is not. */
const stated = fc.constantFrom<"absent" | "valid" | "invalid">(
  "absent",
  "valid",
  "invalid",
);

const values = {
  topic: { valid: "Thesis", invalid: 7 },
  count: { valid: 3, invalid: "three" },
  day: { valid: ["mon"], invalid: ["fri"] },
} as const;

const declared = ["topic", "count", "day"] as const;

const required: readonly string[] = ["topic", "day"];

it("fits an act to its form exactly when every required field is stated, every stated field is declared and each value has its field's type", () => {
  fc.assert(
    fc.property(
      fc.record({ topic: stated, count: stated, day: stated }),
      fc.boolean(),
      (statement, undeclared) => {
        const fields: Record<string, Act["fields"][string]> = {};
        for (const field of declared) {
          const how = statement[field];
          if (how !== "absent") {
            fields[field] = values[field][how];
          }
        }
        if (undeclared) {
          fields.extra = "unasked";
        }
        const failing = [
          ...declared.filter(
            (field) =>
              statement[field] === "invalid" ||
              (statement[field] === "absent" && required.includes(field)),
          ),
          ...(undeclared ? ["extra"] : []),
        ];
        const checked = Effect.runSync(
          Effect.either(
            checkStructure(contract, { name: "open", norm: "sample", fields }),
          ),
        );
        if (failing.length === 0) {
          expect(checked).toMatchObject(Either.right({ act: "open" }));
        } else {
          const problem = Either.getOrThrow(Either.flip(checked));
          expect(problem.stage).toBe("structural");
          for (const field of failing) {
            expect(problem.detail).toContain(`Error: ${field} `);
          }
        }
      },
    ),
  );
});

it("refuses an act of a performative the contract lacks, and an act that names another norm", () => {
  fc.assert(
    fc.property(
      name.filter((performative) => performative !== "open"),
      name.filter((norm) => norm !== "sample"),
      (performative, norm) => {
        const unknownAct = Effect.runSync(
          Effect.flip(
            checkStructure(contract, {
              name: performative,
              norm: "sample",
              fields: {},
            }),
          ),
        );
        expect(unknownAct.stage).toBe("structural");
        const otherNorm = Effect.runSync(
          Effect.flip(
            checkStructure(contract, { name: "open", norm, fields: {} }),
          ),
        );
        expect(otherNorm.stage).toBe("protocol");
      },
    ),
  );
});
