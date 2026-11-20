/** @file A norm document compiles to the contract it declares, and one that declares something its grammar lacks compiles to nothing. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { effect as it } from "@effect/vitest";
import { Effect, Either, FastCheck as fc } from "effect";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it as vitestIt } from "vitest";
import { compileNorm } from "./compiler.js";

const shipped = fileURLToPath(
  new URL("../../config/norms/examples/", import.meta.url),
);

const compiled = (document: string) =>
  Effect.runSync(Effect.either(compileNorm(document)));

/** Whether a document compiles to nothing. */
const refused = (document: string) =>
  Either.match(compiled(document), {
    onLeft: () => true,
    onRight: () => false,
  });

const fieldTypes = {
  string: ":type string :min-length 1",
  integer: ":type integer :maximum 9",
  "multi-select": ":type multi-select :choices (one two) :max-items 2",
} as const;

/** The fields of an act's form: distinct names, each with a type and whether it is optional. */
const fields = fc.uniqueArray(
  fc.record({
    name: fc
      .stringMatching(/^[a-z][a-z0-9-]{0,7}$/u)
      .filter((name) => name !== "ontology"),
    type: fc.constantFrom<keyof typeof fieldTypes>(
      "string",
      "integer",
      "multi-select",
    ),
    optional: fc.boolean(),
  }),
  { selector: (field) => field.name, maxLength: 4 },
);

type Fields =
  typeof fields extends fc.Arbitrary<infer Generated> ? Generated : never;

/** Text spliced into the declaration each key names. */
type Splices = Partial<
  Record<"norm" | "open" | "field" | "close" | "rule", string>
>;

/**
 * A document of one norm: `open` starts an instance with the given fields and
 * `close` ends it. Each of `extra` is spliced into the declaration it names.
 */
function document(declared: Fields, extra: Splices = {}) {
  const lines = declared.map(
    ({ name, type, optional }) =>
      `  (field ${name} ${fieldTypes[type]}${optional ? " :optional true" : ""}${extra.field ?? ""})`,
  );
  return [
    `(norm sample :initiator Asker :participant Answerer${extra.norm ?? ""})`,
    `(content open :by Asker :starts true :initiates begun${extra.open ?? ""}`,
    ...lines,
    ")",
    `(content close :by (Asker Answerer) :when begun :ends true${extra.close ?? ""})`,
    `(rule${extra.rule ?? " :act open"} "Ask one question.")`,
  ].join("\n");
}

it("compiles every shipped norm document to the norm its file is named for", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const files = (yield* fs.readDirectory(shipped)).filter((file) =>
      file.endsWith(".lisp"),
    );
    expect(files.length).toBeGreaterThan(0);
    for (const file of files) {
      const source = yield* fs.readFileString(join(shipped, file));
      expect(yield* compileNorm(source)).toMatchObject({
        name: file.replace(/\.lisp$/u, ""),
        source,
      });
    }
  }).pipe(Effect.provide(NodeContext.layer)));

vitestIt(
  "compiles a document to its source, that source's digest, and a form requiring exactly the fields not marked optional",
  () => {
    fc.assert(
      fc.property(fields, (declared) => {
        const source = document(declared);
        const contract = Either.getOrThrow(compiled(source));
        const open = contract.forms.find((form) => form.act === "open");
        expect(contract.source).toBe(source);
        expect(contract.digest).toBe(
          createHash("sha256").update(source, "utf8").digest("hex"),
        );
        expect(Object.keys(open?.fields.properties ?? {})).toEqual(
          declared.map((field) => field.name),
        );
        expect(open?.fields.required).toEqual(
          declared
            .filter((field) => !field.optional)
            .map((field) => field.name),
        );
        expect(contract.textRules).toMatchObject([
          { id: "sample-rule-1", act: "open" },
        ]);
      }),
    );
  },
);

/** Attributes of the generated length that some declaration takes. */
const known = [
  ":description",
  ":initiates",
  ":initiator",
  ":participant",
  ":terminates",
];

vitestIt(
  "refuses a document with an attribute its declaration does not take",
  () => {
    fc.assert(
      fc.property(
        fields.filter((declared) => declared.length > 0),
        fc.constantFrom<keyof Splices>(
          "norm",
          "open",
          "field",
          "close",
          "rule",
        ),
        fc
          .stringMatching(/^:[a-z]{9,12}$/u)
          .filter((attribute) => !known.includes(attribute)),
        (declared, declaration, attribute) => {
          expect(
            refused(document(declared, { [declaration]: ` ${attribute} x` })),
          ).toBe(true);
        },
      ),
    );
  },
);

vitestIt(
  "refuses a document that refers to a role, fact, act or field it does not declare",
  () => {
    fc.assert(
      fc.property(
        fields,
        fc.constantFrom<(source: string) => string>(
          (source) =>
            source.replace("Answerer)\n", "Answerer :on-refusal absent)\n"),
          (source) => source.replace("(Asker Answerer)", "(Asker Absent)"),
          (source) =>
            source.replace(":ends true)", ":ends true :terminates absent)"),
          (source) =>
            source.replace(
              ":ends true)",
              ":ends true :after absent :delay wait :unit seconds)",
            ),
          (source) =>
            source.replace(
              ":ends true)",
              ":ends true (field copy :type string) (relation equals copy (previous absent copy)))",
            ),
          (source) =>
            source.replace(
              ":ends true)",
              ":ends true (relation equals absent (previous open absent)))",
            ),
          (source) => source.replace(":act open", ":act absent"),
        ),
        (declared, spoiled) => {
          const source = document(declared);
          expect(spoiled(source)).not.toBe(source);
          expect(refused(spoiled(source))).toBe(true);
        },
      ),
    );
  },
);
