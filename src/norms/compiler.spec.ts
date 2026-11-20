/** @file A norm document compiles to the norm it declares, one that declares something its grammar lacks compiles to nothing, and a norms directory loads as the norms of its documents. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Either, FastCheck as fc } from "effect";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it as vitestIt } from "vitest";
import { compareAscii } from "../encoding.js";
import { compileNorm, loadNorms } from "./compiler.js";

const shipped = fileURLToPath(
  new URL("../../config/norms/examples/", import.meta.url),
);

/** Whether a document compiles to nothing. */
const refused = (document: string) =>
  Either.match(compileNorm(document), {
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
      expect(compileNorm(source)).toMatchObject(
        Either.right({ name: file.replace(/\.lisp$/u, ""), source }),
      );
    }
  }).pipe(Effect.provide(NodeContext.layer)));

vitestIt(
  "compiles a document to its source, that source's digest, and a form requiring exactly the fields not marked optional",
  () => {
    fc.assert(
      fc.property(fields, (declared) => {
        const source = document(declared);
        const norm = Either.getOrThrow(compileNorm(source));
        const open = norm.forms.find((form) => form.act === "open");
        expect(norm.source).toBe(source);
        expect(norm.digest).toBe(
          createHash("sha256").update(source, "utf8").digest("hex"),
        );
        expect(
          open?.fields.map(({ name, type, optional }) => ({
            name,
            type,
            optional,
          })),
        ).toEqual(declared);
        expect(norm.textRules).toMatchObject([
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

/** A document of the norm `name`, whose one act opens and ends an instance. */
const documentOf = (name: string) =>
  `(norm ${name} :initiator Asker :participant Answerer)
(content ask :by Asker :starts true :ends true)
`;

const normName = fc.stringMatching(/^[a-z][a-z0-9]{0,7}$/u);

/** A directory holding `files`, by name. */
function directoryWith(files: Readonly<Record<string, string>>) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({
      prefix: "sharness-norms-",
    });
    yield* Effect.forEach(
      Object.entries(files),
      ([file, text]) => fs.writeFileString(join(directory, file), text),
      { concurrency: 1 },
    );
    return directory;
  });
}

it.prop(
  "loads the norm of every .lisp document directly in a directory, and nothing of its other files",
  [fc.uniqueArray(normName, { maxLength: 4 }), fc.uniqueArray(normName)],
  ([norms, others]) =>
    Effect.gen(function* () {
      const directory = yield* directoryWith({
        ...Object.fromEntries(
          others.map((name) => [`${name}.json`, "not a norm document"]),
        ),
        ...Object.fromEntries(
          norms.map((name) => [`${name}.lisp`, documentOf(name)]),
        ),
      });

      const loaded = yield* loadNorms(directory);

      expect(loaded.map((norm) => norm.source).sort(compareAscii)).toEqual(
        norms.map(documentOf).sort(compareAscii),
      );
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 20 } },
);

it("refuses a directory that holds a document that does not compile or two documents of one norm", () =>
  Effect.forEach(
    [
      { "a.lisp": documentOf("ask"), "b.lisp": documentOf("ask") },
      { "a.lisp": documentOf("ask").replace(":by Asker", ":by Absent") },
      { "a.lisp": `${documentOf("ask")}(` },
    ],
    (files: Readonly<Record<string, string>>) =>
      Effect.gen(function* () {
        const directory = yield* directoryWith(files);

        const failure = yield* Effect.flip(loadNorms(directory));

        expect(failure._tag).toBe("NormsConfigurationError");
      }),
    { concurrency: 1, discard: true },
  ).pipe(Effect.provide(NodeContext.layer)));
