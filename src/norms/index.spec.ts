/** @file A norms directory loads as the contracts of its norm documents, and a policy update's norm documents compile before the firewall sees them. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc } from "effect";
import { join } from "node:path";
import { expect } from "vitest";
import { compareAscii } from "../encoding.js";
import { compileNorm } from "./compiler.js";
import { compiledPolicyEdit, loadNorms } from "./index.js";

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
  "loads the contract of every .lisp document directly in a directory, and nothing of its other files",
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

      expect(
        loaded.contracts.map((contract) => contract.source).sort(compareAscii),
      ).toEqual(norms.map(documentOf).sort(compareAscii));
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 20 } },
);

it.prop(
  "refuses a directory that holds a document that does not compile or two documents of one norm",
  [
    normName,
    fc.constantFrom<(name: string) => Readonly<Record<string, string>>>(
      (name) => ({ "a.lisp": documentOf(name), "b.lisp": documentOf(name) }),
      (name) => ({
        "a.lisp": documentOf(name).replace(":by Asker", ":by Absent"),
      }),
      (name) => ({ "a.lisp": `${documentOf(name)}(` }),
    ),
  ],
  ([name, files]) =>
    Effect.gen(function* () {
      const directory = yield* directoryWith(files(name));

      const failure = yield* Effect.flip(loadNorms(directory));

      expect(failure._tag).toBe("NormsConfigurationError");
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 20 } },
);

const rule = { id: "brief", applies: "both", text: "Be brief." } as const;

/** One addition of a policy update: a rule, a behavior, a norm document that compiles or one that does not. */
const addition = fc.oneof(
  fc.constant({ kind: "rule" as const, added: rule }),
  fc.constant({ kind: "behavior" as const, added: { behavior: "a document" } }),
  normName.map((name) => ({
    kind: "norm" as const,
    added: { norm: documentOf(name) },
  })),
  fc.string().map((text) => ({
    kind: "invalid" as const,
    added: { norm: `(norm ${text}` },
  })),
);

it("compiles each norm document an update adds and leaves the rest of the update as written, or fails at the first document that does not compile", () =>
  Effect.sync(() => {
    fc.assert(
      fc.property(
        fc.array(addition, { maxLength: 5 }),
        fc.array(
          fc.oneof(
            normName,
            normName.map((norm) => ({ norm })),
          ),
        ),
        (additions, remove) => {
          const edit = Effect.runSync(
            Effect.either(
              compiledPolicyEdit({
                add: additions.map(({ added }) => added),
                remove,
              }),
            ),
          );
          const invalid = additions.findIndex(({ kind }) => kind === "invalid");
          if (invalid >= 0) {
            expect(edit).toMatchObject({
              left: {
                reason: "invalid-rule",
                problem: "schema",
                path: ["add", invalid],
              },
            });
            return;
          }
          expect(edit).toMatchObject({
            right: {
              remove,
              add: additions.map(({ added }) =>
                "norm" in added
                  ? { contract: Effect.runSync(compileNorm(added.norm)) }
                  : added,
              ),
            },
          });
        },
      ),
    );
  }));
