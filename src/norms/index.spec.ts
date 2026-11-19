/** @file A norms directory loads as the contracts of its norm documents. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc } from "effect";
import { join } from "node:path";
import { expect } from "vitest";
import { compareAscii } from "../encoding.js";
import { loadNorms } from "./index.js";

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
