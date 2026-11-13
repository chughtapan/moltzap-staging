/** @file Norm directories supply validated contracts without an agreement exchange. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Exit } from "effect";
import { join } from "node:path";
import { expect } from "vitest";
import { loadNorms } from "./index.js";

const document = `(norm exchange :initiator Sender :participant Receiver)
(content ask :by Sender :starts true :initiates open)
(content answer :by Receiver :when open :ends true)`;

function directoryWith(files: Readonly<Record<string, string>>) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped();
    yield* Effect.forEach(
      Object.entries(files),
      ([name, text]) => fs.writeFileString(join(directory, name), text),
      { concurrency: 1 },
    );
    return directory;
  });
}

it("loads only configured contracts and retains their source", () =>
  Effect.gen(function* () {
    const directory = yield* directoryWith({ "exchange.lisp": document });
    const loaded = yield* loadNorms(directory);
    expect(loaded.contracts.map((contract) => contract.source)).toEqual([
      document,
    ]);
  }).pipe(Effect.provide(NodeContext.layer)));

const invalidDirectories: ReadonlyArray<Readonly<Record<string, string>>> = [
  { "broken.lisp": "(norm" },
  { "one.lisp": document, "two.lisp": document },
];
it.each(invalidDirectories)(
  "refuses an invalid directory without partially installing it",
  (files) =>
    Effect.gen(function* () {
      const directory = yield* directoryWith(files);
      const result = yield* loadNorms(directory).pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
    }).pipe(Effect.provide(NodeContext.layer)),
);
