/** @file A norms directory loads into settings and back into the chain that checks acts. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { live as it } from "@effect/vitest";
import { Effect, Either } from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect } from "vitest";
import { heldNorms, loadNorms, normTextRules } from "./configuration.js";
import { readNorm } from "./documents.js";
import { ruleMatcher } from "./matchers.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Norm names, rule ids and failure text are the configuration contract under test. */

const shipped = fileURLToPath(new URL("../../config/norms/", import.meta.url));

/** A directory holding `files`, by name. */
function directoryWith(files: Readonly<Record<string, string>>) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({
      prefix: "sharness-norms-",
    });
    yield* Effect.forEach(
      Object.entries(files),
      ([name, text]) => fs.writeFileString(join(directory, name), text),
      { concurrency: 1 },
    );
    return directory;
  });
}

const rules = JSON.stringify({
  rules: [
    {
      id: "refuse-unparsed",
      applies: "outgoing",
      match: "norms.unparsed",
      action: "deny",
    },
  ],
});

describe("loadNorms", () => {
  it("loads the shipped directory into a chain over its norms", () =>
    Effect.gen(function* () {
      const installed = yield* loadNorms(shipped);
      const held = Either.getOrThrow(heldNorms(installed));

      expect(held.norms.map((norm) => norm.name)).toEqual([
        "feedback",
        "norm-agreement",
        "norm-discovery",
        "chat",
      ]);
      expect(held.chain.rules.map((rule) => rule.id)).toContain(
        "not-understood",
      );
      expect(Object.keys(held.chain.matchers)).toEqual(
        expect.arrayContaining(held.chain.rules.map((rule) => rule.match)),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeContext.layer)));

  const refusals: ReadonlyArray<{
    readonly what: string;
    readonly files: Readonly<Record<string, string>>;
    readonly reason: string;
  }> = [
    {
      what: "a document that does not parse",
      files: { "broken.lisp": "(norm", "rules.json": rules },
      reason: "broken.lisp",
    },
    {
      what: "two documents for one norm",
      files: {
        "a.lisp": '(norm chat :description "A.")',
        "b.lisp": '(norm chat :description "B.")',
        "rules.json": rules,
      },
      reason: "two documents name the same norm",
    },
    {
      what: "a document naming a built-in norm",
      files: {
        "mine.lisp": '(norm feedback :description "Mine.")',
        "rules.json": rules,
      },
      reason: "mine.lisp names the built-in norm feedback",
    },
    {
      what: "no rule file",
      files: { "chat.lisp": '(norm chat :description "Chat.")' },
      reason: "rules.json cannot be read",
    },
    {
      what: "a rule naming another matcher",
      files: {
        "rules.json": JSON.stringify({
          rules: [
            { id: "x", applies: "both", match: "text.rules", action: "deny" },
          ],
        }),
      },
      reason: "not a norm matcher",
    },
  ];

  it.each(refusals)("refuses a directory with $what", ({ files, reason }) =>
    Effect.gen(function* () {
      const directory = yield* directoryWith(files);

      const failure = yield* loadNorms(directory).pipe(Effect.flip);

      expect(failure.reason).toContain(reason);
    }).pipe(Effect.scoped, Effect.provide(NodeContext.layer)),
  );
});

describe("normTextRules", () => {
  it("turns each English rule into a fixed rule scoped to the acts it governs, which replies to a peer's breach", () =>
    Effect.sync(() => {
      const norm = Either.getOrThrow(
        readNorm(`
        (norm swap :initiator Asker :participant Owner :description "Swap a shift.")
        (content request :by Asker :initiates asked)
        (content agree :by Owner :when asked :terminates asked)
        (rule :act request "Ask for a swap a day ahead.")
        (rule :act (agree request) "Swap only with a trained colleague.")
      `),
      );

      const [first, second] = normTextRules([norm]);

      expect([first, second].map((rule) => rule?.id)).toEqual([
        "swap-rule-1",
        "swap-rule-2",
      ]);
      expect(first).toMatchObject({
        applies: "both",
        action: "reply",
        match: ruleMatcher("swap", 1),
      });
      expect(second?.match).toBe(ruleMatcher("swap", 2));
      expect(first?.text).toContain("Ask for a swap a day ahead.");
      expect(second?.text).toContain("agree or request act of the swap norm");
      expect(second?.feedback?.text).toContain(
        "Swap only with a trained colleague.",
      );
      expect(second?.guidance).toContain("Swap only with a trained colleague.");
    }));
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore default assertion rules. */
