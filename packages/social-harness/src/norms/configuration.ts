/**
 * @file A daemon's norms: the built-in norms this package ships, plus the
 * documents in its norms directory and the rule chain that directory's
 * `rules.json` applies. The built-ins carry effects the daemon implements, so
 * they ship with the code; the directory holds only policy. The daemon reads
 * and checks them each time it starts, and every agent it serves builds its
 * own chain from them. The documents' English rules become fixed rules of the
 * daemon's message policy.
 */

import { FileSystem } from "@effect/platform";
import { Context, Data, Effect, Either, ParseResult, Schema } from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { exactStruct } from "../encoding/canonical-json.js";
import { compareAscii } from "../encoding/identifier.js";
import { type Chain, chainRuleFileSchema } from "../firewall/chain.js";
import { TextRule } from "../firewall/rules.js";
import { type Norm, readNorm } from "./documents.js";
import { normMatchers, normRuleId, ruleMatcher } from "./matchers.js";

/** The package's built-in norm documents, beside the code that implements their effects. */
const builtInDirectory = fileURLToPath(
  new URL("../../config/norms/built-in/", import.meta.url),
);

/** The settings' copy of a norms directory: each document's text and the rule file. */
export const installedNorms = exactStruct({
  documents: Schema.Array(Schema.String),
  chain: chainRuleFileSchema,
});

/** The norms a daemon holds and the rule file it applies. */
export type InstalledNorms = typeof installedNorms.Type;

/** The norms this daemon holds, which `read_norms` lists. */
export class HeldNorms extends Context.Tag("SocialHarness/HeldNorms")<
  HeldNorms,
  readonly Norm[]
>() {}

/** A norms directory the daemon cannot use, and why. */
export class NormsConfigurationError extends Data.TaggedError(
  "NormsConfigurationError",
)<{ readonly reason: string }> {
  override get message(): string {
    return `The norms directory is invalid: ${this.reason}. Docs: https://docs.moltzap.xyz/reference/spec/norms/norms#norms-directory`;
  }
}

/**
 * Reads the package's built-in norms, every `.lisp` document directly in
 * `directory`, and the directory's `rules.json`. Norm names are unique, the
 * directory names no built-in norm, and every rule names a norm matcher.
 */
export function loadNorms(directory: string) {
  return Effect.gen(function* () {
    const failed = (reason: string) => new NormsConfigurationError({ reason });
    const builtIn = yield* lispDocuments(builtInDirectory);
    const configured = yield* lispDocuments(directory);
    const builtInNames = new Set(builtIn.map(({ norm }) => norm.name));
    const shadowing = configured.find(({ norm }) =>
      builtInNames.has(norm.name),
    );
    if (shadowing !== undefined) {
      return yield* failed(
        `${shadowing.file} names the built-in norm ${shadowing.norm.name}`,
      );
    }
    const documents = [...builtIn, ...configured];
    const names = documents.map(({ norm }) => norm.name);
    if (new Set(names).size !== names.length) {
      return yield* failed("two documents name the same norm");
    }
    const fs = yield* FileSystem.FileSystem;
    const text = yield* fs
      .readFileString(join(directory, "rules.json"))
      .pipe(Effect.mapError(() => failed("rules.json cannot be read")));
    const chain = yield* Schema.decodeUnknown(
      Schema.parseJson(chainRuleFileSchema),
    )(text).pipe(
      Effect.mapError((error) =>
        failed(
          `rules.json: ${ParseResult.TreeFormatter.formatErrorSync(error)}`,
        ),
      ),
    );
    const known = normMatchers([]);
    const unknown = chain.rules.find(
      (rule) => !Object.hasOwn(known, rule.match),
    );
    if (unknown !== undefined) {
      return yield* failed(
        `rule ${unknown.id} names ${unknown.match}, which is not a norm matcher`,
      );
    }
    return {
      documents: documents.map(({ source }) => source),
      chain,
    } satisfies InstalledNorms;
  }).pipe(Effect.withSpan("loadNorms"));
}

/** The norms in installed settings, and the chain that checks acts of them. */
export function heldNorms(
  installed: InstalledNorms,
): Either.Either<
  { readonly norms: readonly Norm[]; readonly chain: Chain },
  NormsConfigurationError
> {
  return Either.all(installed.documents.map((source) => readNorm(source))).pipe(
    Either.mapLeft(
      (error) => new NormsConfigurationError({ reason: error.reason }),
    ),
    Either.map((norms) => ({
      norms,
      chain: { rules: installed.chain.rules, matchers: normMatchers(norms) },
    })),
  );
}

/**
 * The English rules of the norms a daemon holds, as fixed firewall text rules.
 * The daemon adds them to its message policy, so its decision provider judges
 * each act a rule governs, its agent's and its peers', and is not asked about
 * other messages: the rule's `match` names the norm matcher for its acts. A
 * rule's id is the norm's name, `-rule-` and its number. A breach refuses the
 * agent's act, or withholds a peer's act and answers it with `not-understood`
 * naming the rule, so the act changes nothing for any member. When the
 * provider is unsure, the agent's act is refused so it can revise it, and a
 * peer's act is admitted with the rule as a reminder.
 */
export function normTextRules(norms: readonly Norm[]): readonly TextRule[] {
  return norms.flatMap((norm) =>
    norm.rules.map(
      (rule, index) =>
        new TextRule({
          id: normRuleId(norm.name, index + 1),
          applies: "both",
          action: "reply",
          match: ruleMatcher(norm.name, index + 1),
          text: [
            `For a ${rule.acts.join(" or ")} act of the ${norm.name} norm, written as (${rule.acts[0]} :ontology ${norm.name} ...):`,
            rule.text,
            "Judge from this conversation and the owner's facts: without evidence that the act breaks the rule, it conforms.",
          ].join(" "),
          guidance: `This act may break the ${norm.name} norm's rule: ${rule.text}`,
          feedback: {
            text: `Check this act against the ${norm.name} norm's rule: ${rule.text}`,
          },
        }),
    ),
  );
}

/** Every `.lisp` document directly in `directory`, read and parsed, in name order. */
function lispDocuments(directory: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const failed = (reason: string) => new NormsConfigurationError({ reason });
    const files = (yield* fs
      .readDirectory(directory)
      .pipe(Effect.mapError(() => failed("the directory cannot be read"))))
      .filter((file) => file.endsWith(".lisp"))
      .sort(compareAscii);
    return yield* Effect.forEach(
      files,
      (file) =>
        fs.readFileString(join(directory, file)).pipe(
          Effect.mapError(() => failed(`${file} cannot be read`)),
          Effect.flatMap((source) => documentOf(file, source)),
          Effect.map((document) => ({ ...document, file })),
        ),
      { concurrency: 1 },
    );
  });
}

/** One document's text and the norm it describes, or why `file` holds none. */
function documentOf(file: string, source: string) {
  return Either.match(readNorm(source), {
    onLeft: (error) =>
      Effect.fail(
        new NormsConfigurationError({ reason: `${file}: ${error.reason}` }),
      ),
    onRight: (norm) => Effect.succeed({ norm, source }),
  });
}
