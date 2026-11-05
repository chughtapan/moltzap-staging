/**
 * @file A daemon's norms: the documents in its norms directory and the rule
 * chain that directory's `rules.json` applies. The daemon reads and checks
 * them each time it starts, and every agent it serves builds its own chain
 * from them. The documents' English rules become fixed rules of the daemon's
 * message policy.
 */

import { FileSystem } from "@effect/platform";
import { Context, Data, Effect, Either, ParseResult, Schema } from "effect";
import { join } from "node:path";
import { exactStruct } from "../encoding/canonical-json.js";
import { compareAscii } from "../encoding/identifier.js";
import { type Chain, chainRuleFileSchema } from "../firewall/chain.js";
import { TextRule } from "../firewall/rules.js";
import { type Norm, readNorm } from "./documents.js";
import { normMatchers } from "./matchers.js";

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
 * Reads every `.lisp` document directly in `directory` and its `rules.json`.
 * Norm names are unique, and every rule names a norm matcher.
 */
export function loadNorms(directory: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const failed = (reason: string) => new NormsConfigurationError({ reason });
    const files = (yield* fs
      .readDirectory(directory)
      .pipe(Effect.mapError(() => failed("the directory cannot be read"))))
      .filter((file) => file.endsWith(".lisp"))
      .sort(compareAscii);
    const documents = yield* Effect.forEach(
      files,
      (file) =>
        fs.readFileString(join(directory, file)).pipe(
          Effect.mapError(() => failed(`${file} cannot be read`)),
          Effect.flatMap((source) => documentOf(file, source)),
        ),
      { concurrency: 1 },
    );
    const names = documents.map(({ norm }) => norm.name);
    if (new Set(names).size !== names.length) {
      return yield* failed("two documents name the same norm");
    }
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
 * each of its agent's own acts of a norm against that norm's rules: each party
 * to a norm keeps its own agent to the norm. A rule's id is the norm's name,
 * `-rule-` and its number, and its text names the acts it governs, so other
 * messages conform. A refusal quotes the rule as broken; an act the provider
 * passes without certainty carries the rule as guidance instead.
 */
export function normTextRules(norms: readonly Norm[]): readonly TextRule[] {
  return norms.flatMap((norm) =>
    norm.rules.map(
      (rule, index) =>
        new TextRule({
          id: `${norm.name}-rule-${index + 1}`,
          applies: "outgoing",
          text: [
            `This rule governs only acts of the ${norm.name} norm, written as (performative :ontology ${norm.name} ...); any other message conforms.`,
            "Judge from this conversation and the owner's facts: without evidence that an act breaks the rule, it conforms.",
            rule,
          ].join(" "),
          guidance: `Keep to this rule of the ${norm.name} norm: ${rule}`,
          feedback: {
            text: `This act breaks a rule of the ${norm.name} norm: ${rule}`,
          },
        }),
    ),
  );
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
