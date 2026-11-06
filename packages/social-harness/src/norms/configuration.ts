/**
 * @file A daemon's norms. The package ships the built-in norms, whose effects
 * the daemon implements. A deployment's norms directory holds policy: the
 * `rules.json` chain every agent applies, and the norms each new agent holds.
 * After that, each agent holds its own norms in its firewall's editable
 * policy; this module names and compiles them for the firewall, and their
 * English rules become fixed rules of that agent's message policy.
 */

import { FileSystem } from "@effect/platform";
import {
  Context,
  Data,
  Effect,
  Either,
  Layer,
  Option,
  ParseResult,
  Schema,
} from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { NormDocuments } from "../firewall/editable-policy.js";
import { exactStruct } from "../encoding/canonical-json.js";
import { compareAscii } from "../encoding/identifier.js";
import { type Chain, chainRuleFileSchema } from "../firewall/chain.js";
import { type FirewallPolicyError, TextRule } from "../firewall/rules.js";
import { Firewall } from "../firewall/service.js";
import { type Norm, readNorm } from "./documents.js";
import { normMatchers, normRuleId, ruleMatcher } from "./matchers.js";

/** The package's built-in norm documents, beside the code that implements their effects. */
const builtInDirectory = fileURLToPath(
  new URL("../../config/norms/built-in/", import.meta.url),
);

/** The settings' copy of a norms directory: the documents each new agent holds, and the rule file. */
export const installedNorms = exactStruct({
  initial: Schema.Array(Schema.String),
  chain: chainRuleFileSchema,
});

/** The norms a new agent holds and the rule file every agent applies. */
export type InstalledNorms = typeof installedNorms.Type;

/**
 * A daemon's norms once read: the rule chain every agent applies, how each
 * agent's firewall names and compiles the norm documents it holds, and the
 * built-in norms every agent holds besides them.
 */
export interface ServiceNorms {
  readonly chain: Chain;
  readonly documents: NormDocuments;
  readonly builtIn: readonly Norm[];
}

/** The norms one agent holds, which `read_norms` lists: the built-ins, then the norms its active policy holds. */
export class HeldNorms extends Context.Tag("SocialHarness/HeldNorms")<
  HeldNorms,
  { readonly read: () => Effect.Effect<readonly Norm[], FirewallPolicyError> }
>() {
  /** Reads the agent's held norms from its firewall's active revision. */
  static readonly layer = (service: ServiceNorms) =>
    Layer.effect(
      HeldNorms,
      Effect.map(Firewall, (firewall) =>
        HeldNorms.of({
          read: () =>
            firewall
              .readNorms()
              .pipe(
                Effect.map((documents) => [
                  ...service.builtIn,
                  ...savedNorms(documents),
                ]),
              ),
        }),
      ),
    );
}

/** A norms directory the daemon cannot use, and why. */
export class NormsConfigurationError extends Data.TaggedError(
  "NormsConfigurationError",
)<{ readonly reason: string }> {
  override get message(): string {
    return `The norms directory is invalid: ${this.reason}. Docs: https://docs.moltzap.xyz/reference/spec/norms/norms#norms-directory`;
  }
}

/**
 * Reads every `.lisp` document directly in `directory`, which each new agent
 * holds, and the directory's `rules.json`. Norm names are unique, no document
 * names a built-in norm, and every rule names a norm matcher.
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
      initial: configured.map(({ source }) => source),
      chain,
    } satisfies InstalledNorms;
  }).pipe(Effect.withSpan("loadNorms"));
}

/**
 * The daemon's norms from its settings, read once for every agent it serves:
 * the package's built-in norms and the rule chain, whose matchers each agent's
 * firewall compiles from the norms it holds.
 */
export function serviceNorms(installed: InstalledNorms) {
  return lispDocuments(builtInDirectory).pipe(
    Effect.map((documents) => {
      const builtIn = documents.map(({ norm }) => norm);
      return {
        chain: { rules: installed.chain.rules, matchers: {} },
        documents: normDocuments(builtIn, installed.initial),
        builtIn,
      } satisfies ServiceNorms;
    }),
    Effect.withSpan("serviceNorms"),
  );
}

/** Whether the norms each new agent holds have English rules, which need a decision provider. */
export function initialNormsHaveRules(installed: InstalledNorms): boolean {
  return installed.initial.some((source) =>
    Either.match(readNorm(source), {
      onLeft: () => false,
      onRight: (norm) => norm.rules.length > 0,
    }),
  );
}

/**
 * The norms of the documents an agent's firewall saved. Each parses, since the
 * firewall named it before saving it.
 */
function savedNorms(documents: readonly string[]): readonly Norm[] {
  return documents.flatMap((document) =>
    Option.toArray(Either.getRight(readNorm(document))),
  );
}

/**
 * How an agent's firewall names and compiles the norm documents it holds.
 * An agent cannot hold a document that does not parse or names a built-in
 * norm, nor two documents for one norm. The held norms compile with the
 * built-ins into the norm matchers and their English rules.
 */
function normDocuments(
  builtIn: readonly Norm[],
  initial: readonly string[],
): NormDocuments {
  const builtInNames = new Set(builtIn.map((norm) => norm.name));
  const holdable = (document: string) =>
    Either.getRight(readNorm(document)).pipe(
      Option.filter((norm) => !builtInNames.has(norm.name)),
    );
  return {
    initial,
    name: (document) =>
      holdable(document).pipe(Option.map((norm) => norm.name)),
    compile: (documents) =>
      Option.all(documents.map((document) => holdable(document))).pipe(
        Option.filter(
          (held) => new Set(held.map((norm) => norm.name)).size === held.length,
        ),
        Option.map((held) => {
          const norms = [...builtIn, ...held];
          return {
            matchers: normMatchers(norms),
            fixedRules: normTextRules(norms),
          };
        }),
      ),
  };
}

/**
 * The English rules of the norms an agent holds, as fixed firewall text rules.
 * They join the agent's message policy, so its decision provider judges each
 * act a rule governs, its agent's and its peers', and is not asked about
 * other messages: the rule's `match` names the norm matcher for its acts. A
 * rule's id is the norm's name, `-rule-` and its number. A breach refuses the
 * agent's act, or withholds a peer's act and answers it with `not-understood`
 * naming the rule, so the act changes nothing for any member. When the
 * provider is unsure, the agent's act is refused so it can revise it, and a
 * peer's act is admitted with the rule as a reminder.
 */
function normTextRules(norms: readonly Norm[]): readonly TextRule[] {
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
