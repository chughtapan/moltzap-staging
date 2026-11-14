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
import { compareAscii, exactStruct } from "../encoding.js";
import {
  type Chain,
  chainRuleFileSchema,
  type FirewallPolicyError,
  type NormDocuments,
  TextRule,
} from "../firewall/index.js";
import { Firewall } from "../firewall/service.js";
import { type Norm, readNorm } from "./documents.js";
import { normMatchers, normRuleId, ruleMatcher } from "./matchers.js";

/** The package's built-in norm documents, beside the code that implements their effects. */
const builtInDirectory = fileURLToPath(
  new URL("../../config/norms/built-in/", import.meta.url),
);

/** The settings' copy of a norms directory: the norm documents each new agent holds, and the rule file. */
export const installedNorms = exactStruct({
  initial: Schema.Array(Schema.String),
  ruleFile: chainRuleFileSchema,
});

/** The norm documents a new agent holds and the rule file every agent applies. */
export type InstalledNorms = typeof installedNorms.Type;

/**
 * A daemon's norms once read: the rule chain every agent applies, how each
 * agent's firewall names and compiles the norm documents it holds, and the
 * built-in norms every agent holds besides them.
 */
export interface DaemonNorms {
  readonly chain: Chain;
  readonly normDocuments: NormDocuments;
  readonly builtIn: readonly Norm[];
}

/** The norms one agent holds, which `read_norms` lists: the built-ins, then the norms its active policy holds. */
export class HeldNorms extends Context.Tag("SocialHarness/HeldNorms")<
  HeldNorms,
  { readonly read: () => Effect.Effect<readonly Norm[], FirewallPolicyError> }
>() {
  /** Reads the agent's held norms from its firewall's active revision. */
  static readonly layer = (daemon: DaemonNorms) =>
    Layer.effect(
      HeldNorms,
      Effect.map(Firewall, (firewall) =>
        HeldNorms.of({
          read: () =>
            firewall
              .readNorms()
              .pipe(
                Effect.map((documents) => [
                  ...daemon.builtIn,
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
    return `The norms directory is invalid: ${this.reason}`;
  }
}

/**
 * Reads every `.lisp` norm document directly in `directory`, which each new
 * agent holds, and the directory's `rules.json`. Norm names are unique, no
 * document names a built-in norm, and every rule names a norm matcher.
 */
export const loadNorms = Effect.fn("loadNorms")(function* (directory: string) {
  const failed = (reason: string) => new NormsConfigurationError({ reason });
  const builtIn = yield* lispDocuments(builtInDirectory);
  const configured = yield* lispDocuments(directory);
  const builtInNames = new Set(builtIn.map(({ norm }) => norm.name));
  const shadowing = configured.find(({ norm }) => builtInNames.has(norm.name));
  if (shadowing !== undefined) {
    return yield* failed(
      `${shadowing.file} names the built-in norm ${shadowing.norm.name}`,
    );
  }
  const names = [...builtIn, ...configured].map(({ norm }) => norm.name);
  if (new Set(names).size !== names.length) {
    return yield* failed("two documents name the same norm");
  }
  const fs = yield* FileSystem.FileSystem;
  const text = yield* fs
    .readFileString(join(directory, "rules.json"))
    .pipe(Effect.mapError(() => failed("rules.json cannot be read")));
  const ruleFile = yield* Schema.decodeUnknown(
    Schema.parseJson(chainRuleFileSchema),
  )(text).pipe(
    Effect.mapError((error) =>
      failed(`rules.json: ${ParseResult.TreeFormatter.formatErrorSync(error)}`),
    ),
  );
  const known = normMatchers([]);
  const unknown = ruleFile.rules.find(
    (rule) => !Object.hasOwn(known, rule.match),
  );
  if (unknown !== undefined) {
    return yield* failed(
      `rule ${unknown.id} names ${unknown.match}, which is not a norm matcher`,
    );
  }
  return {
    initial: configured.map(({ norm }) => norm.document),
    ruleFile,
  } satisfies InstalledNorms;
});

/**
 * The daemon's norms from its settings, read once for every agent it serves:
 * the package's built-in norms and the rule chain, whose matchers each agent's
 * firewall compiles from the norms it holds.
 */
export const daemonNorms = Effect.fn("daemonNorms")(
  (installed: InstalledNorms) =>
    lispDocuments(builtInDirectory).pipe(
      Effect.map((builtInFiles) => {
        const builtIn = builtInFiles.map(({ norm }) => norm);
        return {
          chain: { rules: installed.ruleFile.rules, matchers: {} },
          normDocuments: normDocuments(builtIn, installed.initial),
          builtIn,
        } satisfies DaemonNorms;
      }),
    ),
);

/** Whether the norms each new agent holds have English rules, which need a decision provider. */
export function initialNormsHaveRules(installed: InstalledNorms): boolean {
  return installed.initial.some((document) =>
    Either.match(readNorm(document), {
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
 * peer's act is allowed with the rule as guidance.
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

/** The norm of every `.lisp` norm document directly in `directory`, with its file's name, in name order. */
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
          Effect.flatMap((document) => normOfFile(file, document)),
          Effect.map((norm) => ({ norm, file })),
        ),
      { concurrency: 1 },
    );
  });
}

/** The norm that `file`'s norm document describes, or why it describes none. */
function normOfFile(file: string, document: string) {
  return Either.match(readNorm(document), {
    onLeft: (error) =>
      Effect.fail(
        new NormsConfigurationError({ reason: `${file}: ${error.reason}` }),
      ),
    onRight: (norm) => Effect.succeed(norm),
  });
}
