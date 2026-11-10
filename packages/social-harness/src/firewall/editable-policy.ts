/**
 * @file An agent's editable policy edits: the rules and norm documents an
 * update leaves the policy holding, checked item by item before the firewall
 * builds, approves and activates the result, and the interface through which
 * the norms layer names and compiles the norm documents.
 */

import { Effect, Option } from "effect";
import type { Chain } from "./chain.js";
import { freezeData } from "./check.js";
import {
  decodeBehavior,
  type editableDocumentSchema,
  FirewallPolicyError,
  MAXIMUM_DOCUMENT_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type PolicyProblem,
  type policyUpdateSchema,
  type TextRule,
} from "./rules.js";

/**
 * The norm documents an agent may hold in its editable policy. The firewall
 * saves, versions and approves them with the agent's rules without reading
 * them; the norms layer implements this interface to name and compile them.
 */
export interface NormDocuments {
  /** The documents a new agent holds. */
  readonly initial: readonly string[];
  /** The name a document gives its norm, or none when the agent cannot hold it. */
  readonly name: (document: string) => Option.Option<string>;
  /**
   * The matchers the rule chain and text rules name, and the fixed text rules,
   * that the held documents add; none when they cannot be held together.
   */
  readonly compile: (
    documents: readonly string[],
  ) => Option.Option<CompiledNorms>;
}

/** What the norms an agent holds add to its installation. */
export interface CompiledNorms {
  readonly matchers: Chain["matchers"];
  readonly fixedRules: readonly TextRule[];
}

/** Where an invalid item sits in an update, such as `["add", 0]`. */
export type PolicyEditPath = NonNullable<FirewallPolicyError["path"]>;

/** A norm document the agent holds, and the name of its norm. */
export interface HeldNorm {
  readonly name: string;
  readonly document: string;
}

type EditableRule = (typeof editableDocumentSchema.Type)[number];

/** An edit's items, each with its index in the request's `add` or `remove` list. */
export interface PolicyChange {
  readonly add: ReadonlyArray<{
    readonly index: number;
    readonly rule: EditableRule;
  }>;
  readonly remove: ReadonlyArray<{
    readonly index: number;
    readonly ruleId: string;
  }>;
  readonly addNorms: ReadonlyArray<{
    readonly index: number;
    readonly norm: HeldNorm;
  }>;
  readonly removeNorms: ReadonlyArray<{
    readonly index: number;
    readonly name: string;
  }>;
}

type AddedItem = NonNullable<typeof policyUpdateSchema.Type.add>[number];

/** One added item once read: a rule, or a norm document and its name. */
type Addition =
  | {
      readonly kind: "rule";
      readonly index: number;
      readonly rule: EditableRule;
    }
  | { readonly kind: "norm"; readonly index: number; readonly norm: HeldNorm };

/** The norm documents of a daemon without norms, which an agent cannot hold. */
const noNorms: NormDocuments = {
  initial: [],
  name: () => Option.none(),
  compile: () => Option.none(),
};

/**
 * Maps each `{behavior}` to a rule and names each `{norm}` document, keeping
 * every item's index in its list, and reports an invalid item at that index.
 */
export function resolvedChange(
  change: typeof policyUpdateSchema.Type,
  norms?: NormDocuments,
) {
  return Effect.gen(function* () {
    const captured = yield* Effect.try({
      try: () => structuredClone(change),
      catch: () => invalidRule("schema"),
    });
    freezeData(captured);
    const additions = yield* Effect.forEach(
      captured.add ?? [],
      (item, index) => addition(item, index, norms ?? noNorms),
      { concurrency: 1 },
    );
    const removals = captured.remove ?? [];
    const resolved: PolicyChange = {
      add: additions.flatMap((item) =>
        item.kind === "rule" ? [{ index: item.index, rule: item.rule }] : [],
      ),
      addNorms: additions.flatMap((item) =>
        item.kind === "norm" ? [{ index: item.index, norm: item.norm }] : [],
      ),
      remove: removals.flatMap((item, index) =>
        typeof item === "string" ? [{ index, ruleId: item }] : [],
      ),
      removeNorms: removals.flatMap((item, index) =>
        typeof item === "string" ? [] : [{ index, name: item.norm }],
      ),
    };
    return { request: captured, change: resolved };
  }).pipe(Effect.withSpan("resolvedChange"));
}

/**
 * The editable document an edit leaves, or the first item it cannot apply:
 * a fixed id, a removal of an id the document lacks, an addition whose id the
 * document keeps or the edit adds twice, or more rules than the limit.
 */
export function editedRules(
  rules: readonly TextRule[],
  fixedRuleIds: readonly string[],
  { add, remove }: PolicyChange,
) {
  return Effect.gen(function* () {
    for (const { index, ruleId } of remove) {
      const path: PolicyEditPath = ["remove", index];
      if (fixedRuleIds.includes(ruleId)) {
        return yield* new FirewallPolicyError({
          reason: "fixed-rule",
          ruleId,
          path,
        });
      }
      if (!rules.some((rule) => rule.id === ruleId)) {
        return yield* invalidRule("unknown-id", { ruleId, path });
      }
    }
    const removed = new Set(remove.map(({ ruleId }) => ruleId));
    const kept = rules.filter((rule) => !removed.has(rule.id));
    const ids = new Set(kept.map((rule) => rule.id));
    for (const { index, rule } of add) {
      const path: PolicyEditPath = ["add", index];
      if (fixedRuleIds.includes(rule.id)) {
        return yield* new FirewallPolicyError({
          reason: "fixed-rule",
          ruleId: rule.id,
          path,
        });
      }
      if (ids.has(rule.id)) {
        return yield* invalidRule("duplicate-id", { ruleId: rule.id, path });
      }
      ids.add(rule.id);
    }
    if (kept.length + add.length > MAXIMUM_EDITABLE_RULES) {
      return yield* invalidRule("editable-limit");
    }
    return Object.freeze([...kept, ...add.map(({ rule }) => rule)]);
  }).pipe(Effect.withSpan("editedRules"));
}

/**
 * The norms an edit leaves the agent holding, or the first item it cannot
 * apply: a removal of a norm the agent does not hold, an addition of a norm
 * it keeps or the edit adds twice, or more norms than the limit.
 */
export function editedNorms(
  norms: readonly HeldNorm[],
  { addNorms, removeNorms }: PolicyChange,
) {
  return Effect.gen(function* () {
    for (const { index, name } of removeNorms) {
      if (!norms.some((norm) => norm.name === name)) {
        return yield* invalidRule("unknown-id", { path: ["remove", index] });
      }
    }
    const removed = new Set(removeNorms.map(({ name }) => name));
    const kept = norms.filter((norm) => !removed.has(norm.name));
    const names = new Set(kept.map((norm) => norm.name));
    for (const { index, norm } of addNorms) {
      if (names.has(norm.name)) {
        return yield* invalidRule("duplicate-id", { path: ["add", index] });
      }
      names.add(norm.name);
    }
    if (kept.length + addNorms.length > MAXIMUM_EDITABLE_RULES) {
      return yield* invalidRule("editable-limit");
    }
    return Object.freeze([...kept, ...addNorms.map(({ norm }) => norm)]);
  }).pipe(Effect.withSpan("editedNorms"));
}

/** An `invalid-rule` refusal for `problem`, at the item `at` locates. */
export function invalidRule(
  problem: PolicyProblem,
  at: { readonly path?: PolicyEditPath; readonly ruleId?: string } = {},
) {
  return new FirewallPolicyError({ reason: "invalid-rule", problem, ...at });
}

/** One added item: an inline rule, a behavior's rule or a named norm document. */
function addition(
  item: AddedItem,
  index: number,
  norms: NormDocuments,
): Effect.Effect<Addition, FirewallPolicyError> {
  const path: PolicyEditPath = ["add", index];
  if ("behavior" in item) {
    return behaviorRule(item.behavior, path).pipe(
      Effect.map((rule) => ({ kind: "rule", index, rule }) as const),
    );
  }
  if ("norm" in item) {
    return heldNorm(item.norm, path, norms).pipe(
      Effect.map((norm) => ({ kind: "norm", index, norm }) as const),
    );
  }
  return Effect.succeed({ kind: "rule", index, rule: item });
}

/** A norm document within the size limit that the daemon's norms name, or `invalid-norm`. */
function heldNorm(
  document: string,
  path: PolicyEditPath,
  norms: NormDocuments,
): Effect.Effect<HeldNorm, FirewallPolicyError> {
  if (Buffer.byteLength(document, "utf8") > MAXIMUM_DOCUMENT_BYTES) {
    return Effect.fail(invalidRule("too-large", { path }));
  }
  return Option.match(norms.name(document), {
    onNone: () => Effect.fail(invalidRule("invalid-norm", { path })),
    onSome: (named) => Effect.succeed({ name: named, document }),
  });
}

/** A behavior supplies the same direction and registered advice as an inline rule. */
function behaviorRule(text: string, path: PolicyEditPath) {
  const bytes = Buffer.from(text, "utf8");
  if (bytes.length > MAXIMUM_DOCUMENT_BYTES) {
    return Effect.fail(invalidRule("too-large", { path }));
  }
  return decodeBehavior(bytes, (problem) =>
    invalidRule(problem, { path }),
  ).pipe(
    Effect.tap((rule) =>
      Effect.sync(() => {
        freezeData(rule);
      }),
    ),
  );
}
