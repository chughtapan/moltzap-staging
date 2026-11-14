/**
 * @file Edits to an agent's editable policy: the text rules and norm documents
 * an update leaves the policy holding, checked one addition and removal at a
 * time before the firewall builds, approves and activates the replacement,
 * and the interface through which the norms layer names and compiles the norm
 * documents.
 */

import { Effect, Option } from "effect";
import type { Chain } from "./chain.js";
import { decodeFrozen, deepFreeze } from "./check.js";
import {
  decodeBehavior,
  editableRuleSchema,
  type editableRulesSchema,
  FirewallPolicyError,
  MAXIMUM_DOCUMENT_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type PolicyProblem,
  type policyUpdateSchema,
  type TextRule,
} from "./rules.js";

/**
 * The norm documents an agent may hold in its editable policy. The firewall
 * saves, versions and approves them with the agent's text rules without
 * reading them; the norms layer implements this interface to name and compile them.
 */
export interface NormDocuments {
  /** The documents a new agent holds. */
  readonly initial: readonly string[];
  /** The name a document gives its norm, or none when the agent cannot hold it. */
  readonly name: (document: string) => Option.Option<string>;
  /**
   * The matchers the chain rules and text rules name, and the fixed text rules,
   * that the held documents add; none when they cannot be held together.
   */
  readonly compile: (
    documents: readonly string[],
  ) => Option.Option<CompiledNorms>;
}

/** What the norms an agent holds add to its message policy. */
export interface CompiledNorms {
  readonly matchers: Chain["matchers"];
  readonly fixedRules: readonly TextRule[];
}

/** Where an invalid addition or removal sits in an update, such as `["add", 0]`. */
type PolicyEditPath = NonNullable<FirewallPolicyError["path"]>;

/** A norm document the agent holds, and the name of its norm. */
export interface HeldNorm {
  readonly name: string;
  readonly document: string;
}

type EditableRule = (typeof editableRulesSchema.Type)[number];

/** What an edit adds and removes, each with its index in the request's `add` or `remove` list. */
interface PolicyChange {
  readonly addRules: ReadonlyArray<{
    readonly index: number;
    readonly rule: EditableRule;
  }>;
  readonly removeRules: ReadonlyArray<{
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

type RequestedAddition = NonNullable<
  typeof policyUpdateSchema.Type.add
>[number];

/** One requested addition once read: a rule, or a norm document and its name. */
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
 * every addition's and removal's index in its list, and reports an invalid
 * one at that index.
 */
export const resolvedChange = Effect.fn("resolvedChange")(function* (
  request: typeof policyUpdateSchema.Type,
  norms?: NormDocuments,
) {
  const frozenRequest = yield* Effect.try({
    try: () => structuredClone(request),
    catch: () => invalidRule("schema"),
  });
  deepFreeze(frozenRequest);
  const additions = yield* Effect.forEach(
    frozenRequest.add ?? [],
    (requested, index) => addition(requested, index, norms ?? noNorms),
    { concurrency: 1 },
  );
  const removals = frozenRequest.remove ?? [];
  const change: PolicyChange = {
    addRules: additions.flatMap((added) =>
      added.kind === "rule" ? [{ index: added.index, rule: added.rule }] : [],
    ),
    addNorms: additions.flatMap((added) =>
      added.kind === "norm" ? [{ index: added.index, norm: added.norm }] : [],
    ),
    removeRules: removals.flatMap((removal, index) =>
      typeof removal === "string" ? [{ index, ruleId: removal }] : [],
    ),
    removeNorms: removals.flatMap((removal, index) =>
      typeof removal === "string" ? [] : [{ index, name: removal.norm }],
    ),
  };
  return { request: frozenRequest, change };
});

/**
 * The editable rules an edit leaves, or the first addition or removal it
 * cannot apply: a fixed id, a removal of an id no editable rule has, an
 * addition whose id an editable rule keeps or the edit adds twice, or more
 * rules than the limit.
 */
export const editedRules = Effect.fn("editedRules")(function* (
  rules: readonly TextRule[],
  fixedRuleIds: readonly string[],
  { addRules, removeRules }: PolicyChange,
) {
  for (const { index, ruleId } of removeRules) {
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
  const removed = new Set(removeRules.map(({ ruleId }) => ruleId));
  const kept = rules.filter((rule) => !removed.has(rule.id));
  const ids = new Set(kept.map((rule) => rule.id));
  for (const { index, rule } of addRules) {
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
  if (kept.length + addRules.length > MAXIMUM_EDITABLE_RULES) {
    return yield* invalidRule("editable-limit");
  }
  return Object.freeze([...kept, ...addRules.map(({ rule }) => rule)]);
});

/**
 * The norms an edit leaves the agent holding, or the first addition or
 * removal it cannot apply: a removal of a norm the agent does not hold, an addition of a norm
 * it keeps or the edit adds twice, or more norms than the limit.
 */
export const editedNorms = Effect.fn("editedNorms")(function* (
  norms: readonly HeldNorm[],
  { addNorms, removeNorms }: PolicyChange,
) {
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
});

/** An `invalid-rule` failure for `problem`, at the addition or removal `at` locates. */
export function invalidRule(
  problem: PolicyProblem,
  at: { readonly path?: PolicyEditPath; readonly ruleId?: string } = {},
) {
  return new FirewallPolicyError({ reason: "invalid-rule", problem, ...at });
}

/**
 * One requested addition: an inline rule, a behavior's rule or a named norm
 * document. An inline rule is decoded, so its members are in its schema's
 * order like those of a rule read back from storage.
 */
function addition(
  requested: RequestedAddition,
  index: number,
  norms: NormDocuments,
): Effect.Effect<Addition, FirewallPolicyError> {
  const path: PolicyEditPath = ["add", index];
  if ("behavior" in requested) {
    return behaviorRule(requested.behavior, path).pipe(
      Effect.map((rule) => ({ kind: "rule", index, rule }) as const),
    );
  }
  if ("norm" in requested) {
    return heldNorm(requested.norm, path, norms).pipe(
      Effect.map((norm) => ({ kind: "norm", index, norm }) as const),
    );
  }
  return decodeFrozen(editableRuleSchema, requested).pipe(
    Effect.mapBoth({
      onFailure: () => invalidRule("schema", { path }),
      onSuccess: (rule) => ({ kind: "rule", index, rule }) as const,
    }),
  );
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

/** A behavior supplies the same direction, guidance and feedback as an inline rule. */
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
        deepFreeze(rule);
      }),
    ),
  );
}
