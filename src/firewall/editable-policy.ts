/**
 * @file Edits to an agent's editable policy: the text rules and contracts an
 * update leaves the policy holding, checked one addition and removal at a
 * time before the firewall builds and activates the replacement.
 */

import { Effect } from "effect";
import type { CompiledContract } from "./contract.js";
import { decodeFrozen, deepFreeze } from "./check.js";
import {
  decodeBehavior,
  editableRuleSchema,
  type editableRulesSchema,
  FirewallPolicyError,
  MAXIMUM_DOCUMENT_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type PolicyProblem,
  type TextRule,
} from "./rules.js";

type EditableRule = (typeof editableRulesSchema.Type)[number];

/**
 * One edit of an agent's editable policy. It adds an inline rule, a
 * `BEHAVIOR.md` document's rule or a contract compiled from a norm document,
 * and removes an editable rule by id or a held contract by its norm's name.
 * Removing and adding one id or name replaces it in one activation.
 */
export interface PolicyEdit {
  readonly add?: ReadonlyArray<
    | EditableRule
    | { readonly behavior: string }
    | { readonly contract: CompiledContract }
  >;
  readonly remove?: ReadonlyArray<string | { readonly norm: string }>;
}

/** Where an invalid addition or removal sits in an update, such as `["add", 0]`. */
type PolicyEditPath = NonNullable<FirewallPolicyError["path"]>;

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
  readonly addContracts: ReadonlyArray<{
    readonly index: number;
    readonly contract: CompiledContract;
  }>;
  readonly removeContracts: ReadonlyArray<{
    readonly index: number;
    readonly name: string;
  }>;
}

type RequestedAddition = NonNullable<PolicyEdit["add"]>[number];

/** One requested addition once read: a rule or a contract. */
type Addition =
  | {
      readonly kind: "rule";
      readonly index: number;
      readonly rule: EditableRule;
    }
  | {
      readonly kind: "contract";
      readonly index: number;
      readonly contract: CompiledContract;
    };

/**
 * Maps each `{behavior}` to a rule, keeping every addition's and removal's
 * index in its list, and reports an invalid one at that index.
 */
export const resolvedChange = Effect.fn("resolvedChange")(function* (
  request: PolicyEdit,
) {
  const frozenRequest = yield* Effect.try({
    try: () => structuredClone(request),
    catch: () => invalidRule("schema"),
  });
  deepFreeze(frozenRequest);
  const additions = yield* Effect.forEach(
    frozenRequest.add ?? [],
    (requested, index) => addition(requested, index),
    { concurrency: 1 },
  );
  const removals = frozenRequest.remove ?? [];
  const change: PolicyChange = {
    addRules: additions.flatMap((added) =>
      added.kind === "rule" ? [{ index: added.index, rule: added.rule }] : [],
    ),
    addContracts: additions.flatMap((added) =>
      added.kind === "contract"
        ? [{ index: added.index, contract: added.contract }]
        : [],
    ),
    removeRules: removals.flatMap((removal, index) =>
      typeof removal === "string" ? [{ index, ruleId: removal }] : [],
    ),
    removeContracts: removals.flatMap((removal, index) =>
      typeof removal === "string" ? [] : [{ index, name: removal.norm }],
    ),
  };
  return change;
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
 * The contracts an edit leaves the agent holding, or the first addition or
 * removal it cannot apply: a removal of a norm the agent does not hold, an
 * addition of a norm it keeps or the edit adds twice, or more contracts than
 * the limit.
 */
export const editedContracts = Effect.fn("editedContracts")(function* (
  contracts: readonly CompiledContract[],
  { addContracts, removeContracts }: PolicyChange,
) {
  for (const { index, name } of removeContracts) {
    if (!contracts.some((contract) => contract.name === name)) {
      return yield* invalidRule("unknown-id", { path: ["remove", index] });
    }
  }
  const removed = new Set(removeContracts.map(({ name }) => name));
  const kept = contracts.filter((contract) => !removed.has(contract.name));
  const names = new Set(kept.map((contract) => contract.name));
  for (const { index, contract } of addContracts) {
    if (names.has(contract.name)) {
      return yield* invalidRule("duplicate-id", { path: ["add", index] });
    }
    names.add(contract.name);
  }
  if (kept.length + addContracts.length > MAXIMUM_EDITABLE_RULES) {
    return yield* invalidRule("editable-limit");
  }
  return Object.freeze([
    ...kept,
    ...addContracts.map(({ contract }) => contract),
  ]);
});

/** An `invalid-rule` failure for `problem`, at the addition or removal `at` locates. */
export function invalidRule(
  problem: PolicyProblem,
  at: { readonly path?: PolicyEditPath; readonly ruleId?: string } = {},
) {
  return new FirewallPolicyError({ reason: "invalid-rule", problem, ...at });
}

/**
 * One requested addition: an inline rule, a behavior's rule or a contract.
 * An inline rule is decoded, so its members are in its schema's order like
 * those of a rule read back from storage.
 */
function addition(
  requested: RequestedAddition,
  index: number,
): Effect.Effect<Addition, FirewallPolicyError> {
  const path: PolicyEditPath = ["add", index];
  if ("behavior" in requested) {
    return behaviorRule(requested.behavior, path).pipe(
      Effect.map((rule) => ({ kind: "rule", index, rule }) as const),
    );
  }
  if ("contract" in requested) {
    return Effect.succeed({
      kind: "contract",
      index,
      contract: requested.contract,
    });
  }
  return decodeFrozen(editableRuleSchema, requested).pipe(
    Effect.mapBoth({
      onFailure: () => invalidRule("schema", { path }),
      onSuccess: (rule) => ({ kind: "rule", index, rule }) as const,
    }),
  );
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
