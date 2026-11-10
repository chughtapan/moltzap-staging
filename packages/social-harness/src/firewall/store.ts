/**
 * @file Saves one agent's editable text rules, completed checks and policy
 * activations. Every operation runs under that agent's ownership, so agents
 * sharing a database never read each other's policy or decisions.
 */

import { SqlClient } from "@effect/sql";
import { Clock, Effect, Schema } from "effect";
import { executeQuery, type Generated, makeQueries } from "../database/sql.js";
import { withAgentTransaction } from "../database/transactions.js";
import { sameBytes } from "../encoding/canonical-json.js";
import { firewallDecisionSummarySchema, FirewallPolicyError } from "./rules.js";

/** A store failure leaves saved rules and decisions as they were. */
export const persistenceFailure = () =>
  new FirewallPolicyError({ reason: "persistence-failed" });

const bytes = Schema.Uint8ArrayFromSelf.pipe(
  Schema.filter((value) => value.byteLength > 0),
);
const time = Schema.Int.pipe(
  Schema.nonNegative(),
  Schema.lessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const ruleIds = Schema.parseJson(Schema.Array(Schema.NonEmptyString));
const storedTime = Schema.Union(
  time,
  Schema.NumberFromString.pipe(
    Schema.int(),
    Schema.nonNegative(),
    Schema.lessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  ),
);
const activationRow = Schema.Struct({
  time: storedTime,
  activeRevision: Schema.NonEmptyString,
  addedRuleIds: ruleIds,
  removedRuleIds: ruleIds,
});
const disposition = Schema.Literal("admitted", "withheld");
const completedCheck = Schema.Struct({
  checkId: Schema.NonEmptyString,
  direction: Schema.Literal("incoming", "outgoing"),
  canonicalCandidate: bytes,
  canonicalInput: bytes,
  canonicalResult: bytes,
  admit: Schema.Boolean,
});
const storedCheck = Schema.Struct({
  direction: completedCheck.fields.direction,
  canonicalCandidate: bytes,
  canonicalInput: bytes,
  canonicalResult: bytes,
  disposition,
});
const decisionRow = Schema.Struct({
  checkId: Schema.NonEmptyString,
  canonicalResult: bytes,
  disposition,
  time: storedTime,
});
const savedSummary = Schema.parseJson(
  Schema.Struct({ summaries: Schema.Array(firewallDecisionSummarySchema) }),
);
const decoder = new TextDecoder("utf-8", { fatal: true });

/** One applied policy edit: when it took effect, its revision and the rule ids it changed. */
export type FirewallActivation = typeof activationRow.Type;

/**
 * One completed check as delivery commits it. Saving the same check again must
 * carry the same bytes, so a retry cannot rewrite a decision.
 */
export type FirewallCompletion = typeof completedCheck.Type;

/** The store of one agent's policy, decisions and activations. */
export type FirewallStore = Effect.Effect.Success<
  ReturnType<typeof makeFirewallStore>
>;

/* eslint-disable @typescript-eslint/naming-convention -- These properties name physical SQL tables and columns. */
interface FirewallTables {
  readonly firewall_policy: {
    readonly agent_key: string;
    readonly canonical_rules: Uint8Array;
  };
  readonly firewall_checks: {
    readonly agent_key: string;
    readonly sequence: Generated<number>;
    readonly check_id: string;
    readonly direction: typeof completedCheck.Type.direction;
    readonly canonical_candidate: Uint8Array;
    readonly canonical_input: Uint8Array;
    readonly canonical_result: Uint8Array;
    readonly decided_at: number;
    readonly disposition: typeof disposition.Type;
  };
  readonly firewall_activations: {
    readonly agent_key: string;
    readonly sequence: Generated<number>;
    readonly activated_at: number;
    readonly active_revision: string;
    readonly added_rule_ids: string;
    readonly removed_rule_ids: string;
  };
}

/* eslint-enable @typescript-eslint/naming-convention -- Restore naming outside physical SQL types. */

/** Captured ownership scopes policy, evidence and activations on every database operation. */
export function makeFirewallStore(agentKey: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const queries = yield* makeQueries<FirewallTables>();
    const run = <A, E, R>(work: Effect.Effect<A, E, R>) =>
      withAgentTransaction(agentKey, work).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.mapError(persistenceFailure),
      );
    const policy = queries
      .selectFrom("firewall_policy")
      .select("canonical_rules as rules")
      .where("agent_key", "=", agentKey);
    const checks = queries
      .selectFrom("firewall_checks")
      .where("agent_key", "=", agentKey);
    const activations = queries
      .selectFrom("firewall_activations")
      .where("agent_key", "=", agentKey);
    const saveDecision = (value: FirewallCompletion) =>
      Effect.gen(function* () {
        const applied = value.admit ? "admitted" : "withheld";
        yield* executeQuery(
          queries
            .insertInto("firewall_checks")
            .values({
              agent_key: agentKey,
              check_id: value.checkId,
              direction: value.direction,
              canonical_candidate: value.canonicalCandidate,
              canonical_input: value.canonicalInput,
              canonical_result: value.canonicalResult,
              disposition: applied,
              decided_at: yield* Clock.currentTimeMillis,
            })
            .onConflict((conflict) =>
              conflict.columns(["agent_key", "check_id"]).doNothing(),
            ),
        );
        const prior = yield* executeQuery(
          checks
            .select([
              "direction",
              "canonical_candidate as canonicalCandidate",
              "canonical_input as canonicalInput",
              "canonical_result as canonicalResult",
              "disposition",
            ])
            .where("check_id", "=", value.checkId),
        ).pipe(Effect.flatMap(Schema.decodeUnknown(Schema.Array(storedCheck))));
        const saved = prior[0];
        if (saved === undefined) {
          return yield* persistenceFailure();
        }
        const sameEvidence =
          sameBytes(saved.canonicalCandidate, value.canonicalCandidate) &&
          sameBytes(saved.canonicalInput, value.canonicalInput) &&
          sameBytes(saved.canonicalResult, value.canonicalResult);
        if (
          saved.direction !== value.direction ||
          saved.disposition !== applied ||
          !sameEvidence
        ) {
          return yield* persistenceFailure();
        }
      });
    return {
      readPolicy: () =>
        run(
          executeQuery(policy).pipe(
            Effect.flatMap(
              Schema.decodeUnknown(
                Schema.Array(Schema.Struct({ rules: bytes })),
              ),
            ),
            Effect.map((rows) => rows[0]?.rules),
          ),
        ),
      writePolicy: (rules: Uint8Array, activation?: FirewallActivation) =>
        run(
          Effect.gen(function* () {
            yield* executeQuery(
              queries
                .insertInto("firewall_policy")
                .values({
                  agent_key: agentKey,
                  canonical_rules: rules,
                })
                .onConflict((conflict) =>
                  conflict
                    .column("agent_key")
                    .doUpdateSet({ canonical_rules: rules }),
                ),
            );
            if (activation !== undefined) {
              yield* executeQuery(
                queries.insertInto("firewall_activations").values({
                  agent_key: agentKey,
                  activated_at: activation.time,
                  active_revision: activation.activeRevision,
                  added_rule_ids: yield* Schema.encode(ruleIds)(
                    activation.addedRuleIds,
                  ),
                  removed_rule_ids: yield* Schema.encode(ruleIds)(
                    activation.removedRuleIds,
                  ),
                }),
              );
            }
          }),
        ),
      saveDecisions: (decisions: readonly FirewallCompletion[]) =>
        run(
          Effect.forEach(decisions, saveDecision, {
            discard: true,
            concurrency: 1,
          }),
        ),
      readDecisions: (request: { readonly since?: number }) =>
        run(
          Effect.gen(function* () {
            const since = request.since ?? 0;
            const rows = yield* executeQuery(
              checks
                .select([
                  "check_id as checkId",
                  "canonical_result as canonicalResult",
                  "disposition",
                  "decided_at as time",
                ])
                .where("decided_at", ">=", since)
                .orderBy("decided_at", "desc")
                .orderBy("sequence", "desc")
                .limit(100),
            ).pipe(
              Effect.flatMap(Schema.decodeUnknown(Schema.Array(decisionRow))),
            );
            const summaries = yield* Effect.forEach(
              rows,
              decodeDecisionSummaries,
              { concurrency: 1 },
            );
            const edits = yield* executeQuery(
              activations
                .select([
                  "activated_at as time",
                  "active_revision as activeRevision",
                  "added_rule_ids as addedRuleIds",
                  "removed_rule_ids as removedRuleIds",
                ])
                .where("activated_at", ">=", since)
                .orderBy("activated_at", "desc")
                .orderBy("sequence", "desc")
                .limit(100),
            ).pipe(
              Effect.flatMap(Schema.decodeUnknown(Schema.Array(activationRow))),
            );
            return { decisions: summaries.flat(), activations: edits };
          }),
        ),
    };
  }).pipe(Effect.withSpan("makeFirewallStore"));
}

function decodeDecisionSummaries(row: typeof decisionRow.Type) {
  return Effect.try({
    try: () => decoder.decode(row.canonicalResult),
    catch: persistenceFailure,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(savedSummary)),
    Effect.map(({ summaries }) =>
      summaries.map((summary) => ({
        ...summary,
        checkId: row.checkId,
        disposition: row.disposition,
        time: row.time,
      })),
    ),
  );
}
