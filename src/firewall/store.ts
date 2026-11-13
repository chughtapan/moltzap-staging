/**
 * @file Saves one agent's editable text rules, completed checks and policy
 * activations. Every operation runs under that agent's ownership, so agents
 * sharing a database never read each other's policy or decisions.
 */

import { SqlClient } from "@effect/sql";
import { Clock, Effect, Schema } from "effect";
import {
  executeQuery,
  type Generated,
  makeQueries,
  withAgentTransaction,
} from "../database/index.js";
import { sameJson } from "../encoding.js";
import { firewallDecisionSummarySchema, FirewallPolicyError } from "./rules.js";

/** A store failure leaves saved rules and decisions as they were. */
export const persistenceFailure = () =>
  new FirewallPolicyError({ reason: "persistence-failed" });

const bytes = Schema.NonEmptyString;
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
  messageKey: Schema.String,
  direction: Schema.Literal("incoming", "outgoing"),
  candidate: bytes,
  input: bytes,
  result: bytes,
  admit: Schema.Boolean,
});
const storedCheck = Schema.Struct({
  direction: completedCheck.fields.direction,
  candidate: bytes,
  input: bytes,
  result: bytes,
  disposition,
});
const decisionRow = Schema.Struct({
  checkId: Schema.NonEmptyString,
  result: bytes,
  disposition,
  time: storedTime,
});
const auditRow = Schema.Struct({
  sequence: storedTime,
  checkId: Schema.String,
  direction: completedCheck.fields.direction,
  candidate: bytes,
  input: bytes,
  result: bytes,
  time: storedTime,
  disposition,
});
const savedSummary = Schema.parseJson(
  Schema.Struct({ summaries: Schema.Array(firewallDecisionSummarySchema) }),
);

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
    readonly policy: string;
  };
  readonly firewall_checks: {
    readonly agent_key: string;
    readonly sequence: Generated<number>;
    readonly check_id: string;
    readonly message_key: string | null;
    readonly direction: typeof completedCheck.Type.direction;
    readonly candidate: string;
    readonly input: string;
    readonly result: string;
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
export const makeFirewallStore = Effect.fn("makeFirewallStore")(function* (
  agentKey: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const queries = yield* makeQueries<FirewallTables>();
  const run = <A, E, R>(work: Effect.Effect<A, E, R>) =>
    withAgentTransaction(agentKey, work).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.mapError(persistenceFailure),
    );
  const policy = queries
    .selectFrom("firewall_policy")
    .select("policy as rules")
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
            message_key: value.messageKey,
            direction: value.direction,
            candidate: value.candidate,
            input: value.input,
            result: value.result,
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
            "candidate as candidate",
            "input as input",
            "result as result",
            "disposition",
          ])
          .where("check_id", "=", value.checkId),
      ).pipe(Effect.flatMap(Schema.decodeUnknown(Schema.Array(storedCheck))));
      const saved = prior[0];
      if (saved === undefined) {
        return yield* persistenceFailure();
      }
      const sameEvidence =
        sameJson(saved.candidate, value.candidate) &&
        sameJson(saved.input, value.input) &&
        sameJson(saved.result, value.result);
      if (
        saved.direction !== value.direction ||
        saved.disposition !== applied ||
        !sameEvidence
      ) {
        return yield* persistenceFailure();
      }
    });
  return {
    readAssessments: (messageKeys: readonly string[]) =>
      run(
        Effect.gen(function* () {
          const rows: Array<{
            readonly messageKey: string;
            readonly at: number;
            readonly disposition: "admitted" | "withheld";
            readonly result: string;
            readonly candidate: string;
          }> = [];
          for (let start = 0; start < messageKeys.length; start += 400) {
            const batch = yield* executeQuery(
              checks
                .select([
                  "message_key as messageKey",
                  "decided_at as at",
                  "disposition",
                  "result as result",
                  "candidate as candidate",
                ])
                .where(
                  "message_key",
                  "in",
                  messageKeys.slice(start, start + 400),
                )
                .orderBy("sequence", "asc"),
            ).pipe(
              Effect.flatMap(
                Schema.decodeUnknown(
                  Schema.Array(
                    Schema.Struct({
                      messageKey: Schema.String,
                      at: storedTime,
                      disposition,
                      result: bytes,
                      candidate: bytes,
                    }),
                  ),
                ),
              ),
            );
            rows.push(...batch);
          }
          return rows;
        }),
      ),
    auditSequences: () =>
      run(
        executeQuery(checks.select("sequence").orderBy("sequence", "asc")).pipe(
          Effect.flatMap(
            Schema.decodeUnknown(
              Schema.Array(Schema.Struct({ sequence: Schema.Int })),
            ),
          ),
          Effect.map((rows) => rows.map((row) => row.sequence)),
        ),
      ),
    exportAssessments: (sequences: readonly number[]) =>
      run(
        executeQuery(
          checks
            .select([
              "sequence",
              "check_id as checkId",
              "direction",
              "candidate as candidate",
              "input as input",
              "result as result",
              "decided_at as time",
              "disposition",
            ])
            .where("sequence", "in", sequences)
            .orderBy("sequence", "asc")
            .limit(100),
        ).pipe(Effect.flatMap(Schema.decodeUnknown(Schema.Array(auditRow)))),
      ),
    readPolicy: () =>
      run(
        executeQuery(policy).pipe(
          Effect.flatMap(
            Schema.decodeUnknown(Schema.Array(Schema.Struct({ rules: bytes }))),
          ),
          Effect.map((rows) => rows[0]?.rules),
        ),
      ),
    writePolicy: (rules: string, activation?: FirewallActivation) =>
      run(
        Effect.gen(function* () {
          yield* executeQuery(
            queries
              .insertInto("firewall_policy")
              .values({
                agent_key: agentKey,
                policy: rules,
              })
              .onConflict((conflict) =>
                conflict.column("agent_key").doUpdateSet({ policy: rules }),
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
                "result as result",
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
});

function decodeDecisionSummaries(row: typeof decisionRow.Type) {
  return Effect.try({
    try: () => row.result,
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
