/**
 * @file Saves one agent's editable policy, completed checks and policy
 * activations. Every operation runs under that agent's ownership, so agents
 * sharing a database never read each other's policy or decisions.
 */

import { SqlClient } from "@effect/sql";
import { Array as Arr, Clock, Effect, Schema } from "effect";
import {
  executeQuery,
  type Generated,
  makeQueries,
  withAgentRows,
} from "../database.js";
import {
  type Direction,
  firewallDecisionSummarySchema,
  FirewallPolicyError,
} from "./rules.js";

/** A store failure leaves saved rules and decisions as they were. */
export const persistenceFailure = () =>
  new FirewallPolicyError({ reason: "persistence-failed" });

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
const decision = Schema.Literal("allowed", "refused", "withheld");
const decisionRow = Schema.Struct({
  checkId: Schema.NonEmptyString,
  result: Schema.NonEmptyString,
  decision,
  time: storedTime,
});
const messageCheck = Schema.Struct({
  messageKey: Schema.NonEmptyString,
  at: storedTime,
  decision,
  result: Schema.NonEmptyString,
  message: Schema.NonEmptyString,
});
const savedSummary = Schema.parseJson(
  Schema.Struct({ summaries: Schema.Array(firewallDecisionSummarySchema) }),
);

/** One applied policy edit: when it took effect, its revision and the rule ids it changed. */
export type FirewallActivation = typeof activationRow.Type;

/**
 * One completed check as delivery commits it. `messageKey` names the message
 * it checked by author and message id, `messageJson` is that message, `inputJson` the revision and evidence it applied and `resultJson`
 * what it concluded with its decision summaries, each as JSON text. Saving
 * the same check again keeps the first save.
 */
export interface CheckRecord {
  readonly checkId: string;
  readonly messageKey: string;
  readonly direction: Direction;
  readonly messageJson: string;
  readonly inputJson: string;
  readonly resultJson: string;
  readonly allowed: boolean;
}

/** The store of one agent's editable policy, completed checks and activations. */
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
    readonly message_key: string;
    readonly direction: Direction;
    readonly message: string;
    readonly input: string;
    readonly result: string;
    readonly decided_at: number;
    readonly decision: typeof decision.Type;
  };
  readonly firewall_activations: {
    readonly agent_key: string;
    readonly sequence: Generated<number>;
    readonly activated_at: number;
    readonly revision: string;
    readonly added_rule_ids: string;
    readonly removed_rule_ids: string;
  };
}

/* eslint-enable @typescript-eslint/naming-convention -- Restore naming outside physical SQL types. */

/** Captured ownership scopes the editable policy, checks and activations on every database operation. */
export const makeFirewallStore = Effect.fn("makeFirewallStore")(function* (
  agentKey: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const queries = yield* makeQueries<FirewallTables>();
  const run = <A, E, R>(work: Effect.Effect<A, E, R>) =>
    withAgentRows(agentKey, work).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.mapError(persistenceFailure),
    );
  const policyQuery = queries
    .selectFrom("firewall_policy")
    .select("policy")
    .where("agent_key", "=", agentKey);
  const checksQuery = queries
    .selectFrom("firewall_checks")
    .where("agent_key", "=", agentKey);
  const activationsQuery = queries
    .selectFrom("firewall_activations")
    .where("agent_key", "=", agentKey);
  return {
    /**
     * The saved checks of the messages `messageKeys` name, oldest first: each
     * with the message it checked, its result, its decision and when it was
     * decided.
     */
    readCheckRecords: (messageKeys: readonly string[]) =>
      run(
        Effect.forEach(
          Arr.chunksOf(messageKeys, 400),
          (keys) =>
            executeQuery(
              checksQuery
                .select([
                  "message_key as messageKey",
                  "decided_at as at",
                  "decision",
                  "result",
                  "message",
                ])
                .where("message_key", "in", keys)
                .orderBy("sequence", "asc"),
            ).pipe(
              Effect.flatMap(Schema.decodeUnknown(Schema.Array(messageCheck))),
            ),
          { concurrency: 1 },
        ).pipe(Effect.map(Arr.flatten)),
      ),
    readEditablePolicy: () =>
      run(
        executeQuery(policyQuery).pipe(
          Effect.flatMap(
            Schema.decodeUnknown(
              Schema.Array(Schema.Struct({ policy: Schema.NonEmptyString })),
            ),
          ),
          Effect.map((rows) => rows[0]?.policy),
        ),
      ),
    writeEditablePolicy: (
      editablePolicyJson: string,
      activation?: FirewallActivation,
    ) =>
      run(
        Effect.gen(function* () {
          yield* executeQuery(
            queries
              .insertInto("firewall_policy")
              .values({ agent_key: agentKey, policy: editablePolicyJson })
              .onConflict((conflict) =>
                conflict
                  .column("agent_key")
                  .doUpdateSet({ policy: editablePolicyJson }),
              ),
          );
          if (activation !== undefined) {
            yield* executeQuery(
              queries.insertInto("firewall_activations").values({
                agent_key: agentKey,
                activated_at: activation.time,
                revision: activation.activeRevision,
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
    saveCheckRecord: (record: CheckRecord) =>
      run(
        Effect.gen(function* () {
          const notAllowed =
            record.direction === "outgoing" ? "refused" : "withheld";
          yield* executeQuery(
            queries
              .insertInto("firewall_checks")
              .values({
                agent_key: agentKey,
                check_id: record.checkId,
                message_key: record.messageKey,
                direction: record.direction,
                message: record.messageJson,
                input: record.inputJson,
                result: record.resultJson,
                decision: record.allowed ? "allowed" : notAllowed,
                decided_at: yield* Clock.currentTimeMillis,
              })
              .onConflict((conflict) =>
                conflict.columns(["agent_key", "check_id"]).doNothing(),
              ),
          );
        }),
      ),
    readDecisions: (request: { readonly since?: number }) =>
      run(
        Effect.gen(function* () {
          const since = request.since ?? 0;
          const rows = yield* executeQuery(
            checksQuery
              .select([
                "check_id as checkId",
                "result",
                "decision",
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
            activationsQuery
              .select([
                "activated_at as time",
                "revision as activeRevision",
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
  return Schema.decodeUnknown(savedSummary)(row.result).pipe(
    Effect.map(({ summaries }) =>
      summaries.map((summary) => ({
        ...summary,
        checkId: row.checkId,
        decision: row.decision,
        time: row.time,
      })),
    ),
  );
}
