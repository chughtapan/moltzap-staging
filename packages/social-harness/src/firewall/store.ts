/** @file Retains policy activations and completed assessments in the shared database. */

import { SqlClient } from "@effect/sql";
import { findAll, findOne } from "@effect/sql/SqlSchema";
import { Effect, Option, Schema } from "effect";
import { sameBytes } from "../encoding/canonical-json.js";
import {
  type firewallDecisionsSchema,
  firewallDecisionSummarySchema,
  FirewallPolicyError,
} from "./rules/config.js";

const schemaSql = `
  CREATE TABLE IF NOT EXISTS firewall_checks (
    check_id TEXT PRIMARY KEY,
    direction TEXT NOT NULL CHECK (direction IN ('incoming', 'outgoing')),
    canonical_candidate BLOB NOT NULL,
    canonical_input BLOB NOT NULL,
    canonical_result BLOB NOT NULL,
    decided_at INTEGER NOT NULL,
    disposition TEXT NOT NULL CHECK(disposition IN ('admitted','withheld'))
  ) STRICT;
  CREATE TABLE IF NOT EXISTS firewall_policy (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    canonical_rules BLOB NOT NULL
  ) STRICT;
  CREATE TABLE IF NOT EXISTS firewall_activations (
    sequence INTEGER PRIMARY KEY,
    activated_at INTEGER NOT NULL,
    active_revision TEXT NOT NULL,
    added_rule_ids TEXT NOT NULL,
    removed_rule_ids TEXT NOT NULL
  ) STRICT;
  CREATE INDEX IF NOT EXISTS firewall_activations_time ON firewall_activations(activated_at);
  CREATE INDEX IF NOT EXISTS firewall_decisions_time ON firewall_checks(decided_at);
`;

const bytes = Schema.Uint8ArrayFromSelf.pipe(
  Schema.filter((value) => value.byteLength > 0),
);
const time = Schema.Int.pipe(
  Schema.nonNegative(),
  Schema.lessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const ruleIds = Schema.parseJson(Schema.Array(Schema.NonEmptyString));
const activationRow = Schema.Struct({
  time,
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
  time,
});
const savedSummary = Schema.parseJson(
  Schema.Struct({ summaries: Schema.Array(firewallDecisionSummarySchema) }),
);
const decoder = new TextDecoder("utf-8", { fatal: true });
const persistenceFailure = () =>
  new FirewallPolicyError({ reason: "persistence-failed" });

/** An edit remains visible even when no later assessment uses its rules. */
export type FirewallActivation = typeof activationRow.Type;
/** Preserve every field of the original completed assessment when saving it. */
export type FirewallCompletion = typeof completedCheck.Type;

/** Initialization joins the process's existing database transaction. */
export function createFirewallTables(): Effect.Effect<
  void,
  FirewallPolicyError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.withTransaction(
      Effect.forEach(
        schemaSql.split(";").filter((statement) => statement.trim().length > 0),
        (statement) => sql.unsafe(statement),
        { discard: true, concurrency: 1 },
      ),
    );
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.withSpan("createFirewallTables"),
  );
}

/** A missing row distinguishes unseeded policy from an explicitly empty rule set. */
export function readFirewallPolicy(): Effect.Effect<
  Uint8Array | undefined,
  FirewallPolicyError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const read = findOne({
      Request: Schema.Void,
      Result: Schema.Struct({ rules: bytes }),
      execute: () =>
        sql`SELECT canonical_rules AS rules FROM firewall_policy WHERE singleton = 1`,
    });
    const row = yield* read(undefined);
    return Option.isNone(row) ? undefined : Uint8Array.from(row.value.rules);
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.withSpan("readFirewallPolicy"),
  );
}

/** The editable document and its activation commit together. */
export function writeFirewallPolicy(
  rules: Uint8Array,
  activation?: FirewallActivation,
): Effect.Effect<void, FirewallPolicyError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const document = Uint8Array.from(yield* Schema.decodeUnknown(bytes)(rules));
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO firewall_policy (singleton, canonical_rules) VALUES (1, ${document}) ON CONFLICT(singleton) DO UPDATE SET canonical_rules = excluded.canonical_rules`;
        if (activation !== undefined) {
          const added = yield* Schema.encode(ruleIds)(activation.addedRuleIds);
          const removed = yield* Schema.encode(ruleIds)(
            activation.removedRuleIds,
          );
          yield* sql`INSERT INTO firewall_activations (activated_at, active_revision, added_rule_ids, removed_rule_ids) VALUES (${activation.time}, ${activation.activeRevision}, ${added}, ${removed})`;
        }
      }),
    );
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.withSpan("writeFirewallPolicy"),
  );
}

/** Recent edits use reverse insertion order when their activation times match. */
export function readFirewallActivations(request: {
  readonly since?: number;
}): Effect.Effect<
  readonly FirewallActivation[],
  FirewallPolicyError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const read = findAll({
      Request: time,
      Result: activationRow,
      execute: (since) =>
        sql`SELECT activated_at AS time, active_revision AS activeRevision, added_rule_ids AS addedRuleIds, removed_rule_ids AS removedRuleIds FROM firewall_activations WHERE activated_at >= ${since} ORDER BY activated_at DESC, sequence DESC LIMIT 100`,
    });
    return yield* read(request.since ?? 0);
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.withSpan("readFirewallActivations"),
  );
}

/** Repeating an exact save retains its original database timestamp and evidence. */
export function saveCheckDecision(
  completion: FirewallCompletion,
): Effect.Effect<
  typeof disposition.Type,
  FirewallPolicyError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const decoded = yield* Schema.decodeUnknown(completedCheck)(completion);
    const value = {
      ...decoded,
      canonicalCandidate: Uint8Array.from(decoded.canonicalCandidate),
      canonicalInput: Uint8Array.from(decoded.canonicalInput),
      canonicalResult: Uint8Array.from(decoded.canonicalResult),
    };
    const applied = value.admit ? "admitted" : "withheld";
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const read = findOne({
          Request: Schema.NonEmptyString,
          Result: storedCheck,
          execute: (checkId) =>
            sql`SELECT direction, canonical_candidate AS canonicalCandidate, canonical_input AS canonicalInput, canonical_result AS canonicalResult, disposition FROM firewall_checks WHERE check_id = ${checkId}`,
        });
        const prior = yield* read(value.checkId);
        if (Option.isSome(prior)) {
          const saved = prior.value;
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
          return applied;
        }
        yield* sql`INSERT INTO firewall_checks (check_id, direction, canonical_candidate, canonical_input, canonical_result, disposition, decided_at) VALUES (${value.checkId}, ${value.direction}, ${value.canonicalCandidate}, ${value.canonicalInput}, ${value.canonicalResult}, ${applied}, CAST(unixepoch('subsec') * 1000 AS INTEGER))`;
        return applied;
      }),
    );
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.withSpan("saveCheckDecision"),
  );
}

/** Reads saved summaries without exposing message text, private inputs or provider evidence. */
export function readDecisions(request: {
  readonly since?: number;
}): Effect.Effect<
  typeof firewallDecisionsSchema.Type,
  FirewallPolicyError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const read = findAll({
      Request: time,
      Result: decisionRow,
      execute: (since) =>
        sql`SELECT check_id AS checkId, canonical_result AS canonicalResult, disposition, decided_at AS time FROM firewall_checks WHERE decided_at >= ${since} ORDER BY decided_at DESC, rowid DESC LIMIT 100`,
    });
    const rows = yield* read(request.since ?? 0);
    const summaries = yield* Effect.forEach(rows, decodeDecisionSummaries, {
      concurrency: 1,
    });
    return {
      decisions: summaries.flat(),
      activations: yield* readFirewallActivations(request),
    };
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.withSpan("readDecisions"),
  );
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
