/** @file Retained policy checks and editable rule installations. */

import * as SqlClient from "@effect/sql/SqlClient";
import * as SqlSchema from "@effect/sql/SqlSchema";
import { Effect, Schema } from "effect";

import type {
  FirewallActivation,
  FirewallCommit,
  FirewallCompletion,
} from "./types.js";
import {
  EndpointStoreError,
  readBytes,
  readInteger,
  readText,
  requireBytes,
  requireSameBytes,
  requireText,
  storeOperation,
  transaction,
} from "../../communication/store/database/index.js";
import { decodeRuntimeValue } from "../../communication/store/index.js";
import {
  type firewallDecisionsSchema,
  firewallDecisionSummarySchema,
} from "../config.js";
const ruleIds = Schema.parseJson(Schema.Array(Schema.NonEmptyString));
const activationRow = Schema.Struct({
  time: Schema.Int,
  activeRevision: Schema.String,
  addedRuleIds: ruleIds,
  removedRuleIds: ruleIds,
});
const savedSummarySchema = Schema.Struct({
  summaries: Schema.Array(firewallDecisionSummarySchema),
});
const auditDisposition = Schema.Literal("admitted", "withheld");
/** Missing policy means initial rules have never been seeded, distinct from an empty list. */
export function readFirewallPolicy(): Effect.Effect<
  Uint8Array | undefined,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT canonical_rules FROM firewall_policy WHERE singleton = 1",
      [],
    ))[0];
    return row === undefined ? undefined : readBytes(row, "canonical_rules");
  }).pipe(storeOperation, Effect.withSpan("readFirewallPolicy"));
}
/** The editable document and its activation either both commit or neither does. */
export function writeFirewallPolicy(
  rules: Uint8Array,
  activation?: FirewallActivation,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    requireBytes(rules);
    yield* transaction(
      Effect.gen(function* () {
        yield* database.unsafe<Readonly<Record<string, unknown>>>(
          `INSERT INTO firewall_policy (singleton, canonical_rules) VALUES (1, ?)
      ON CONFLICT(singleton) DO UPDATE SET canonical_rules = excluded.canonical_rules`,
          [rules],
        );
        if (activation !== undefined) {
          yield* database.unsafe<Readonly<Record<string, unknown>>>(
            `INSERT INTO firewall_activations
        (activated_at, active_revision, added_rule_ids, removed_rule_ids)
        VALUES (?, ?, ?, ?)`,
            [
              activation.time,
              activation.activeRevision,
              Schema.encodeSync(ruleIds)(activation.addedRuleIds),
              Schema.encodeSync(ruleIds)(activation.removedRuleIds),
            ],
          );
        }
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("writeFirewallPolicy"));
}
/** Recent edits preserve activation order when timestamps coincide. */
export function readFirewallActivations(request: { readonly since?: number }) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const since = request.since ?? 0;
    if (!Number.isSafeInteger(since) || since < 0) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    const query = SqlSchema.findAll({
      Request: Schema.Int,
      Result: activationRow,
      execute: (after) =>
        sql.unsafe(
          "SELECT activated_at AS time,active_revision AS activeRevision,added_rule_ids AS addedRuleIds,removed_rule_ids AS removedRuleIds FROM firewall_activations WHERE activated_at>=? ORDER BY activated_at DESC,sequence DESC LIMIT 100",
          [after],
        ),
    });
    return yield* query(since).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
      ),
    );
  }).pipe(storeOperation, Effect.withSpan("readFirewallActivations"));
}
/** Saves one completed assessment; interruption before completion leaves no audit row. */
export function saveCheckDecision(
  completion: FirewallCompletion,
): Effect.Effect<FirewallCommit, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    requireText(completion.checkId);
    requireBytes(completion.canonicalCandidate);
    requireBytes(completion.canonicalInput);
    requireBytes(completion.canonicalResult);
    const disposition = completion.admit ? "admitted" : "withheld";
    const prior = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT direction,canonical_candidate,canonical_input,canonical_result,disposition FROM firewall_checks WHERE check_id=?",
      [completion.checkId],
    ))[0];
    if (prior !== undefined) {
      if (
        readText(prior, "direction") !== completion.direction ||
        readText(prior, "disposition") !== disposition
      ) {
        return yield* new EndpointStoreError({ reason: "conflict" });
      }
      requireSameBytes(
        readBytes(prior, "canonical_candidate"),
        completion.canonicalCandidate,
      );
      requireSameBytes(
        readBytes(prior, "canonical_input"),
        completion.canonicalInput,
      );
      requireSameBytes(
        readBytes(prior, "canonical_result"),
        completion.canonicalResult,
      );
      return disposition;
    }
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "INSERT INTO firewall_checks(check_id,direction,canonical_candidate,canonical_input,canonical_result,disposition,decided_at) VALUES(?,?,?,?,?,?,CAST(unixepoch('subsec')*1000 AS INTEGER))",
      [
        completion.checkId,
        completion.direction,
        completion.canonicalCandidate,
        completion.canonicalInput,
        completion.canonicalResult,
        disposition,
      ],
    );
    return disposition;
  }).pipe(storeOperation, Effect.withSpan("saveCheckDecision"));
}
/**
 * Returns retained decisions without message content, private inputs,
 * diagnostics or rule text. Includes registered advice only for rules that
 * determined the decision. Completion time includes checks that started
 * before a caller's previous poll.
 */
export function readDecisions(request: { readonly since?: number }) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const since = request.since ?? 0;
    if (!Number.isSafeInteger(since) || since < 0) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT check_id, canonical_result, disposition, decided_at
    FROM firewall_checks WHERE decided_at >= ?
    ORDER BY decided_at DESC, rowid DESC LIMIT 100`,
      [since],
    );
    const decisions: Array<
      (typeof firewallDecisionsSchema.Type.decisions)[number]
    > = [];
    for (const row of rows) {
      const checkId = readText(row, "check_id");
      const disposition = Schema.decodeUnknownSync(auditDisposition)(
        readText(row, "disposition"),
      );
      const time = readInteger(row, "decided_at");
      const { summaries } = yield* decodeRuntimeValue(
        savedSummarySchema,
        readBytes(row, "canonical_result"),
      );
      for (const summary of summaries) {
        decisions.push({
          ...summary,
          disposition,
          checkId,
          time,
        });
      }
    }
    return {
      decisions,
      activations: yield* readFirewallActivations(request),
    } satisfies typeof firewallDecisionsSchema.Type;
  }).pipe(storeOperation, Effect.withSpan("readDecisions"));
}
